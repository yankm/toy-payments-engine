mod worker;

use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::account::{Account, AccountId, CSV_HEADERS};

use crate::error::Result;
use worker::AccountWorker;

use crate::error::TransactionErrorKind::DuplicatedTransaction;
use crate::types::{Dispute, Transaction, TransactionId, TransactionKind};

/// Worker channel buffer size, in messages.
pub const WORKER_CHAN_BUF_SIZE: usize = 64;

/// Represents commands payment engine can process.
#[derive(Debug, Clone)]
pub enum PaymentsEngineCommand {
    TransactionCommand(TxCmd),
    DisputeCommand(DisputeCmd),
    StreamAccountsCSV(mpsc::Sender<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TxCmdAction {
    Deposit,
    Withdraw,
}

/// Represents transaction-related commands.
#[derive(Debug, Clone, PartialEq)]
pub struct TxCmd {
    action: TxCmdAction,
    tx: Transaction,
}

impl From<Transaction> for TxCmd {
    fn from(tx: Transaction) -> Self {
        let action = match tx.kind() {
            TransactionKind::Deposit => TxCmdAction::Deposit,
            TransactionKind::Withdrawal => TxCmdAction::Withdraw,
        };
        Self { action, tx }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DisputeCmdAction {
    OpenDispute,
    CancelDispute,
    ChargebackDispute,
}

/// Represents dispute-related commands.
#[derive(Debug, Clone, PartialEq)]
pub struct DisputeCmd {
    action: DisputeCmdAction,
    dispute: Dispute,
}

impl DisputeCmd {
    pub fn new(action: DisputeCmdAction, dispute: Dispute) -> Self {
        Self { action, dispute }
    }
}

pub struct PaymentsEngine {
    receiver: mpsc::Receiver<PaymentsEngineCommand>,
    /// Ledger containing sender-channels of account workers spawned.
    account_workers: HashMap<AccountId, mpsc::Sender<PaymentsEngineCommand>>,
    /// Contains join handles of spawned workers, used for worker graceful shutdown.
    worker_joins: Vec<(AccountId, JoinHandle<Result<()>>)>,
    /// Contains ids of processed transactions.
    processed_tx_ids: HashSet<TransactionId>,
}

impl PaymentsEngine {
    pub fn new(receiver: mpsc::Receiver<PaymentsEngineCommand>) -> Self {
        Self {
            receiver,
            account_workers: HashMap::new(),
            worker_joins: Vec::new(),
            processed_tx_ids: HashSet::new(),
        }
    }

    /// Lazily spawns account workers and delegates commands to them.
    pub async fn process_command(&mut self, cmd: PaymentsEngineCommand) -> Result<()> {
        log::debug!("Engine: got command {:?}", cmd);

        match cmd {
            PaymentsEngineCommand::TransactionCommand(tx) => self.process_transaction(tx).await,
            PaymentsEngineCommand::DisputeCommand(d) => self.process_dispute(d).await,
            PaymentsEngineCommand::StreamAccountsCSV(sender) => {
                self.process_stream_accounts_csv(sender).await
            }
        }?;

        Ok(())
    }

    async fn process_transaction(&mut self, cmd: TxCmd) -> Result<()> {
        let tx_id = cmd.tx.id();

        // Avoid processing same transactions twice.
        if self.processed_tx_ids.contains(&tx_id) {
            return Err(DuplicatedTransaction(tx_id).into());
        }

        let account_id = cmd.tx.account_id();
        let send_cmd = PaymentsEngineCommand::TransactionCommand(cmd);
        match self.account_workers.get(&account_id) {
            Some(s) => s.send(send_cmd).await?,
            None => self.spawn_worker_and_send(account_id, send_cmd).await?,
        }

        self.processed_tx_ids.insert(tx_id);

        Ok(())
    }

    async fn process_dispute(&mut self, cmd: DisputeCmd) -> Result<()> {
        let account_id = cmd.dispute.account_id();
        let send_cmd = PaymentsEngineCommand::DisputeCommand(cmd);
        match self.account_workers.get(&account_id) {
            Some(s) => s.send(send_cmd).await?,
            None => self.spawn_worker_and_send(account_id, send_cmd).await?,
        }
        Ok(())
    }

    /// Sends CSV records to `sender`. First message is headers, other are rows, unordered.
    async fn process_stream_accounts_csv(&self, chan: mpsc::Sender<String>) -> Result<()> {
        // Send headers
        chan.send(String::from(CSV_HEADERS)).await?;
        for (_, worker_sender) in self.account_workers.iter() {
            worker_sender
                .send(PaymentsEngineCommand::StreamAccountsCSV(chan.clone()))
                .await?;
        }
        Ok(())
    }

    /// Spawns a new account worker and sends command to it.
    async fn spawn_worker_and_send(
        &mut self,
        account_id: AccountId,
        cmd: PaymentsEngineCommand,
    ) -> Result<()> {
        let (sender, receiver) = mpsc::channel(WORKER_CHAN_BUF_SIZE);
        let worker = AccountWorker::new(receiver, Account::new(account_id));
        let join = tokio::spawn(worker::run(worker));
        sender.send(cmd).await?;
        self.account_workers.insert(account_id, sender);
        self.worker_joins.push((account_id, join));
        Ok(())
    }

    async fn shutdown(&mut self) {
        // Drop worker senders to allow workers to terminate
        self.drop_worker_senders();
        // Wait until all workers terminate gracefully
        while let Some((acc_id, join)) = self.worker_joins.pop() {
            match join.await {
                Ok(result) => {
                    if let Err(result_e) = result {
                        log::error!("worker {} tokio task failed: {}", acc_id, result_e);
                    }
                }
                Err(e) => log::error!("await worker {} failed: {}", acc_id, e),
            };
        }
    }

    fn drop_worker_senders(&mut self) {
        self.account_workers = HashMap::new();
    }
}

pub async fn run_engine(mut engine: PaymentsEngine) -> Result<()> {
    while let Some(cmd) = engine.receiver.recv().await {
        // Do not abort engine on command processing errors
        if let Err(e) = engine.process_command(cmd.clone()).await {
            log::error!("PaymentsEngine: Failed to process command {:?}: {}", cmd, e);
        };
    }

    // Shutdown gracefully
    engine.shutdown().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TPEError;
    use crate::types::TransactionKind;
    use anyhow::Result;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_engine_process_duplicate_transaction() -> Result<()> {
        let (_, receiver) = mpsc::channel(2);
        let mut engine = PaymentsEngine::new(receiver);

        let tx_id = 0;
        let tx1 = Transaction::new(TransactionKind::Deposit, tx_id, 0, dec!(10));
        engine
            .process_command(PaymentsEngineCommand::TransactionCommand(tx1.into()))
            .await?;
        let tx2 = Transaction::new(TransactionKind::Withdrawal, tx_id, 0, dec!(10));
        let result = engine
            .process_command(PaymentsEngineCommand::TransactionCommand(tx2.into()))
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            TPEError::from(DuplicatedTransaction(0))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_spawn_workers() -> Result<()> {
        let tests = vec![
            PaymentsEngineCommand::TransactionCommand(
                Transaction::new(TransactionKind::Deposit, 0, 0, dec!(0)).try_into()?,
            ),
            PaymentsEngineCommand::DisputeCommand(DisputeCmd::new(
                DisputeCmdAction::OpenDispute,
                Dispute::new(0, 0),
            )),
        ];

        for cmd in tests.into_iter() {
            let (_, receiver) = mpsc::channel(2);
            let mut engine = PaymentsEngine::new(receiver);

            engine.process_command(cmd).await?;
            assert_eq!(engine.worker_joins.len(), 1);
            assert!(engine.account_workers.contains_key(&0));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_stream_accounts_csv() -> Result<()> {
        // one header and two rows
        let expected_records_received = 3;

        let (_, engine_receiver) = mpsc::channel(3);
        let mut engine = PaymentsEngine::new(engine_receiver);

        let tx1 = Transaction::new(TransactionKind::Deposit, 0, 0, dec!(1.234));
        engine
            .process_command(PaymentsEngineCommand::TransactionCommand(tx1.into()))
            .await?;
        let tx2 = Transaction::new(TransactionKind::Withdrawal, 1, 1, dec!(2.1932));
        engine
            .process_command(PaymentsEngineCommand::TransactionCommand(tx2.into()))
            .await?;

        let (csv_sender, mut csv_receiver) = mpsc::channel(3);
        engine
            .process_command(PaymentsEngineCommand::StreamAccountsCSV(csv_sender))
            .await?;

        let mut records_received = 0;
        while (csv_receiver.recv().await).is_some() {
            records_received += 1;
        }

        assert_eq!(records_received, expected_records_received);

        Ok(())
    }
}
