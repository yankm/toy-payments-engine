mod worker;

use std::collections::{HashMap, HashSet};

use anyhow;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::account::{Account, AccountId};
use crate::error::TransactionError;
use worker::AccountWorker;

use crate::error::TransactionError::DuplicatedTransaction;
use crate::types::{Dispute, Transaction, TransactionId, TransactionKind};

// Engine and worker channel buffer sizes, in messages.
pub const ENGINE_CHAN_BUF_SIZE: usize = 512;
pub const WORKER_CHAN_BUF_SIZE: usize = 64;

/// Represents commands payment engine can process.
#[derive(Debug)]
pub enum PaymentsEngineCommand {
    TransactionCommand(TxCmd),
    DisputeCommand(DisputeCmd),
    PrintOutput,
}

#[derive(Debug)]
pub enum TxCmdAction {
    Deposit,
    Withdraw,
}

/// Represents transaction-related commands.
#[derive(Debug)]
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

#[derive(Debug, Clone)]
pub enum DisputeCmdAction {
    OpenDispute,
    CancelDispute,
    ChargebackDispute,
}

/// Represents dispute-related commands.
#[derive(Debug)]
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
    worker_joins: Vec<(AccountId, JoinHandle<Result<(), TransactionError>>)>,
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
    pub async fn process_command(&mut self, cmd: PaymentsEngineCommand) -> anyhow::Result<()> {
        log::debug!("Engine: got command {:?}", cmd);

        match cmd {
            PaymentsEngineCommand::TransactionCommand(tx) => self.process_transaction(tx).await,
            PaymentsEngineCommand::DisputeCommand(d) => self.process_dispute(d).await,
            PaymentsEngineCommand::PrintOutput => self.process_print_output().await,
        }?;

        Ok(())
    }

    async fn process_transaction(&mut self, cmd: TxCmd) -> anyhow::Result<()> {
        let tx_id = cmd.tx.id();

        // Avoid processing same transactions twice.
        if self.processed_tx_ids.contains(&tx_id) {
            return Err(anyhow::anyhow!(DuplicatedTransaction(tx_id)));
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

    async fn process_dispute(&mut self, cmd: DisputeCmd) -> anyhow::Result<()> {
        let account_id = cmd.dispute.account_id();
        let send_cmd = PaymentsEngineCommand::DisputeCommand(cmd);
        match self.account_workers.get(&account_id) {
            Some(s) => s.send(send_cmd).await?,
            None => self.spawn_worker_and_send(account_id, send_cmd).await?,
        }
        Ok(())
    }

    async fn process_print_output(&self) -> anyhow::Result<()> {
        // Print headers.
        // This must be changed along with `Account.fmt` implementation since actual rows
        // are printed there.
        println!("client,available,held,total,locked");
        for (_, sender) in self.account_workers.iter() {
            sender.send(PaymentsEngineCommand::PrintOutput).await?;
        }
        Ok(())
    }

    /// Spawns a new account worker and sends command to it.
    async fn spawn_worker_and_send(
        &mut self,
        account_id: AccountId,
        cmd: PaymentsEngineCommand,
    ) -> anyhow::Result<()> {
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

pub async fn run_engine(mut engine: PaymentsEngine) -> anyhow::Result<()> {
    while let Some(cmd) = engine.receiver.recv().await {
        engine.process_command(cmd).await?
    }
    engine.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TransactionKind;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_engine_process_duplicate_transaction() -> anyhow::Result<()> {
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
            format!("{}", result.err().unwrap()),
            format!("{}", DuplicatedTransaction(0))
        );

        Ok(())
    }
}
