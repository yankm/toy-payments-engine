mod worker;

use std::collections::{HashMap, HashSet};

use anyhow;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::account::{Account, AccountId};
use crate::error::TransactionError;
use worker::AccountWorker;

use crate::error::TransactionError::DuplicatedTransaction;
use crate::types::{Dispute, Transaction, TransactionId};

#[derive(Debug)]
pub enum PaymentsCommand {
    DepositFunds(Transaction),
    WithdrawFunds(Transaction),
    OpenDispute(Dispute),
    CancelDispute(Dispute),
    ChargebackDispute(Dispute),
}

impl PaymentsCommand {
    /// Account id of the command subject.
    pub fn account_id(&self) -> AccountId {
        match self {
            Self::DepositFunds(t) | Self::WithdrawFunds(t) => t.account_id(),
            Self::OpenDispute(d) | Self::CancelDispute(d) | Self::ChargebackDispute(d) => {
                d.account_id()
            }
        }
    }

    /// Transaction id from the command payload.
    pub fn transaction_id(&self) -> TransactionId {
        match self {
            Self::DepositFunds(t) | Self::WithdrawFunds(t) => t.id(),
            Self::OpenDispute(d) | Self::CancelDispute(d) | Self::ChargebackDispute(d) => d.tx_id(),
        }
    }

    /// Returns `true` if command is a transaction, e.g. deposit/withdrawal.
    pub fn is_transaction(&self) -> bool {
        match self {
            Self::DepositFunds(_) | Self::WithdrawFunds(_) => true,
            _ => false,
        }
    }
}

pub struct PaymentsEngine {
    receiver: mpsc::Receiver<PaymentsCommand>,
    /// Ledger containing sender-channels of account workers spawned.
    account_workers: HashMap<AccountId, mpsc::Sender<PaymentsCommand>>,
    /// Contains join handles of spawned workers, used for worker graceful shutdown.
    worker_joins: Vec<(AccountId, JoinHandle<Result<(), TransactionError>>)>,
    /// Contains ids of processed deposit/withdraw transactions.
    processed_tx_ids: HashSet<TransactionId>,
}

impl PaymentsEngine {
    pub fn new(receiver: mpsc::Receiver<PaymentsCommand>) -> Self {
        Self {
            receiver,
            account_workers: HashMap::new(),
            worker_joins: Vec::new(),
            processed_tx_ids: HashSet::new(),
        }
    }

    /// Lazily spawns account workers and delegates commands to them.
    pub async fn process_command(&mut self, cmd: PaymentsCommand) -> anyhow::Result<()> {
        eprintln!("Engine: got command {:?}", cmd);

        let tx_id = cmd.transaction_id();
        let is_transaction = cmd.is_transaction();
        // Avoid processing same transactions twice.
        // Should avoid processing same disputes twice as well, but they don't have unique ids
        // as of 01.05.2022.
        if is_transaction {
            if self.processed_tx_ids.contains(&tx_id) {
                return Err(anyhow::anyhow!(DuplicatedTransaction(tx_id)));
            }
        }

        let account_id = cmd.account_id();
        match self.account_workers.get(&account_id) {
            Some(s) => s.send(cmd).await?,
            None => self.spawn_worker_and_send(account_id, cmd).await?,
        }

        if is_transaction {
            self.processed_tx_ids.insert(tx_id);
        }

        Ok(())
    }

    /// Spawns a new account worker and sends command to it.
    async fn spawn_worker_and_send(
        &mut self,
        _account_id: AccountId,
        cmd: PaymentsCommand,
    ) -> anyhow::Result<()> {
        let account_id = cmd.account_id();
        let (sender, receiver) = mpsc::channel(64);
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
                        eprintln!("worker {} tokio task failed: {}", acc_id, result_e);
                    }
                }
                Err(e) => eprintln!("await worker {} failed: {}", acc_id, e),
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
            .process_command(PaymentsCommand::DepositFunds(tx1))
            .await?;
        let tx2 = Transaction::new(TransactionKind::Withdrawal, tx_id, 0, dec!(10));
        let result = engine
            .process_command(PaymentsCommand::WithdrawFunds(tx2))
            .await;
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.err().unwrap()),
            format!("{}", DuplicatedTransaction(0))
        );

        Ok(())
    }
}
