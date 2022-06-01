use std::collections::HashMap;

use tokio::sync::mpsc;

use super::PaymentsCommand;
use crate::account::{Account, AccountId};
use crate::error::TransactionError;
use crate::error::TransactionError::WorkerAccountIdMismatch;
use crate::types::{Dispute, Transaction, TransactionId, TransactionStatus};

/// A stateful worker capable of processing transactions and disputes for a single account.
pub struct AccountWorker {
    receiver: mpsc::Receiver<PaymentsCommand>,
    account: Account,
    transactions: HashMap<TransactionId, Transaction>,
    disputes: HashMap<TransactionId, Dispute>,
}

impl AccountWorker {
    pub fn new(receiver: mpsc::Receiver<PaymentsCommand>, account: Account) -> Self {
        Self {
            receiver,
            account,
            transactions: HashMap::new(),
            disputes: HashMap::new(),
        }
    }

    pub fn id(&self) -> AccountId {
        self.account.id()
    }

    pub fn process_command(&mut self, cmd: PaymentsCommand) -> Result<(), TransactionError> {
        eprintln!("Worker {} got cmd {:?}", self.id(), cmd);

        if cmd.account_id() != self.account.id() {
            return Err(WorkerAccountIdMismatch(cmd.account_id(), self.account.id()));
        }

        let result = match cmd {
            PaymentsCommand::DepositFunds(ref t) => self.process_deposit(t),
            PaymentsCommand::WithdrawFunds(ref t) => self.process_withdrawal(t),
        };

        if let Err(e) = result {
            eprintln!(
                "worker {} failed to process command {:?}: {}",
                self.id(),
                cmd,
                e
            );
        };

        Ok(())
    }

    /// Deposit funds to account and save transaction.
    pub fn process_deposit(&mut self, tx: &Transaction) -> Result<(), TransactionError> {
        self.account.deposit_funds(tx.amount())?;
        let mut updated_tx: Transaction = tx.clone().into();
        updated_tx.status = TransactionStatus::Processed;
        self.transactions.insert(updated_tx.id(), updated_tx);
        Ok(())
    }

    /// Withdraw funds from account and save transaction.
    pub fn process_withdrawal(&mut self, tx: &Transaction) -> Result<(), TransactionError> {
        self.account.withdraw_funds(tx.amount())?;
        let mut updated_tx: Transaction = tx.clone().into();
        updated_tx.status = TransactionStatus::Processed;
        self.transactions.insert(updated_tx.id(), updated_tx);
        Ok(())
    }
}

pub async fn run(mut worker: AccountWorker) -> Result<(), TransactionError> {
    while let Some(cmd) = worker.receiver.recv().await {
        worker.process_command(cmd)?;
    }
    Ok(())
}
