use std::collections::HashMap;

use tokio::sync::mpsc;

use super::PaymentsCommand;
use crate::account::{Account, AccountId};
use crate::error::TransactionError;
use crate::error::TransactionError::{TransactionNotFound, WorkerAccountIdMismatch};
use crate::types::{Dispute, DisputeStatus, Transaction, TransactionId, TransactionStatus};

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
            PaymentsCommand::OpenDispute(ref d) => self.process_open_dispute(d),
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
        if self.transactions.get(&tx.id()).is_some() {
            return Err(TransactionError::DuplicatedTransaction(tx.id()));
        }

        self.account.deposit_funds(tx.amount())?;
        let mut updated_tx: Transaction = tx.clone().into();
        updated_tx.status = TransactionStatus::Processed;
        self.transactions.insert(updated_tx.id(), updated_tx);
        Ok(())
    }

    /// Withdraw funds from account and save transaction.
    pub fn process_withdrawal(&mut self, tx: &Transaction) -> Result<(), TransactionError> {
        if self.transactions.get(&tx.id()).is_some() {
            return Err(TransactionError::DuplicatedTransaction(tx.id()));
        }

        self.account.withdraw_funds(tx.amount())?;
        let mut updated_tx: Transaction = tx.clone().into();
        updated_tx.status = TransactionStatus::Processed;
        self.transactions.insert(updated_tx.id(), updated_tx);
        Ok(())
    }

    /// Open dispute regarding previous account transaction, hold funds, save dispute.
    pub fn process_open_dispute(&mut self, d: &Dispute) -> Result<(), TransactionError> {
        let disputed_tx = self
            .transactions
            .get_mut(&d.tx_id())
            .ok_or(TransactionNotFound(d.tx_id()))?;

        // only processed transactions can be disputed
        if disputed_tx.status != TransactionStatus::Processed {
            let reason = match disputed_tx.status {
                TransactionStatus::Created => "transaction has not been processed yet",
                TransactionStatus::DisputeInProgress => {
                    "transaction has another dispute in progress"
                }
                TransactionStatus::ChargedBack => "transaction has been charged back",
                // Use empty str instead of `unreachable!()` macro to avoid panics that might lead
                // to inconsistent state or crash loops. In the worst case we can tolerate non-expressive
                // error message.
                TransactionStatus::Processed => "",
            };
            return Err(TransactionError::TransactionDisputeNotAllowed(
                disputed_tx.id(),
                reason,
            ));
        }

        self.account.hold_funds(disputed_tx.amount())?;

        disputed_tx.status = TransactionStatus::DisputeInProgress;

        let mut updated_d: Dispute = d.clone().into();
        updated_d.status = DisputeStatus::InProgress;
        self.disputes.insert(disputed_tx.id(), updated_d);

        Ok(())
    }
}

pub async fn run(mut worker: AccountWorker) -> Result<(), TransactionError> {
    while let Some(cmd) = worker.receiver.recv().await {
        worker.process_command(cmd)?;
    }
    Ok(())
}

// Test deposit, open dispute, deposit again with same id
