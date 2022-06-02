use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::account::{Account, AccountId};
use crate::engine::{DisputeCmdAction, PaymentsEngineCommand, TxCmdAction};
use crate::error::TransactionError;
use crate::error::TransactionError::{
    DisputeNotSupported, TransactionDisputeNotFound, TransactionNotFound, WorkerAccountIdMismatch,
};
use crate::types::{
    Dispute, DisputeResolution, DisputeStatus, Transaction, TransactionId, TransactionKind,
    TransactionStatus,
};

/// A stateful worker capable of processing transactions and disputes for a single account.
pub struct AccountWorker {
    receiver: mpsc::Receiver<PaymentsEngineCommand>,
    account: Account,
    transactions: HashMap<TransactionId, Transaction>,
    disputes: HashMap<TransactionId, Dispute>,
}

impl AccountWorker {
    pub fn new(receiver: mpsc::Receiver<PaymentsEngineCommand>, account: Account) -> Self {
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

    pub fn process_command(&mut self, cmd: PaymentsEngineCommand) -> Result<(), TransactionError> {
        log::debug!("Worker {} got cmd {:?}", self.id(), cmd);

        let result = match cmd {
            PaymentsEngineCommand::TransactionCommand(ref t_cmd) => {
                if t_cmd.tx.account_id() != self.account.id() {
                    return Err(WorkerAccountIdMismatch(
                        t_cmd.tx.account_id(),
                        self.account.id(),
                    ));
                };
                match t_cmd.action {
                    TxCmdAction::Deposit => self.process_deposit(&t_cmd.tx),
                    TxCmdAction::Withdraw => self.process_withdrawal(&t_cmd.tx),
                }
            }
            PaymentsEngineCommand::DisputeCommand(ref d_cmd) => {
                if d_cmd.dispute.account_id() != self.account.id() {
                    return Err(WorkerAccountIdMismatch(
                        d_cmd.dispute.account_id(),
                        self.account.id(),
                    ));
                };
                match d_cmd.action {
                    DisputeCmdAction::OpenDispute => self.process_open_dispute(&d_cmd.dispute),
                    DisputeCmdAction::CancelDispute => {
                        self.process_resolve_dispute(&d_cmd.dispute, DisputeResolution::Cancelled)
                    }
                    DisputeCmdAction::ChargebackDispute => {
                        self.process_resolve_dispute(&d_cmd.dispute, DisputeResolution::ChargedBack)
                    }
                }
            }
            PaymentsEngineCommand::PrintOutput => Ok(println!("{}", self.account)),
        };

        // Do not abort worker on command processing errors
        if let Err(e) = result {
            log::error!(
                "worker {} failed to process command {:?}: {}",
                self.id(),
                cmd,
                e
            );
        };

        Ok(())
    }

    /// Deposit funds to account.
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

    /// Withdraw funds from account.
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

    /// Open dispute regarding previous account transaction, hold funds equals to the tx amount.
    pub fn process_open_dispute(&mut self, d: &Dispute) -> Result<(), TransactionError> {
        let disputed_tx = self
            .transactions
            .get_mut(&d.tx_id())
            .ok_or(TransactionNotFound(d.tx_id()))?;

        // only deposit transactions can be disputed
        if disputed_tx.kind() != TransactionKind::Deposit {
            return Err(DisputeNotSupported(disputed_tx.kind()));
        }

        // only processed transactions can be disputed
        if disputed_tx.status != TransactionStatus::Processed {
            let reason = match disputed_tx.status {
                TransactionStatus::Created => "has not been processed yet",
                TransactionStatus::DisputeInProgress => "has another dispute in progress",
                TransactionStatus::ChargedBack => "has been charged back",
                // Use empty str instead of `unreachable!()` macro to avoid panics that might lead
                // to inconsistent state or crash loops. In the worst case we can tolerate non-expressive
                // error message.
                TransactionStatus::Processed => "",
            };
            return Err(TransactionError::TransactionPreconditionFailed(
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

    /// Resolve an ongoing dispute. Depending on the resolution held amount is either released or withdrawn.
    pub fn process_resolve_dispute(
        &mut self,
        d: &Dispute,
        resolution: DisputeResolution,
    ) -> Result<(), TransactionError> {
        let stored_dispute = self
            .disputes
            .get_mut(&d.tx_id())
            .ok_or(TransactionDisputeNotFound(d.tx_id()))?;

        // only in-progress disputes can be resolved
        if stored_dispute.status != DisputeStatus::InProgress {
            let reason = match stored_dispute.status {
                DisputeStatus::Created => "has not been processed yet",
                DisputeStatus::Resolved(_) => "has already been resolved",
                // Use empty str instead of `unreachable!()` macro to avoid panics that might lead
                // to inconsistent state or crash loops. In the worst case we can tolerate non-expressive
                // error message.
                DisputeStatus::InProgress => "",
            };
            return Err(TransactionError::TransactionDisputePreconditionFailed(
                stored_dispute.tx_id(),
                reason,
            ));
        }

        let disputed_tx = self
            .transactions
            .get_mut(&stored_dispute.tx_id())
            .ok_or(TransactionNotFound(d.tx_id()))?;

        self.account.unhold_funds(disputed_tx.amount())?;

        match resolution {
            DisputeResolution::Cancelled => {
                disputed_tx.status = TransactionStatus::Processed;
            }
            DisputeResolution::ChargedBack => {
                self.account.withdraw_funds(disputed_tx.amount())?;
                disputed_tx.status = TransactionStatus::ChargedBack;
                self.account.is_locked = true;
            }
        }

        stored_dispute.status = DisputeStatus::Resolved(resolution);

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
// Test deposit, open dispute, resolve dispute, open new dispute: success expected, old resolved dispute is expected to purge
// Test deposit, deposit, open dispute, chargeback, withdraw.
