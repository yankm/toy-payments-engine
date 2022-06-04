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

    pub async fn process_command(
        &mut self,
        cmd: &PaymentsEngineCommand,
    ) -> Result<(), TransactionError> {
        log::debug!("Worker {} got cmd {:?}", self.id(), cmd);
        match cmd {
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
            PaymentsEngineCommand::StreamAccountsCSV(sender) => {
                // FIXME: will be fixed in the next commit
                sender.send(format!("{}", self.account)).await.unwrap();
                Ok(())
            }
        }
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
        let disputed_tx = self
            .transactions
            .get_mut(&d.tx_id())
            .ok_or(TransactionNotFound(d.tx_id()))?;

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
        // Do not abort worker on command processing errors
        if let Err(e) = worker.process_command(&cmd).await {
            log::error!(
                "worker {} failed to process command {:?}: {}",
                worker.id(),
                cmd,
                e
            );
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::DisputeCmd;
    use crate::error::TransactionError::{
        TransactionDisputePreconditionFailed, TransactionPreconditionFailed,
    };
    use anyhow::Result;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn create_worker(account_id: AccountId, account_total_funds: Decimal) -> AccountWorker {
        let (_, receiver) = mpsc::channel(1);
        AccountWorker::new(
            receiver,
            Account::new_with_funds(account_id, account_total_funds, dec!(0)),
        )
    }

    async fn deposit(
        worker: &mut AccountWorker,
        tx_id: TransactionId,
        amount: Decimal,
    ) -> Result<()> {
        do_tx(TransactionKind::Deposit, worker, tx_id, amount).await
    }

    async fn withdraw(
        worker: &mut AccountWorker,
        tx_id: TransactionId,
        amount: Decimal,
    ) -> Result<()> {
        do_tx(TransactionKind::Withdrawal, worker, tx_id, amount).await
    }

    async fn do_tx(
        tx_kind: TransactionKind,
        worker: &mut AccountWorker,
        tx_id: TransactionId,
        amount: Decimal,
    ) -> Result<()> {
        let tx = Transaction::new(tx_kind, tx_id, worker.account.id(), amount);
        worker
            .process_command(&PaymentsEngineCommand::TransactionCommand(tx.try_into()?))
            .await?;
        Ok(())
    }

    async fn open_dispute(
        worker: &mut AccountWorker,
        account_id: AccountId,
        tx_id: TransactionId,
    ) -> Result<Dispute> {
        let d = Dispute::new(account_id, tx_id);
        let d_cmd = DisputeCmd::new(DisputeCmdAction::OpenDispute, d.clone());
        worker
            .process_command(&PaymentsEngineCommand::DisputeCommand(d_cmd))
            .await?;
        Ok(d)
    }

    #[tokio::test]
    async fn test_worker_account_id_mismatch() -> Result<(), anyhow::Error> {
        let cmd_acc_id = 0;
        let worker_acc_id = 1;
        let tx_deposit = Transaction::new(TransactionKind::Deposit, 0, cmd_acc_id, dec!(10));
        let tx_withdrawal = Transaction::new(TransactionKind::Withdrawal, 0, cmd_acc_id, dec!(10));
        let d = Dispute::new(cmd_acc_id, 0);

        let commands = vec![
            PaymentsEngineCommand::TransactionCommand(tx_deposit.clone().try_into()?),
            PaymentsEngineCommand::TransactionCommand(tx_withdrawal.clone().try_into()?),
            PaymentsEngineCommand::DisputeCommand(DisputeCmd::new(
                DisputeCmdAction::OpenDispute,
                d.clone(),
            )),
            PaymentsEngineCommand::DisputeCommand(DisputeCmd::new(
                DisputeCmdAction::CancelDispute,
                d.clone(),
            )),
            PaymentsEngineCommand::DisputeCommand(DisputeCmd::new(
                DisputeCmdAction::ChargebackDispute,
                d.clone(),
            )),
        ];

        let mut worker = create_worker(worker_acc_id, dec!(0));
        for cmd in commands.iter() {
            assert_eq!(
                worker.process_command(cmd).await,
                Err(WorkerAccountIdMismatch(cmd_acc_id, worker_acc_id))
            )
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_success() -> Result<()> {
        let mut worker = create_worker(0, dec!(0));

        let tests = vec![
            Transaction::new(TransactionKind::Deposit, 0, 0, dec!(10)),
            Transaction::new(TransactionKind::Withdrawal, 1, 0, dec!(10)),
        ];
        for tx in tests.into_iter() {
            let tx_id = tx.id();
            assert!(worker
                .process_command(&PaymentsEngineCommand::TransactionCommand(tx.try_into()?))
                .await
                .is_ok());
            assert!(worker.transactions.contains_key(&tx_id));
            let stored_tx = worker
                .transactions
                .get(&tx_id)
                .expect("tx has not been saved");
            assert_eq!(stored_tx.status, TransactionStatus::Processed);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_failure() -> Result<()> {
        let mut worker = create_worker(0, dec!(0));

        let tests = vec![
            Transaction::new(TransactionKind::Deposit, 0, 0, dec!(0)),
            Transaction::new(TransactionKind::Withdrawal, 1, 0, dec!(0)),
        ];
        for tx in tests.into_iter() {
            let tx_id = tx.id();
            assert!(worker
                .process_command(&PaymentsEngineCommand::TransactionCommand(tx.try_into()?))
                .await
                .is_err());
            assert!(!worker.transactions.contains_key(&tx_id));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_tx() -> Result<()> {
        let mut worker = create_worker(0, dec!(0));

        let tx_id = 0;
        let tx1 = Transaction::new(TransactionKind::Deposit, tx_id, 0, dec!(1));
        let tx2 = Transaction::new(TransactionKind::Withdrawal, tx_id, 0, dec!(1));

        worker
            .process_command(&PaymentsEngineCommand::TransactionCommand(tx1.try_into()?))
            .await?;
        let result = worker
            .process_command(&PaymentsEngineCommand::TransactionCommand(tx2.try_into()?))
            .await;
        assert_eq!(result, Err(TransactionError::DuplicatedTransaction(tx_id)));

        Ok(())
    }

    #[tokio::test]
    async fn test_disputed_tx_not_found() {
        let mut worker = create_worker(0, dec!(0));

        let d = Dispute::new(0, 0);
        let tests = vec![
            DisputeCmd::new(DisputeCmdAction::OpenDispute, d.clone()),
            DisputeCmd::new(DisputeCmdAction::CancelDispute, d.clone()),
            DisputeCmd::new(DisputeCmdAction::ChargebackDispute, d),
        ];

        for cmd in tests.into_iter() {
            let result = worker
                .process_command(&PaymentsEngineCommand::DisputeCommand(cmd))
                .await;
            assert_eq!(result, Err(TransactionNotFound(0)));
        }
    }

    #[tokio::test]
    async fn test_open_dispute_success() -> Result<()> {
        let mut worker = create_worker(0, dec!(0));
        deposit(&mut worker, 0, dec!(1.23)).await?;

        let d_cmd = DisputeCmd::new(DisputeCmdAction::OpenDispute, Dispute::new(0, 0));
        assert!(worker
            .process_command(&PaymentsEngineCommand::DisputeCommand(d_cmd))
            .await
            .is_ok());

        let stored_tx = worker.transactions.get(&0).expect("tx has not been saved");
        assert_eq!(stored_tx.status, TransactionStatus::DisputeInProgress);

        let stored_dispute = worker.disputes.get(&0).expect("dispute has not been saved");
        assert_eq!(stored_dispute.status, DisputeStatus::InProgress);

        Ok(())
    }

    #[tokio::test]
    async fn test_open_dispute_tx_invalid_type() -> Result<()> {
        let mut worker = create_worker(0, dec!(0));
        deposit(&mut worker, 0, dec!(1.23)).await?;
        let withdrawal_tx_id = 1;
        withdraw(&mut worker, withdrawal_tx_id, dec!(1.23)).await?;

        let d_cmd = DisputeCmd::new(
            DisputeCmdAction::OpenDispute,
            Dispute::new(0, withdrawal_tx_id),
        );
        let result = worker
            .process_command(&PaymentsEngineCommand::DisputeCommand(d_cmd))
            .await;
        assert_eq!(
            result,
            Err(DisputeNotSupported(TransactionKind::Withdrawal))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_open_dispute_tx_invalid_status() -> Result<()> {
        let mut worker = create_worker(0, dec!(0));
        deposit(&mut worker, 0, dec!(1.23)).await?;

        let tests = vec![
            (TransactionStatus::Created, "has not been processed yet"),
            (
                TransactionStatus::DisputeInProgress,
                "has another dispute in progress",
            ),
            (TransactionStatus::ChargedBack, "has been charged back"),
        ];
        for (tx_status, reason) in tests.into_iter() {
            let stored_tx = worker
                .transactions
                .get_mut(&0)
                .expect("tx has not been saved");
            stored_tx.status = tx_status;

            let d_cmd = DisputeCmd::new(DisputeCmdAction::OpenDispute, Dispute::new(0, 0));
            let result = worker
                .process_command(&PaymentsEngineCommand::DisputeCommand(d_cmd))
                .await;
            assert_eq!(result, Err(TransactionPreconditionFailed(0, reason)));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_dispute_success() -> Result<()> {
        let tests = vec![
            (
                DisputeCmdAction::CancelDispute,
                DisputeResolution::Cancelled,
                TransactionStatus::Processed,
            ),
            (
                DisputeCmdAction::ChargebackDispute,
                DisputeResolution::ChargedBack,
                TransactionStatus::ChargedBack,
            ),
        ];

        for (cmd_action, dispute_resolution, tx_status) in tests.into_iter() {
            let mut worker = create_worker(0, dec!(0));
            deposit(&mut worker, 0, dec!(1.23)).await?;
            let d = open_dispute(&mut worker, 0, 0).await?;

            let d_cmd = DisputeCmd::new(cmd_action, d);
            assert!(worker
                .process_command(&PaymentsEngineCommand::DisputeCommand(d_cmd))
                .await
                .is_ok());

            let stored_dispute = worker.disputes.get(&0).expect("dispute has not been saved");
            assert_eq!(
                stored_dispute.status,
                DisputeStatus::Resolved(dispute_resolution)
            );

            let stored_tx = worker.transactions.get(&0).expect("tx has not been saved");
            assert_eq!(stored_tx.status, tx_status);
        }

        Ok(())
    }

    /// Dispute and tx data should be not corrupted if `account.unhold_funds()` failed.
    #[tokio::test]
    async fn test_resolve_dispute_failure_no_state_corruption() -> Result<()> {
        let tests = vec![
            DisputeCmdAction::CancelDispute,
            DisputeCmdAction::ChargebackDispute,
        ];

        for cmd_action in tests.into_iter() {
            let mut worker = create_worker(0, dec!(0));
            deposit(&mut worker, 0, dec!(1.23)).await?;
            let d = open_dispute(&mut worker, 0, 0).await?;
            // drop existing account to reset held funds
            worker.account = Account::new(0);

            let d_cmd = DisputeCmd::new(cmd_action, d);
            let result = worker
                .process_command(&PaymentsEngineCommand::DisputeCommand(d_cmd))
                .await;
            assert_eq!(result, Err(TransactionError::InsufficientFunds));

            // no change is expected for dispute status
            let stored_dispute = worker.disputes.get(&0).expect("dispute has not been saved");
            assert_eq!(stored_dispute.status, DisputeStatus::InProgress);

            // no change is expected for tx status
            let stored_tx = worker.transactions.get(&0).expect("tx has not been saved");
            assert_eq!(stored_tx.status, TransactionStatus::DisputeInProgress);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_dispute_not_found() -> Result<()> {
        let tests = vec![
            DisputeCmdAction::CancelDispute,
            DisputeCmdAction::ChargebackDispute,
        ];

        let mut worker = create_worker(0, dec!(0));
        deposit(&mut worker, 0, dec!(1.23)).await?;
        let d = open_dispute(&mut worker, 0, 0).await?;
        // drop worker disputes storage
        worker.disputes = HashMap::new();

        for cmd_action in tests.into_iter() {
            let d_cmd = DisputeCmd::new(cmd_action, d.clone());
            let result = worker
                .process_command(&PaymentsEngineCommand::DisputeCommand(d_cmd))
                .await;
            assert_eq!(result, Err(TransactionDisputeNotFound(0)));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_dispute_invalid_status() -> Result<()> {
        let mut worker = create_worker(0, dec!(0));
        deposit(&mut worker, 0, dec!(1.23)).await?;
        let d = open_dispute(&mut worker, 0, 0).await?;

        let tests = vec![
            (DisputeStatus::Created, "has not been processed yet"),
            (
                DisputeStatus::Resolved(DisputeResolution::Cancelled),
                "has already been resolved",
            ),
            (
                DisputeStatus::Resolved(DisputeResolution::ChargedBack),
                "has already been resolved",
            ),
        ];
        let cmd_actions = vec![
            DisputeCmdAction::CancelDispute,
            DisputeCmdAction::ChargebackDispute,
        ];
        for (dispute_status, reason) in tests.into_iter() {
            let stored_dispute = worker
                .disputes
                .get_mut(&0)
                .expect("dispute has not been saved");
            stored_dispute.status = dispute_status;

            for cmd_action in cmd_actions.clone().into_iter() {
                let d_cmd = DisputeCmd::new(cmd_action, d.clone());
                let result = worker
                    .process_command(&PaymentsEngineCommand::DisputeCommand(d_cmd))
                    .await;
                assert_eq!(result, Err(TransactionDisputePreconditionFailed(0, reason)));
            }
        }

        Ok(())
    }
}
