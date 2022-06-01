use crate::error::PaymentsEngineError::WorkerAccountIdMismatch;
use anyhow::{anyhow, Result};

use tokio::sync::mpsc;

use super::PaymentsCommand;
use crate::account::{Account, AccountId};

pub struct AccountWorker {
    receiver: mpsc::Receiver<PaymentsCommand>,
    account: Account,
}

impl AccountWorker {
    pub fn new(receiver: mpsc::Receiver<PaymentsCommand>, account: Account) -> Self {
        Self { receiver, account }
    }

    pub fn id(&self) -> AccountId {
        self.account.id()
    }

    pub fn process_command(&mut self, cmd: PaymentsCommand) -> Result<()> {
        println!("Worker {} got cmd {:?}", self.id(), cmd);

        if cmd.account_id() != self.account.id() {
            return Err(anyhow!(WorkerAccountIdMismatch(
                cmd.account_id(),
                self.account.id()
            )));
        }

        let result = match cmd {
            PaymentsCommand::DepositFunds(ref p) => self.account.deposit_funds(p.amount),
            PaymentsCommand::WithdrawFunds(ref p) => self.account.withdraw_funds(p.amount),
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
}

pub async fn run(mut worker: AccountWorker) -> Result<()> {
    while let Some(cmd) = worker.receiver.recv().await {
        worker.process_command(cmd)?;
    }
    Ok(())
}
