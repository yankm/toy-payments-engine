use std::collections::HashMap;

use crate::error::PaymentsEngineError::WorkerAccountIdMismatch;
use anyhow::{anyhow, Result};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::account::{Account, AccountId, TransactionId};

pub type Handle<T> = JoinHandle<Result<T>>;

#[derive(Debug)]
pub enum PaymentsCommand {
    DepositFunds(TxPayload),
    WithdrawFunds(TxPayload),
}

impl PaymentsCommand {
    /// Account id of the message subject.
    pub fn account_id(&self) -> AccountId {
        match self {
            Self::DepositFunds(p) => p.account_id(),
            Self::WithdrawFunds(p) => p.account_id(),
        }
    }
}

/// A payload of the `PaymentsCommand` messages related to transactions.
#[derive(Debug)]
pub struct TxPayload {
    account_id: AccountId,
    tx_id: TransactionId,
    amount: Decimal,
}

impl TxPayload {
    pub fn new(account_id: AccountId, tx_id: TransactionId, amount: Decimal) -> Self {
        Self {
            account_id,
            tx_id,
            amount,
        }
    }

    pub fn account_id(&self) -> AccountId {
        self.account_id
    }
}

pub struct PaymentsEngine {
    receiver: mpsc::Receiver<PaymentsCommand>,
    account_workers: HashMap<AccountId, mpsc::Sender<PaymentsCommand>>,
    worker_joins: Vec<(AccountId, Handle<()>)>,
}

impl PaymentsEngine {
    pub fn new(receiver: mpsc::Receiver<PaymentsCommand>) -> Self {
        Self {
            receiver,
            account_workers: HashMap::new(),
            worker_joins: Vec::new(),
        }
    }

    /// Lazily spawns account workers and delegates commands to them.
    pub async fn process_command(&mut self, cmd: PaymentsCommand) -> Result<()> {
        println!("Engine: got command {:?}", cmd);

        let account_id = cmd.account_id();
        match self.account_workers.get(&account_id) {
            Some(s) => s.send(cmd).await?,
            None => self.spawn_worker_and_send(account_id, cmd).await?,
        }

        Ok(())
    }

    /// Spawns a new account worker and sends command to it.
    async fn spawn_worker_and_send(
        &mut self,
        _account_id: AccountId,
        cmd: PaymentsCommand,
    ) -> Result<()> {
        let account_id = cmd.account_id();
        let (sender, receiver) = mpsc::channel(64);
        let worker = AccountWorker::new(receiver, Account::new(account_id));
        let join = tokio::spawn(run_worker(worker));
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

pub async fn run_engine(mut engine: PaymentsEngine) -> Result<()> {
    while let Some(cmd) = engine.receiver.recv().await {
        engine.process_command(cmd).await?
    }
    engine.shutdown().await;
    Ok(())
}

struct AccountWorker {
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

    fn process_command(&mut self, cmd: PaymentsCommand) -> Result<()> {
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
                "worker {} failed to process message {:?}: {}",
                self.id(),
                cmd,
                e
            );
        };

        Ok(())
    }
}

async fn run_worker(mut worker: AccountWorker) -> Result<()> {
    while let Some(cmd) = worker.receiver.recv().await {
        worker.process_command(cmd)?;
    }
    Ok(())
}
