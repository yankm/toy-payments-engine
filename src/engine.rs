use std::collections::HashMap;

use crate::error::PaymentsEngineError::WorkerAccountIdMismatch;
use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::payments::{Account, AccountId, Transaction};

pub type Handle<T> = JoinHandle<Result<T>>;

#[derive(Debug)]
pub enum PaymentsEngineMessage {
    ProcessTx(Transaction),
}

pub struct PaymentsEngine {
    receiver: mpsc::Receiver<PaymentsEngineMessage>,
    account_workers: HashMap<AccountId, mpsc::Sender<WorkerMessage>>,
    worker_joins: Vec<(AccountId, Handle<()>)>,
}

impl PaymentsEngine {
    pub fn new(receiver: mpsc::Receiver<PaymentsEngineMessage>) -> Self {
        Self {
            receiver,
            account_workers: HashMap::new(),
            worker_joins: Vec::new(),
        }
    }

    pub async fn handle_message(&mut self, msg: PaymentsEngineMessage) -> Result<()> {
        match msg {
            PaymentsEngineMessage::ProcessTx(tx) => self.process_transaction(tx),
        }
        .await
    }

    pub async fn process_transaction(&mut self, tx: Transaction) -> Result<()> {
        println!("Engine: got tx {:?}", tx);
        match self.account_workers.get(&tx.account_id()) {
            Some(s) => s.send(WorkerMessage::ProcessTx(tx)).await?,
            None => {
                let account_id = tx.account_id();
                let (sender, receiver) = mpsc::channel(64);
                let worker = AccountWorker::new(receiver, Account::new(account_id));
                let join = tokio::spawn(run_worker(worker));
                sender.send(WorkerMessage::ProcessTx(tx)).await?;
                self.account_workers.insert(account_id, sender);
                self.worker_joins.push((account_id, join));
            }
        };
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
    while let Some(msg) = engine.receiver.recv().await {
        engine.handle_message(msg).await?
    }
    engine.shutdown().await;
    Ok(())
}

#[derive(Debug)]
enum WorkerMessage {
    ProcessTx(Transaction),
}

struct AccountWorker {
    receiver: mpsc::Receiver<WorkerMessage>,
    account: Account,
}

impl AccountWorker {
    pub fn new(receiver: mpsc::Receiver<WorkerMessage>, account: Account) -> Self {
        Self { receiver, account }
    }

    pub fn id(&self) -> AccountId {
        self.account.id()
    }

    fn handle_message(&mut self, msg: WorkerMessage) {
        let result = match msg {
            WorkerMessage::ProcessTx(tx) => self.process_transaction(tx),
        };
        if let Err(e) = result {
            eprintln!(
                "worker {} failed to handle message {:?}: {}",
                self.id(),
                msg,
                e
            );
        };
    }

    fn process_transaction(&mut self, tx: Transaction) -> Result<()> {
        println!("Worker {} got tx {:?}", self.id(), tx);
        match tx {
            Transaction::Deposit {
                account_id, amount, ..
            } => {
                if account_id != self.account.id() {
                    return Err(anyhow!(WorkerAccountIdMismatch(
                        account_id,
                        self.account.id()
                    )));
                }
                self.account.deposit_funds(amount)?;
            }
            Transaction::Withdraw {
                account_id, amount, ..
            } => {
                if account_id != self.account.id() {
                    return Err(anyhow!(WorkerAccountIdMismatch(
                        account_id,
                        self.account.id()
                    )));
                }
                self.account.withdraw_funds(amount)?;
            }
        };
        Ok(())
    }
}

async fn run_worker(mut worker: AccountWorker) -> Result<()> {
    while let Some(msg) = worker.receiver.recv().await {
        worker.handle_message(msg);
    }
    Ok(())
}
