use anyhow::Result;
use tokio::sync::mpsc;

use crate::engine::{run_engine, PaymentsEngine};
use crate::producer::{run_producer, CSVTransactionEventProducer};
use crate::types::{TransactionEvent, TxType};

const DECIMAL_PRECISION: u32 = 4;

mod types {
    use anyhow::Result;
    use rust_decimal::Decimal;
    use tokio::task::JoinHandle;

    pub type AccountId = u16;
    pub type TxId = u32;

    #[derive(Debug, Default)]
    struct Funds {
        total: Decimal,
        held: Decimal,
    }

    #[derive(Debug)]
    pub struct Account {
        id: AccountId,
        funds: Funds,
    }

    impl Account {
        pub fn new(id: AccountId) -> Self {
            Self {
                id,
                funds: Funds::default(),
            }
        }

        pub fn id(&self) -> AccountId {
            self.id
        }

        pub fn add_funds(&mut self, amount: Decimal) {
            self.funds.total += amount;
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub enum TxType {
        Deposit,
        Withdrawal,
        Dispute,
        Resolve,
        Chargeback,
    }

    #[derive(Debug)]
    pub struct TransactionEvent {
        pub type_: TxType,
        pub id: TxId,
        pub account_id: AccountId,
        pub amount: Option<Decimal>,
    }

    impl TransactionEvent {
        pub fn new(
            type_: TxType,
            id: TxId,
            account_id: AccountId,
            amount: Option<Decimal>,
        ) -> Self {
            Self {
                type_,
                id,
                account_id,
                amount,
            }
        }
    }

    pub type Handle<T> = JoinHandle<Result<T>>;
}

mod engine {
    use std::collections::HashMap;

    use anyhow::Result;
    use tokio::sync::mpsc;

    use crate::types::{Account, AccountId, Handle, TransactionEvent};
    use crate::TxType;

    #[derive(Debug)]
    pub enum PaymentsEngineMessage {
        ProcessTxEvent(TransactionEvent),
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
                PaymentsEngineMessage::ProcessTxEvent(tx) => self.process_transaction_event(tx),
            }
            .await
        }

        pub async fn process_transaction_event(&mut self, tx: TransactionEvent) -> Result<()> {
            println!("Engine: got tx {:?}", tx);
            match self.account_workers.get(&tx.account_id) {
                Some(s) => s.send(WorkerMessage::ProcessTxEvent(tx)).await?,
                None => {
                    let account_id = tx.account_id;
                    let (sender, receiver) = mpsc::channel(64);
                    let worker = AccountWorker::new(receiver, Account::new(account_id));
                    let join = tokio::spawn(run_worker(worker));
                    sender.send(WorkerMessage::ProcessTxEvent(tx)).await?;
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
        ProcessTxEvent(TransactionEvent),
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
                WorkerMessage::ProcessTxEvent(ref tx) => self.process_transaction_event(tx),
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

        fn process_transaction_event(&mut self, tx: &TransactionEvent) -> Result<()> {
            match tx.type_ {
                TxType::Deposit => {
                    if let Some(amount) = tx.amount {
                        self.account.add_funds(amount)
                    }
                    println!("Worker {} got tx {:?}", self.id(), tx);
                }
                _ => unimplemented!("process_transaction unimplemented"),
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
}

mod producer {
    use std::path::PathBuf;

    use anyhow::Result;
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    use crate::engine::PaymentsEngineMessage;
    use crate::types::Handle;
    use crate::{TransactionEvent, TxType};

    pub struct CSVTransactionEventProducer {
        csv_path: PathBuf,
        payment_engine_sender: mpsc::Sender<PaymentsEngineMessage>,
    }

    impl CSVTransactionEventProducer {
        pub fn new(
            csv_path: &str,
            payment_engine_sender: mpsc::Sender<PaymentsEngineMessage>,
        ) -> Self {
            Self {
                csv_path: PathBuf::from(csv_path),
                payment_engine_sender,
            }
        }
    }

    pub async fn run_producer(p: CSVTransactionEventProducer) -> Result<()> {
        for i in 0..10 {
            let tx = TransactionEvent::new(
                TxType::Deposit,
                i.clone().into(),
                i % 2,
                Some(Decimal::new(12345, 4)),
            );
            p.payment_engine_sender
                .send(PaymentsEngineMessage::ProcessTxEvent(tx))
                .await?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let csv_path = "test.csv";

    let (engine_sender, engine_receiver) = mpsc::channel(64);
    let engine = PaymentsEngine::new(engine_receiver);
    let engine_join = tokio::spawn(run_engine(engine));

    let producer = CSVTransactionEventProducer::new(csv_path, engine_sender);
    run_producer(producer).await?;

    engine_join.await?
}
