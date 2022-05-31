use anyhow::Result;
use tokio::sync::mpsc;

use crate::engine::{run_engine, PaymentsEngine};
use crate::producer::{run_producer, CSVTransactionEventProducer};
use crate::types::TransactionEvent;

const DECIMAL_MAX_PRECISION: u32 = 4;

mod types {
    use anyhow::Result;
    use rust_decimal::Decimal;
    use tokio::task::JoinHandle;

    pub type AccountId = u16;
    /// A transaction id.
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
    pub enum TransactionEvent {
        Deposit {
            account_id: AccountId,
            transaction_id: TxId,
            amount: Decimal,
        },
    }

    impl TransactionEvent {
        pub fn account_id(&self) -> AccountId {
            match *self {
                Self::Deposit { account_id, .. } => account_id,
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
            match self.account_workers.get(&tx.account_id()) {
                Some(s) => s.send(WorkerMessage::ProcessTxEvent(tx)).await?,
                None => {
                    let account_id = tx.account_id();
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
                WorkerMessage::ProcessTxEvent(tx) => self.process_transaction_event(tx),
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

        fn process_transaction_event(&mut self, tx: TransactionEvent) -> Result<()> {
            match tx {
                TransactionEvent::Deposit {
                    account_id,
                    transaction_id: _,
                    amount,
                } => {
                    assert!(account_id == self.account.id());

                    self.account.add_funds(amount);

                    println!("Worker {} got tx {:?}", self.id(), tx);
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
}

mod producer {
    use std::fs::File;
    use std::path::PathBuf;

    use anyhow::Result;
    use rust_decimal::Decimal;
    use serde::Deserialize;

    use tokio::sync::mpsc;

    use crate::engine::PaymentsEngineMessage;
    use crate::types::{AccountId, TxId};
    use crate::{TransactionEvent, DECIMAL_MAX_PRECISION};

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    enum TransactionRecordType {
        Deposit,
        Withdrawal,
        Dispute,
        Resolve,
        Chargeback,
    }

    /// A CSV record representing a transaction.
    #[derive(Debug, Deserialize)]
    struct TransactionRecord {
        #[serde(rename = "type")]
        type_: TransactionRecordType,
        client: AccountId,
        tx: TxId,
        amount: Option<Decimal>,
    }

    impl TryInto<TransactionEvent> for TransactionRecord {
        type Error = anyhow::Error;

        fn try_into(self) -> std::result::Result<TransactionEvent, Self::Error> {
            match self.type_ {
                TransactionRecordType::Deposit => {
                    let amount = self
                        .amount
                        .ok_or_else(|| anyhow::anyhow!("amount should be present for deposits"))?;
                    if amount.scale() > DECIMAL_MAX_PRECISION {
                        return Err(anyhow::anyhow!(
                            "expected precision <{}, got {}",
                            DECIMAL_MAX_PRECISION,
                            amount.scale()
                        ));
                    }
                    Ok(TransactionEvent::Deposit {
                        account_id: self.client,
                        transaction_id: self.tx,
                        amount,
                    })
                }
                _ => unimplemented!("try_into unimplemented"),
            }
        }
    }

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
        let f = File::open(p.csv_path)?;
        let mut rdr = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(f);

        let headers = rdr.byte_headers()?.clone();
        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            let tx_record: TransactionRecord = record.deserialize(Some(&headers))?;
            p.payment_engine_sender
                .send(PaymentsEngineMessage::ProcessTxEvent(tx_record.try_into()?))
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
