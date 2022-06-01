mod account;
mod engine;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::engine::{run_engine, PaymentsEngine};

use crate::producer::{run_producer, CSVTransactionProducer};

const DECIMAL_MAX_PRECISION: u32 = 4;

mod error {
    use crate::account::{AccountId, TransactionId};
    use thiserror::Error;

    /// Errors that may happen during transaction processing.
    /// They are safe to be exposed to users.
    #[derive(Debug, Clone, Error, PartialEq)]
    pub enum TransactionError {
        #[error("account has too much money")]
        TooMuchMoney,

        #[error("amount should be positive")]
        NonPositiveAmount,

        #[error("insufficient funds")]
        InsufficientFunds,

        #[error("transaction {0} has already been processed")]
        DuplicatedTransaction(TransactionId),
    }

    /// Internal engine errors.
    /// Not safe to be exposed.
    #[derive(Debug, Clone, Error, PartialEq)]
    pub enum PaymentsEngineError {
        #[error("worker account id mismatch (got {0:?}, expected {1:?})")]
        WorkerAccountIdMismatch(AccountId, AccountId),
    }
}

mod producer {
    use std::fs::File;
    use std::path::PathBuf;

    use anyhow::Result;
    use rust_decimal::Decimal;
    use serde::Deserialize;

    use tokio::sync::mpsc;

    use crate::account::{AccountId, TransactionId};
    use crate::engine::{PaymentsCommand, TxPayload};
    use crate::DECIMAL_MAX_PRECISION;

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
        tx: TransactionId,
        amount: Option<Decimal>,
    }

    impl TryInto<PaymentsCommand> for TransactionRecord {
        type Error = anyhow::Error;

        fn try_into(self) -> std::result::Result<PaymentsCommand, Self::Error> {
            match self.type_ {
                TransactionRecordType::Deposit => {
                    let amount = checked_amount(self.amount)?;
                    let payload = TxPayload::new(self.client, self.tx, amount);
                    Ok(PaymentsCommand::DepositFunds(payload))
                }
                TransactionRecordType::Withdrawal => {
                    let amount = checked_amount(self.amount)?;
                    let payload = TxPayload::new(self.client, self.tx, amount);
                    Ok(PaymentsCommand::WithdrawFunds(payload))
                }
                _ => unimplemented!("try_into unimplemented"),
            }
        }
    }

    /// Returns an error if amount is missing or has unsupported precision.
    fn checked_amount(maybe_amount: Option<Decimal>) -> Result<Decimal, anyhow::Error> {
        let amount =
            maybe_amount.ok_or_else(|| anyhow::anyhow!("amount should be present for deposits"))?;
        if amount.scale() > DECIMAL_MAX_PRECISION {
            return Err(anyhow::anyhow!(
                "expected precision <={}, got {}",
                DECIMAL_MAX_PRECISION,
                amount.scale(),
            ));
        }
        Ok(amount)
    }

    pub struct CSVTransactionProducer {
        csv_path: PathBuf,
        payment_engine_sender: mpsc::Sender<PaymentsCommand>,
    }

    impl CSVTransactionProducer {
        pub fn new(csv_path: &str, payment_engine_sender: mpsc::Sender<PaymentsCommand>) -> Self {
            Self {
                csv_path: PathBuf::from(csv_path),
                payment_engine_sender,
            }
        }
    }

    pub async fn run_producer(p: CSVTransactionProducer) -> Result<()> {
        let f = File::open(p.csv_path)?;
        let mut rdr = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(f);

        let headers = rdr.headers()?.clone();
        let mut record = csv::StringRecord::new();
        while rdr.read_record(&mut record)? {
            let tx_record: TransactionRecord = record.deserialize(Some(&headers))?;
            p.payment_engine_sender.send(tx_record.try_into()?).await?;
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

    let producer = CSVTransactionProducer::new(csv_path, engine_sender);
    run_producer(producer).await?;

    engine_join.await?
}
