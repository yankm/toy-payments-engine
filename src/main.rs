mod account;
mod engine;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::engine::{run_engine, PaymentsEngine};

use crate::producer::{run_producer, CSVTransactionProducer};

const DECIMAL_MAX_PRECISION: u32 = 4;

mod types {
    use crate::account::AccountId;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    pub type TransactionId = u32;

    #[derive(Debug, Clone)]
    pub enum TransactionKind {
        Deposit,
        Withdrawal,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum TransactionStatus {
        /// Transaction has been created, but not have been processed.
        Created,
        /// Transaction has been processed and money has been moved in/out of account.
        Processed,
        /// Indicates there is a dispute ongoing related to this transaction. Depending on dispute
        /// resolution, transaction goes either back to `Processed` or to `ChargedBack`.
        DisputeInProgress,
        /// Transaction has been charged back and money has been moved out of account.
        ChargedBack,
    }

    /// A reversible action of moving funds in and out of customer account.
    #[derive(Debug, Clone)]
    pub struct Transaction {
        kind: TransactionKind,
        id: TransactionId,
        account_id: AccountId,
        amount: Decimal,
        pub status: TransactionStatus,
    }

    impl Transaction {
        pub fn new(
            kind: TransactionKind,
            id: TransactionId,
            account_id: AccountId,
            amount: Decimal,
        ) -> Self {
            Self {
                kind,
                id,
                account_id,
                amount,
                status: TransactionStatus::Created,
            }
        }

        pub fn id(&self) -> TransactionId {
            self.id
        }

        pub fn account_id(&self) -> AccountId {
            self.account_id
        }

        pub fn amount(&self) -> Decimal {
            self.amount
        }
    }

    pub type DisputeId = Uuid;

    #[derive(Debug, Clone)]
    pub enum DisputeStatus {
        /// Dispute has been created, but not started yet.
        Created,
        /// Dispute has been initiated and is currently in progress.
        InProgress,
        /// Dispute has been cancelled, related transaction **has not been reversed**.
        Cancelled,
        /// Dispute has been resolved, related transaction **has been reversed**.
        Resolved,
    }

    /// A customer's claim that a past transaction was erroneous and should be reversed.
    #[derive(Debug, Clone)]
    pub struct Dispute {
        id: DisputeId,
        account_id: AccountId,
        tx_id: TransactionId,
        pub status: DisputeStatus,
    }

    impl Dispute {
        pub fn new(account_id: AccountId, tx_id: TransactionId) -> Self {
            Self {
                id: Uuid::new_v4(),
                account_id,
                tx_id,
                status: DisputeStatus::Created,
            }
        }

        pub fn account_id(&self) -> AccountId {
            self.account_id
        }

        pub fn tx_id(&self) -> TransactionId {
            self.tx_id
        }
    }
}

mod error {
    use crate::account::AccountId;
    use crate::types::TransactionId;
    use thiserror::Error;

    /// Errors that may happen during transaction processing.
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

        #[error("worker account id mismatch (got {0:?}, expected {1:?})")]
        WorkerAccountIdMismatch(AccountId, AccountId),

        #[error("transaction {0} not found")]
        TransactionNotFound(TransactionId),

        #[error("cannot open dispute for transaction {0}: {1}")]
        TransactionDisputeNotAllowed(TransactionId, &'static str),
    }
}

mod producer {
    use std::fs::File;
    use std::path::PathBuf;

    use anyhow::Result;
    use rust_decimal::Decimal;
    use serde::Deserialize;

    use tokio::sync::mpsc;

    use crate::account::AccountId;
    use crate::engine::PaymentsCommand;
    use crate::types::{Dispute, Transaction, TransactionId, TransactionKind};
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
                    let tx =
                        Transaction::new(TransactionKind::Deposit, self.tx, self.client, amount);
                    Ok(PaymentsCommand::DepositFunds(tx))
                }
                TransactionRecordType::Withdrawal => {
                    let amount = checked_amount(self.amount)?;
                    let tx =
                        Transaction::new(TransactionKind::Withdrawal, self.tx, self.client, amount);
                    Ok(PaymentsCommand::WithdrawFunds(tx))
                }
                TransactionRecordType::Dispute => {
                    let d = Dispute::new(self.client, self.tx);
                    Ok(PaymentsCommand::OpenDispute(d))
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
