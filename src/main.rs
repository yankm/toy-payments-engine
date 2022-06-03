extern crate core;

mod account;
mod engine;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;

use crate::engine::{run_engine, PaymentsEngine, PaymentsEngineCommand, ENGINE_CHAN_BUF_SIZE};
use crate::producer::{run_producer, CSVTransactionProducer};

const DECIMAL_MAX_PRECISION: u32 = 4;

mod types {
    use crate::account::AccountId;
    use core::fmt;
    use rust_decimal::Decimal;

    pub type TransactionId = u32;

    #[derive(Debug, Copy, Clone, PartialEq)]
    pub enum TransactionKind {
        Deposit,
        Withdrawal,
    }

    impl fmt::Display for TransactionKind {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match *self {
                TransactionKind::Deposit => write!(f, "Deposit"),
                TransactionKind::Withdrawal => write!(f, "Withdrawal"),
            }
        }
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

        pub fn kind(&self) -> TransactionKind {
            self.kind
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

    #[derive(Debug, Clone, PartialEq)]
    pub enum DisputeResolution {
        /// Dispute resolved with a cancel, related transaction **has not been reversed**.
        Cancelled,
        /// Dispute resolved with a chargeback, related transaction **has been reversed**.
        ChargedBack,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum DisputeStatus {
        /// Dispute has been created, but not started yet.
        Created,
        /// Dispute has been initiated and is currently in progress.
        InProgress,
        /// Dispute has been resolved.
        Resolved(DisputeResolution),
    }

    /// A customer's claim that a past transaction was erroneous and should be reversed.
    #[derive(Debug, Clone)]
    pub struct Dispute {
        account_id: AccountId,
        tx_id: TransactionId,
        pub status: DisputeStatus,
    }

    impl Dispute {
        pub fn new(account_id: AccountId, tx_id: TransactionId) -> Self {
            Self {
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
    use crate::types::{TransactionId, TransactionKind};
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

        #[error("account {0} is locked")]
        AccountLocked(AccountId),

        #[error("transaction {0} has already been processed")]
        DuplicatedTransaction(TransactionId),

        #[error("worker account id mismatch (got {0:?}, expected {1:?})")]
        WorkerAccountIdMismatch(AccountId, AccountId),

        #[error("transaction {0} not found")]
        TransactionNotFound(TransactionId),

        #[error("disputes are not supported for transactions of type '{0}'")]
        DisputeNotSupported(TransactionKind),

        #[error("transaction {0} {1}")]
        TransactionPreconditionFailed(TransactionId, &'static str),

        #[error("dispute for transaction {0} not found")]
        TransactionDisputeNotFound(TransactionId),

        #[error("transaction {0} {1}")]
        TransactionDisputePreconditionFailed(TransactionId, &'static str),
    }
}

mod producer {
    use std::path::PathBuf;

    use anyhow::Result;
    use csv_async;
    use rust_decimal::Decimal;
    use serde::Deserialize;

    use tokio::sync::mpsc;

    use crate::account::AccountId;
    use crate::engine::{DisputeCmd, DisputeCmdAction, PaymentsEngineCommand};
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

    impl TryInto<PaymentsEngineCommand> for TransactionRecord {
        type Error = anyhow::Error;

        fn try_into(self) -> std::result::Result<PaymentsEngineCommand, Self::Error> {
            match self.type_ {
                TransactionRecordType::Deposit => {
                    let amount = checked_amount(self.amount)?;
                    let tx =
                        Transaction::new(TransactionKind::Deposit, self.tx, self.client, amount);
                    Ok(PaymentsEngineCommand::TransactionCommand(tx.into()))
                }
                TransactionRecordType::Withdrawal => {
                    let amount = checked_amount(self.amount)?;
                    let tx =
                        Transaction::new(TransactionKind::Withdrawal, self.tx, self.client, amount);
                    Ok(PaymentsEngineCommand::TransactionCommand(tx.into()))
                }
                TransactionRecordType::Dispute => {
                    let d = Dispute::new(self.client, self.tx);
                    let cmd = DisputeCmd::new(DisputeCmdAction::OpenDispute, d);
                    Ok(PaymentsEngineCommand::DisputeCommand(cmd))
                }
                TransactionRecordType::Resolve => {
                    let d = Dispute::new(self.client, self.tx);
                    let cmd = DisputeCmd::new(DisputeCmdAction::CancelDispute, d);
                    Ok(PaymentsEngineCommand::DisputeCommand(cmd))
                }
                TransactionRecordType::Chargeback => {
                    let d = Dispute::new(self.client, self.tx);
                    let cmd = DisputeCmd::new(DisputeCmdAction::ChargebackDispute, d);
                    Ok(PaymentsEngineCommand::DisputeCommand(cmd))
                }
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
        payment_engine_sender: mpsc::Sender<PaymentsEngineCommand>,
    }

    impl CSVTransactionProducer {
        pub fn new(
            csv_path: String,
            payment_engine_sender: mpsc::Sender<PaymentsEngineCommand>,
        ) -> Self {
            Self {
                csv_path: PathBuf::from(csv_path),
                payment_engine_sender,
            }
        }
    }

    pub async fn run_producer(p: CSVTransactionProducer) -> Result<()> {
        let f = tokio::fs::File::open(p.csv_path).await?;
        let mut rdr = csv_async::AsyncReaderBuilder::new()
            .trim(csv_async::Trim::All)
            .flexible(true)
            .create_reader(f);

        let headers = rdr.byte_headers().await?.clone();
        let mut record = csv_async::ByteRecord::new();
        while rdr.read_byte_record(&mut record).await? {
            let tx_record: TransactionRecord = record.deserialize(Some(&headers))?;
            p.payment_engine_sender.send(tx_record.try_into()?).await?;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let csv_path = std::env::args().nth(1).ok_or(anyhow!(
        "missing input file name. Usage: {} <filename>",
        std::env::args().nth(0).unwrap()
    ))?;

    let (engine_sender, engine_receiver) = mpsc::channel(ENGINE_CHAN_BUF_SIZE);
    let engine = PaymentsEngine::new(engine_receiver);
    let engine_join = tokio::spawn(run_engine(engine));

    let producer = CSVTransactionProducer::new(csv_path, engine_sender.clone());
    run_producer(producer).await?;

    engine_sender
        .send(PaymentsEngineCommand::PrintOutput)
        .await?;
    drop(engine_sender);

    engine_join.await?
}
