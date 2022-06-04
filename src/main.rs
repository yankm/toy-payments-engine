extern crate core;

mod account;
mod engine;
mod producer;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;

use tokio::io::AsyncWriteExt;

use crate::engine::{run_engine, PaymentsEngine, PaymentsEngineCommand, ENGINE_CHAN_BUF_SIZE};
use crate::producer::{run_producer, CSVTransactionProducer};

/// Maximum allowed precision for input decimals, in places past decimal.
const DECIMAL_MAX_PRECISION: u32 = 4;
/// Size of the channel used to gather CSV records from engine, in messages.
const STREAM_ACCOUNTS_CSV_CHANNEL_SIZE: usize = 1024;

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
    #[derive(Debug, Clone, PartialEq)]
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
    #[derive(Debug, Clone, PartialEq)]
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

/// Collects CSV records from engine and prints them.
async fn print_accounts_csv(engine_sender: mpsc::Sender<PaymentsEngineCommand>) -> Result<()> {
    let (csv_sender, mut csv_receiver) = mpsc::channel(STREAM_ACCOUNTS_CSV_CHANNEL_SIZE);
    engine_sender
        .send(PaymentsEngineCommand::StreamAccountsCSV(csv_sender))
        .await?;

    let mut stdout = tokio::io::stdout();
    while let Some(record) = csv_receiver.recv().await {
        stdout.write(record.as_bytes()).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let csv_path = std::env::args().nth(1).ok_or(anyhow!(
        "missing input file name. Usage: {} <filename>",
        std::env::args().nth(0).unwrap()
    ))?;
    let csv_file = tokio::fs::File::open(csv_path).await?;

    let (engine_sender, engine_receiver) = mpsc::channel(ENGINE_CHAN_BUF_SIZE);
    let engine = PaymentsEngine::new(engine_receiver);
    let engine_join = tokio::spawn(run_engine(engine));

    let producer = CSVTransactionProducer::new(csv_file, engine_sender.clone());
    run_producer(producer).await?;

    print_accounts_csv(engine_sender).await?;

    engine_join.await?
}
