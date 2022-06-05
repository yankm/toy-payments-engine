extern crate core;

mod account;
mod engine;
mod producer;

use tokio::sync::mpsc;

use tokio::io::AsyncWriteExt;

use crate::engine::{run_engine, PaymentsEngine, PaymentsEngineCommand, ENGINE_CHAN_BUF_SIZE};
use crate::error::Result;
use crate::error::TPEError::CLIError;
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
    use tokio::sync::mpsc;

    pub type Result<T> = std::result::Result<T, TPEError>;

    /// Composite error type to encompass all error types toy payments engine produces.
    /// Short for ToyPaymentsEngineError.
    #[derive(Debug, Error, PartialEq)]
    pub enum TPEError {
        /// Logical errors that may happen during transaction processing.
        #[error("Failed to process transaction: {0}")]
        TransactionError(#[from] TransactionErrorKind),

        /// Tokio runtime errors.
        #[error("TokioRuntimeError: {0}")]
        TokioRuntimeError(TokioRuntimeErrorKind),

        /// Errors related to invalid input, e.g. empty amount field or unsupported precision.
        #[error("{0}")]
        InvalidInputError(String),

        /// Errors that may happen while processing CSV data.
        #[error("CSVError: {0}")]
        CSVError(String),

        /// Errors regarding the command-line interface.
        #[error("CLIError: {0}")]
        CLIError(String),

        /// Errors related to I/O.
        #[error("IOError: {0}")]
        IOError(String),
    }

    impl<T> From<mpsc::error::SendError<T>> for TPEError {
        fn from(e: mpsc::error::SendError<T>) -> Self {
            Self::TokioRuntimeError(TokioRuntimeErrorKind::ChannelSendError(format!(
                "Failed to send PaymentsEngineCommand: {}",
                e
            )))
        }
    }

    impl From<tokio::task::JoinError> for TPEError {
        fn from(e: tokio::task::JoinError) -> Self {
            Self::TokioRuntimeError(TokioRuntimeErrorKind::TaskJoinError(format!("{}", e)))
        }
    }

    impl From<csv_async::Error> for TPEError {
        fn from(e: csv_async::Error) -> Self {
            Self::CSVError(format!("{}", e))
        }
    }

    impl From<std::io::Error> for TPEError {
        fn from(e: std::io::Error) -> Self {
            Self::IOError(format!("{}", e))
        }
    }

    /// Tokio runtime errors.
    #[derive(Debug, Error, PartialEq)]
    pub enum TokioRuntimeErrorKind {
        #[error("Failed to send message over channel: {0}")]
        ChannelSendError(String),

        #[error("Failed to join task: {0}")]
        TaskJoinError(String),
    }

    /// Errors that may happen during transaction processing.
    #[derive(Debug, Clone, Error, PartialEq)]
    pub enum TransactionErrorKind {
        /// Decimal overflow.
        #[error("Account has too much money")]
        TooMuchMoney,

        /// Zero or negative amount was specified.
        #[error("Amount should be positive")]
        NonPositiveAmount,

        /// Insufficient funds to perform requested transaction.
        #[error("Insufficient funds")]
        InsufficientFunds,

        /// Account is locked.
        #[error("Account {0} is locked")]
        AccountLocked(AccountId),

        /// Transaction with id `TransactionId` was already processed.
        #[error("Transaction {0} has already been processed")]
        DuplicatedTransaction(TransactionId),

        /// Command received by worker relates to another worker.
        #[error("Worker account id mismatch (got {0:?}, expected {1:?})")]
        WorkerAccountIdMismatch(AccountId, AccountId),

        /// Transaction not found.
        #[error("Transaction {0} not found")]
        TransactionNotFound(TransactionId),

        /// Disputes are not supported for the `TransactionKind`.
        #[error("Disputes are not supported for transactions of type '{0}'")]
        DisputeNotSupported(TransactionKind),

        /// Used when operation cannot be processed due to the current state of the transaction.
        #[error("Transaction {0} {1}")]
        TransactionPreconditionFailed(TransactionId, &'static str),

        /// Dispute not found.
        #[error("Dispute for transaction {0} not found")]
        TransactionDisputeNotFound(TransactionId),

        /// Used when operation cannot be processed due to the current state of the transaction dispute.
        #[error("Transaction {0} {1}")]
        TransactionDisputePreconditionFailed(TransactionId, &'static str),
    }
}

/// Collects CSV records from engine and writes them using writer.
async fn write_accounts_csv<W: AsyncWriteExt + Unpin>(
    engine_sender: mpsc::Sender<PaymentsEngineCommand>,
    writer: &mut W,
) -> Result<()> {
    let (csv_sender, mut csv_receiver) = mpsc::channel(STREAM_ACCOUNTS_CSV_CHANNEL_SIZE);
    engine_sender
        .send(PaymentsEngineCommand::StreamAccountsCSV(csv_sender))
        .await?;

    while let Some(record) = csv_receiver.recv().await {
        writer.write(record.as_bytes()).await?;
    }

    writer.flush().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let csv_path = std::env::args().nth(1).ok_or(CLIError(format!(
        "Missing input file name. Usage: {} <filename>",
        std::env::args().nth(0).unwrap()
    )))?;
    let csv_file = tokio::fs::File::open(csv_path).await?;

    let (engine_sender, engine_receiver) = mpsc::channel(ENGINE_CHAN_BUF_SIZE);
    let engine = PaymentsEngine::new(engine_receiver);
    let engine_join = tokio::spawn(run_engine(engine));

    let producer = CSVTransactionProducer::new(csv_file, engine_sender.clone());
    run_producer(producer).await?;

    let mut stdout = tokio::io::stdout();
    write_accounts_csv(engine_sender, &mut stdout).await?;

    engine_join.await?
}
