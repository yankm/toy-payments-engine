mod engine;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::engine::{run_engine, PaymentsEngine};
use crate::producer::{run_producer, CSVTransactionProducer};
use crate::types::Transaction;

const DECIMAL_MAX_PRECISION: u32 = 4;

mod types {
    use anyhow;
    use rust_decimal::Decimal;
    use tokio::task::JoinHandle;
    use crate::error::TransactionError;

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

        pub fn deposit_funds(&mut self, amount: Decimal) -> Result<(), TransactionError> {
            if amount < Decimal::ZERO {
                return Err(TransactionError::InvalidAmount("amount cannot be negative"));
            }
            self.funds.total = self.funds.total.checked_add(amount).ok_or(TransactionError::TooMuchMoney(self.id))?;
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub enum Transaction {
        Deposit {
            account_id: AccountId,
            transaction_id: TxId,
            amount: Decimal,
        },
    }

    impl Transaction {
        pub fn account_id(&self) -> AccountId {
            match *self {
                Self::Deposit { account_id, .. } => account_id,
            }
        }
    }

    pub type Handle<T> = JoinHandle<anyhow::Result<T>>;

    #[cfg(test)]
    mod tests {
        use rust_decimal_macros::dec;
        use crate::error::TransactionError::{InvalidAmount, TooMuchMoney};
        use super::*;

        #[test]
        fn test_account_add_funds_success() {
            let mut acc = Account::new(0);
            acc.deposit_funds(dec!(1.23));
            assert_eq!(acc.funds.total, dec!(1.23));
            acc.deposit_funds(dec!(1.23));
            assert_eq!(acc.funds.total, dec!(2.46));
        }

        #[test]
        fn test_account_add_negative_funds() {
            let mut acc = Account::new(0);
            let result = acc.deposit_funds(dec!(-1));
            let expected = Err(InvalidAmount("amount cannot be negative"));
            assert_eq!(expected, result);
        }

        #[test]
        fn test_account_add_funds_overflow() -> Result<(), TransactionError> {
            let mut acc = Account::new(0);
            let expected_funds = Decimal::MAX - dec!(10);
            acc.deposit_funds(expected_funds)?;

            let expected_error = Err(TooMuchMoney(0));
            let result = acc.deposit_funds(dec!(15));
            assert_eq!(expected_error, result);
            assert_eq!(expected_funds, acc.funds.total);
            Ok(())
        }
    }
}

mod error {
    use crate::types::AccountId;
    use thiserror::Error;

    /// Error that may happen during transaction processing.
    #[derive(Debug, Clone, Error, PartialEq)]
    pub enum TransactionError {
        #[error("Account {0} has too much money")]
        TooMuchMoney(AccountId),

        #[error("{0}")]
        InvalidAmount(&'static str),
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
    use crate::{Transaction, DECIMAL_MAX_PRECISION};

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

    impl TryInto<Transaction> for TransactionRecord {
        type Error = anyhow::Error;

        fn try_into(self) -> std::result::Result<Transaction, Self::Error> {
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
                    Ok(Transaction::Deposit {
                        account_id: self.client,
                        transaction_id: self.tx,
                        amount,
                    })
                }
                _ => unimplemented!("try_into unimplemented"),
            }
        }
    }

    pub struct CSVTransactionProducer {
        csv_path: PathBuf,
        payment_engine_sender: mpsc::Sender<PaymentsEngineMessage>,
    }

    impl CSVTransactionProducer {
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

    pub async fn run_producer(p: CSVTransactionProducer) -> Result<()> {
        let f = File::open(p.csv_path)?;
        let mut rdr = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(f);

        let headers = rdr.headers()?.clone();
        let mut record = csv::StringRecord::new();
        while rdr.read_record(&mut record)? {
            let tx_record: TransactionRecord = record.deserialize(Some(&headers))?;
            p.payment_engine_sender
                .send(PaymentsEngineMessage::ProcessTx(tx_record.try_into()?))
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

    let producer = CSVTransactionProducer::new(csv_path, engine_sender);
    run_producer(producer).await?;

    engine_join.await?
}
