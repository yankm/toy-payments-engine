mod engine;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::engine::{run_engine, PaymentsEngine};
use crate::producer::{run_producer, CSVTransactionProducer};
use crate::types::Transaction;

const DECIMAL_MAX_PRECISION: u32 = 4;

mod types {
    use crate::error::TransactionError;
    use anyhow;
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

    impl Funds {
        pub fn available(&self) -> Decimal {
            self.total - self.held
        }
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

        #[cfg(test)]
        fn new_with_funds(id: AccountId, funds: Funds) -> Self {
            Self { id, funds }
        }

        pub fn id(&self) -> AccountId {
            self.id
        }

        pub fn deposit_funds(&mut self, amount: Decimal) -> Result<(), TransactionError> {
            if amount <= Decimal::ZERO {
                return Err(TransactionError::NonPositiveAmount);
            }
            self.funds.total = self
                .funds
                .total
                .checked_add(amount)
                .ok_or(TransactionError::TooMuchMoney)?;
            Ok(())
        }

        pub fn withdraw_funds(&mut self, amount: Decimal) -> Result<(), TransactionError> {
            if amount <= Decimal::ZERO {
                return Err(TransactionError::NonPositiveAmount);
            }
            if amount > self.funds.available() {
                return Err(TransactionError::InsufficientFunds);
            }
            self.funds.total -= amount;
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
        Withdraw {
            account_id: AccountId,
            transaction_id: TxId,
            amount: Decimal,
        },
    }

    impl Transaction {
        pub fn account_id(&self) -> AccountId {
            match *self {
                Self::Deposit { account_id, .. } => account_id,
                Self::Withdraw { account_id, .. } => account_id,
            }
        }
    }

    pub type Handle<T> = JoinHandle<anyhow::Result<T>>;

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::error::TransactionError::*;
        use rust_decimal_macros::dec;

        #[test]
        fn test_account_add_funds_success() -> Result<(), TransactionError> {
            let mut acc = Account::new(0);
            acc.deposit_funds(dec!(1.23))?;
            assert_eq!(acc.funds.total, dec!(1.23));
            acc.deposit_funds(dec!(1.23))?;
            assert_eq!(acc.funds.total, dec!(2.46));
            Ok(())
        }

        #[test]
        fn test_account_add_non_positive_funds() {
            let amounts = [dec!(-1), dec!(-0.1), dec!(0)];
            let mut acc = Account::new(0);
            for amount in amounts {
                let result = acc.deposit_funds(amount);
                let expected = Err(NonPositiveAmount);
                assert_eq!(result, expected);
            }
        }

        #[test]
        fn test_account_add_funds_overflow() -> Result<(), TransactionError> {
            let mut acc = Account::new(0);
            let expected_funds = Decimal::MAX - dec!(10);
            acc.deposit_funds(expected_funds)?;

            let expected_error = Err(TooMuchMoney);
            let result = acc.deposit_funds(dec!(15));
            assert_eq!(result, expected_error);
            assert_eq!(acc.funds.total, expected_funds);
            Ok(())
        }

        #[test]
        fn test_account_withdraw_funds_success() -> Result<(), TransactionError> {
            let tests = [
                // funds_total, funds_held, withdraw_amount, expected_total
                (dec!(1.23), dec!(0), dec!(1.23), dec!(0)),
                (dec!(1.23), dec!(1.22), dec!(0.01), dec!(1.22)),
                (dec!(10), dec!(1), dec!(5), dec!(5)),
            ];
            for (total, held, withdraw_amount, expected_total) in tests {
                let mut acc = Account::new_with_funds(0, Funds { total, held });
                acc.withdraw_funds(withdraw_amount)?;
                assert_eq!(acc.funds.total, expected_total);
            }
            Ok(())
        }

        #[test]
        fn test_account_withdraw_non_positive_funds() {
            let amounts = [dec!(-1), dec!(-0.1), dec!(0)];

            let mut acc = Account::new_with_funds(
                0,
                Funds {
                    total: dec!(2.46),
                    held: dec!(0),
                },
            );
            for amount in amounts {
                let result = acc.withdraw_funds(amount);
                let expected = Err(NonPositiveAmount);
                assert_eq!(result, expected);
            }
        }

        #[test]
        fn test_account_withdraw_more_than_available_funds() {
            let tests = [
                // funds_total, funds_held, withdraw_amount
                (dec!(0), dec!(0), dec!(0.1)),
                (dec!(10), dec!(10), dec!(1)),
                (dec!(5), dec!(3), dec!(3)),
            ];

            for (total, held, withdraw_amount) in tests {
                let mut acc = Account::new_with_funds(0, Funds { total, held });
                let result = acc.withdraw_funds(withdraw_amount);
                assert_eq!(result, Err(InsufficientFunds));
            }
        }
    }
}

mod error {
    use crate::types::AccountId;
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
                TransactionRecordType::Deposit => Ok(Transaction::Deposit {
                    account_id: self.client,
                    transaction_id: self.tx,
                    amount: checked_amount(self.amount)?,
                }),
                TransactionRecordType::Withdrawal => Ok(Transaction::Withdraw {
                    account_id: self.client,
                    transaction_id: self.tx,
                    amount: checked_amount(self.amount)?,
                }),
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
