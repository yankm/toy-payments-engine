use anyhow::{Context, Result};
use csv_async;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::io::AsyncRead;

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
                let tx = Transaction::new(TransactionKind::Deposit, self.tx, self.client, amount);
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

pub struct CSVTransactionProducer<R: AsyncRead + Unpin + Send> {
    reader: R,
    payment_engine_sender: mpsc::Sender<PaymentsEngineCommand>,
}

impl<R: AsyncRead + Unpin + Send> CSVTransactionProducer<R> {
    pub fn new(reader: R, payment_engine_sender: mpsc::Sender<PaymentsEngineCommand>) -> Self {
        Self {
            reader,
            payment_engine_sender,
        }
    }
}

pub async fn run_producer<R: AsyncRead + Unpin + Send>(p: CSVTransactionProducer<R>) -> Result<()> {
    let mut rdr = csv_async::AsyncReaderBuilder::new()
        .trim(csv_async::Trim::All)
        .flexible(true)
        .create_reader(p.reader);

    let headers = rdr.byte_headers().await?.clone();
    let mut record = csv_async::ByteRecord::new();
    while rdr.read_byte_record(&mut record).await? {
        let tx_record: TransactionRecord = record.deserialize(Some(&headers))?;
        match tx_record.try_into() {
            Ok(cmd) => p.payment_engine_sender.send(cmd).await?,
            Err(e) => {
                return Err(e).with_context(|| format!("failed to process record {:?}", record));
            }
        };
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_deserialize_csv_success() -> Result<()> {
        let tests = vec![
            b"\
type,client,tx,amount
deposit,1,1,0.1234
"
            .as_slice(),
            b"\
type,       client,    tx,  amount
deposit,         1,     1,  0.1234
"
            .as_slice(),
        ];

        let expected_tx_cmd =
            Transaction::new(TransactionKind::Deposit, 1, 1, dec!(0.1234)).try_into()?;
        for data in tests.into_iter() {
            let (sender, mut receiver) = mpsc::channel(1);
            let p = CSVTransactionProducer::new(data, sender);

            run_producer(p).await?;

            let cmd = receiver.recv().await.expect("cmd has not been received");
            match cmd {
                PaymentsEngineCommand::TransactionCommand(tx_cmd) => {
                    assert_eq!(tx_cmd, expected_tx_cmd)
                }
                // One cannot simply derive `PartialEq` for `PaymentsEngineCommand` due to channel sender
                // in one of the enum variants. So we use this hack to compare just inner command.
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_deserialize_unsupported_precision() -> Result<()> {
        let data = b"\
type,       client,    tx,  amount
deposit,         1,     1,  1.23456
";

        let (sender, _) = mpsc::channel(1);
        let p = CSVTransactionProducer::new(data.as_slice(), sender);

        let error = run_producer(p).await.err().expect("must be error");
        assert_eq!(
            format!("{}", error),
            "failed to process record ByteRecord([\"deposit\", \"1\", \"1\", \"1.23456\"])"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_deserialize_amount_missing() -> Result<()> {
        let data = b"\
type,       client,    tx,
deposit,         1,     1,
";

        let (sender, _) = mpsc::channel(1);
        let p = CSVTransactionProducer::new(data.as_slice(), sender);

        let error = run_producer(p).await.err().expect("must be error");
        assert_eq!(
            format!("{}", error),
            "failed to process record ByteRecord([\"deposit\", \"1\", \"1\", \"\"])"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_deserialize_malformed_csv() -> Result<()> {
        let tests = vec![
            // No headers
            b"\
deposit,1,1,0.1234
deposit,2,2,0.1234
"
            .as_slice(),
            // Invalid types
            b"\
client,type,amount,tx
deposit,1,1,0.1234
"
            .as_slice(),
            // Invalid tx type
            b"\
type,client,tx,amount
foobarbaz,1,1,0.1234
"
            .as_slice(),
        ];

        for data in tests.into_iter() {
            let (sender, _) = mpsc::channel(1);
            let p = CSVTransactionProducer::new(data, sender);

            assert!(
                run_producer(p).await.is_err(),
                "{}",
                String::from_utf8_lossy(data)
            );
        }

        Ok(())
    }
}
