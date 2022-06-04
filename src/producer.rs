use std::path::PathBuf;

use anyhow::{Context, Result};
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
        match tx_record.try_into() {
            Ok(cmd) => p.payment_engine_sender.send(cmd).await?,
            Err(e) => {
                return Err(e).with_context(|| format!("failed to process record {:?}", record));
            }
        };
    }

    Ok(())
}
