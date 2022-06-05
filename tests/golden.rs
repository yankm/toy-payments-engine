use csv_async::{StringRecord, Trim};
use std::fmt;
use std::fmt::Formatter;
use std::iter::zip;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

use toy_payments_engine::engine::{run_engine, PaymentsEngine};
use toy_payments_engine::producer::{run_producer, CSVTransactionProducer};
use toy_payments_engine::write_accounts_csv;

/// Engine channel buffer size, in messages.
const ENGINE_CHAN_BUF_SIZE: usize = 512;

/// A helper struct to allow sorting and better record comparison error messages.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct GenericCSVRecord {
    fields: Vec<String>,
}

impl From<StringRecord> for GenericCSVRecord {
    fn from(sr: StringRecord) -> Self {
        Self {
            fields: sr.iter().map(String::from).collect(),
        }
    }
}

impl fmt::Debug for GenericCSVRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.fields.join(","))
    }
}

/// Processes transactions from `transactions.csv` and compares with output from `accounts.csv`
async fn run_test_case(dir_path: &str) -> anyhow::Result<()> {
    let pb = PathBuf::from(dir_path);
    let accounts_csv_path = pb.join("accounts.csv");
    let transactions_csv_path = pb.join("transactions.csv");

    let output_bytes = process_transactions(transactions_csv_path).await?;
    let mut output_records = get_csv_records(output_bytes.as_slice()).await?;
    let accounts_file = File::open(accounts_csv_path).await?;
    let mut expected_records = get_csv_records(accounts_file).await?;

    output_records.sort();
    expected_records.sort();

    assert_eq!(
        output_records.len(),
        expected_records.len(),
        "CSV lengths mismatch"
    );

    for (actual, expected) in zip(output_records, expected_records) {
        assert_eq!(actual, expected)
    }

    Ok(())
}

/// Spawns an engine to process transactions from `csv_path` and returns output bytes.
async fn process_transactions(csv_path: PathBuf) -> anyhow::Result<Vec<u8>> {
    let csv_file = File::open(csv_path).await?;

    let (engine_sender, engine_receiver) = mpsc::channel(ENGINE_CHAN_BUF_SIZE);
    let engine = PaymentsEngine::new(engine_receiver);
    let engine_join = tokio::spawn(run_engine(engine));

    let producer = CSVTransactionProducer::new(csv_file, engine_sender.clone());
    run_producer(producer).await?;

    let mut accounts_csv_bytes: Vec<u8> = Vec::new();
    write_accounts_csv(engine_sender, &mut accounts_csv_bytes).await?;

    engine_join.await??;

    Ok(accounts_csv_bytes)
}

/// Collects CSV records from a reader into a vector of records. Each record is represented as
/// a vector of fields, each field is a String.
async fn get_csv_records<R: AsyncRead + Unpin + Send>(
    reader: R,
) -> anyhow::Result<Vec<GenericCSVRecord>> {
    let mut records: Vec<GenericCSVRecord> = Vec::new();

    let mut csv_rdr = csv_async::AsyncReaderBuilder::new()
        .trim(Trim::All)
        .create_reader(reader);

    // push headers first
    records.push(csv_rdr.headers().await?.clone().into());
    let mut r = StringRecord::new();
    while csv_rdr.read_record(&mut r).await? {
        records.push(r.clone().into());
    }

    Ok(records)
}

/// Tests engine can consume and produce transactions for 10k accounts.
#[tokio::test]
async fn _10k_accounts() -> anyhow::Result<()> {
    run_test_case("testdata/10k-accounts").await
}

/// Tests a single deposit.
#[tokio::test]
async fn deposit() -> anyhow::Result<()> {
    run_test_case("testdata/deposit").await
}

/// Tests a single withdrawal after successful deposit.
#[tokio::test]
async fn deposit_withdraw() -> anyhow::Result<()> {
    run_test_case("testdata/deposit-withdraw").await
}

/// Tests a single chargeback.
#[tokio::test]
async fn chargeback() -> anyhow::Result<()> {
    run_test_case("testdata/chargeback").await
}

/// Tests the following flow:
/// 1. Make deposit
/// 2. Open a dispute for tx from step 1
/// 3. Resolve (cancel) a dispute from step 2
/// 4. Open another dispute for tx from step 1
/// 5. Chargeback dispute from step 4
/// Chargeback is expected to be completed.
#[tokio::test]
async fn chargeback_after_resolve() -> anyhow::Result<()> {
    run_test_case("testdata/chargeback-after-resolve").await
}

/// Tests a deposit after chargeback.
#[tokio::test]
async fn deposit_after_chargeback() -> anyhow::Result<()> {
    run_test_case("testdata/deposit-after-chargeback").await
}

/// Tests a withdraw after chargeback.
#[tokio::test]
async fn withdraw_after_chargeback() -> anyhow::Result<()> {
    run_test_case("testdata/withdraw-after-chargeback").await
}

/// Tests opening a dispute for withdraw and resolving it with chargeback.
#[tokio::test]
async fn dispute_withdraw() -> anyhow::Result<()> {
    run_test_case("testdata/dispute-withdraw").await
}

/// Tests trying to perform a chargeback twice for the same transaction.
#[tokio::test]
async fn double_chargeback() -> anyhow::Result<()> {
    run_test_case("testdata/double-chargeback").await
}

/// Tests a single opened, but not resolved dispute.
#[tokio::test]
async fn unresolved_dispute() -> anyhow::Result<()> {
    run_test_case("testdata/unresolved-dispute").await
}

/// Tests series of transactions with multiple disputes (resolved and not)
#[tokio::test]
async fn multiple_disputes() -> anyhow::Result<()> {
    run_test_case("testdata/multiple_disputes").await
}

/// Tests a sequence where client 1 opens a dispute for transaction of client 2
#[tokio::test]
async fn dispute_another_account() -> anyhow::Result<()> {
    run_test_case("testdata/dispute-another-account").await
}

/// Tests a sequence where chargeback from client 1 comes for transaction of client 2
#[tokio::test]
async fn chargeback_another_account() -> anyhow::Result<()> {
    run_test_case("testdata/chargeback-another-account").await
}

/// Tests two deposits with the same tx id.
#[tokio::test]
async fn duplicate_tx_ids() -> anyhow::Result<()> {
    run_test_case("testdata/duplicate-tx-ids").await
}

/// Tests opening a new dispute by a locked client.
#[tokio::test]
async fn new_dispute_after_chargeback() -> anyhow::Result<()> {
    run_test_case("testdata/new-dispute-after-chargeback").await
}

/// Tests sample input from the task description.
#[tokio::test]
async fn sample_input() -> anyhow::Result<()> {
    run_test_case("testdata/sample-input").await
}

/// Tests malformed transactions (amount missing in deposit/withdrawal, unsupported precision, etc)
#[tokio::test]
async fn malformed_transactions() -> anyhow::Result<()> {
    run_test_case("testdata/malformed-transactions").await
}
