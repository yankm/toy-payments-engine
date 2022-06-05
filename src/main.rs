use tokio::sync::mpsc;
use toy_payments_engine::engine::{PaymentsEngine, run_engine};
use toy_payments_engine::error::Result;
use toy_payments_engine::error::TPEError::CLIError;
use toy_payments_engine::producer::{CSVTransactionProducer, run_producer};
use toy_payments_engine::write_accounts_csv;

/// Engine channel buffer size, in messages.
pub const ENGINE_CHAN_BUF_SIZE: usize = 512;

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
