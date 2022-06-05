use tokio::sync::mpsc;
use toy_payments_engine::engine::{run_engine, PaymentsEngine};
use toy_payments_engine::error::Result;
use toy_payments_engine::error::TPEError::CLIError;
use toy_payments_engine::producer::{run_producer, CSVTransactionProducer};
use toy_payments_engine::write_accounts_csv;

/// Engine channel buffer size, in messages.
pub const ENGINE_CHAN_BUF_SIZE: usize = 512;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let csv_path = std::env::args().nth(1).ok_or_else(|| {
        CLIError(format!(
            "Missing input file name. Usage: {} <filename>",
            std::env::args().next().unwrap()
        ))
    })?;
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
