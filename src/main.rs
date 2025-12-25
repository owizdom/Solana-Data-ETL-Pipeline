use clap::{Parser, Subcommand};
use solana_etl::config::Config;
use solana_etl::error::ETLError;

#[derive(Parser)]
#[command(name = "solana-etl")]
#[command(about = "Solana Telemetry & ETL Pipeline")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Backfill historical slots
    Backfill {
        /// Start slot (inclusive)
        #[arg(long)]
        start_slot: u64,
        /// End slot (exclusive)
        #[arg(long)]
        end_slot: u64,
        /// Number of parallel workers
        #[arg(long, default_value = "4")]
        workers: usize,
    },
    /// Run incremental loader
    Incremental {
        /// Interval in seconds between runs
        #[arg(long, default_value = "30")]
        interval: u64,
    },
    /// Check pipeline health
    Health,
    /// Generate analytics report
    Analytics,
}

#[tokio::main]
async fn main() -> Result<(), ETLError> {
    // Initialize logging - use try_init to avoid panics
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .try_init();

    let cli = Cli::parse();
    let config = Config::load()?;

    match cli.command {
        Commands::Backfill {
            start_slot,
            end_slot,
            workers,
        } => {
            solana_etl::backfill::run_backfill(config, start_slot, end_slot, workers).await?;
        }
        Commands::Incremental { interval } => {
            solana_etl::incremental::run_incremental(config, interval).await?;
        }
        Commands::Health => {
            solana_etl::health::check_health(config).await?;
        }
        Commands::Analytics => {
            solana_etl::analytics::run_analytics(config).await?;
        }
    }

    Ok(())
}

