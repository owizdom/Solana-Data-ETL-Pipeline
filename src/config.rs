use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub alchemy: AlchemyConfig,
    pub warehouse: WarehouseConfig,
    pub etl: ETLConfig,
}

#[derive(Debug, Clone)]
pub struct AlchemyConfig {
    pub rpc_url: String,
    pub max_retries: u32,
    pub timeout_seconds: u64,
    pub rate_limit_per_second: u32,
}

#[derive(Debug, Clone)]
pub struct WarehouseConfig {
    pub warehouse_type: String, // "bigquery", "snowflake", "postgres"
    pub connection_string: Option<String>,
    pub project_id: Option<String>, // For BigQuery
    pub dataset_id: Option<String>, // For BigQuery
    pub credentials_path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ETLConfig {
    pub batch_size: usize,
    pub checkpoint_interval: u64,
    pub backfill_chunk_size: u64,
    pub incremental_interval_seconds: u64,
    pub max_slot_lag: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            alchemy: AlchemyConfig {
                rpc_url: env::var("ALCHEMY_RPC_URL")
                    .unwrap_or_else(|_| "https://solana-mainnet.g.alchemy.com/v2/YOUR_API_KEY".to_string()),
                max_retries: env::var("ALCHEMY_MAX_RETRIES")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(5),
                timeout_seconds: env::var("ALCHEMY_TIMEOUT_SECONDS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(30),
                rate_limit_per_second: env::var("ALCHEMY_RATE_LIMIT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(50),
            },
            warehouse: WarehouseConfig {
                warehouse_type: env::var("WAREHOUSE_TYPE")
                    .unwrap_or_else(|_| "postgres".to_string())
                    .to_lowercase(),
                connection_string: env::var("WAREHOUSE_CONNECTION").ok(),
                project_id: env::var("BIGQUERY_PROJECT_ID").ok(),
                dataset_id: env::var("BIGQUERY_DATASET_ID").ok().or(Some("solana_etl".to_string())),
                credentials_path: env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
            },
            etl: ETLConfig {
                batch_size: env::var("ETL_BATCH_SIZE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000),
                checkpoint_interval: env::var("ETL_CHECKPOINT_INTERVAL")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(100),
                backfill_chunk_size: env::var("ETL_BACKFILL_CHUNK_SIZE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000),
                incremental_interval_seconds: env::var("ETL_INTERVAL_SECONDS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(30),
                max_slot_lag: env::var("ETL_MAX_SLOT_LAG")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000),
            },
        }
    }
}

impl Config {
    pub fn load() -> crate::Result<Self> {
        // Try to load from config file first, then fall back to env/defaults
        Ok(Config::default())
    }
}
