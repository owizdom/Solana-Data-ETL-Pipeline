use crate::config::WarehouseConfig;
use crate::error::{ETLError, Result};
use crate::events::CanonicalEvent;
use async_trait::async_trait;
use sqlx::{PgPool, Row, postgres::PgArguments, Arguments};
use std::sync::{Arc, Mutex};

#[async_trait]
pub trait Warehouse: Send + Sync {
    /// Initialize warehouse connection
    async fn connect(&self) -> Result<()>;

    /// Insert batch of events
    async fn insert_events(&self, events: Vec<CanonicalEvent>) -> Result<()>;

    /// Get last processed slot
    async fn get_last_slot(&self) -> Result<Option<u64>>;

    /// Update last processed slot
    async fn update_last_slot(&self, slot: u64) -> Result<()>;

    /// Check if slot has been processed (for idempotency)
    async fn is_slot_processed(&self, slot: u64) -> Result<bool>;

    /// Health check
    async fn health_check(&self) -> Result<()>;
}

/// Factory to create warehouse instances
pub fn create_warehouse(config: WarehouseConfig) -> Result<Box<dyn Warehouse>> {
    match config.warehouse_type.as_str() {
        "bigquery" => Ok(Box::new(BigQueryWarehouse::new(config)?)),
        "postgres" => Ok(Box::new(PostgresWarehouse::new(config)?)),
        _ => Err(ETLError::Config(format!(
            "Unsupported warehouse type: {}. Use 'postgres' or 'bigquery'",
            config.warehouse_type
        ))),
    }
}

/// BigQuery warehouse implementation
pub struct BigQueryWarehouse {
    config: WarehouseConfig,
}

impl BigQueryWarehouse {
    pub fn new(config: WarehouseConfig) -> Result<Self> {
        if config.project_id.is_none() {
            return Err(ETLError::Config("BigQuery requires project_id. Set BIGQUERY_PROJECT_ID env var".to_string()));
        }
        Ok(Self { config })
    }
}

#[async_trait]
impl Warehouse for BigQueryWarehouse {
    async fn connect(&self) -> Result<()> {
        tracing::info!("Connecting to BigQuery project: {:?}", self.config.project_id);
        // TODO: Implement actual BigQuery connection
        Ok(())
    }

    async fn insert_events(&self, events: Vec<CanonicalEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        tracing::info!("Inserting {} events to BigQuery (placeholder)", events.len());
        // TODO: Implement actual BigQuery insert
        Ok(())
    }

    async fn get_last_slot(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn update_last_slot(&self, slot: u64) -> Result<()> {
        tracing::info!("Updating last slot to {} (BigQuery placeholder)", slot);
        Ok(())
    }

    async fn is_slot_processed(&self, _slot: u64) -> Result<bool> {
        Ok(false)
    }

    async fn health_check(&self) -> Result<()> {
        Ok(())
    }
}

/// Postgres warehouse implementation
pub struct PostgresWarehouse {
    config: WarehouseConfig,
    pool: Arc<Mutex<Option<Arc<PgPool>>>>,
}

impl PostgresWarehouse {
    pub fn new(config: WarehouseConfig) -> Result<Self> {
        if config.connection_string.is_none() {
            return Err(ETLError::Config(
                "Postgres requires connection_string. Set WAREHOUSE_CONNECTION env var (e.g., postgresql://user:pass@localhost/solana_etl)".to_string(),
            ));
        }
        Ok(Self {
            config,
            pool: Arc::new(Mutex::new(None)),
        })
    }

    async fn get_pool(&self) -> Result<Arc<PgPool>> {
        // Check if pool exists
        {
            let pool_guard = self.pool.lock().unwrap();
            if let Some(ref pool) = *pool_guard {
                return Ok(pool.clone());
            }
        }

        // Connect if not connected
        let conn_str = self.config.connection_string.as_ref()
            .ok_or_else(|| ETLError::Config("Postgres connection string not set".to_string()))?;
        
        tracing::info!("Connecting to Postgres...");
        let pool = PgPool::connect(conn_str).await
            .map_err(|e| ETLError::Database(format!("Failed to connect to Postgres: {}", e)))?;
        
        let pool_arc = Arc::new(pool);
        
        // Store pool
        {
            let mut pool_guard = self.pool.lock().unwrap();
            *pool_guard = Some(pool_arc.clone());
        }
        
        // Initialize schema
        self.init_schema(&pool_arc).await?;
        
        tracing::info!("Connected to Postgres successfully");
        Ok(pool_arc)
    }

    async fn init_schema(&self, pool: &PgPool) -> Result<()> {
        // Create etl_metadata table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS etl_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to create etl_metadata: {}", e)))?;

        // Create fact_transactions table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS fact_transactions (
                event_id TEXT PRIMARY KEY,
                slot BIGINT NOT NULL,
                block_time TIMESTAMP NOT NULL,
                tx_signature TEXT NOT NULL,
                program_id TEXT,
                instruction_index INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                raw_payload JSONB,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            "#
        )
        .execute(pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to create fact_transactions: {}", e)))?;

        // Create index on slot for faster queries
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_fact_transactions_slot ON fact_transactions(slot)")
            .execute(pool)
            .await
            .ok(); // Ignore error if index already exists

        tracing::info!("Postgres schema initialized");
        Ok(())
    }
}

#[async_trait]
impl Warehouse for PostgresWarehouse {
    async fn connect(&self) -> Result<()> {
        // Lazy connection - will connect on first use
        tracing::info!("Postgres will connect on first use");
        Ok(())
    }

    async fn insert_events(&self, events: Vec<CanonicalEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let pool = self.get_pool().await?;
        tracing::info!("Inserting {} events to Postgres", events.len());

        // Batch insert with ON CONFLICT for idempotency
        // Use a transaction for better performance and error handling
        let mut tx = pool.begin().await
            .map_err(|e| ETLError::Database(format!("Failed to begin transaction: {}", e)))?;

        for event in events {
            // Serialize JSON to string first, then Postgres will parse it as JSONB
            // This properly handles Unicode escape sequences
            let json_string = serde_json::to_string(&event.raw_payload)
                .map_err(|e| ETLError::Json(e))?;
            
            sqlx::query(
                r#"
                INSERT INTO fact_transactions (
                    event_id, slot, block_time, tx_signature, program_id, 
                    instruction_index, event_type, raw_payload, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, NOW(), NOW())
                ON CONFLICT (event_id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    raw_payload = EXCLUDED.raw_payload
                "#
            )
            .bind(&event.event_id)
            .bind(event.slot as i64)
            .bind(event.block_time)
            .bind(&event.tx_signature)
            .bind(&event.program_id)
            .bind(event.instruction_index as i32)
            .bind(&event.event_type)
            .bind(&json_string) // Pass as string, Postgres will cast to JSONB
            .execute(&mut *tx)
            .await
            .map_err(|e| ETLError::Database(format!("Failed to insert event {}: {}", event.event_id, e)))?;
        }

        tx.commit().await
            .map_err(|e| ETLError::Database(format!("Failed to commit transaction: {}", e)))?;

        Ok(())
    }

    async fn get_last_slot(&self) -> Result<Option<u64>> {
        let pool = self.get_pool().await?;

        let row = sqlx::query("SELECT value FROM etl_metadata WHERE key = 'last_confirmed_slot'")
            .fetch_optional(&*pool)
            .await
            .map_err(|e| ETLError::Database(format!("Failed to get last slot: {}", e)))?;

        if let Some(row) = row {
            let value: String = row.get(0);
            Ok(value.parse().ok())
        } else {
            Ok(None)
        }
    }

    async fn update_last_slot(&self, slot: u64) -> Result<()> {
        let pool = self.get_pool().await?;

        sqlx::query(
            r#"
            INSERT INTO etl_metadata (key, value, updated_at)
            VALUES ('last_confirmed_slot', $1, NOW())
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = EXCLUDED.updated_at
            "#
        )
        .bind(slot.to_string())
        .execute(&*pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to update last slot: {}", e)))?;

        Ok(())
    }

    async fn is_slot_processed(&self, slot: u64) -> Result<bool> {
        let pool = self.get_pool().await?;

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM fact_transactions WHERE slot = $1"
        )
        .bind(slot as i64)
        .fetch_one(&*pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to check slot: {}", e)))?;

        Ok(count > 0)
    }

    async fn health_check(&self) -> Result<()> {
        let pool = self.get_pool().await?;
        sqlx::query("SELECT 1")
            .execute(&*pool)
            .await
            .map_err(|e| ETLError::Database(format!("Health check failed: {}", e)))?;
        Ok(())
    }
}
