use crate::config::Config;
use crate::error::{ETLError, Result};
use chrono::{DateTime, Utc, NaiveDate};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

pub async fn run_analytics(config: Config) -> Result<()> {
    // Get database connection
    let conn_str = config.warehouse.connection_string
        .ok_or_else(|| ETLError::Config("WAREHOUSE_CONNECTION not set".to_string()))?;
    
    tracing::info!("Connecting to database for analytics...");
    let pool = PgPool::connect(&conn_str).await
        .map_err(|e| ETLError::Database(format!("Failed to connect: {}", e)))?;
    
    // Create analytics tables
    create_analytics_tables(&pool).await?;
    
    tracing::info!("Computing and storing analytics...");
    
    // Compute and store all analytics
    compute_and_store_transaction_volume(&pool).await?;
    compute_and_store_active_programs(&pool).await?;
    compute_and_store_token_transfers(&pool).await?;
    compute_and_store_failed_transactions(&pool).await?;
    compute_and_store_wallet_activity(&pool).await?;
    compute_and_store_program_trends(&pool).await?;
    
    tracing::info!("Analytics computed and stored in database tables");
    
    Ok(())
}

async fn create_analytics_tables(pool: &PgPool) -> Result<()> {
    // Migrate existing tables if they have wrong timestamp types
    migrate_timestamp_columns(pool).await?;
    
    // Transaction volume summary
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_transaction_volume (
            id SERIAL PRIMARY KEY,
            period_type TEXT NOT NULL, -- 'total', 'today', 'week', 'month'
            transaction_count BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(period_type)
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create transaction volume table: {}", e)))?;

    // Hourly volume
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_hourly_volume (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            hour INTEGER NOT NULL,
            transaction_count BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(date, hour)
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create hourly volume table: {}", e)))?;

    // Active programs
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_active_programs (
            id SERIAL PRIMARY KEY,
            program_id TEXT NOT NULL,
            transaction_count BIGINT NOT NULL,
            unique_wallets BIGINT NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(program_id)
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create active programs table: {}", e)))?;

    // Token transfer stats
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_token_transfers (
            id SERIAL PRIMARY KEY,
            total_transfers BIGINT NOT NULL,
            unique_tokens BIGINT NOT NULL,
            unique_senders BIGINT NOT NULL,
            unique_receivers BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create token transfers table: {}", e)))?;

    // Top tokens
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_top_tokens (
            id SERIAL PRIMARY KEY,
            token_mint TEXT NOT NULL,
            transfer_count BIGINT NOT NULL,
            unique_wallets BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(token_mint)
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create top tokens table: {}", e)))?;

    // Failed transactions
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_failed_transactions (
            id SERIAL PRIMARY KEY,
            total_failed BIGINT NOT NULL,
            failure_rate NUMERIC(5,2) NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create failed transactions table: {}", e)))?;

    // Top errors
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_top_errors (
            id SERIAL PRIMARY KEY,
            error_type TEXT NOT NULL,
            error_count BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(error_type)
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create top errors table: {}", e)))?;

    // Wallet activity
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_wallet_activity (
            id SERIAL PRIMARY KEY,
            total_unique_wallets BIGINT NOT NULL,
            active_today BIGINT NOT NULL,
            active_this_week BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create wallet activity table: {}", e)))?;

    // Top wallets
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_top_wallets (
            id SERIAL PRIMARY KEY,
            wallet TEXT NOT NULL,
            transaction_count BIGINT NOT NULL,
            first_seen TIMESTAMPTZ NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(wallet)
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create top wallets table: {}", e)))?;

    // Program trends (daily volume)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_program_trends (
            id SERIAL PRIMARY KEY,
            program_id TEXT NOT NULL,
            date DATE NOT NULL,
            transaction_count BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(program_id, date)
        )
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to create program trends table: {}", e)))?;

    // Create indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_analytics_hourly_date ON analytics_hourly_volume(date, hour)")
        .execute(pool).await.ok();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_analytics_programs_tx_count ON analytics_active_programs(transaction_count DESC)")
        .execute(pool).await.ok();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_analytics_tokens_transfer_count ON analytics_top_tokens(transfer_count DESC)")
        .execute(pool).await.ok();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_analytics_wallets_tx_count ON analytics_top_wallets(transaction_count DESC)")
        .execute(pool).await.ok();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_analytics_trends_program_date ON analytics_program_trends(program_id, date)")
        .execute(pool).await.ok();

    Ok(())
}

async fn migrate_timestamp_columns(pool: &PgPool) -> Result<()> {
    // Drop and recreate tables with correct types (simplest approach)
    // This will lose existing data, but analytics are recomputed anyway
    let drop_queries = vec![
        "DROP TABLE IF EXISTS analytics_transaction_volume CASCADE",
        "DROP TABLE IF EXISTS analytics_hourly_volume CASCADE",
        "DROP TABLE IF EXISTS analytics_active_programs CASCADE",
        "DROP TABLE IF EXISTS analytics_token_transfers CASCADE",
        "DROP TABLE IF EXISTS analytics_top_tokens CASCADE",
        "DROP TABLE IF EXISTS analytics_failed_transactions CASCADE",
        "DROP TABLE IF EXISTS analytics_top_errors CASCADE",
        "DROP TABLE IF EXISTS analytics_wallet_activity CASCADE",
        "DROP TABLE IF EXISTS analytics_top_wallets CASCADE",
        "DROP TABLE IF EXISTS analytics_program_trends CASCADE",
    ];

    for query in drop_queries {
        sqlx::query(query).execute(pool).await.ok();
    }

    Ok(())
}

async fn compute_and_store_transaction_volume(pool: &PgPool) -> Result<()> {
    // Clear existing data
    sqlx::query("DELETE FROM analytics_transaction_volume")
        .execute(pool).await.ok();
    sqlx::query("DELETE FROM analytics_hourly_volume")
        .execute(pool).await.ok();

    // Total
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM fact_transactions WHERE event_type = 'transaction'"
    )
    .fetch_one(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute total: {}", e)))?;

    sqlx::query(
        "INSERT INTO analytics_transaction_volume (period_type, transaction_count) 
         VALUES ('total', $1)
         ON CONFLICT (period_type) DO UPDATE SET transaction_count = EXCLUDED.transaction_count, updated_at = NOW()"
    )
    .bind(total)
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to insert total: {}", e)))?;

    // Today
    let today: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM fact_transactions 
         WHERE event_type = 'transaction' 
         AND DATE(block_time) = CURRENT_DATE"
    )
    .fetch_one(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute today: {}", e)))?;

    sqlx::query(
        "INSERT INTO analytics_transaction_volume (period_type, transaction_count) 
         VALUES ('today', $1)
         ON CONFLICT (period_type) DO UPDATE SET transaction_count = EXCLUDED.transaction_count, updated_at = NOW()"
    )
    .bind(today)
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to insert today: {}", e)))?;

    // This week
    let this_week: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM fact_transactions 
         WHERE event_type = 'transaction' 
         AND block_time >= CURRENT_DATE - INTERVAL '7 days'"
    )
    .fetch_one(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute week: {}", e)))?;

    sqlx::query(
        "INSERT INTO analytics_transaction_volume (period_type, transaction_count) 
         VALUES ('week', $1)
         ON CONFLICT (period_type) DO UPDATE SET transaction_count = EXCLUDED.transaction_count, updated_at = NOW()"
    )
    .bind(this_week)
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to insert week: {}", e)))?;

    // This month
    let this_month: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM fact_transactions 
         WHERE event_type = 'transaction' 
         AND block_time >= CURRENT_DATE - INTERVAL '30 days'"
    )
    .fetch_one(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute month: {}", e)))?;

    sqlx::query(
        "INSERT INTO analytics_transaction_volume (period_type, transaction_count) 
         VALUES ('month', $1)
         ON CONFLICT (period_type) DO UPDATE SET transaction_count = EXCLUDED.transaction_count, updated_at = NOW()"
    )
    .bind(this_month)
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to insert month: {}", e)))?;

    // Hourly volume (last 24 hours)
    let hourly_rows = sqlx::query(
        "SELECT DATE(block_time) as date, 
                EXTRACT(HOUR FROM block_time)::int as hour,
                COUNT(*)::bigint as count
         FROM fact_transactions 
         WHERE event_type = 'transaction' 
         AND block_time >= NOW() - INTERVAL '24 hours'
         GROUP BY DATE(block_time), EXTRACT(HOUR FROM block_time)"
    )
    .fetch_all(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute hourly: {}", e)))?;

    for row in hourly_rows {
        sqlx::query(
            "INSERT INTO analytics_hourly_volume (date, hour, transaction_count) 
             VALUES ($1, $2, $3)
             ON CONFLICT (date, hour) DO UPDATE SET transaction_count = EXCLUDED.transaction_count, updated_at = NOW()"
        )
        .bind(row.get::<NaiveDate, _>(0))
        .bind(row.get::<i32, _>(1))
        .bind(row.get::<i64, _>(2))
        .execute(pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to insert hourly: {}", e)))?;
    }

    Ok(())
}

async fn compute_and_store_active_programs(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM analytics_active_programs")
        .execute(pool).await.ok();

    let rows = sqlx::query(
        "SELECT 
            program_id,
            COUNT(*)::bigint as tx_count,
            COUNT(DISTINCT (raw_payload->'transaction'->'message'->'accountKeys'->>0))::bigint as unique_wallets,
            MAX(block_time)::timestamptz as last_seen
         FROM fact_transactions 
         WHERE program_id IS NOT NULL 
         AND event_type = 'program_instruction'
         GROUP BY program_id
         ORDER BY tx_count DESC
         LIMIT 50"
    )
    .fetch_all(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute active programs: {}", e)))?;

    for row in rows {
        sqlx::query(
            "INSERT INTO analytics_active_programs (program_id, transaction_count, unique_wallets, last_seen) 
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (program_id) DO UPDATE SET 
                transaction_count = EXCLUDED.transaction_count,
                unique_wallets = EXCLUDED.unique_wallets,
                last_seen = EXCLUDED.last_seen,
                updated_at = NOW()"
        )
        .bind(row.get::<String, _>(0))
        .bind(row.get::<i64, _>(1))
        .bind(row.get::<i64, _>(2))
        .bind(row.get::<DateTime<Utc>, _>(3))
        .execute(pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to insert program: {}", e)))?;
    }

    Ok(())
}

async fn compute_and_store_token_transfers(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM analytics_token_transfers").execute(pool).await.ok();
    sqlx::query("DELETE FROM analytics_top_tokens").execute(pool).await.ok();

    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM fact_transactions WHERE event_type = 'token_transfer'"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let unique_tokens: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT raw_payload->'mint') 
         FROM fact_transactions 
         WHERE event_type = 'token_transfer'"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let unique_senders: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT raw_payload->'from') 
         FROM fact_transactions 
         WHERE event_type = 'token_transfer'"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let unique_receivers: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT raw_payload->'to') 
         FROM fact_transactions 
         WHERE event_type = 'token_transfer'"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    sqlx::query(
        "INSERT INTO analytics_token_transfers (total_transfers, unique_tokens, unique_senders, unique_receivers) 
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE SET 
            total_transfers = EXCLUDED.total_transfers,
            unique_tokens = EXCLUDED.unique_tokens,
            unique_senders = EXCLUDED.unique_senders,
            unique_receivers = EXCLUDED.unique_receivers,
            updated_at = NOW()"
    )
    .bind(total)
    .bind(unique_tokens)
    .bind(unique_senders)
    .bind(unique_receivers)
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to insert token transfers: {}", e)))?;

    // Top tokens
    let token_rows = sqlx::query(
        "SELECT 
            raw_payload->>'mint' as token_mint,
            COUNT(*)::bigint as transfer_count,
            COUNT(DISTINCT raw_payload->'to')::bigint as unique_wallets
         FROM fact_transactions 
         WHERE event_type = 'token_transfer'
         AND raw_payload->>'mint' IS NOT NULL
         GROUP BY raw_payload->>'mint'
         ORDER BY transfer_count DESC
         LIMIT 20"
    )
    .fetch_all(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute top tokens: {}", e)))?;

    for row in token_rows {
        sqlx::query(
            "INSERT INTO analytics_top_tokens (token_mint, transfer_count, unique_wallets) 
             VALUES ($1, $2, $3)
             ON CONFLICT (token_mint) DO UPDATE SET 
                transfer_count = EXCLUDED.transfer_count,
                unique_wallets = EXCLUDED.unique_wallets,
                updated_at = NOW()"
        )
        .bind(row.get::<Option<String>, _>(0).unwrap_or_else(|| "unknown".to_string()))
        .bind(row.get::<i64, _>(1))
        .bind(row.get::<i64, _>(2))
        .execute(pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to insert token: {}", e)))?;
    }

    Ok(())
}

async fn compute_and_store_failed_transactions(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM analytics_failed_transactions").execute(pool).await.ok();
    sqlx::query("DELETE FROM analytics_top_errors").execute(pool).await.ok();

    let total_failed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM fact_transactions 
         WHERE event_type = 'transaction' 
         AND raw_payload->'meta'->'err' IS NOT NULL"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM fact_transactions WHERE event_type = 'transaction'"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(1);

    let failure_rate = if total > 0 {
        (total_failed as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    sqlx::query(
        "INSERT INTO analytics_failed_transactions (total_failed, failure_rate) 
         VALUES ($1, $2)
         ON CONFLICT (id) DO UPDATE SET 
            total_failed = EXCLUDED.total_failed,
            failure_rate = EXCLUDED.failure_rate,
            updated_at = NOW()"
    )
    .bind(total_failed)
    .bind(failure_rate)
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to insert failed transactions: {}", e)))?;

    // Top errors
    let error_rows = sqlx::query(
        "SELECT 
            COALESCE(raw_payload->'meta'->'err'->>'type', 'unknown') as error_type,
            COUNT(*)::bigint as count
         FROM fact_transactions 
         WHERE event_type = 'transaction' 
         AND raw_payload->'meta'->'err' IS NOT NULL
         GROUP BY raw_payload->'meta'->'err'->>'type'
         ORDER BY count DESC
         LIMIT 10"
    )
    .fetch_all(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute errors: {}", e)))?;

    for row in error_rows {
        sqlx::query(
            "INSERT INTO analytics_top_errors (error_type, error_count) 
             VALUES ($1, $2)
             ON CONFLICT (error_type) DO UPDATE SET 
                error_count = EXCLUDED.error_count,
                updated_at = NOW()"
        )
        .bind(row.get::<String, _>(0))
        .bind(row.get::<i64, _>(1))
        .execute(pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to insert error: {}", e)))?;
    }

    Ok(())
}

async fn compute_and_store_wallet_activity(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM analytics_wallet_activity").execute(pool).await.ok();
    sqlx::query("DELETE FROM analytics_top_wallets").execute(pool).await.ok();

    let total_unique: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT (raw_payload->'transaction'->'message'->'accountKeys'->>0)) 
         FROM fact_transactions
         WHERE raw_payload->'transaction'->'message'->'accountKeys'->>0 IS NOT NULL"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let active_today: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT (raw_payload->'transaction'->'message'->'accountKeys'->>0)) 
         FROM fact_transactions 
         WHERE DATE(block_time) = CURRENT_DATE
         AND raw_payload->'transaction'->'message'->'accountKeys'->>0 IS NOT NULL"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let active_week: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT (raw_payload->'transaction'->'message'->'accountKeys'->>0)) 
         FROM fact_transactions 
         WHERE block_time >= CURRENT_DATE - INTERVAL '7 days'
         AND raw_payload->'transaction'->'message'->'accountKeys'->>0 IS NOT NULL"
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    sqlx::query(
        "INSERT INTO analytics_wallet_activity (total_unique_wallets, active_today, active_this_week) 
         VALUES ($1, $2, $3)
         ON CONFLICT (id) DO UPDATE SET 
            total_unique_wallets = EXCLUDED.total_unique_wallets,
            active_today = EXCLUDED.active_today,
            active_this_week = EXCLUDED.active_this_week,
            updated_at = NOW()"
    )
    .bind(total_unique)
    .bind(active_today)
    .bind(active_week)
    .execute(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to insert wallet activity: {}", e)))?;

    // Top wallets
    let wallet_rows = sqlx::query(
        "SELECT 
            raw_payload->'transaction'->'message'->'accountKeys'->>0 as wallet,
            COUNT(*)::bigint as tx_count,
            MIN(block_time::timestamptz) as first_seen,
            MAX(block_time::timestamptz) as last_seen
         FROM fact_transactions 
         WHERE raw_payload->'transaction'->'message'->'accountKeys'->>0 IS NOT NULL
         GROUP BY raw_payload->'transaction'->'message'->'accountKeys'->>0
         ORDER BY tx_count DESC
         LIMIT 20"
    )
    .fetch_all(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute wallet activity: {}", e)))?;

    for row in wallet_rows {
        sqlx::query(
            "INSERT INTO analytics_top_wallets (wallet, transaction_count, first_seen, last_seen) 
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (wallet) DO UPDATE SET 
                transaction_count = EXCLUDED.transaction_count,
                first_seen = EXCLUDED.first_seen,
                last_seen = EXCLUDED.last_seen,
                updated_at = NOW()"
        )
        .bind(row.get::<String, _>(0))
        .bind(row.get::<i64, _>(1))
        .bind(row.get::<DateTime<Utc>, _>(2))
        .bind(row.get::<DateTime<Utc>, _>(3))
        .execute(pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to insert wallet: {}", e)))?;
    }

    Ok(())
}

async fn compute_and_store_program_trends(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM analytics_program_trends").execute(pool).await.ok();

    // Get top 10 programs
    let program_rows = sqlx::query(
        "SELECT program_id, COUNT(*)::bigint as tx_count
         FROM fact_transactions 
         WHERE program_id IS NOT NULL 
         AND event_type = 'program_instruction'
         GROUP BY program_id
         ORDER BY tx_count DESC
         LIMIT 10"
    )
    .fetch_all(pool)
    .await
    .map_err(|e| ETLError::Database(format!("Failed to compute program trends: {}", e)))?;

    for row in program_rows {
        let program_id: String = row.get(0);

        // Get daily volume for this program
        let daily_rows = sqlx::query(
            "SELECT 
                DATE(block_time) as date,
                COUNT(*)::bigint as count
             FROM fact_transactions 
             WHERE program_id = $1 
             AND event_type = 'program_instruction'
             AND block_time >= CURRENT_DATE - INTERVAL '30 days'
             GROUP BY DATE(block_time)
             ORDER BY date"
        )
        .bind(&program_id)
        .fetch_all(pool)
        .await
        .map_err(|e| ETLError::Database(format!("Failed to compute daily volume: {}", e)))?;

        for daily_row in daily_rows {
            sqlx::query(
                "INSERT INTO analytics_program_trends (program_id, date, transaction_count) 
                 VALUES ($1, $2, $3)
                 ON CONFLICT (program_id, date) DO UPDATE SET 
                    transaction_count = EXCLUDED.transaction_count,
                    updated_at = NOW()"
            )
            .bind(&program_id)
            .bind(daily_row.get::<NaiveDate, _>(0))
            .bind(daily_row.get::<i64, _>(1))
            .execute(pool)
            .await
            .map_err(|e| ETLError::Database(format!("Failed to insert trend: {}", e)))?;
        }
    }

    Ok(())
}
