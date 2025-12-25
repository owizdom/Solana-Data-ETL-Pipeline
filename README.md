# Solana Data ETL Pipeline

A production-grade Solana data pipeline for ingesting on-chain data into Postgres, enabling analysis of transaction volume over time, token transfers, failed transactions, wallet activity patterns, program usage trends, and the most active programs such as DEXs and NFT markets.

<img width="1920" height="1080" alt="Screenshot 2025-12-25 at 09 43 18 (2)" src="https://github.com/user-attachments/assets/4f493390-25c4-4eea-a56a-02ce5343ce51" />

#<img width="719" height="453" alt="Screenshot 2025-12-25 at 09 43 12" src="https://github.com/user-attachments/assets/f50c8c0c-776b-47ce-ace3-6d49e71fa68b" />

<img width="414" height="1074" alt="Screenshot 2025-12-25 at 10 15 08" src="https://github.com/user-attachments/assets/973c0132-e427-4679-9749-2ec173fbbc50" />

# Quick Start

### 1. Set up Postgres

```bash
brew install postgresql
brew services start postgresql
createdb solana_etl
```

### 2. Configure Environment

```bash
export WAREHOUSE_TYPE=postgres
export WAREHOUSE_CONNECTION="postgresql://localhost/solana_etl"
```

### 3. Build & Run

```bash
# Build
cargo build --release

# Test connection
cargo run --release -- health

# Backfill historical data
cargo run --release -- backfill --start-slot 280000000 --end-slot 280001000 --workers 2

# Run incremental loader (real-time)
cargo run --release -- incremental --interval 30

# Generate analytics report
cargo run --release -- analytics
```

### 4. View Data

```bash
# Command line
psql solana_etl -c "SELECT COUNT(*) FROM fact_transactions;"

# Or use pgAdmin4 to browse visually
```

## Commands

- `health` - Check RPC and database connectivity
- `backfill --start-slot X --end-slot Y --workers N` - Backfill historical slots
- `incremental --interval N` - Run continuous incremental loader (N = seconds between runs)
- `analytics` - Generate analytics report with:
  - Transaction volume over time
  - Most active programs (DEXs, NFT markets, etc.)
  - Token transfer statistics
  - Failed transactions and errors
  - Wallet activity patterns
  - Program usage trends

## Database Schema

See `docs/SCHEMA.md` for complete schema documentation.

Main tables:
- `fact_transactions` - All transaction events
- `etl_metadata` - Pipeline state (last processed slot, etc.)

## Docker

```bash
# Build image
docker build -t solana-etl .

# Run with docker-compose
docker-compose up -d
```

## Configuration

Set via environment variables:
- `ALCHEMY_RPC_URL` - Your Alchemy RPC endpoint (defaults to hardcoded endpoint)
- `WAREHOUSE_TYPE` - `postgres` or `bigquery` (default: `postgres`)
- `WAREHOUSE_CONNECTION` - Postgres connection string
- `ETL_BATCH_SIZE` - Events per batch insert (default: 1000)
- `ETL_INTERVAL_SECONDS` - Incremental loader interval (default: 30)
