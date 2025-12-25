# Canonical Event Schema

## Design Principles

1. **Minimize Joins**: Denormalize frequently queried fields
2. **Append-Only**: Immutable event log for auditability
3. **Flexible Payload**: JSON field preserves all raw data
4. **Deterministic IDs**: Enable idempotent replay
5. **Time-Based Partitioning**: Optimize for time-range queries

## Base Event Model

All events share these core fields:

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | STRING | Deterministic hash: `sha256(slot:tx_signature:instruction_index:event_type)` |
| `slot` | INT64 | Solana slot number |
| `block_time` | TIMESTAMP | Unix timestamp of block |
| `tx_signature` | STRING | Transaction signature (base58) |
| `program_id` | STRING | Program ID that emitted event |
| `instruction_index` | INT64 | Index of instruction within transaction |
| `event_type` | STRING | Type of event (see below) |
| `raw_payload` | JSON | Complete raw event data |
| `created_at` | TIMESTAMP | Pipeline insertion timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp (for upserts) |

## Event Types

- `transaction`: Base transaction event
- `instruction`: Program instruction execution
- `log`: Program log message
- `token_transfer`: SPL token transfer
- `lamports_transfer`: SOL transfer
- `program_instruction`: Specific program instruction
- `telemetry_api_call`: API usage telemetry
- `telemetry_feature_usage`: Product feature usage

## Fact Tables

### fact_transactions

Core transaction events with denormalized fields.

```sql
CREATE TABLE fact_transactions (
    -- Base fields
    event_id STRING NOT NULL,
    slot INT64 NOT NULL,
    block_time TIMESTAMP NOT NULL,
    tx_signature STRING NOT NULL,
    program_id STRING,
    instruction_index INT64 NOT NULL,
    event_type STRING NOT NULL,
    
    -- Denormalized fields (reduces joins)
    wallet STRING,  -- Primary wallet (first signer)
    wallet_secondary STRING,  -- Secondary wallet (if transfer)
    token_mint STRING,  -- Token mint (if token event)
    lamports INT64,  -- SOL amount transferred
    token_amount NUMERIC,  -- Token amount (if token event)
    fee_payer STRING,  -- Fee payer wallet
    transaction_fee INT64,  -- Transaction fee in lamports
    
    -- Computed fields
    success BOOLEAN,  -- Transaction succeeded
    error_message STRING,  -- Error message if failed
    
    -- Raw data
    raw_payload JSON,
    
    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(block_time)
CLUSTER BY slot, program_id, wallet;
```

**Indexes:**
- Primary key: `event_id`
- Unique constraint: `(slot, tx_signature, instruction_index, event_type)`

### fact_program_events

Program-specific events extracted from instructions and logs.

```sql
CREATE TABLE fact_program_events (
    -- Base fields
    event_id STRING NOT NULL,
    slot INT64 NOT NULL,
    block_time TIMESTAMP NOT NULL,
    tx_signature STRING NOT NULL,
    program_id STRING NOT NULL,
    instruction_index INT64 NOT NULL,
    event_type STRING NOT NULL,
    
    -- Program-specific fields
    instruction_type STRING,  -- e.g., "transfer", "swap", "mint"
    accounts ARRAY<STRING>,  -- Account addresses involved
    data_hex STRING,  -- Instruction data (hex)
    
    -- Log parsing
    log_messages ARRAY<STRING>,  -- All log messages from instruction
    log_pattern_match STRING,  -- Matched log pattern (e.g., "Program log: Transfer")
    
    -- Raw data
    raw_payload JSON,
    
    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(block_time)
CLUSTER BY slot, program_id;
```

### fact_token_transfers

SPL token transfers with normalized amounts.

```sql
CREATE TABLE fact_token_transfers (
    -- Base fields
    event_id STRING NOT NULL,
    slot INT64 NOT NULL,
    block_time TIMESTAMP NOT NULL,
    tx_signature STRING NOT NULL,
    program_id STRING NOT NULL,
    instruction_index INT64 NOT NULL,
    event_type STRING NOT NULL DEFAULT 'token_transfer',
    
    -- Transfer-specific fields
    token_mint STRING NOT NULL,
    from_wallet STRING,
    to_wallet STRING NOT NULL,
    token_amount NUMERIC NOT NULL,  -- Normalized amount (accounts for decimals)
    decimals INT64,  -- Token decimals
    raw_amount STRING,  -- Raw amount from chain (for verification)
    
    -- Authority
    authority STRING,  -- Account that authorized transfer
    
    -- Raw data
    raw_payload JSON,
    
    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(block_time)
CLUSTER BY slot, token_mint, from_wallet, to_wallet;
```

### fact_telemetry

Product telemetry events (API usage, feature usage, webhooks).

```sql
CREATE TABLE fact_telemetry (
    -- Base fields
    event_id STRING NOT NULL,
    slot INT64,  -- Optional: link to on-chain event
    block_time TIMESTAMP NOT NULL,  -- Event timestamp
    tx_signature STRING,  -- Optional: related transaction
    program_id STRING,  -- Optional: related program
    instruction_index INT64,
    event_type STRING NOT NULL,  -- 'telemetry_api_call', 'telemetry_feature_usage', etc.
    
    -- Telemetry-specific fields
    user_id STRING,  -- User identifier
    api_endpoint STRING,  -- API endpoint called
    feature_name STRING,  -- Feature used
    request_id STRING,  -- Request identifier
    response_code INT64,  -- HTTP response code
    latency_ms INT64,  -- Request latency
    
    -- Raw data
    raw_payload JSON,
    
    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(block_time)
CLUSTER BY event_type, user_id;
```

## Dimension Tables

### dim_wallets

Wallet metadata and computed aggregates.

```sql
CREATE TABLE dim_wallets (
    wallet STRING NOT NULL,
    first_seen_slot INT64 NOT NULL,
    first_seen_time TIMESTAMP NOT NULL,
    last_seen_slot INT64 NOT NULL,
    last_seen_time TIMESTAMP NOT NULL,
    
    -- Computed aggregates (updated via materialized views or scheduled jobs)
    total_transactions INT64,
    total_sol_sent NUMERIC,
    total_sol_received NUMERIC,
    total_tokens_sent INT64,  -- Count of token transfers sent
    total_tokens_received INT64,  -- Count of token transfers received
    
    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    
    PRIMARY KEY (wallet)
)
CLUSTER BY wallet;
```

### dim_programs

Program metadata.

```sql
CREATE TABLE dim_programs (
    program_id STRING NOT NULL,
    program_name STRING,  -- Human-readable name (e.g., "Raydium", "Jupiter")
    program_type STRING,  -- 'DEX', 'NFT', 'Gaming', etc.
    first_seen_slot INT64 NOT NULL,
    first_seen_time TIMESTAMP NOT NULL,
    last_seen_slot INT64 NOT NULL,
    last_seen_time TIMESTAMP NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    
    PRIMARY KEY (program_id)
)
CLUSTER BY program_id;
```

### dim_tokens

Token metadata (SPL tokens).

```sql
CREATE TABLE dim_tokens (
    token_mint STRING NOT NULL,
    token_symbol STRING,
    token_name STRING,
    decimals INT64 NOT NULL,
    supply NUMERIC,  -- Total supply
    
    -- Metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    
    PRIMARY KEY (token_mint)
)
CLUSTER BY token_mint;
```

## Metadata Tables

### etl_metadata

Pipeline metadata for tracking state.

```sql
CREATE TABLE etl_metadata (
    key STRING NOT NULL PRIMARY KEY,
    value STRING NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Key values:
-- 'last_confirmed_slot': Last processed slot
-- 'last_backfill_slot': Last backfilled slot
-- 'chain_tip_slot': Current chain tip (from RPC)
```

### etl_checkpoints

Backfill progress tracking.

```sql
CREATE TABLE etl_checkpoints (
    checkpoint_id STRING NOT NULL,
    start_slot INT64 NOT NULL,
    end_slot INT64 NOT NULL,
    last_processed_slot INT64 NOT NULL,
    status STRING NOT NULL,  -- 'in_progress', 'completed', 'failed'
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    
    PRIMARY KEY (checkpoint_id)
)
CLUSTER BY start_slot;
```

### etl_errors

Error tracking for debugging.

```sql
CREATE TABLE etl_errors (
    error_id STRING NOT NULL,
    slot INT64,
    tx_signature STRING,
    error_type STRING NOT NULL,
    error_message STRING NOT NULL,
    error_context JSON,
    created_at TIMESTAMP NOT NULL,
    
    PRIMARY KEY (error_id)
)
PARTITION BY DATE(created_at);
```

## Schema Rationale

### Why This Schema Minimizes Joins

1. **Denormalized Core Fields**: `wallet`, `token_mint`, `program_id` stored directly in fact tables
2. **Single-Table Queries**: Most analytics queries can run on `fact_transactions` alone
3. **Clustered Columns**: Frequently queried fields are clustered for performance
4. **Array Fields**: Related data (accounts, logs) stored as arrays to avoid joins

### Example: Wallet Activity Query (0 joins)

```sql
-- Get wallet activity (no joins needed)
SELECT 
    wallet,
    COUNT(*) as tx_count,
    SUM(lamports) as total_lamports
FROM fact_transactions
WHERE wallet = '...'
  AND block_time >= TIMESTAMP('2024-01-01')
GROUP BY wallet;
```

### Example: Token Transfer Query (0 joins)

```sql
-- Get token transfers (no joins needed)
SELECT 
    token_mint,
    from_wallet,
    to_wallet,
    SUM(token_amount) as total_amount
FROM fact_token_transfers
WHERE block_time >= TIMESTAMP('2024-01-01')
GROUP BY token_mint, from_wallet, to_wallet;
```

### Example: Program Analysis (0-1 joins)

```sql
-- Program analysis with optional program name join
SELECT 
    t.program_id,
    COALESCE(p.program_name, t.program_id) as program_name,
    COUNT(*) as event_count
FROM fact_transactions t
LEFT JOIN dim_programs p ON t.program_id = p.program_id
WHERE t.block_time >= TIMESTAMP('2024-01-01')
GROUP BY t.program_id, p.program_name;
```

## Flexibility via JSON

The `raw_payload` JSON field preserves all original data, enabling:

1. **Schema Evolution**: New fields can be queried without table migrations
2. **Debugging**: Complete event context available
3. **Custom Parsing**: Analysts can extract custom fields as needed
4. **Audit Trail**: Full event reconstruction possible

Example JSON query:

```sql
SELECT 
    event_id,
    JSON_EXTRACT_SCALAR(raw_payload, '$.meta.err') as error_details
FROM fact_transactions
WHERE JSON_EXTRACT_SCALAR(raw_payload, '$.meta.err') IS NOT NULL;
```

