use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Canonical event model - base fields shared by all events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub event_id: String,
    pub slot: u64,
    pub block_time: DateTime<Utc>,
    pub tx_signature: String,
    pub program_id: Option<String>,
    pub instruction_index: i32,
    pub event_type: String,
    pub raw_payload: Value,
}

/// Transaction event with denormalized fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEvent {
    #[serde(flatten)]
    pub base: CanonicalEvent,
    pub wallet: Option<String>,
    pub wallet_secondary: Option<String>,
    pub token_mint: Option<String>,
    pub lamports: Option<i64>,
    pub token_amount: Option<String>, // Use string for precision
    pub fee_payer: Option<String>,
    pub transaction_fee: Option<u64>,
    pub success: Option<bool>,
    pub error_message: Option<String>,
}

/// Program event extracted from instructions/logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramEvent {
    #[serde(flatten)]
    pub base: CanonicalEvent,
    pub instruction_type: Option<String>,
    pub accounts: Vec<String>,
    pub data_hex: Option<String>,
    pub log_messages: Vec<String>,
    pub log_pattern_match: Option<String>,
}

/// Token transfer event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenTransferEvent {
    #[serde(flatten)]
    pub base: CanonicalEvent,
    pub token_mint: String,
    pub from_wallet: Option<String>,
    pub to_wallet: String,
    pub token_amount: String, // Normalized amount
    pub decimals: Option<u8>,
    pub raw_amount: Option<String>,
    pub authority: Option<String>,
}

/// Telemetry event (API usage, feature usage, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    #[serde(flatten)]
    pub base: CanonicalEvent,
    pub user_id: Option<String>,
    pub api_endpoint: Option<String>,
    pub feature_name: Option<String>,
    pub request_id: Option<String>,
    pub response_code: Option<u16>,
    pub latency_ms: Option<u64>,
}

impl CanonicalEvent {
    /// Generate deterministic event_id
    pub fn generate_event_id(
        slot: u64,
        tx_signature: &str,
        instruction_index: i32,
        event_type: &str,
    ) -> String {
        let input = format!("{}:{}:{}:{}", slot, tx_signature, instruction_index, event_type);
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn new(
        slot: u64,
        block_time: DateTime<Utc>,
        tx_signature: String,
        program_id: Option<String>,
        instruction_index: i32,
        event_type: String,
        raw_payload: Value,
    ) -> Self {
        let event_id = Self::generate_event_id(slot, &tx_signature, instruction_index, &event_type);

        Self {
            event_id,
            slot,
            block_time,
            tx_signature,
            program_id,
            instruction_index,
            event_type,
            raw_payload,
        }
    }
}

