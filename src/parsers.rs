use crate::events::CanonicalEvent;
use crate::error::{ETLError, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// Parse a Solana block into canonical events
pub fn parse_block(block: &Value, slot: u64) -> Result<Vec<CanonicalEvent>> {
    let block_time = extract_block_time(block)?;
    let transactions = block
        .get("transactions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ETLError::Parse("Missing transactions array".to_string()))?;

    let mut events = Vec::new();

    for (tx_idx, tx) in transactions.iter().enumerate() {
        match parse_transaction(tx, slot, block_time, tx_idx) {
            Ok(mut tx_events) => events.append(&mut tx_events),
            Err(e) => {
                tracing::warn!("Failed to parse transaction {}: {}", tx_idx, e);
                // Continue processing other transactions
            }
        }
    }

    Ok(events)
}

/// Extract block timestamp
fn extract_block_time(block: &Value) -> Result<DateTime<Utc>> {
    let timestamp = block
        .get("blockTime")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| ETLError::Parse("Missing blockTime".to_string()))?;

    DateTime::from_timestamp(timestamp, 0)
        .ok_or_else(|| ETLError::Parse(format!("Invalid timestamp: {}", timestamp)))
}

/// Parse a single transaction into events
fn parse_transaction(
    tx: &Value,
    slot: u64,
    block_time: DateTime<Utc>,
    _tx_idx: usize,
) -> Result<Vec<CanonicalEvent>> {
    let meta = tx
        .get("meta")
        .ok_or_else(|| ETLError::Parse("Missing transaction meta".to_string()))?;

    let tx_data = tx
        .get("transaction")
        .ok_or_else(|| ETLError::Parse("Missing transaction data".to_string()))?;

    let signature = extract_signature(tx_data)?;
    let _success = meta
        .get("err")
        .map(|v| v.is_null())
        .unwrap_or(false);

    let instructions = extract_instructions(tx_data)?;
    let mut events = Vec::new();

    // Create base transaction event
    let base_event = CanonicalEvent::new(
        slot,
        block_time,
        signature.clone(),
        None,
        -1, // Transaction-level event
        "transaction".to_string(),
        tx.clone(),
    );
    events.push(base_event);

    // Parse each instruction
    for (inst_idx, instruction) in instructions.iter().enumerate() {
        match parse_instruction(instruction, slot, block_time, &signature, inst_idx as i32) {
            Ok(inst_events) => events.extend(inst_events),
            Err(e) => {
                tracing::warn!(
                    "Failed to parse instruction {} in tx {}: {}",
                    inst_idx,
                    signature,
                    e
                );
            }
        }
    }

    // Extract token transfers from meta
    if let Ok(transfers) = extract_token_transfers(meta, slot, block_time, &signature) {
        events.extend(transfers);
    }

    Ok(events)
}

/// Extract transaction signature
fn extract_signature(tx: &Value) -> Result<String> {
    tx.get("signatures")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| ETLError::Parse("Missing transaction signature".to_string()))
}

/// Extract instructions from transaction
fn extract_instructions(tx: &Value) -> Result<Vec<Value>> {
    tx.get("message")
        .and_then(|m| m.get("instructions"))
        .and_then(|v| v.as_array())
        .cloned()
        .ok_or_else(|| ETLError::Parse("Missing instructions".to_string()))
}

/// Parse an instruction into events
fn parse_instruction(
    instruction: &Value,
    slot: u64,
    block_time: DateTime<Utc>,
    tx_signature: &str,
    instruction_index: i32,
) -> Result<Vec<CanonicalEvent>> {
    let program_id = instruction
        .get("programId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let program_id_str = program_id.as_deref().unwrap_or("unknown");

    // Determine instruction type based on program
    let event_type = if program_id_str == TOKEN_PROGRAM_ID || program_id_str == TOKEN_2022_PROGRAM_ID {
        "token_instruction".to_string()
    } else {
        "program_instruction".to_string()
    };

    let base_event = CanonicalEvent::new(
        slot,
        block_time,
        tx_signature.to_string(),
        program_id.clone(),
        instruction_index,
        event_type,
        instruction.clone(),
    );

    let mut events = vec![base_event];

    // Extract log messages if available
    // Note: Logs are typically in transaction meta, not instruction
    // This is a simplified version

    Ok(events)
}

/// Extract token transfers from transaction meta
fn extract_token_transfers(
    meta: &Value,
    slot: u64,
    block_time: DateTime<Utc>,
    tx_signature: &str,
) -> Result<Vec<CanonicalEvent>> {
    let _pre_token_balances = meta
        .get("preTokenBalances")
        .and_then(|v| v.as_array());

    let empty_vec: Vec<Value> = Vec::new();
    let post_token_balances = meta
        .get("postTokenBalances")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty_vec);

    // This is simplified - full implementation would:
    // 1. Match pre/post balances by account
    // 2. Calculate net transfers
    // 3. Extract mint, amounts, decimals

    let mut events = Vec::new();

    // For now, create events for each balance change
    for (idx, post_balance) in post_token_balances.iter().enumerate() {
        if let Some(_mint) = post_balance.get("mint").and_then(|v| v.as_str()) {
            let event = CanonicalEvent::new(
                slot,
                block_time,
                tx_signature.to_string(),
                Some(TOKEN_PROGRAM_ID.to_string()),
                idx as i32,
                "token_transfer".to_string(),
                post_balance.clone(),
            );
            events.push(event);
        }
    }

    Ok(events)
}

/// Flatten instructions - expand into individual instruction events
pub fn flatten_instructions(events: Vec<CanonicalEvent>) -> Vec<CanonicalEvent> {
    let mut flattened = Vec::new();

    for event in events {
        if event.event_type == "transaction" {
            flattened.push(event);
        } else if event.event_type == "program_instruction" || event.event_type == "token_instruction" {
            // For now, just add the instruction event
            // Could expand inner instructions here if needed
            flattened.push(event);
        } else {
            flattened.push(event);
        }
    }

    flattened
}

/// Extract wallet addresses from transaction
pub fn extract_wallets(tx: &Value) -> Vec<String> {
    let mut wallets = Vec::new();

    if let Some(message) = tx.get("transaction").and_then(|t| t.get("message")) {
        // Extract account keys
        if let Some(account_keys) = message.get("accountKeys").and_then(|v| v.as_array()) {
            for key in account_keys {
                if let Some(addr) = key.as_str() {
                    wallets.push(addr.to_string());
                } else if let Some(addr) = key.get("pubkey").and_then(|v| v.as_str()) {
                    wallets.push(addr.to_string());
                }
            }
        }
    }

    wallets
}

