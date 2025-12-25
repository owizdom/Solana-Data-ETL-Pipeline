use crate::config::Config;
use crate::error::{ETLError, Result};
use crate::parsers::{flatten_instructions, parse_block};
use crate::rpc::AlchemyRPCClient;
use crate::warehouse::Warehouse;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{info, warn};

/// Run backfill for slot range
pub async fn run_backfill(
    config: Config,
    start_slot: u64,
    end_slot: u64,
    workers: usize,
) -> Result<()> {
    info!("Starting backfill from slot {} to {} with {} workers", start_slot, end_slot, workers);

    let rpc_client = Arc::new(AlchemyRPCClient::new(config.alchemy.clone()));

    // Divide slot range into chunks
    let chunk_size = config.etl.backfill_chunk_size;
    let chunks: Vec<(u64, u64)> = (start_slot..end_slot)
        .step_by(chunk_size as usize)
        .map(|start| {
            let end = std::cmp::min(start + chunk_size, end_slot);
            (start, end)
        })
        .collect();

    info!("Split into {} chunks", chunks.len());

    // Process chunks in parallel with semaphore for rate limiting
    let semaphore = Arc::new(Semaphore::new(workers));
    let mut handles = Vec::new();

    for (chunk_start, chunk_end) in chunks {
        let permit = semaphore.clone().acquire_owned().await
            .map_err(|e| ETLError::Generic(anyhow::anyhow!("Semaphore acquire error: {}", e)))?;
        let rpc = rpc_client.clone();
        let warehouse_config = config.warehouse.clone();
        let config_clone = config.clone();

        let handle = tokio::spawn(async move {
            let _permit = permit;
            let wh = crate::warehouse::create_warehouse(warehouse_config)
                .expect("Failed to create warehouse - check your WAREHOUSE_CONNECTION or WAREHOUSE_TYPE config");
            wh.connect().await.expect("Failed to connect to warehouse");
            match process_chunk(rpc, &*wh, config_clone, chunk_start, chunk_end).await {
                Ok(_) => {
                    info!("Completed chunk {}-{}", chunk_start, chunk_end);
                }
                Err(e) => {
                    warn!("Failed chunk {}-{}: {}", chunk_start, chunk_end, e);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all chunks to complete
    for handle in handles {
        handle.await.map_err(|e| ETLError::Generic(anyhow::anyhow!("Join error: {}", e)))?;
    }

    info!("Backfill completed");
    Ok(())
}

/// Process a single chunk of slots
async fn process_chunk(
    rpc_client: Arc<AlchemyRPCClient>,
    warehouse: &dyn Warehouse,
    config: Config,
    start_slot: u64,
    end_slot: u64,
) -> Result<()> {
    let mut slot = start_slot;
    let mut batch = Vec::new();

    while slot < end_slot {
        // Check if already processed
        if warehouse.is_slot_processed(slot).await? {
            slot += 1;
            continue;
        }

        // Fetch block
        match rpc_client.get_block(slot, None).await? {
            Some(block) => {
                // Parse block into events
                match parse_block(&block, slot) {
                    Ok(mut events) => {
                        // Flatten instructions
                        events = flatten_instructions(events);
                        batch.extend(events);

                        // Batch insert when batch size reached
                        if batch.len() >= config.etl.batch_size {
                            warehouse.insert_events(batch.clone()).await?;
                            batch.clear();
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse block at slot {}: {}", slot, e);
                        // Continue to next slot
                    }
                }
            }
            None => {
                warn!("Block not found at slot {} (skipping)", slot);
            }
        }

        slot += 1;

        // Checkpoint periodically
        if (slot - start_slot) % config.etl.checkpoint_interval == 0 {
            if !batch.is_empty() {
                warehouse.insert_events(batch.clone()).await?;
                batch.clear();
            }
            warehouse.update_last_slot(slot - 1).await?;
            info!("Checkpoint at slot {}", slot - 1);
        }
    }

    // Insert remaining batch
    if !batch.is_empty() {
        warehouse.insert_events(batch).await?;
    }

    // Final checkpoint
    warehouse.update_last_slot(end_slot - 1).await?;

    Ok(())
}

