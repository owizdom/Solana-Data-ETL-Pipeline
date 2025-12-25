use crate::config::Config;
use crate::error::Result;
use crate::parsers::{flatten_instructions, parse_block};
use crate::rpc::AlchemyRPCClient;
use crate::warehouse::Warehouse;
use std::time::Duration;
use tracing::{info, warn};

/// Run incremental loader
pub async fn run_incremental(config: Config, interval_seconds: u64) -> Result<()> {
    info!("Starting incremental loader with {}s interval", interval_seconds);

    let rpc_client = AlchemyRPCClient::new(config.alchemy.clone());
    let warehouse = crate::warehouse::create_warehouse(config.warehouse.clone())?;
    warehouse.connect().await?;

    let interval = Duration::from_secs(interval_seconds);

    loop {
        match process_incremental(&rpc_client, &*warehouse, &config).await {
            Ok(_) => {
                info!("Incremental run completed");
            }
            Err(e) => {
                warn!("Incremental run failed: {}", e);
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// Process incremental update (new slots since last processed)
async fn process_incremental(
    rpc_client: &AlchemyRPCClient,
    warehouse: &dyn Warehouse,
    config: &Config,
) -> Result<()> {
    // Get current chain tip
    let chain_tip = rpc_client.get_slot().await?;

    // Get last processed slot
    let last_slot = warehouse.get_last_slot().await?.unwrap_or(0);

    if chain_tip <= last_slot {
        info!("No new slots (tip: {}, last: {})", chain_tip, last_slot);
        return Ok(());
    }

    let start_slot = last_slot + 1;
    let end_slot = chain_tip + 1; // Exclusive end

    info!("Processing slots {} to {} ({} slots)", start_slot, end_slot, end_slot - start_slot);

    let mut batch = Vec::new();
    let mut processed_slot = start_slot;

    // Process slots in order (important for incremental)
    while processed_slot < end_slot {
        match rpc_client.get_block(processed_slot, None).await? {
            Some(block) => {
                match parse_block(&block, processed_slot) {
                    Ok(mut events) => {
                        events = flatten_instructions(events);
                        batch.extend(events);

                        // Batch insert periodically
                        if batch.len() >= config.etl.batch_size {
                            warehouse.insert_events(batch.clone()).await?;
                            batch.clear();
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse block at slot {}: {}", processed_slot, e);
                    }
                }
            }
            None => {
                warn!("Block not found at slot {} (may be skipped slot)", processed_slot);
            }
        }

        processed_slot += 1;

        // Update checkpoint periodically
        if (processed_slot - start_slot) % config.etl.checkpoint_interval == 0 {
            if !batch.is_empty() {
                warehouse.insert_events(batch.clone()).await?;
                batch.clear();
            }
            warehouse.update_last_slot(processed_slot - 1).await?;
        }
    }

    // Insert remaining batch
    if !batch.is_empty() {
        warehouse.insert_events(batch).await?;
    }

    // Update to chain tip
    warehouse.update_last_slot(chain_tip).await?;

    info!("Processed up to slot {}", chain_tip);
    Ok(())
}

