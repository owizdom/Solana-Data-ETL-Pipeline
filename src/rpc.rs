use crate::config::AlchemyConfig;
use crate::error::{ETLError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use governor::{Quota, RateLimiter, state::direct::NotKeyed, state::InMemoryState, clock::DefaultClock, middleware::NoOpMiddleware};
use std::num::NonZeroU32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RPCRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RPCResponse {
    jsonrpc: String,
    id: u64,
    result: Option<Value>,
    error: Option<RPCError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RPCError {
    code: i64,
    message: String,
    data: Option<Value>,
}

pub struct AlchemyRPCClient {
    config: AlchemyConfig,
    client: reqwest::Client,
    rate_limiter: RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>,
}

impl AlchemyRPCClient {
    pub fn new(config: AlchemyConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .no_proxy() // Disable system proxy detection to avoid system-configuration issues
            .danger_accept_invalid_certs(false) // Use proper cert validation
            .build()
            .expect("Failed to create HTTP client");

        let rate_limit = std::cmp::max(1, config.rate_limit_per_second);
        let quota = Quota::per_second(
            NonZeroU32::new(rate_limit).unwrap_or(NonZeroU32::new(1).unwrap())
        );
        let rate_limiter: RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware> = RateLimiter::direct(quota);

        Self {
            config,
            client,
            rate_limiter,
        }
    }

    async fn rpc_call(&self, method: &str, params: Value) -> Result<Value> {
        // Rate limit
        self.rate_limiter.until_ready().await;

        let request = RPCRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: method.to_string(),
            params,
        };

        let mut retries = 0;
        loop {
            let response = self
                .client
                .post(&self.config.rpc_url)
                .json(&request)
                .send()
                .await?;

            let rpc_response: RPCResponse = response.json().await?;

            if let Some(error) = rpc_response.error {
                // Rate limit or server error - retry
                if error.code == 429 || (error.code >= 500 && error.code < 600) {
                    if retries < self.config.max_retries {
                        let backoff = Duration::from_secs(2_u64.pow(retries));
                        tracing::warn!(
                            "RPC error {}, retrying in {:?} (attempt {}/{})",
                            error.message,
                            backoff,
                            retries + 1,
                            self.config.max_retries
                        );
                        sleep(backoff).await;
                        retries += 1;
                        continue;
                    }
                }
                return Err(ETLError::RPC(format!(
                    "RPC error {}: {}",
                    error.code, error.message
                )));
            }

            return Ok(rpc_response.result.unwrap_or(Value::Null));
        }
    }

    pub async fn get_slot(&self) -> Result<u64> {
        let result = self
            .rpc_call("getSlot", json!([{"commitment": "confirmed"}]))
            .await?;
        Ok(result.as_u64().ok_or_else(|| ETLError::RPC("Invalid slot response".to_string()))?)
    }

    pub async fn get_block(&self, slot: u64, encoding: Option<&str>) -> Result<Option<Value>> {
        let encoding = encoding.unwrap_or("jsonParsed");
        let params = json!([
            slot,
            {
                "encoding": encoding,
                "transactionDetails": "full",
                "rewards": false,
                "maxSupportedTransactionVersion": 0,
            }
        ]);

        let result = self.rpc_call("getBlock", params).await?;

        // Null means slot doesn't exist
        if result.is_null() {
            return Ok(None);
        }

        Ok(Some(result))
    }

    pub async fn get_transaction(
        &self,
        signature: &str,
        encoding: Option<&str>,
    ) -> Result<Option<Value>> {
        let encoding = encoding.unwrap_or("jsonParsed");
        let params = json!([
            signature,
            {
                "encoding": encoding,
                "maxSupportedTransactionVersion": 0,
            }
        ]);

        let result = self.rpc_call("getTransaction", params).await?;

        if result.is_null() {
            return Ok(None);
        }

        Ok(Some(result))
    }

    pub async fn get_signatures_for_address(
        &self,
        address: &str,
        limit: Option<u64>,
        before: Option<&str>,
        until: Option<&str>,
    ) -> Result<Vec<Value>> {
        let mut params_obj = json!({});
        if let Some(limit) = limit {
            params_obj["limit"] = json!(limit);
        }
        if let Some(before) = before {
            params_obj["before"] = json!(before);
        }
        if let Some(until) = until {
            params_obj["until"] = json!(until);
        }

        let params = json!([address, params_obj]);
        let result = self.rpc_call("getSignaturesForAddress", params).await?;

        match result.as_array() {
            Some(arr) => Ok(arr.clone()),
            None => Ok(vec![]),
        }
    }

    pub async fn get_program_accounts(
        &self,
        program_id: &str,
        encoding: Option<&str>,
        filters: Option<Value>,
    ) -> Result<Vec<Value>> {
        let encoding = encoding.unwrap_or("jsonParsed");
        let mut params_obj = json!({"encoding": encoding});
        if let Some(filters) = filters {
            params_obj["filters"] = filters;
        }

        let params = json!([program_id, params_obj]);
        let result = self.rpc_call("getProgramAccounts", params).await?;

        match result.as_array() {
            Some(arr) => Ok(arr.clone()),
            None => Ok(vec![]),
        }
    }

    pub async fn get_block_height(&self) -> Result<u64> {
        let result = self.rpc_call("getBlockHeight", json!([])).await?;
        Ok(result.as_u64().ok_or_else(|| ETLError::RPC("Invalid block height response".to_string()))?)
    }
}

