use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};
use alloy::primitives::{TxHash, U256};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use strum::{AsRefStr, Display, EnumString};
use utoipa::ToSchema;

#[derive(Debug, Clone, Eq, Hash, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct TokenConfig {
    pub symbol: String,
    pub contract: String,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChainConfig {
    pub name: String,
    pub rpc_url: String,
    pub chain_type: ChainType,
    pub xpub: String,
    pub native_symbol: String,
    pub decimals: u8,
    pub last_processed_block: u64,
    pub block_lag: u8,
    pub required_confirmations: u64,

    #[schema(ignore)]
    #[serde(skip)]
    pub watch_addresses: Arc<RwLock<HashSet<String>>>,

    #[schema(ignore)]
    #[serde(skip)]
    pub tokens: Arc<RwLock<HashSet<TokenConfig>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Payment {
    pub id: String,
    pub invoice_id: String,
    pub from: String,
    pub to: String,
    pub network: String,
    pub tx_hash: String,
    #[schema(value_type = String, example = "1000000000000000000")]
    pub amount_raw: U256,
    pub block_number: u64,
    pub status: PaymentStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, ToSchema,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ChainType {
    EVM
}

#[derive(Debug, Clone)]
pub struct PaymentEvent {
    pub network: String,
    pub tx_hash: TxHash,
    pub from: String,
    pub to: String,
    pub token: String,
    pub amount: String,
    pub amount_raw: U256,
    pub decimals: u8,
    pub block_number: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, ToSchema,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "PascalCase")]
pub enum InvoiceStatus {
    Pending,
    Paid,
    Expired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, ToSchema,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "PascalCase")]
pub enum PaymentStatus {
    Confirming,
    Confirmed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct Invoice {
    pub id: String,
    pub address_index: u32,
    pub address: String,
    pub amount: String,
    #[schema(value_type = String, example = "1000000000000000000")]
    pub amount_raw: U256,
    pub paid: String,
    #[schema(value_type = String, example = "0")]
    pub paid_raw: U256,
    pub token: String,
    pub network: String,
    pub decimals: u8,
    pub webhook_url: Option<String>,
    pub webhook_secret: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub status: InvoiceStatus,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct PartialChainUpdate {
    pub rpc_url: Option<String>,
    pub last_processed_block: Option<u64>,
    pub xpub: Option<String>,
    pub block_lag: Option<u8>,
    pub required_confirmations: Option<u64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct WebhookJob {
    pub id: uuid::Uuid,
    pub url: String,
    pub secret_key: String,
    pub payload: Json<WebhookEvent>,
    pub attempts: i32,
    pub max_retries: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema,
    Display, EnumString, AsRefStr)]
#[serde(tag = "event_type", content = "data", rename_all = "snake_case")]
pub enum WebhookEvent {
    TxDetected {
        invoice_id: String,
        tx_hash: String,
        amount: String,
        currency: String,
    },
    TxConfirmed {
        invoice_id: String,
        tx_hash: String,
        confirmations: u64,
    },
    InvoicePaid {
        invoice_id: String,
        paid_amount: String,
    },
    InvoiceExpired {
        invoice_id: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, ToSchema,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "PascalCase")]
pub enum WebhookStatus {
    Pending,
    Processing,
    Sent,
    Failed
}