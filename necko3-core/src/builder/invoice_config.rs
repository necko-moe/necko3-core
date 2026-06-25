pub mod error;

use alloy_primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum PaymentAddress {
    UseExisting(String),
    GenerateNew,
    WithXpub { xpub: String, derivation_index: u32 },
}

#[derive(Serialize, Deserialize)]
pub enum PaymentAmount {
    Raw(U256),
    Human(String),
}

impl From<&str> for PaymentAmount {
    fn from(value: &str) -> Self {
        Self::Human(value.to_string())
    }
}

impl From<f64> for PaymentAmount {
    fn from(value: f64) -> Self {
        Self::Human(value.to_string())
    }
}

impl From<f32> for PaymentAmount {
    fn from(value: f32) -> Self {
        Self::Human(value.to_string())
    }
}

impl From<U256> for PaymentAmount {
    fn from(value: U256) -> Self {
        Self::Raw(value)
    }
}

#[derive(Default, Serialize, Deserialize)]
pub enum PaymentAsset {
    /// use native asset
    #[default]
    Native,
    /// token symbol
    Token(String),
    /// could be native or token symbol...
    Unknown(String)
}

#[derive(Serialize, Deserialize)]
pub struct PaymentSpec {
    pub amount: PaymentAmount,
    pub network: String,
    pub asset: PaymentAsset,
}

impl PaymentSpec {
    pub fn new(network: impl Into<String>, amount: impl Into<PaymentAmount>) -> Self {
        Self {
            network: network.into(),
            amount: amount.into(),
            asset: PaymentAsset::Native,
        }
    }

    pub fn with_asset(mut self, asset: impl Into<PaymentAsset>) -> Self {
        self.asset = asset.into();
        self
    }
}

#[derive(Serialize, Deserialize)]
pub enum ExpirationTime {
    Timestamp(DateTime<Utc>),
    Duration(std::time::Duration),
}

impl From<DateTime<Utc>> for ExpirationTime {
    fn from(value: DateTime<Utc>) -> Self {
        Self::Timestamp(value)
    }
}

impl From<std::time::Duration> for ExpirationTime {
    fn from(value: std::time::Duration) -> Self {
        Self::Duration(value)
    }
}

#[derive(Serialize, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
    pub secret: String,
    pub max_retries: u32,
}

impl WebhookConfig {
    pub fn new(url: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            secret: secret.into(),
            max_retries: 5,
        }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
}