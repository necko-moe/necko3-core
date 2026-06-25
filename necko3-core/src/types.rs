use alloy_primitives::U256;
use necko3_types::blockchain::Asset;
use uuid::Uuid;
use necko3_types::InvoiceStatus;

pub mod core {
    pub mod db {
        pub use necko3_database::model::*;
    }

    pub use necko3_types::*;
}

#[derive(Debug, Clone)]
pub enum NeckoEvent {
    Core(CoreEvent),
    Ext(ExternalEvent),
}

impl TryFrom<NeckoEvent> for CoreEvent {
    type Error = ();
    fn try_from(event: NeckoEvent) -> Result<Self, Self::Error> {
        if let NeckoEvent::Core(core) = event { Ok(core) } else { Err(()) }
    }
}

impl TryFrom<NeckoEvent> for ExternalEvent {
    type Error = ();
    fn try_from(event: NeckoEvent) -> Result<Self, Self::Error> {
        if let NeckoEvent::Ext(ext) = event { Ok(ext) } else { Err(()) }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionDetectedData {
    pub db_transaction_id: Uuid,
    pub tx_hash: String,

    pub network: String,
    pub asset: Asset,
    pub from: String,
    pub to: String,

    pub amount_raw: U256,
    pub amount_human: String,

    pub block_number: u64,
    pub block_hash: String,

    pub log_index: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum CoreEvent {
    TransactionDetected(Box<TransactionDetectedData>),

    TransactionConfirmed {
        db_transaction_id: Uuid,
        tx_hash: String,

        block_number: u64,
        block_hash: String,
        confirmed_after: u64,
    },

    TransactionReorged {
        db_transaction_id: Uuid,
        tx_hash: String,

        new_block_number: u64,
        new_block_hash: String
    },

    TransactionFailed {
        db_transaction_id: Uuid,
        tx_hash: String,
    },

    TransactionLost {
        tx_hash: String,
    }
}

#[derive(Debug, Clone)]
pub enum ExternalEvent {
    InvoicePaymentApplied {
        invoice_id: Uuid,
        payment_id: Uuid,

        paid_raw_before: U256,
        paid_raw_after: U256,

        old_status: InvoiceStatus,
        new_status: InvoiceStatus
    },
    InvoicePaid {
        invoice_id: Uuid,
    },
    InvoiceExpired {
        invoice_id: Uuid,
    },
    InvoiceCancelled {
        invoice_id: Uuid,
    },

    WebhookDelivered {
        webhook_id: Uuid,
        invoice_id: Uuid,
        attempt: u32,
        url: String,
    },
    WebhookFailed {
        webhook_id: Uuid,
        invoice_id: Uuid,
        attempt: u32,
        max_attempts: u32,
        url: String,
    },
}