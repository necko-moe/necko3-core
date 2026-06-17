use alloy_primitives::U256;
use necko3_types::blockchain::Asset;
use uuid::Uuid;
use necko3_types::InvoiceStatus;

#[derive(Debug, Clone)]
pub enum NeckoEvent {
    Core(CoreEvent),
    Ext(ExternalEvent),
}

impl NeckoEvent {
    pub fn as_core(&self) -> Option<&CoreEvent> {
        match self {
            NeckoEvent::Core(e) => Some(e),
            _ => None,
        }
    }

    pub fn as_ext(&self) -> Option<&ExternalEvent> {
        match self {
            NeckoEvent::Ext(e) => Some(e),
            _ => None,
        }
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
    PaymentCancelled {
        payment_id: Uuid,
    },

    InvoicePaymentApplied {
        invoice_id: Uuid,

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
    WebhookCancelled {
        webhook_id: Uuid,
    },
}