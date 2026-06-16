use alloy_primitives::U256;
use necko3_types::blockchain::Asset;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum CoreEvent {
    TransactionDetected {
        db_transaction_id: Uuid,
        tx_hash: String,

        network: String,
        asset: Asset,
        from: String,
        to: String,

        amount_raw: U256,
        amount_human: String,

        block_number: u64,
        block_hash: String,

        log_index: Option<u64>,
    },

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