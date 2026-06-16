use async_trait::async_trait;
use uuid::Uuid;
use necko3_types::UpsertPayment;
use crate::model::{PaginatedVec, Payment, PaymentFilter, PaymentStatus};

#[async_trait]
pub trait PaymentStore: Sync + Send {
    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>>;
    async fn get_payment(&self, payment_id: Uuid) -> anyhow::Result<Option<Payment>>;
    async fn get_payment_by_tx_hash(&self, tx_hash: String) -> anyhow::Result<Option<Payment>>;
    async fn get_payments_by_status(&self, status: PaymentStatus) -> anyhow::Result<Vec<Payment>>;
    async fn upsert_payment(&self, payment: &UpsertPayment) -> anyhow::Result<(Uuid, bool)>;

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> anyhow::Result<()>;
    async fn update_payment_block(&self, payment_id: Uuid, block_number: u64, block_hash: String) -> anyhow::Result<()>;
}