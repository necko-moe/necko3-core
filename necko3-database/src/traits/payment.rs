use async_trait::async_trait;
use uuid::Uuid;
use crate::model::{PaginatedVec, Payment, PaymentFilter, PaymentStatus};

#[async_trait]
pub trait PaymentStore: Sync + Send {
    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>>;
    async fn get_payment(&self, payment_id: Uuid) -> anyhow::Result<Option<Payment>>;
    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>>;
    async fn upsert_payment(&self, payment: &Payment) -> anyhow::Result<bool>;

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> anyhow::Result<()>;
    async fn update_payment_block_number(&self, payment_id: Uuid, block_num: u64) -> anyhow::Result<()>;
}