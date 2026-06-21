use async_trait::async_trait;
use uuid::Uuid;
use necko3_types::{UpsertPayment, Payment, PaymentStatus};
use crate::error::DbResult;
use crate::model::{PaginatedVec, PaymentFilter};

#[async_trait]
pub trait PaymentStore: Sync + Send {
    async fn get_payments(&self, filter: PaymentFilter) -> DbResult<PaginatedVec<Payment>>;
    async fn get_payment(&self, payment_id: Uuid) -> DbResult<Option<Payment>>;
    async fn get_payment_by_tx_hash(&self, tx_hash: String) -> DbResult<Option<Payment>>;
    async fn get_payments_by_status(&self, status: PaymentStatus) -> DbResult<Vec<Payment>>;
    async fn upsert_payment(&self, payment: &UpsertPayment) -> DbResult<(Uuid, bool)>;

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> DbResult<()>;
    async fn update_payment_block(&self, payment_id: Uuid, block_number: u64, block_hash: String) -> DbResult<()>;
}