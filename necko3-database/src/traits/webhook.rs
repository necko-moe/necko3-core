use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use necko3_types::{Webhook, WebhookStatus};
use crate::model::{PaginatedVec, WebhookFilter, WebhookJob};

#[async_trait]
pub trait WebhookStore: Sync + Send {
    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>>;
    async fn get_webhook(&self, webhook_id: Uuid) -> anyhow::Result<Option<Webhook>>;
    async fn add_webhook(&self, webhook: &Webhook) -> anyhow::Result<()>;
    
    async fn select_pending_webhooks(&self, limit: usize) -> anyhow::Result<Vec<WebhookJob>>;
    async fn update_webhook_status(&self, webhook_id: Uuid, status: WebhookStatus) -> anyhow::Result<()>;
    async fn schedule_webhook_retry(&self, webhook_id: Uuid, attempts: i32, next_retry: DateTime<Utc>) -> anyhow::Result<()>;
}