use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use necko3_types::{Webhook, WebhookStatus};
use crate::backends::in_memory::InMemoryAdapter;
use crate::error::{DbError, DbResult};
use crate::model::{PaginatedVec, WebhookFilter, WebhookJob};
use crate::traits::WebhookStore;

#[async_trait]
impl WebhookStore for InMemoryAdapter {
    async fn get_webhooks(&self, filter: WebhookFilter) -> DbResult<PaginatedVec<Webhook>> {
        let mut filtered: Vec<Webhook> = self.webhooks.iter()
            .filter(|x| {
                let wh = x.value();

                filter.invoice_id.as_ref().map_or(true, |i| wh.invoice_id == *i)
                    && filter.event_type.as_ref().map_or(true, |e| wh.payload.to_string() == *e)
                    && filter.url.as_ref().map_or(true, |u| wh.url == *u)
                    && filter.status.as_ref().map_or(true, |s| wh.status == *s)
            })
            .map(|w| w.value().clone())
            .collect();

        let total = filtered.len() as u64;

        filtered.sort_unstable_by(|a, b| b.created_at.cmp(&a.created_at));

        let webhooks: Vec<Webhook> = filtered
            .into_iter()
            .skip(filter.pagination.offset as usize)
            .take(filter.pagination.limit as usize)
            .collect();

        Ok(PaginatedVec::new(
            webhooks,
            total,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_webhook(&self, webhook_id: Uuid) -> DbResult<Option<Webhook>> {
        Ok(self.webhooks.get(&webhook_id).map(|x| x.value().clone()))
    }

    async fn add_webhook(&self, webhook: Webhook) -> DbResult<Webhook> {
        self.webhooks.insert(webhook.id, webhook.clone());

        Ok(webhook)
    }

    async fn select_pending_webhooks(&self, limit: usize) -> DbResult<Vec<WebhookJob>> {
        let now = Utc::now();

        let target_ids: Vec<Uuid> = self.webhooks
            .iter()
            .filter(|r| r.status == WebhookStatus::Pending
                && r.next_retry <= now)
            .take(limit)
            .map(|r| r.key().clone())
            .collect();

        let mut jobs = Vec::with_capacity(target_ids.len());

        target_ids.iter().for_each(|id| {
            if let Some(mut job) = self.webhooks.get_mut(&id) {
                job.status = WebhookStatus::Processing;

                let secret = self.invoices
                    .get(&job.invoice_id)
                    .and_then(|inv| inv.webhook_secret.clone())
                    .unwrap_or_else(|| "default_secret".to_owned());

                jobs.push(WebhookJob {
                    id: job.id,
                    url: job.url.clone(),
                    secret_key: secret,
                    payload: sqlx::types::Json(job.payload.clone()),
                    max_retries: job.max_retries as i32,
                    attempts: job.attempts as i32,
                });
            }
        });

        Ok(jobs)
    }

    async fn update_webhook_status(&self, webhook_id: Uuid, status: WebhookStatus) -> DbResult<()> {
        if !self.webhooks.contains_key(&webhook_id) {
            return Err(DbError::NotFound {
                entity: "Webhook",
                id: webhook_id.to_string(),
            })
        }
        
        if let Some(mut job) = self.webhooks
            .get_mut(&webhook_id) {
            
            job.status = status;
        }

        Ok(())
    }

    async fn schedule_webhook_retry(&self, webhook_id: Uuid, attempts: i32, next_retry: DateTime<Utc>) -> DbResult<()> {
        if !self.webhooks.contains_key(&webhook_id) {
            return Err(DbError::NotFound {
                entity: "Webhook",
                id: webhook_id.to_string(),
            })
        }
        
        if let Some(mut job) = self.webhooks
            .get_mut(&webhook_id) {
            
            job.status = WebhookStatus::Pending;
            job.attempts = attempts as u32;
            job.next_retry = next_retry;
        }

        Ok(())
    }
}