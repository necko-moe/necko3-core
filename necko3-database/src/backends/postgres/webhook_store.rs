use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::QueryBuilder;
use uuid::Uuid;
use necko3_types::{Webhook, WebhookStatus};
use crate::backends::postgres::PostgresAdapter;
use crate::model::{PaginatedVec, WebhookFilter, WebhookJob};
use crate::traits::WebhookStore;

#[async_trait]
impl WebhookStore for PostgresAdapter {
    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>> {
        fn apply_filters(
            builder: &mut QueryBuilder<sqlx::Postgres>,
            filter: &WebhookFilter
        ) -> anyhow::Result<()> {
            if let Some(ref invoice_id) = filter.invoice_id {
                builder.push(" AND invoice_id = ");
                builder.push_bind(invoice_id);
            }

            if let Some(ref event_type) = filter.event_type {
                builder.push(" AND event_type = ");
                builder.push_bind(event_type);
            }

            if let Some(ref url) = filter.url {
                builder.push(" AND url = ");
                builder.push_bind(url);
            }

            if let Some(ref status) = filter.status {
                builder.push(" AND status = ");
                builder.push_bind(status.to_string());
            }

            Ok(())
        }

        let mut count_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "SELECT count(*) FROM webhooks WHERE TRUE");

        apply_filters(&mut count_builder, &filter)?;

        let total: i64 = count_builder
            .build_query_as::<(i64,)>()
            .fetch_one(&self.pool)
            .await?
            .0;

        let mut data_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT * FROM webhooks WHERE TRUE"#);

        apply_filters(&mut data_builder, &filter)?;

        data_builder.push(" ORDER BY created_at DESC LIMIT ");
        data_builder.push_bind(filter.pagination.limit as i64);
        data_builder.push(" OFFSET ");
        data_builder.push_bind(filter.pagination.offset as i64);

        let rows = data_builder
            .build()
            .fetch_all(&self.pool)
            .await?;

        let webhooks: Vec<Webhook> = rows
            .into_iter()
            .map(Self::map_row_to_webhook)
            .collect::<anyhow::Result<_>>()?;

        Ok(PaginatedVec::new(
            webhooks,
            total as u64,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_webhook(&self, webhook_id: Uuid) -> anyhow::Result<Option<Webhook>> {
        sqlx::query(
            r#"SELECT * FROM webhooks WHERE id = $1"#
        )
            .bind(webhook_id)
            .fetch_optional(&self.pool)
            .await?
            .map(Self::map_row_to_webhook)
            .transpose()
    }

    async fn add_webhook(&self, webhook: &Webhook) -> anyhow::Result<()> {
        let event_type = webhook.payload.as_ref();
        let payload = serde_json::to_value(&webhook.payload)?;

        sqlx::query(
            r#"INSERT INTO webhooks (id, invoice_id, event_type, url, payload, max_retries, status)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)"#
        )
            .bind(webhook.id)
            .bind(webhook.invoice_id)
            .bind(event_type)
            .bind(&webhook.url)
            .bind(payload)
            .bind(webhook.max_retries as i32)
            .bind(webhook.status.to_string())
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn select_pending_webhooks(&self, limit: usize) -> anyhow::Result<Vec<WebhookJob>> {
        let jobs = sqlx::query_as::<_, WebhookJob>(
            r#"UPDATE webhooks w
                   SET status = 'Processing'
                   FROM invoices i
                   WHERE w.invoice_id = i.id
                       AND w.id IN (
                           SELECT id FROM webhooks
                           WHERE status = 'Pending' AND next_retry <= NOW()
                           LIMIT $1
                           FOR UPDATE SKIP LOCKED
                       )
                   RETURNING w.id, w.url, w.payload, w.max_retries, w.attempts,
                       COALESCE(i.webhook_secret, 'default_secret') as secret_key"#
        )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;

        Ok(jobs)
    }

    async fn update_webhook_status(&self, webhook_id: Uuid, status: WebhookStatus) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE webhooks SET status = $1 WHERE id = $2"
        )
            .bind(status.to_string())
            .bind(webhook_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn schedule_webhook_retry(&self, webhook_id: Uuid, attempts: i32, next_retry: DateTime<Utc>) -> anyhow::Result<()> {
        sqlx::query(
            r#"UPDATE webhooks SET status = 'Pending', attempts = $1,
                       next_retry = $2 WHERE id = $3"#
        )
            .bind(attempts)
            .bind(next_retry)
            .bind(webhook_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}