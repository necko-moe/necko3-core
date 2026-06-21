pub mod error;

use std::sync::Arc;
use std::time::Duration;
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::Client;
use sha2::Sha256;
use tokio::sync::Semaphore;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, error, info, instrument, trace, warn, Instrument};
use necko3_database::model::WebhookJob;
use necko3_database::traits::WebhookStore;
use necko3_types::WebhookStatus;
use crate::tasks::webhook_dispatcher::error::WebhookError;

pub struct NeckoWebhookDispatcher<D> {
    db: Arc<D>,
    client: Arc<Client>,

    interval: Interval,

    webhooks_selection_limit: usize,

    semaphore: Arc<Semaphore>,
}

impl<D: WebhookStore + 'static> NeckoWebhookDispatcher<D> {
    pub fn new(
        db: Arc<D>,
        interval_duration: Duration,

        webhooks_selection_limit: usize,
        concurrency_limit: usize,
    ) -> Self {
        let mut interval = tokio::time::interval(interval_duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self {
            db,
            client: Arc::new(Client::new()),
            interval,
            webhooks_selection_limit,
            semaphore: Arc::new(Semaphore::new(concurrency_limit)),
        }
    }

    #[instrument(skip(self), name = "webhook_dispatcher")]
    pub async fn run(mut self) {
        info!(interval = ?self.interval, "Starting webhook dispatcher service");
        self.interval.tick().await; // skip first tick

        loop {
            self.interval.tick().await;

            let jobs = match self.db.select_pending_webhooks(
                self.webhooks_selection_limit).await {
                Ok(js) => js,
                Err(e) => {
                    error!(error = %e, "Failed to select pending webhooks");
                    continue;
                }
            };

            if jobs.is_empty() {
                trace!("No pending webhooks found");
                continue;
            }

            debug!(count = jobs.len(), "Found pending webhooks");

            for job in jobs {
                let db = self.db.clone();
                let client = self.client.clone();

                let sem_clone = self.semaphore.clone();

                let job_span = tracing::info_span!(
                    "webhook_job",
                    job_id = %job.id,
                    url = %job.url,
                    attempt = job.attempts
                );

                let permit = sem_clone.acquire_owned().await.expect("Semaphore closed");

                tokio::spawn(async move {
                    let _permit = permit;

                    if let Err(e) = process_webhook(db, client, job).await {
                        error!(error = %e, "Failed to process webhook");
                    }
                }.instrument(job_span));
            }
        }
    }
}

#[instrument(level = "trace", skip(secret, body))] // :)
fn generate_signature(timestamp: &str, secret: &str, body: &str) -> Result<String, WebhookError> {
    trace!("Generating HMAC signature");
    let signed_body = format!("{}.{}", timestamp, body);

    let mut mac: Hmac<Sha256> = Hmac::new_from_slice(secret.as_bytes())
        .map_err(|e| WebhookError::Crypto(format!("HMAC key init failed: {}", e)))?;

    mac.update(signed_body.as_bytes());
    let result = mac.finalize();

    Ok(hex::encode(result.into_bytes()))
}

#[instrument(skip_all, err)]
async fn process_webhook<D: WebhookStore>(
    db: Arc<D>,
    client: Arc<Client>,
    job: WebhookJob
) -> Result<(), WebhookError> {
    let body_string = match serde_json::to_string(&job.payload.0) {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, job_id = %job.id,
                "Failed to serialize webhook payload. Dropping job");

            db.update_webhook_status(job.id, WebhookStatus::Failed).await?;
            return Err(WebhookError::Serialization(e));
        }
    };

    let now = Utc::now().timestamp().to_string();

    let signature = match generate_signature(&now, &job.secret_key, &body_string) {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, job_id = %job.id,
                "Signature generation failed. Dropping job");

            db.update_webhook_status(job.id, WebhookStatus::Failed).await?;
            return Err(e);
        }
    };

    debug!(
        current_attempt = job.attempts,
        max_retries = job.max_retries,
        "Sending HTTP POST request"
    );

    let result = client
        .post(&job.url)
        .header("Content-Type", "application/json")
        .header("X-Webhook-Timestamp", &now)
        .header("X-Webhook-Signature", &signature)
        .body(body_string.clone())
        .timeout(Duration::from_secs(10))
        .send()
        .await;

    match result {
        Ok(res) if res.status().is_success() => {
            info!(status = %res.status(), "Webhook sent successfully");
            db.update_webhook_status(job.id, WebhookStatus::Sent).await?;
        }
        Ok(res) => {
            let status = res.status();
            warn!(status = %status, "Webhook server returned error status");
            handle_retry(db, job, format!("HTTP Status {}", status)).await?;
        }
        Err(e) => {
            warn!(error = %e, "Network error while sending webhook");
            handle_retry(db, job, e.to_string()).await?;
        }
    }

    Ok(())
}

async fn handle_retry<D: WebhookStore>(
    db: Arc<D>,
    job: WebhookJob,
    reason: String,
) -> Result<(), WebhookError> {
    let new_attempts = job.attempts + 1;

    if new_attempts > job.max_retries {
        warn!(
            reason = %reason,
            attempts = new_attempts,
            "Failed to send webhook after max retries. Giving up."
        );

        db.update_webhook_status(job.id, WebhookStatus::Failed).await?;
    } else {
        let wait_time = 2_u64.pow(job.attempts as u32);

        debug!(
            reason = %reason,
            next_attempt_in = %format!("{}s", wait_time),
            attempt = new_attempts,
            "Scheduling webhook retry"
        );

        let next_retry = Utc::now() + Duration::from_secs(wait_time);

        db.schedule_webhook_retry(job.id, new_attempts, next_retry).await?;
    }

    Ok(())
}