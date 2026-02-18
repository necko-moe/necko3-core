use crate::db::{Database, DatabaseAdapter};
use crate::model::{WebhookJob, WebhookStatus};
use crate::AppState;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

pub fn start_webhook_dispatcher(state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = Arc::new(Client::new());

        loop {
            let jobs_result: anyhow::Result<Vec<WebhookJob>> = state.db.select_webhooks_job().await;
            let jobs = jobs_result.unwrap_or_default();

            if jobs.is_empty() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            for job in jobs {
                tokio::spawn(process_webhook(state.db.clone(), client.clone(), job));
            }
        }
    })
}

fn generate_signature(timestamp: &str, secret: &str, body: &str) -> anyhow::Result<String> {
    let signed_body = format!("{}.{}", timestamp, body);

    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(signed_body.as_bytes());
    let result = mac.finalize();

    Ok(hex::encode(result.into_bytes()))
}

pub async fn process_webhook(
    db: Arc<Database>,
    client: Arc<Client>,
    job: WebhookJob,
) -> anyhow::Result<()> {
    let now = Utc::now().timestamp().to_string();
    let body_string = serde_json::to_string(&job.payload.0)
        .map_err(|e| anyhow::anyhow!(e))?;

    let signature = generate_signature(&now, &job.secret_key, &body_string)?;

    println!("Sending webhook (attempt {}/{}) to {}", job.attempts, job.max_retries, job.url);

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
            println!("Successfully sent webhook to {}", job.url);
            db.set_webhook_status(&job.id.to_string(), WebhookStatus::Sent).await?;
        }
        _ => {
            let new_attempts = job.attempts + 1;
            if new_attempts >= 10 {
                eprintln!("Failed to send webhook after {} attempts", job.attempts);
                db.set_webhook_status(&job.id.to_string(), WebhookStatus::Failed).await?;
                return Ok(())
            }

            let wait_time = 2_u64.pow(new_attempts as u32);

            db.schedule_webhook_retry(&job.id.to_string(), new_attempts, wait_time as f64).await?;
        }
    }

    Ok(())
}