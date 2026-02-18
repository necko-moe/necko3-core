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
            let jobs = match jobs_result {
                Ok(j) => j,
                Err(e) => {
                    eprintln!("Failed to select webhooks job: {}... Retrying in 5 seconds", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue
                }
            };

            if jobs.is_empty() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            for job in jobs {
                let client_clone = client.clone();
                let db_clone = state.db.clone();

                tokio::spawn(async move {
                    if let Err(e) = process_webhook(db_clone, client_clone, job).await {
                        eprintln!("Failed to process webhook: {:?}", e);
                    }
                });
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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::mock::MockDatabase;
    use crate::model::{Invoice, InvoiceStatus, WebhookEvent};
    use wiremock::matchers::{header, header_exists, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_webhook_delivery_with_signature() {
        let mock_server = MockServer::start().await;
        let secret = "test_secret";

        Mock::given(method("POST"))
            .and(header("Content-Type", "application/json"))
            .and(header_exists("X-Webhook-Signature"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = Arc::new(Client::new());
        let invoice_uid = uuid::Uuid::new_v4().to_string();

        let event = WebhookEvent::InvoicePaid {
            invoice_id: invoice_uid.clone(),
            paid_amount: "100.0".to_string(),
        };

        let db = Arc::new(Database::Mock(MockDatabase::new()));
        db.add_invoice(&Invoice {
            id: invoice_uid.clone(),
            address_index: 0,
            address: "".to_string(),
            amount: "".to_string(),
            amount_raw: Default::default(),
            paid: "".to_string(),
            paid_raw: Default::default(),
            token: "".to_string(),
            network: "".to_string(),
            decimals: 0,
            webhook_url: Some(mock_server.uri()),
            webhook_secret: Some(secret.to_string()),
            created_at: Default::default(),
            expires_at: Default::default(),
            status: InvoiceStatus::Pending,
        }).await.unwrap();

        db.add_webhook_job(&invoice_uid.clone(), &event).await.unwrap();

        let mut jobs = db.select_webhooks_job().await.unwrap();
        assert!(!jobs.is_empty(), "Job was not created in DB");

        let job = jobs.remove(0);

        process_webhook(db, client, job).await.unwrap();
    }
}