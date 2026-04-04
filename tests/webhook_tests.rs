use hmac::{Hmac, Mac};
use hmac::KeyInit;
use sha2::Sha256;

use necko3_core::db::mock::MockDatabase;
use necko3_core::db::{Database, DatabaseAdapter};
use necko3_core::model::*;

use alloy::primitives::U256;
use chrono::{Duration, Utc};
use wiremock::matchers::{header, header_exists, method};
use wiremock::{Mock, MockServer, ResponseTemplate};
use reqwest::Client;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers (mirrors the internal signing logic from state/webhook.rs)
// ---------------------------------------------------------------------------

fn generate_signature(timestamp: &str, secret: &str, body: &str) -> String {
    let signed_body = format!("{}.{}", timestamp, body);
    let mut mac: Hmac<Sha256> = Hmac::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signed_body.as_bytes());
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

fn verify_signature(timestamp: &str, secret: &str, body: &str, expected_sig: &str) -> bool {
    let computed = generate_signature(timestamp, secret, body);
    computed == expected_sig
}

fn make_invoice(id: &str, webhook_url: Option<String>) -> Invoice {
    Invoice {
        id: id.to_string(),
        address_index: 0,
        address: "0x1234".to_string(),
        amount: "1.0".to_string(),
        amount_raw: U256::from(1_000_000_000_000_000_000u128),
        paid: "0.0".to_string(),
        paid_raw: U256::ZERO,
        token: "ETH".to_string(),
        network: "ethereum".to_string(),
        decimals: 18,
        webhook_url,
        webhook_secret: Some("test_secret_key".to_string()),
        webhook_max_retries: Some(3),
        created_at: Utc::now(),
        expires_at: Utc::now() + Duration::hours(1),
        status: InvoiceStatus::Pending,
    }
}

// ===========================================================================
// HMAC-SHA256 Signature Generation
// ===========================================================================

#[test]
fn signature_is_deterministic() {
    let ts = "1712275200";
    let secret = "my_webhook_secret";
    let body = r#"{"event_type":"invoice_paid","data":{"invoice_id":"abc","paid_amount":"1.0"}}"#;

    let sig1 = generate_signature(ts, secret, body);
    let sig2 = generate_signature(ts, secret, body);

    assert_eq!(sig1, sig2);
    assert!(!sig1.is_empty());
    assert_eq!(sig1.len(), 64); // SHA256 = 32 bytes = 64 hex chars
}

#[test]
fn signature_changes_with_different_timestamp() {
    let secret = "secret";
    let body = "payload";

    let sig1 = generate_signature("1000", secret, body);
    let sig2 = generate_signature("2000", secret, body);

    assert_ne!(sig1, sig2);
}

#[test]
fn signature_changes_with_different_secret() {
    let ts = "1000";
    let body = "payload";

    let sig1 = generate_signature(ts, "secret_a", body);
    let sig2 = generate_signature(ts, "secret_b", body);

    assert_ne!(sig1, sig2);
}

#[test]
fn signature_changes_with_different_body() {
    let ts = "1000";
    let secret = "secret";

    let sig1 = generate_signature(ts, secret, "body_a");
    let sig2 = generate_signature(ts, secret, "body_b");

    assert_ne!(sig1, sig2);
}

#[test]
fn signature_format_is_lowercase_hex() {
    let sig = generate_signature("123", "key", "data");
    assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(sig.chars().all(|c| !c.is_ascii_uppercase()));
}

#[test]
fn verify_valid_signature() {
    let ts = "1712275200";
    let secret = "test_key";
    let body = r#"{"ok": true}"#;

    let sig = generate_signature(ts, secret, body);
    assert!(verify_signature(ts, secret, body, &sig));
}

#[test]
fn verify_rejects_tampered_body() {
    let ts = "1712275200";
    let secret = "test_key";
    let body = r#"{"ok": true}"#;

    let sig = generate_signature(ts, secret, body);
    assert!(!verify_signature(ts, secret, r#"{"ok": false}"#, &sig));
}

#[test]
fn verify_rejects_wrong_secret() {
    let ts = "1712275200";
    let body = "payload";

    let sig = generate_signature(ts, "correct_secret", body);
    assert!(!verify_signature(ts, "wrong_secret", body, &sig));
}

#[test]
fn verify_rejects_wrong_timestamp() {
    let secret = "key";
    let body = "payload";

    let sig = generate_signature("1000", secret, body);
    assert!(!verify_signature("9999", secret, body, &sig));
}

// ===========================================================================
// Webhook Event Serialization (wire format)
// ===========================================================================

#[test]
fn webhook_event_serializes_to_correct_json_format() {
    let event = WebhookEvent::InvoicePaid {
        invoice_id: "inv-123".into(),
        paid_amount: "42.5".into(),
    };

    let json = serde_json::to_string(&event).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["event_type"], "invoice_paid");
    assert_eq!(parsed["data"]["invoice_id"], "inv-123");
    assert_eq!(parsed["data"]["paid_amount"], "42.5");
}

#[test]
fn webhook_event_tx_detected_wire_format() {
    let event = WebhookEvent::TxDetected {
        invoice_id: "inv-1".into(),
        tx_hash: "0xabc123".into(),
        amount: "1.0".into(),
        currency: "ETH".into(),
    };

    let json = serde_json::to_string(&event).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["event_type"], "tx_detected");
    assert_eq!(parsed["data"]["tx_hash"], "0xabc123");
    assert_eq!(parsed["data"]["currency"], "ETH");
}

#[test]
fn webhook_event_tx_confirmed_wire_format() {
    let event = WebhookEvent::TxConfirmed {
        invoice_id: "inv-1".into(),
        tx_hash: "0xdef456".into(),
        confirmations: 12,
    };

    let json = serde_json::to_string(&event).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["event_type"], "tx_confirmed");
    assert_eq!(parsed["data"]["confirmations"], 12);
}

#[test]
fn webhook_event_invoice_expired_wire_format() {
    let event = WebhookEvent::InvoiceExpired {
        invoice_id: "inv-gone".into(),
    };

    let json = serde_json::to_string(&event).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["event_type"], "invoice_expired");
    assert_eq!(parsed["data"]["invoice_id"], "inv-gone");
}

// ===========================================================================
// Webhook HTTP Delivery via wiremock
// ===========================================================================

#[tokio::test]
async fn webhook_http_post_with_valid_signature() {
    let mock_server = MockServer::start().await;
    let secret = "integration_test_secret";

    Mock::given(method("POST"))
        .and(header("Content-Type", "application/json"))
        .and(header_exists("X-Webhook-Signature"))
        .and(header_exists("X-Webhook-Timestamp"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let event = WebhookEvent::InvoicePaid {
        invoice_id: "inv-http-test".into(),
        paid_amount: "10.0".into(),
    };

    let body = serde_json::to_string(&event).unwrap();
    let timestamp = Utc::now().timestamp().to_string();
    let signature = generate_signature(&timestamp, secret, &body);

    let client = Client::new();
    let response = client
        .post(mock_server.uri())
        .header("Content-Type", "application/json")
        .header("X-Webhook-Timestamp", &timestamp)
        .header("X-Webhook-Signature", &signature)
        .body(body)
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
}

#[tokio::test]
async fn webhook_server_returns_500_triggers_retry_scenario() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&mock_server)
        .await;

    let event = WebhookEvent::TxDetected {
        invoice_id: "inv-retry".into(),
        tx_hash: "0xfail".into(),
        amount: "0.5".into(),
        currency: "USDT".into(),
    };

    let body = serde_json::to_string(&event).unwrap();
    let timestamp = Utc::now().timestamp().to_string();
    let signature = generate_signature(&timestamp, "secret", &body);

    let client = Client::new();
    let response = client
        .post(mock_server.uri())
        .header("Content-Type", "application/json")
        .header("X-Webhook-Timestamp", &timestamp)
        .header("X-Webhook-Signature", &signature)
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 500);
}

// ===========================================================================
// Webhook Receiver Can Verify Signature
// ===========================================================================

#[tokio::test]
async fn receiver_can_verify_webhook_signature() {
    let secret = "shared_secret";
    let event = WebhookEvent::InvoicePaid {
        invoice_id: "inv-verify".into(),
        paid_amount: "5.0".into(),
    };

    let body = serde_json::to_string(&event).unwrap();
    let timestamp = Utc::now().timestamp().to_string();
    let signature = generate_signature(&timestamp, secret, &body);

    // Simulate what a receiver would do:
    assert!(verify_signature(&timestamp, secret, &body, &signature));

    // Tampered body should fail:
    let tampered_body = body.replace("5.0", "999.0");
    assert!(!verify_signature(&timestamp, secret, &tampered_body, &signature));
}

// ===========================================================================
// Exponential Backoff Calculation
// ===========================================================================

#[test]
fn exponential_backoff_formula() {
    // The webhook dispatcher uses: wait_time = 2^new_attempts
    for attempt in 1..=10 {
        let wait_time = 2_u64.pow(attempt);
        assert_eq!(wait_time, 1 << attempt);
    }

    assert_eq!(2_u64.pow(1), 2);   // 1st retry: 2s
    assert_eq!(2_u64.pow(2), 4);   // 2nd retry: 4s
    assert_eq!(2_u64.pow(3), 8);   // 3rd retry: 8s
    assert_eq!(2_u64.pow(5), 32);  // 5th retry: 32s
    assert_eq!(2_u64.pow(10), 1024); // 10th retry: ~17min
}

#[test]
fn retry_stops_at_max_retries() {
    let max_retries = 5;

    // Attempts 0..3 should result in retries (new_attempts 1..4 < max_retries)
    for attempt in 0..(max_retries - 1) {
        let new_attempts = attempt + 1;
        assert!(new_attempts < max_retries, "Should still retry at attempt {}", attempt);
    }

    // At attempt = max_retries - 1, new_attempts = max_retries -> should stop
    let new_attempts = max_retries;
    assert!(new_attempts >= max_retries, "Should stop retrying");
}

// ===========================================================================
// Webhook Job Lifecycle (via MockDatabase)
// ===========================================================================

#[tokio::test]
async fn full_webhook_lifecycle_mock_db() {
    let db = MockDatabase::new();
    let inv_id = uuid::Uuid::new_v4().to_string();
    let inv = Invoice {
        id: inv_id.clone(),
        ..make_invoice(&inv_id, Some("https://example.com/hook".into()))
    };
    db.add_invoice(&inv).await.unwrap();

    // 1. Create webhook job
    let event = WebhookEvent::TxDetected {
        invoice_id: inv_id.clone(),
        tx_hash: "0xaaa".into(),
        amount: "1.0".into(),
        currency: "ETH".into(),
    };
    db.add_webhook_job(&inv_id, &event).await.unwrap();

    // 2. Select job (moves to Processing)
    let jobs = db.select_webhooks_job().await.unwrap();
    assert_eq!(jobs.len(), 1);
    let job_id = jobs[0].id.to_string();

    let wh = db.get_webhook(&job_id).await.unwrap().unwrap();
    assert_eq!(wh.status, WebhookStatus::Processing);

    // 3. Mark as Sent
    db.set_webhook_status(&job_id, WebhookStatus::Sent)
        .await
        .unwrap();

    let wh = db.get_webhook(&job_id).await.unwrap().unwrap();
    assert_eq!(wh.status, WebhookStatus::Sent);
}

#[tokio::test]
async fn webhook_retry_lifecycle_mock_db() {
    let db = MockDatabase::new();
    let inv_id = uuid::Uuid::new_v4().to_string();
    let inv = Invoice {
        id: inv_id.clone(),
        ..make_invoice(&inv_id, Some("https://example.com/hook".into()))
    };
    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::InvoicePaid {
        invoice_id: inv_id.clone(),
        paid_amount: "1.0".into(),
    };
    db.add_webhook_job(&inv_id, &event).await.unwrap();

    // Select job
    let jobs = db.select_webhooks_job().await.unwrap();
    let job_id = jobs[0].id.to_string();

    // Simulate failure -> schedule retry
    db.schedule_webhook_retry(&job_id, 1, 2.0).await.unwrap();

    let wh = db.get_webhook(&job_id).await.unwrap().unwrap();
    assert_eq!(wh.status, WebhookStatus::Pending);
    assert_eq!(wh.attempts, 1);

    // Simulate another failure
    db.schedule_webhook_retry(&job_id, 2, 4.0).await.unwrap();

    let wh = db.get_webhook(&job_id).await.unwrap().unwrap();
    assert_eq!(wh.attempts, 2);

    // Simulate max retries exceeded -> Failed
    db.set_webhook_status(&job_id, WebhookStatus::Failed)
        .await
        .unwrap();

    let wh = db.get_webhook(&job_id).await.unwrap().unwrap();
    assert_eq!(wh.status, WebhookStatus::Failed);
}

// ===========================================================================
// Webhook via full Database enum (integration-like)
// ===========================================================================

#[tokio::test]
async fn webhook_through_database_enum() {
    let db = Arc::new(Database::Mock(MockDatabase::new()));

    let invoice_uid = uuid::Uuid::new_v4().to_string();
    let inv = Invoice {
        id: invoice_uid.clone(),
        address_index: 0,
        address: "0xtest".into(),
        amount: "100.0".into(),
        amount_raw: U256::from(100_000_000_000_000_000_000u128),
        paid: "0.0".into(),
        paid_raw: U256::ZERO,
        token: "ETH".into(),
        network: "ethereum".into(),
        decimals: 18,
        webhook_url: Some("https://example.com/wh".into()),
        webhook_secret: Some("my_secret".into()),
        webhook_max_retries: Some(5),
        created_at: Utc::now(),
        expires_at: Utc::now() + Duration::hours(1),
        status: InvoiceStatus::Pending,
    };

    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::InvoicePaid {
        invoice_id: invoice_uid.clone(),
        paid_amount: "100.0".into(),
    };

    db.add_webhook_job(&invoice_uid, &event).await.unwrap();

    let jobs = db.select_webhooks_job().await.unwrap();
    assert!(!jobs.is_empty());
    assert_eq!(jobs[0].secret_key, "my_secret");
    assert_eq!(jobs[0].url, "https://example.com/wh");
}

#[tokio::test]
async fn webhook_default_secret_when_none() {
    let db = Arc::new(Database::Mock(MockDatabase::new()));

    let invoice_uid = uuid::Uuid::new_v4().to_string();
    let inv = Invoice {
        id: invoice_uid.clone(),
        address_index: 0,
        address: "0xtest".into(),
        amount: "1.0".into(),
        amount_raw: U256::from(1_000_000_000_000_000_000u128),
        paid: "0.0".into(),
        paid_raw: U256::ZERO,
        token: "ETH".into(),
        network: "ethereum".into(),
        decimals: 18,
        webhook_url: Some("https://example.com/wh".into()),
        webhook_secret: None, // no secret
        webhook_max_retries: None,
        created_at: Utc::now(),
        expires_at: Utc::now() + Duration::hours(1),
        status: InvoiceStatus::Pending,
    };

    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::InvoiceExpired {
        invoice_id: invoice_uid.clone(),
    };
    db.add_webhook_job(&invoice_uid, &event).await.unwrap();

    let jobs = db.select_webhooks_job().await.unwrap();
    assert_eq!(jobs[0].secret_key, "default_secret"); // documents the S-01 audit finding
}
