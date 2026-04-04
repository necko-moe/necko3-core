use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use necko3_core::model::*;
use alloy::primitives::U256;
use chrono::{Duration, Utc};

// ---------------------------------------------------------------------------
// ChainConfig::patch
// ---------------------------------------------------------------------------

fn make_chain_config() -> ChainConfig {
    ChainConfig {
        name: "ethereum".to_string(),
        active: true,
        rpc_urls: vec!["https://rpc1.example.com".to_string()],
        chain_type: ChainType::EVM,
        xpub: "xpub_original".to_string(),
        native_symbol: "ETH".to_string(),
        decimals: 18,
        last_processed_block: 1000,
        block_lag: 3,
        required_confirmations: 12,
        logo_url: None,
        watch_addresses: Arc::new(RwLock::new(HashSet::new())),
        tokens: Arc::new(RwLock::new(HashSet::new())),
    }
}

#[test]
fn patch_updates_rpc_urls() {
    let mut cfg = make_chain_config();
    let update = PartialChainUpdate {
        rpc_urls: Some(vec!["https://new-rpc.example.com".to_string()]),
        last_processed_block: None,
        xpub: None,
        block_lag: None,
        required_confirmations: None,
        active: None,
        logo_url: None,
    };
    cfg.patch(&update);
    assert_eq!(cfg.rpc_urls, vec!["https://new-rpc.example.com".to_string()]);
}

#[test]
fn patch_updates_last_processed_block() {
    let mut cfg = make_chain_config();
    let update = PartialChainUpdate {
        rpc_urls: None,
        last_processed_block: Some(5000),
        xpub: None,
        block_lag: None,
        required_confirmations: None,
        active: None,
        logo_url: None,
    };
    cfg.patch(&update);
    assert_eq!(cfg.last_processed_block, 5000);
}

#[test]
fn patch_updates_xpub() {
    let mut cfg = make_chain_config();
    let update = PartialChainUpdate {
        rpc_urls: None,
        last_processed_block: None,
        xpub: Some("xpub_new".to_string()),
        block_lag: None,
        required_confirmations: None,
        active: None,
        logo_url: None,
    };
    cfg.patch(&update);
    assert_eq!(cfg.xpub, "xpub_new");
}

#[test]
fn patch_updates_block_lag() {
    let mut cfg = make_chain_config();
    let update = PartialChainUpdate {
        rpc_urls: None,
        last_processed_block: None,
        xpub: None,
        block_lag: Some(10),
        required_confirmations: None,
        active: None,
        logo_url: None,
    };
    cfg.patch(&update);
    assert_eq!(cfg.block_lag, 10);
}

#[test]
fn patch_updates_required_confirmations() {
    let mut cfg = make_chain_config();
    let update = PartialChainUpdate {
        rpc_urls: None,
        last_processed_block: None,
        xpub: None,
        block_lag: None,
        required_confirmations: Some(64),
        active: None,
        logo_url: None,
    };
    cfg.patch(&update);
    assert_eq!(cfg.required_confirmations, 64);
}

#[test]
fn patch_updates_active() {
    let mut cfg = make_chain_config();
    assert!(cfg.active);
    let update = PartialChainUpdate {
        rpc_urls: None,
        last_processed_block: None,
        xpub: None,
        block_lag: None,
        required_confirmations: None,
        active: Some(false),
        logo_url: None,
    };
    cfg.patch(&update);
    assert!(!cfg.active);
}

#[test]
fn patch_updates_logo_url() {
    let mut cfg = make_chain_config();
    assert!(cfg.logo_url.is_none());
    let update = PartialChainUpdate {
        rpc_urls: None,
        last_processed_block: None,
        xpub: None,
        block_lag: None,
        required_confirmations: None,
        active: None,
        logo_url: Some("https://logo.png".to_string()),
    };
    cfg.patch(&update);
    assert_eq!(cfg.logo_url, Some("https://logo.png".to_string()));
}

#[test]
fn patch_with_all_none_is_noop() {
    let mut cfg = make_chain_config();
    let original = cfg.clone();
    let update = PartialChainUpdate {
        rpc_urls: None,
        last_processed_block: None,
        xpub: None,
        block_lag: None,
        required_confirmations: None,
        active: None,
        logo_url: None,
    };
    cfg.patch(&update);

    assert_eq!(cfg.rpc_urls, original.rpc_urls);
    assert_eq!(cfg.last_processed_block, original.last_processed_block);
    assert_eq!(cfg.xpub, original.xpub);
    assert_eq!(cfg.block_lag, original.block_lag);
    assert_eq!(cfg.required_confirmations, original.required_confirmations);
    assert_eq!(cfg.active, original.active);
    assert_eq!(cfg.logo_url, original.logo_url);
}

#[test]
fn patch_updates_multiple_fields_at_once() {
    let mut cfg = make_chain_config();
    let update = PartialChainUpdate {
        rpc_urls: Some(vec!["https://a.com".into(), "https://b.com".into()]),
        last_processed_block: Some(9999),
        xpub: Some("xpub_multi".into()),
        block_lag: Some(5),
        required_confirmations: Some(100),
        active: Some(false),
        logo_url: Some("https://img.png".into()),
    };
    cfg.patch(&update);

    assert_eq!(cfg.rpc_urls.len(), 2);
    assert_eq!(cfg.last_processed_block, 9999);
    assert_eq!(cfg.xpub, "xpub_multi");
    assert_eq!(cfg.block_lag, 5);
    assert_eq!(cfg.required_confirmations, 100);
    assert!(!cfg.active);
    assert_eq!(cfg.logo_url, Some("https://img.png".to_string()));
}

// ---------------------------------------------------------------------------
// Enum Display / FromStr round-trips
// ---------------------------------------------------------------------------

#[test]
fn chain_type_display_and_parse() {
    let ct = ChainType::EVM;
    let s = ct.to_string();
    assert_eq!(s, "EVM");
    let parsed: ChainType = s.parse().unwrap();
    assert!(matches!(parsed, ChainType::EVM));
}

#[test]
fn invoice_status_display_and_parse() {
    let statuses = [
        (InvoiceStatus::Pending, "Pending"),
        (InvoiceStatus::Paid, "Paid"),
        (InvoiceStatus::Expired, "Expired"),
        (InvoiceStatus::Cancelled, "Cancelled"),
    ];

    for (variant, expected) in &statuses {
        let s = variant.to_string();
        assert_eq!(&s, expected);
        let parsed: InvoiceStatus = s.parse().unwrap();
        assert_eq!(&parsed, variant);
    }
}

#[test]
fn payment_status_display_and_parse() {
    let statuses = [
        (PaymentStatus::Confirming, "Confirming"),
        (PaymentStatus::Confirmed, "Confirmed"),
        (PaymentStatus::Cancelled, "Cancelled"),
    ];

    for (variant, expected) in &statuses {
        let s = variant.to_string();
        assert_eq!(&s, expected);
        let parsed: PaymentStatus = s.parse().unwrap();
        assert_eq!(&parsed, variant);
    }
}

#[test]
fn webhook_status_display_and_parse() {
    let statuses = [
        (WebhookStatus::Pending, "Pending"),
        (WebhookStatus::Processing, "Processing"),
        (WebhookStatus::Sent, "Sent"),
        (WebhookStatus::Failed, "Failed"),
        (WebhookStatus::Cancelled, "Cancelled"),
    ];

    for (variant, expected) in &statuses {
        let s = variant.to_string();
        assert_eq!(&s, expected);
        let parsed: WebhookStatus = s.parse().unwrap();
        assert_eq!(&parsed, variant);
    }
}

#[test]
fn invalid_enum_parse_returns_error() {
    assert!("NotAStatus".parse::<InvoiceStatus>().is_err());
    assert!("".parse::<PaymentStatus>().is_err());
    assert!("pending".parse::<WebhookStatus>().is_err()); // case-sensitive
}

// ---------------------------------------------------------------------------
// WebhookEvent serde round-trip (tagged enum)
// ---------------------------------------------------------------------------

#[test]
fn webhook_event_tx_detected_serde() {
    let event = WebhookEvent::TxDetected {
        invoice_id: "inv-123".into(),
        tx_hash: "0xabc".into(),
        amount: "1.5".into(),
        currency: "ETH".into(),
    };

    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"event_type\":\"tx_detected\""));
    assert!(json.contains("\"invoice_id\":\"inv-123\""));

    let parsed: WebhookEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, event);
}

#[test]
fn webhook_event_tx_confirmed_serde() {
    let event = WebhookEvent::TxConfirmed {
        invoice_id: "inv-456".into(),
        tx_hash: "0xdef".into(),
        confirmations: 12,
    };

    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"event_type\":\"tx_confirmed\""));

    let parsed: WebhookEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, event);
}

#[test]
fn webhook_event_invoice_paid_serde() {
    let event = WebhookEvent::InvoicePaid {
        invoice_id: "inv-789".into(),
        paid_amount: "42.0".into(),
    };

    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"event_type\":\"invoice_paid\""));

    let parsed: WebhookEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, event);
}

#[test]
fn webhook_event_invoice_expired_serde() {
    let event = WebhookEvent::InvoiceExpired {
        invoice_id: "inv-expired".into(),
    };

    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"event_type\":\"invoice_expired\""));

    let parsed: WebhookEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, event);
}

// ---------------------------------------------------------------------------
// PaginatedVec
// ---------------------------------------------------------------------------

#[test]
fn paginated_vec_new_stores_metadata() {
    let items = vec![1, 2, 3];
    let pv = PaginatedVec::new(items, 100, 10, 25);

    assert_eq!(pv.items.len(), 3);
    assert_eq!(pv.total, 100);
    assert_eq!(pv.offset, 10);
    assert_eq!(pv.limit, 25);
}

#[test]
fn paginated_vec_into_iter() {
    let pv = PaginatedVec::new(vec![10, 20, 30], 3, 0, 10);
    let collected: Vec<i32> = pv.into_iter().collect();
    assert_eq!(collected, vec![10, 20, 30]);
}

#[test]
fn paginated_vec_empty() {
    let pv: PaginatedVec<String> = PaginatedVec::new(vec![], 0, 0, 50);
    assert!(pv.items.is_empty());
    assert_eq!(pv.total, 0);
    let collected: Vec<String> = pv.into_iter().collect();
    assert!(collected.is_empty());
}

// ---------------------------------------------------------------------------
// Pagination defaults
// ---------------------------------------------------------------------------

#[test]
fn pagination_default_is_zero() {
    let p = Pagination::default();
    assert_eq!(p.limit, 0);
    assert_eq!(p.offset, 0);
}

// ---------------------------------------------------------------------------
// Filter struct defaults
// ---------------------------------------------------------------------------

#[test]
fn invoice_filter_default_has_no_filters() {
    let f = InvoiceFilter::default();
    assert!(f.status.is_none());
    assert!(f.address.is_none());
    assert!(f.network.is_none());
    assert!(f.token.is_none());
    assert_eq!(f.pagination.limit, 0);
    assert_eq!(f.pagination.offset, 0);
}

#[test]
fn payment_filter_default_has_no_filters() {
    let f = PaymentFilter::default();
    assert!(f.invoice_id.is_none());
    assert!(f.from.is_none());
    assert!(f.to.is_none());
    assert!(f.network.is_none());
    assert!(f.token.is_none());
    assert!(f.block_number.is_none());
    assert!(f.status.is_none());
}

#[test]
fn webhook_filter_default_has_no_filters() {
    let f = WebhookFilter::default();
    assert!(f.invoice_id.is_none());
    assert!(f.event_type.is_none());
    assert!(f.url.is_none());
    assert!(f.status.is_none());
}

// ---------------------------------------------------------------------------
// TokenConfig equality (used in HashSet)
// ---------------------------------------------------------------------------

#[test]
fn token_config_hash_and_eq() {
    let t1 = TokenConfig {
        symbol: "USDT".into(),
        contract: "0xdead".into(),
        decimals: 6,
        logo_url: None,
    };
    let t2 = t1.clone();

    let mut set = HashSet::new();
    set.insert(t1.clone());
    assert!(set.contains(&t2));

    let t3 = TokenConfig {
        symbol: "USDC".into(),
        contract: "0xbeef".into(),
        decimals: 6,
        logo_url: None,
    };
    set.insert(t3.clone());
    assert_eq!(set.len(), 2);
}

#[test]
fn token_config_serde_round_trip() {
    let tc = TokenConfig {
        symbol: "USDT".into(),
        contract: "0xdAC17F958D2ee523a2206206994597C13D831ec7".into(),
        decimals: 6,
        logo_url: Some("https://example.com/usdt.png".into()),
    };

    let json = serde_json::to_string(&tc).unwrap();
    let parsed: TokenConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, tc);
}

// ---------------------------------------------------------------------------
// Invoice construction and equality
// ---------------------------------------------------------------------------

fn make_invoice(id: &str, status: InvoiceStatus) -> Invoice {
    Invoice {
        id: id.to_string(),
        address_index: 0,
        address: "0x1234".into(),
        amount: "1.0".into(),
        amount_raw: U256::from(1_000_000_000_000_000_000u128),
        paid: "0.0".into(),
        paid_raw: U256::ZERO,
        token: "ETH".into(),
        network: "ethereum".into(),
        decimals: 18,
        webhook_url: None,
        webhook_secret: None,
        webhook_max_retries: None,
        created_at: Utc::now(),
        expires_at: Utc::now() + Duration::hours(1),
        status,
    }
}

#[test]
fn invoice_equality() {
    let inv1 = make_invoice("a", InvoiceStatus::Pending);
    let inv2 = inv1.clone();
    assert_eq!(inv1, inv2);
}

#[test]
fn invoice_serde_round_trip() {
    let inv = make_invoice("test-id", InvoiceStatus::Paid);
    let json = serde_json::to_string(&inv).unwrap();
    let parsed: Invoice = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.id, "test-id");
    assert_eq!(parsed.status, InvoiceStatus::Paid);
}

// ---------------------------------------------------------------------------
// PartialChainUpdate serde
// ---------------------------------------------------------------------------

#[test]
fn partial_chain_update_serde_all_none() {
    let upd = PartialChainUpdate {
        active: None,
        rpc_urls: None,
        last_processed_block: None,
        xpub: None,
        block_lag: None,
        required_confirmations: None,
        logo_url: None,
    };
    let json = serde_json::to_string(&upd).unwrap();
    let parsed: PartialChainUpdate = serde_json::from_str(&json).unwrap();
    assert!(parsed.active.is_none());
    assert!(parsed.rpc_urls.is_none());
}

#[test]
fn partial_chain_update_serde_with_values() {
    let json = r#"{"active": true, "block_lag": 5}"#;
    let parsed: PartialChainUpdate = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.active, Some(true));
    assert_eq!(parsed.block_lag, Some(5));
    assert!(parsed.rpc_urls.is_none());
}

// ---------------------------------------------------------------------------
// ChainConfig serde (watch_addresses and tokens are skipped)
// ---------------------------------------------------------------------------

#[test]
fn chain_config_serde_skips_runtime_fields() {
    let cfg = make_chain_config();
    cfg.watch_addresses.write().unwrap().insert("0xABC".into());
    cfg.tokens.write().unwrap().insert(TokenConfig {
        symbol: "USDT".into(),
        contract: "0xdead".into(),
        decimals: 6,
        logo_url: None,
    });

    let json = serde_json::to_string(&cfg).unwrap();
    assert!(!json.contains("watch_addresses"));
    assert!(!json.contains("USDT")); // tokens are skipped

    let parsed: ChainConfig = serde_json::from_str(&json).unwrap();
    assert!(parsed.watch_addresses.read().unwrap().is_empty());
    assert!(parsed.tokens.read().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Payment serde
// ---------------------------------------------------------------------------

#[test]
fn payment_serde_round_trip() {
    let payment = Payment {
        id: "pay-1".into(),
        invoice_id: "inv-1".into(),
        from: "0xsender".into(),
        to: "0xreceiver".into(),
        network: "ethereum".into(),
        token: "ETH".into(),
        tx_hash: "0xtxhash".into(),
        amount_raw: U256::from(500u64),
        block_number: 12345,
        log_index: 0,
        status: PaymentStatus::Confirming,
        created_at: Utc::now(),
    };

    let json = serde_json::to_string(&payment).unwrap();
    let parsed: Payment = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.id, "pay-1");
    assert_eq!(parsed.status, PaymentStatus::Confirming);
    assert_eq!(parsed.block_number, 12345);
}

// ---------------------------------------------------------------------------
// Webhook serde
// ---------------------------------------------------------------------------

#[test]
fn webhook_serde_round_trip() {
    let wh = Webhook {
        id: "wh-1".into(),
        invoice_id: "inv-1".into(),
        url: "https://example.com/webhook".into(),
        payload: WebhookEvent::InvoiceExpired {
            invoice_id: "inv-1".into(),
        },
        status: WebhookStatus::Pending,
        attempts: 0,
        max_retries: 5,
        next_retry: Utc::now(),
        created_at: Utc::now(),
    };

    let json = serde_json::to_string(&wh).unwrap();
    let parsed: Webhook = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.id, "wh-1");
    assert_eq!(parsed.status, WebhookStatus::Pending);
    assert_eq!(parsed.max_retries, 5);
}
