use alloy::primitives::utils::{format_units, parse_units};
use alloy::primitives::{Address, U256};
use chrono::{DateTime, Utc};
use coins_bip32::prelude::{Parent, XPub};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use hmac::{Hmac, KeyInit, Mac};
use necko3_core::model::{
    ChainConfig, ChainType, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec, PartialChainUpdate,
    Payment, PaymentFilter, PaymentStatus, TokenConfig, Webhook, WebhookEvent, WebhookFilter,
    WebhookStatus,
};
use sha2::Sha256;
use sqlx::types::BigDecimal;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

const XPUB_BENCH: &str = "xpub6EeaXhbbgvtV6KF1fvBeEn7DZnd1Gd4xh36eMAAeBB4KA73ZV5pXmjyddjPziE5QqkcoHtRRpkce9UP5qxsd2Q9qi3zmeXtEz5sc7NFGcvN";

fn sign_like_webhook(timestamp: &str, secret: &str, body: &str) -> anyhow::Result<String> {
    let signed_body = format!("{}.{}", timestamp, body);
    let mut mac: Hmac<Sha256> = Hmac::new_from_slice(secret.as_bytes())?;
    mac.update(signed_body.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn fixed_ts() -> DateTime<Utc> {
    "2024-06-01T12:00:00Z".parse().unwrap()
}

fn sample_webhook_event_small() -> WebhookEvent {
    WebhookEvent::InvoicePaid {
        invoice_id: "inv-bench".into(),
        paid_amount: "1.0".into(),
    }
}

fn sample_webhook_event_large() -> WebhookEvent {
    let long = "x".repeat(8000);
    WebhookEvent::TxDetected {
        invoice_id: long.clone(),
        tx_hash: format!("0x{}", "a".repeat(64)),
        amount: long.clone(),
        currency: long,
    }
}

fn bench_webhook_hmac_small(c: &mut Criterion) {
    let ts = "1700000000";
    let secret = "bench_webhook_secret_key_32bytes!!";
    let body = serde_json::to_string(&sample_webhook_event_small()).unwrap();
    c.bench_function("webhook_hmac_small_payload", |b| {
        b.iter(|| {
            sign_like_webhook(
                black_box(ts),
                black_box(secret),
                black_box(body.as_str()),
            )
            .unwrap()
        })
    });
}

fn bench_webhook_hmac_large(c: &mut Criterion) {
    let ts = "1700000000";
    let secret = "bench_webhook_secret_key_32bytes!!";
    let body = serde_json::to_string(&sample_webhook_event_large()).unwrap();
    c.bench_function("webhook_hmac_large_payload", |b| {
        b.iter(|| {
            sign_like_webhook(
                black_box(ts),
                black_box(secret),
                black_box(body.as_str()),
            )
            .unwrap()
        })
    });
}

fn bench_webhook_json(c: &mut Criterion) {
    let cases: Vec<(&str, WebhookEvent)> = vec![
        (
            "tx_detected",
            WebhookEvent::TxDetected {
                invoice_id: "i".into(),
                tx_hash: "0xabc".into(),
                amount: "1".into(),
                currency: "ETH".into(),
            },
        ),
        (
            "tx_confirmed",
            WebhookEvent::TxConfirmed {
                invoice_id: "i".into(),
                tx_hash: "0xdef".into(),
                confirmations: 12,
            },
        ),
        (
            "invoice_paid",
            WebhookEvent::InvoicePaid {
                invoice_id: "i".into(),
                paid_amount: "10".into(),
            },
        ),
        (
            "invoice_expired",
            WebhookEvent::InvoiceExpired {
                invoice_id: "i".into(),
            },
        ),
    ];
    for (name, ev) in cases {
        c.bench_function(&format!("webhook_json_{name}"), |b| {
            b.iter(|| serde_json::to_string(black_box(&ev)).unwrap())
        });
    }
}

fn make_chain_config() -> ChainConfig {
    ChainConfig {
        name: "ethereum".into(),
        active: true,
        rpc_urls: vec!["https://rpc.example.com".into()],
        chain_type: ChainType::EVM,
        xpub: XPUB_BENCH.into(),
        native_symbol: "ETH".into(),
        decimals: 18,
        last_processed_block: 1_234_567,
        block_lag: 2,
        required_confirmations: 12,
        logo_url: Some("https://x.png".into()),
        watch_addresses: Arc::new(RwLock::new(HashSet::new())),
        tokens: Arc::new(RwLock::new(HashSet::new())),
    }
}

fn sample_payment() -> Payment {
    Payment {
        id: "550e8400-e29b-41d4-a716-446655440000".into(),
        invoice_id: "660e8400-e29b-41d4-a716-446655440001".into(),
        from: "0x1111111111111111111111111111111111111111".into(),
        to: "0x2222222222222222222222222222222222222222".into(),
        network: "ethereum".into(),
        token: "ETH".into(),
        tx_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        amount_raw: U256::from(10u64).pow(U256::from(18)) * U256::from(42u64),
        block_number: 18_000_000,
        log_index: 7,
        status: PaymentStatus::Confirming,
        created_at: fixed_ts(),
    }
}

fn sample_invoice() -> Invoice {
    Invoice {
        id: "770e8400-e29b-41d4-a716-446655440002".into(),
        address_index: 3,
        address: "0x3333333333333333333333333333333333333333".into(),
        amount: "1.0".into(),
        amount_raw: U256::from(10u64).pow(U256::from(18)),
        paid: "0".into(),
        paid_raw: U256::ZERO,
        token: "ETH".into(),
        network: "ethereum".into(),
        decimals: 18,
        webhook_url: Some("https://hook.example.com/h".into()),
        webhook_secret: Some("sec".into()),
        webhook_max_retries: Some(5),
        created_at: fixed_ts(),
        expires_at: fixed_ts(),
        status: InvoiceStatus::Pending,
    }
}

fn sample_webhook() -> Webhook {
    Webhook {
        id: "wh-1".into(),
        invoice_id: "inv-1".into(),
        url: "https://example.com/w".into(),
        payload: sample_webhook_event_small(),
        status: WebhookStatus::Pending,
        attempts: 0,
        max_retries: 5,
        next_retry: fixed_ts(),
        created_at: fixed_ts(),
    }
}

fn bench_model_serde_roundtrips(c: &mut Criterion) {
    let payment = sample_payment();
    c.bench_function("model_serde_payment", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&payment)).unwrap();
            let _: Payment = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let invoice = sample_invoice();
    c.bench_function("model_serde_invoice", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&invoice)).unwrap();
            let _: Invoice = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let webhook = sample_webhook();
    c.bench_function("model_serde_webhook", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&webhook)).unwrap();
            let _: Webhook = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let inv_f = InvoiceFilter {
        status: Some(InvoiceStatus::Paid),
        address: Some("0xabc".into()),
        network: Some("eth".into()),
        token: Some("USDT".into()),
        pagination: necko3_core::model::Pagination {
            limit: 50,
            offset: 100,
        },
    };
    c.bench_function("model_serde_invoice_filter", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&inv_f)).unwrap();
            let _: InvoiceFilter = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let pay_f = PaymentFilter {
        invoice_id: Some("x".into()),
        from: None,
        to: None,
        network: Some("eth".into()),
        token: None,
        block_number: Some(123),
        status: Some(PaymentStatus::Confirmed),
        pagination: necko3_core::model::Pagination {
            limit: u32::MAX,
            offset: 1,
        },
    };
    c.bench_function("model_serde_payment_filter", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&pay_f)).unwrap();
            let _: PaymentFilter = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let wh_f = WebhookFilter {
        invoice_id: Some("i".into()),
        event_type: Some("tx_detected".into()),
        url: None,
        status: Some(WebhookStatus::Sent),
        pagination: Default::default(),
    };
    c.bench_function("model_serde_webhook_filter", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&wh_f)).unwrap();
            let _: WebhookFilter = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let tc = TokenConfig {
        symbol: "USDT".into(),
        contract: "0xdAC17F958D2ee523a2206206994597C13D831ec7".into(),
        decimals: 6,
        logo_url: None,
    };
    c.bench_function("model_serde_token_config", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&tc)).unwrap();
            let _: TokenConfig = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let partial = PartialChainUpdate {
        active: Some(true),
        rpc_urls: Some(vec!["https://a.com".into(), "https://b.com".into()]),
        last_processed_block: Some(99),
        xpub: Some("xpub_test".into()),
        block_lag: Some(4),
        required_confirmations: Some(20),
        logo_url: Some("https://l.png".into()),
    };
    c.bench_function("model_serde_partial_chain_update", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&partial)).unwrap();
            let _: PartialChainUpdate = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let chain = make_chain_config();
    c.bench_function("model_serde_chain_config", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&chain)).unwrap();
            let _: ChainConfig = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });

    let pv: PaginatedVec<u64> = PaginatedVec::new(vec![1, 2, 3], 1000, 10, 25);
    c.bench_function("model_serde_paginated_vec_u64", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&pv)).unwrap();
            let _: PaginatedVec<u64> = serde_json::from_str(black_box(s.as_str())).unwrap();
        })
    });
}

fn bench_amount_format(c: &mut Criterion) {
    let v = U256::from(10u64).pow(U256::from(18)) * U256::from(1_337u64);
    c.bench_function("amount_format_units_dec18", |b| {
        b.iter(|| format_units(black_box(v), black_box(18)).unwrap())
    });
    c.bench_function("amount_format_units_dec6", |b| {
        b.iter(|| format_units(black_box(v), black_box(6)).unwrap())
    });

    let s = "1234567.890123";
    c.bench_function("amount_parse_units_dec6", |b| {
        b.iter(|| parse_units(black_box(s), black_box(6)).unwrap())
    });
}

fn bench_xpub_derive(c: &mut Criterion) {
    let xpub = XPub::from_str(XPUB_BENCH).unwrap();
    c.bench_function("xpub_derive_child_and_address", |b| {
        let mut i: u32 = 0;
        b.iter(|| {
            let idx = black_box(i % 512);
            i = i.wrapping_add(1);
            let child = xpub.derive_child(idx).unwrap();
            let vk = child.as_ref();
            let _addr = Address::from_public_key(&vk).to_string();
        })
    });
}

fn bench_ids_and_decimal(c: &mut Criterion) {
    let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
    c.bench_function("ids_uuid_parse_str", |b| {
        b.iter(|| Uuid::parse_str(black_box(uuid_str)).unwrap())
    });

    let umax = U256::MAX;
    let dec_str = umax.to_string();
    c.bench_function("ids_bigdecimal_from_u256_string", |b| {
        b.iter(|| BigDecimal::from_str(black_box(dec_str.as_str())).unwrap())
    });

    let rpc = "https://eth.llamarpc.com";
    c.bench_function("ids_url_parse_rpc", |b| {
        b.iter(|| url::Url::parse(black_box(rpc)).unwrap())
    });
}

criterion_group!(
    webhook_hmac,
    bench_webhook_hmac_small,
    bench_webhook_hmac_large
);
criterion_group!(webhook_json, bench_webhook_json);
criterion_group!(model_serde, bench_model_serde_roundtrips);
criterion_group!(amount_format, bench_amount_format);
criterion_group!(xpub_derive, bench_xpub_derive);
criterion_group!(ids_and_decimal, bench_ids_and_decimal);

criterion_main!(
    webhook_hmac,
    webhook_json,
    model_serde,
    amount_format,
    xpub_derive,
    ids_and_decimal
);
