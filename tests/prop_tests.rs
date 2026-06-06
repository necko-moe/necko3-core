use alloy::primitives::{Address, TxHash, U256};
use alloy::primitives::utils::{format_units, parse_units};
use coins_bip32::prelude::{Parent, XPub};
use necko3_core::model::{
    ChainConfig, ChainType, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec, PartialChainUpdate,
    Payment, PaymentFilter, PaymentStatus, TokenConfig, Webhook, WebhookEvent, WebhookFilter,
    WebhookStatus,
};
use proptest::prelude::*;
use serde_json::{json, Value};
use sqlx::types::BigDecimal;
use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

const XPUB_VALID: &str = "xpub6EeaXhbbgvtV6KF1fvBeEn7DZnd1Gd4xh36eMAAeBB4KA73ZV5pXmjyddjPziE5QqkcoHtRRpkce9UP5qxsd2Q9qi3zmeXtEz5sc7NFGcvN";

fn assert_no_panic<F: FnOnce()>(f: F) {
    let r = catch_unwind(AssertUnwindSafe(f));
    assert!(r.is_ok(), "unexpected panic");
}

fn arb_bounded_string(max: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(any::<char>(), 0..=max).prop_map(|v| v.into_iter().collect())
}

fn arb_ascii_string(max: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(0u8..128u8, 0..=max).prop_map(|b| {
        String::from_utf8(b).expect("ASCII subset is valid UTF-8")
    })
}

fn arb_hexish() -> impl Strategy<Value = String> {
    let alphabet = prop::sample::select(vec![
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'A', 'B',
        'C', 'D', 'E', 'F', 'x', 'X', 'g', 'Z', ' ', '\n', '\0',
    ]);
    prop::collection::vec(alphabet, 0..=256).prop_map(|v| v.into_iter().collect())
}

fn arb_amountish() -> impl Strategy<Value = String> {
    prop::string::string_regex(r"[-+eE0-9.]*").unwrap()
}

fn arb_json_for_amount_raw() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(|n| json!(n)),
        any::<u64>().prop_map(|n| json!(n)),
        arb_hexish().prop_map(|s| json!(s)),
        Just(Value::Null),
        Just(json!([])),
        Just(json!({})),
        Just(json!({"x": true})),
    ]
}

fn base_payment_json(
    id: String,
    invoice_id: String,
    from: String,
    to: String,
    network: String,
    token: String,
    tx_hash: String,
    amount_raw: Value,
    status: String,
) -> Value {
    json!({
        "id": id,
        "invoice_id": invoice_id,
        "from": from,
        "to": to,
        "network": network,
        "token": token,
        "tx_hash": tx_hash,
        "amount_raw": amount_raw,
        "block_number": 1u64,
        "log_index": 0u64,
        "status": status,
        "created_at": "2024-01-01T00:00:00Z",
    })
}

fn base_invoice_json(
    id: String,
    address: String,
    amount: String,
    paid: String,
    token: String,
    network: String,
    amount_raw: Value,
    paid_raw: Value,
    status: String,
) -> Value {
    json!({
        "id": id,
        "address_index": 0u32,
        "address": address,
        "amount": amount,
        "amount_raw": amount_raw,
        "paid": paid,
        "paid_raw": paid_raw,
        "token": token,
        "network": network,
        "decimals": 18u8,
        "webhook_url": null,
        "webhook_secret": null,
        "webhook_max_retries": null,
        "created_at": "2024-01-01T00:00:00Z",
        "expires_at": "2024-01-02T00:00:00Z",
        "status": status,
    })
}

fn base_webhook_json(
    id: String,
    invoice_id: String,
    url: String,
    payload: Value,
    status: String,
) -> Value {
    json!({
        "id": id,
        "invoice_id": invoice_id,
        "url": url,
        "payload": payload,
        "status": status,
        "attempts": 0u32,
        "max_retries": 3u32,
        "next_retry": "2024-01-01T00:00:00Z",
        "created_at": "2024-01-01T00:00:00Z",
    })
}

fn make_chain_config() -> ChainConfig {
    ChainConfig {
        name: "ethereum".into(),
        active: true,
        rpc_urls: vec!["https://rpc.example.com".into()],
        chain_type: ChainType::EVM,
        xpub: XPUB_VALID.into(),
        native_symbol: "ETH".into(),
        decimals: 18,
        last_processed_block: 1000,
        block_lag: 3,
        required_confirmations: 12,
        logo_url: None,
        watch_addresses: Arc::new(RwLock::new(HashSet::new())),
        tokens: Arc::new(RwLock::new(HashSet::new())),
    }
}

fn simulate_process_transaction_fragment(tx: &Value) {
    let to_str = tx["to"].as_str().unwrap_or_default();
    let _ = to_str.parse::<Address>();
    let value_hex = tx["value"].as_str().unwrap_or("0x0");
    let radix = value_hex
        .strip_prefix("0x")
        .or_else(|| value_hex.strip_prefix("0X"))
        .unwrap_or(value_hex);
    let _ = U256::from_str_radix(radix, 16);
    let h = tx["hash"].as_str().unwrap_or_default();
    let _ = h.parse::<TxHash>();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    #[test]
    fn payment_deserialize_no_panic(
        id in arb_bounded_string(512),
        inv in arb_bounded_string(512),
        from in arb_bounded_string(256),
        to in arb_bounded_string(256),
        net in arb_bounded_string(128),
        tok in arb_bounded_string(64),
        txh in arb_hexish(),
        st in arb_bounded_string(32),
        araw in arb_json_for_amount_raw(),
    ) {
        let j = base_payment_json(id, inv, from, to, net, tok, txh, araw, st);
        let s = j.to_string();
        assert_no_panic(|| {
            let _: Result<Payment, _> = serde_json::from_str(&s);
        });
    }

    #[test]
    fn invoice_deserialize_no_panic(
        id in arb_bounded_string(512),
        addr in arb_hexish(),
        amt in arb_amountish(),
        paid in arb_amountish(),
        tok in arb_bounded_string(64),
        net in arb_bounded_string(128),
        araw in arb_json_for_amount_raw(),
        praw in arb_json_for_amount_raw(),
        st in arb_bounded_string(32),
    ) {
        let j = base_invoice_json(id, addr, amt, paid, tok, net, araw, praw, st);
        let s = j.to_string();
        assert_no_panic(|| {
            let _: Result<Invoice, _> = serde_json::from_str(&s);
        });
    }

    #[test]
    fn webhook_deserialize_no_panic(
        id in arb_bounded_string(256),
        inv in arb_bounded_string(256),
        url in arb_bounded_string(512),
        st in arb_bounded_string(32),
        tag in prop::sample::select(vec![
            "tx_detected".to_string(),
            "tx_confirmed".to_string(),
            "invoice_paid".to_string(),
            "invoice_expired".to_string(),
            "bogus".to_string(),
        ]),
        inv2 in arb_bounded_string(256),
        txh in arb_hexish(),
        amt in arb_amountish(),
        cur in arb_bounded_string(32),
        conf in any::<u64>(),
        paid_amt in arb_amountish(),
    ) {
        let payload = match &*tag {
            "tx_detected" => json!({
                "event_type": "tx_detected",
                "data": {
                    "invoice_id": inv2,
                    "tx_hash": txh,
                    "amount": amt,
                    "currency": cur,
                }
            }),
            "tx_confirmed" => json!({
                "event_type": "tx_confirmed",
                "data": {
                    "invoice_id": inv2,
                    "tx_hash": txh,
                    "confirmations": conf,
                }
            }),
            "invoice_paid" => json!({
                "event_type": "invoice_paid",
                "data": {
                    "invoice_id": inv2,
                    "paid_amount": paid_amt,
                }
            }),
            "invoice_expired" => json!({
                "event_type": "invoice_expired",
                "data": { "invoice_id": inv2 }
            }),
            _ => json!({
                "event_type": tag,
                "data": { "x": amt, "y": txh }
            }),
        };
        let j = base_webhook_json(id, inv, url, payload, st);
        let s = j.to_string();
        assert_no_panic(|| {
            let _: Result<Webhook, _> = serde_json::from_str(&s);
        });
    }

    #[test]
    fn webhook_event_from_value_no_panic(obj in prop::collection::hash_map(arb_bounded_string(40), arb_json_for_amount_raw(), 0..=12)) {
        let v = Value::Object(obj.into_iter().collect());
        assert_no_panic(|| {
            let _: Result<WebhookEvent, _> = serde_json::from_value(v.clone());
        });
    }

    #[test]
    fn webhook_event_to_value_no_panic(
        inv in arb_bounded_string(200),
        txh in arb_hexish(),
        amt in arb_amountish(),
        cur in arb_bounded_string(40),
        conf in any::<u64>(),
        paid in arb_amountish(),
    ) {
        let events = vec![
            WebhookEvent::TxDetected { invoice_id: inv.clone(), tx_hash: txh.clone(), amount: amt.clone(), currency: cur.clone() },
            WebhookEvent::TxConfirmed { invoice_id: inv.clone(), tx_hash: txh.clone(), confirmations: conf },
            WebhookEvent::InvoicePaid { invoice_id: inv.clone(), paid_amount: paid.clone() },
            WebhookEvent::InvoiceExpired { invoice_id: inv },
        ];
        for ev in events {
            assert_no_panic(|| {
                let _: Result<Value, _> = serde_json::to_value(&ev);
            });
        }
    }

    #[test]
    fn invoice_filter_no_panic(
        st in arb_bounded_string(24),
        addr in arb_hexish(),
        net in arb_bounded_string(64),
        tok in arb_bounded_string(32),
        lim in any::<u32>(),
        off in any::<u64>(),
    ) {
        let j = json!({
            "status": st,
            "address": addr,
            "network": net,
            "token": tok,
            "pagination": { "limit": lim, "offset": off },
        });
        let s = j.to_string();
        assert_no_panic(|| {
            let _: Result<InvoiceFilter, _> = serde_json::from_str(&s);
        });
    }

    #[test]
    fn payment_filter_no_panic(
        iid in arb_bounded_string(128),
        from in arb_hexish(),
        to in arb_hexish(),
        net in arb_bounded_string(64),
        tok in arb_bounded_string(32),
        bn in any::<u64>(),
        st in arb_bounded_string(24),
        lim in any::<u32>(),
        off in any::<u64>(),
    ) {
        let j = json!({
            "invoice_id": iid,
            "from": from,
            "to": to,
            "network": net,
            "token": tok,
            "block_number": bn,
            "status": st,
            "pagination": { "limit": lim, "offset": off },
        });
        let s = j.to_string();
        assert_no_panic(|| {
            let _: Result<PaymentFilter, _> = serde_json::from_str(&s);
        });
    }

    #[test]
    fn webhook_filter_no_panic(
        iid in arb_bounded_string(128),
        et in arb_bounded_string(64),
        url in arb_bounded_string(256),
        st in arb_bounded_string(24),
        lim in any::<u32>(),
        off in any::<u64>(),
    ) {
        let j = json!({
            "invoice_id": iid,
            "event_type": et,
            "url": url,
            "status": st,
            "pagination": { "limit": lim, "offset": off },
        });
        let s = j.to_string();
        assert_no_panic(|| {
            let _: Result<WebhookFilter, _> = serde_json::from_str(&s);
        });
    }

    #[test]
    fn enum_parse_no_panic(s in arb_bounded_string(64)) {
        assert_no_panic(|| {
            let _: Result<ChainType, _> = s.parse();
        });
        assert_no_panic(|| {
            let _: Result<InvoiceStatus, _> = s.parse();
        });
        assert_no_panic(|| {
            let _: Result<PaymentStatus, _> = s.parse();
        });
        assert_no_panic(|| {
            let _: Result<WebhookStatus, _> = s.parse();
        });
    }

    #[test]
    fn alloy_address_txhash_u256_no_panic(s in arb_hexish()) {
        assert_no_panic(|| {
            let _: Result<Address, _> = Address::from_str(&s);
        });
        assert_no_panic(|| {
            let _: Result<TxHash, _> = s.parse();
        });
        assert_no_panic(|| {
            let _: Result<U256, _> = U256::from_str(&s);
        });
        assert_no_panic(|| {
            let _: Result<U256, _> = U256::from_str_radix(s.trim_start_matches("0x").trim_start_matches("0X"), 16);
        });
    }

    #[test]
    fn uuid_url_bigdecimal_no_panic(s in arb_bounded_string(4096)) {
        assert_no_panic(|| {
            let _: Result<uuid::Uuid, _> = uuid::Uuid::parse_str(&s);
        });
        assert_no_panic(|| {
            let _: Result<url::Url, _> = url::Url::parse(&s);
        });
        assert_no_panic(|| {
            let _: Result<BigDecimal, _> = BigDecimal::from_str(&s);
        });
    }

    #[test]
    fn xpub_derive_valid_no_panic(idx in 0u32..4096u32) {
        assert_no_panic(|| {
            if let Ok(xp) = XPub::from_str(XPUB_VALID) {
                let _ = xp.derive_child(idx).map(|c| {
                    let vk = c.as_ref();
                    let _ = Address::from_public_key(&vk);
                });
            }
        });
    }

    #[test]
    fn parse_format_units_no_panic(s in arb_ascii_string(512), dec in 0u8..=255u8) {
        assert_no_panic(|| {
            let _ = parse_units(&s, dec);
        });
        let u = U256::MAX;
        assert_no_panic(|| {
            let _ = format_units(u, dec % 80);
        });
    }

    #[test]
    fn chain_config_patch_no_panic(
        active in any::<bool>(),
        rpc_a in arb_bounded_string(200),
        rpc_b in arb_bounded_string(200),
        lpb in any::<u64>(),
        xpub in arb_bounded_string(300),
        bl in any::<u8>(),
        rc in any::<u64>(),
        logo in arb_bounded_string(300),
    ) {
        let upd = PartialChainUpdate {
            active: Some(active),
            rpc_urls: Some(vec![rpc_a, rpc_b]),
            last_processed_block: Some(lpb),
            xpub: Some(xpub),
            block_lag: Some(bl),
            required_confirmations: Some(rc),
            logo_url: Some(logo),
        };
        let mut cfg = make_chain_config();
        assert_no_panic(|| {
            cfg.patch(&upd);
        });
    }

    #[test]
    fn rpc_tx_fragment_no_panic(
        to in arb_hexish(),
        val in arb_hexish(),
        hash in arb_hexish(),
        from in arb_hexish(),
    ) {
        let tx = json!({
            "to": to,
            "value": val,
            "hash": hash,
            "from": from,
            "input": "0x",
            "data": null,
        });
        assert_no_panic(|| {
            simulate_process_transaction_fragment(&tx);
        });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn token_config_roundtrip_no_panic(sym in arb_bounded_string(16), contract in arb_hexish(), dec in any::<u8>(), logo in prop::option::of(arb_bounded_string(256))) {
        let tc = TokenConfig { symbol: sym, contract, decimals: dec, logo_url: logo };
        assert_no_panic(|| {
            let s = serde_json::to_string(&tc).unwrap();
            let _: Result<TokenConfig, _> = serde_json::from_str(&s);
        });
    }

    #[test]
    fn paginated_vec_roundtrip_no_panic(items in prop::collection::vec(any::<i32>(), 0..=32), total in any::<u64>(), off in any::<u64>(), lim in any::<u32>()) {
        let pv: PaginatedVec<i32> = PaginatedVec::new(items, total, off, lim);
        assert_no_panic(|| {
            let s = serde_json::to_string(&pv).unwrap();
            let _: Result<PaginatedVec<i32>, _> = serde_json::from_str(&s);
        });
    }
}

#[test]
fn bigdecimal_u256_max_string_no_panic() {
    let s = U256::MAX.to_string();
    assert_no_panic(|| {
        let _: Result<BigDecimal, _> = BigDecimal::from_str(&s);
    });
}
