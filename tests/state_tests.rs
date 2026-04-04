use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use alloy::primitives::U256;
use chrono::{Duration, Utc};

use necko3_core::db::mock::MockDatabase;
use necko3_core::db::{Database, DatabaseAdapter};
use necko3_core::model::*;
use necko3_core::AppState;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_mock_db() -> Database {
    Database::Mock(MockDatabase::new())
}

fn make_chain_config(name: &str) -> ChainConfig {
    ChainConfig {
        name: name.to_string(),
        active: true,
        rpc_urls: vec!["https://rpc.example.com".to_string()],
        chain_type: ChainType::EVM,
        xpub: "xpub6CUGRUonZSQ4TWtTMmzXdrXDtypWKiKrhko4egpiMZbpiaQL2jkwSB1icqYh2cfDfVxdx4df189oLKnC5fSwqPfgyP3hooxujYzAu3fDVmz".to_string(),
        native_symbol: "ETH".to_string(),
        decimals: 18,
        last_processed_block: 0,
        block_lag: 3,
        required_confirmations: 12,
        logo_url: None,
        watch_addresses: Arc::new(RwLock::new(HashSet::new())),
        tokens: Arc::new(RwLock::new(HashSet::new())),
    }
}

fn make_pending_invoice(id: &str, network: &str, address: &str, address_index: u32) -> Invoice {
    Invoice {
        id: id.to_string(),
        address_index,
        address: address.to_string(),
        amount: "1.0".to_string(),
        amount_raw: U256::from(1_000_000_000_000_000_000u128),
        paid: "0.0".to_string(),
        paid_raw: U256::ZERO,
        token: "ETH".to_string(),
        network: network.to_string(),
        decimals: 18,
        webhook_url: None,
        webhook_secret: None,
        webhook_max_retries: None,
        created_at: Utc::now(),
        expires_at: Utc::now() + Duration::hours(1),
        status: InvoiceStatus::Pending,
    }
}

// ===========================================================================
// AppState::new
// ===========================================================================

#[test]
fn appstate_new_creates_state_and_receiver() {
    let db = make_mock_db();
    let (state, _rx) = AppState::new(db, "test-api-key");

    assert_eq!(state.api_key, "test-api-key");
}

#[test]
fn appstate_new_stores_api_key() {
    let db = make_mock_db();
    let (state, _rx) = AppState::new(db, "my-secret-key-123");

    assert_eq!(state.api_key, "my-secret-key-123");
}

#[test]
fn appstate_new_with_empty_api_key() {
    let db = make_mock_db();
    let (state, _rx) = AppState::new(db, "");

    assert_eq!(state.api_key, "");
}

#[tokio::test]
async fn appstate_active_chains_starts_empty() {
    let db = make_mock_db();
    let (state, _rx) = AppState::new(db, "key");

    let chains = state.active_chains.read().await;
    assert!(chains.is_empty());
}

#[tokio::test]
async fn appstate_channel_works() {
    let db = make_mock_db();
    let (state, mut rx) = AppState::new(db, "key");

    let event = PaymentEvent {
        network: "ethereum".into(),
        tx_hash: "0x0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap(),
        from: "0xSender".into(),
        to: "0xReceiver".into(),
        token: "ETH".into(),
        amount: "1.0".into(),
        amount_raw: U256::from(1_000_000_000_000_000_000u128),
        decimals: 18,
        block_number: 12345,
        log_index: None,
    };

    state.tx.send(event).await.unwrap();

    let received = rx.recv().await.unwrap();
    assert_eq!(received.network, "ethereum");
    assert_eq!(received.block_number, 12345);
}

#[tokio::test]
async fn appstate_channel_capacity_is_100() {
    let db = make_mock_db();
    let (state, _rx) = AppState::new(db, "key");

    // Send 100 events (should all succeed without blocking since capacity is 100)
    for i in 0..100 {
        let event = PaymentEvent {
            network: "ethereum".into(),
            tx_hash: "0x0000000000000000000000000000000000000000000000000000000000000000"
                .parse()
                .unwrap(),
            from: "0xSender".into(),
            to: "0xReceiver".into(),
            token: "ETH".into(),
            amount: "1.0".into(),
            amount_raw: U256::from(i as u128),
            decimals: 18,
            block_number: i,
            log_index: None,
        };
        state.tx.send(event).await.unwrap();
    }

    // 101st should not complete immediately (channel full), use try_send
    let event = PaymentEvent {
        network: "ethereum".into(),
        tx_hash: "0x0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap(),
        from: "0xSender".into(),
        to: "0xReceiver".into(),
        token: "ETH".into(),
        amount: "1.0".into(),
        amount_raw: U256::from(9999u128),
        decimals: 18,
        block_number: 9999,
        log_index: None,
    };
    let try_result = state.tx.try_send(event);
    assert!(try_result.is_err()); // channel full
}

// ===========================================================================
// get_free_slot
// ===========================================================================

#[tokio::test]
async fn get_free_slot_empty_returns_zero() {
    let db = make_mock_db();
    let (state, _rx) = AppState::new(db, "key");
    let state = Arc::new(state);

    let slot = state.get_free_slot("ethereum").await;
    assert_eq!(slot, Some(0));
}

#[tokio::test]
async fn get_free_slot_with_index_zero_busy() {
    let db = make_mock_db();
    db.add_invoice(&make_pending_invoice("inv-0", "ethereum", "0xA", 0))
        .await
        .unwrap();

    let (state, _rx) = AppState::new(db, "key");
    let state = Arc::new(state);

    let slot = state.get_free_slot("ethereum").await;
    assert_eq!(slot, Some(1));
}

#[tokio::test]
async fn get_free_slot_with_gap() {
    let db = make_mock_db();
    // Indexes 0, 1, 3 are busy -- 2 is the gap
    db.add_invoice(&make_pending_invoice("inv-0", "ethereum", "0xA", 0))
        .await
        .unwrap();
    db.add_invoice(&make_pending_invoice("inv-1", "ethereum", "0xB", 1))
        .await
        .unwrap();
    db.add_invoice(&make_pending_invoice("inv-3", "ethereum", "0xC", 3))
        .await
        .unwrap();

    let (state, _rx) = AppState::new(db, "key");
    let state = Arc::new(state);

    let slot = state.get_free_slot("ethereum").await;
    assert_eq!(slot, Some(2));
}

#[tokio::test]
async fn get_free_slot_contiguous_returns_next() {
    let db = make_mock_db();
    // Indexes 0, 1, 2 busy -- next free is 3
    db.add_invoice(&make_pending_invoice("inv-0", "ethereum", "0xA", 0))
        .await
        .unwrap();
    db.add_invoice(&make_pending_invoice("inv-1", "ethereum", "0xB", 1))
        .await
        .unwrap();
    db.add_invoice(&make_pending_invoice("inv-2", "ethereum", "0xC", 2))
        .await
        .unwrap();

    let (state, _rx) = AppState::new(db, "key");
    let state = Arc::new(state);

    let slot = state.get_free_slot("ethereum").await;
    assert_eq!(slot, Some(3));
}

#[tokio::test]
async fn get_free_slot_different_chains_independent() {
    let db = make_mock_db();
    db.add_invoice(&make_pending_invoice("inv-0", "ethereum", "0xA", 0))
        .await
        .unwrap();
    db.add_invoice(&make_pending_invoice("inv-1", "ethereum", "0xB", 1))
        .await
        .unwrap();

    let (state, _rx) = AppState::new(db, "key");
    let state = Arc::new(state);

    let eth_slot = state.get_free_slot("ethereum").await;
    assert_eq!(eth_slot, Some(2));

    let poly_slot = state.get_free_slot("polygon").await;
    assert_eq!(poly_slot, Some(0));
}

#[tokio::test]
async fn get_free_slot_ignores_non_pending() {
    let db = make_mock_db();
    let mut inv = make_pending_invoice("inv-0", "ethereum", "0xA", 0);
    inv.status = InvoiceStatus::Paid; // not Pending
    db.add_invoice(&inv).await.unwrap();

    let (state, _rx) = AppState::new(db, "key");
    let state = Arc::new(state);

    let slot = state.get_free_slot("ethereum").await;
    assert_eq!(slot, Some(0)); // index 0 is free because the invoice is Paid
}

// ===========================================================================
// AppState with Database enum
// ===========================================================================

#[tokio::test]
async fn appstate_db_is_accessible() {
    let db = make_mock_db();
    let (state, _rx) = AppState::new(db, "key");

    state.db.add_chain(&make_chain_config("ethereum")).await.unwrap();

    let exists = state.db.chain_exists("ethereum").await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn appstate_db_invoice_operations() {
    let db = make_mock_db();
    let (state, _rx) = AppState::new(db, "key");
    let state = Arc::new(state);

    let inv = make_pending_invoice("inv-1", "ethereum", "0xABC", 0);
    state.db.add_invoice(&inv).await.unwrap();

    let fetched = state.db.get_invoice("inv-1").await.unwrap();
    assert!(fetched.is_some());

    let slot = state.get_free_slot("ethereum").await;
    assert_eq!(slot, Some(1)); // index 0 is busy
}
