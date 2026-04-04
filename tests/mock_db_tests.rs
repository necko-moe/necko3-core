use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use alloy::primitives::U256;
use chrono::{Duration, Utc};

use necko3_core::chain::BlockchainAdapter;
use necko3_core::db::mock::MockDatabase;
use necko3_core::db::DatabaseAdapter;
use necko3_core::model::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn mock_db() -> MockDatabase {
    MockDatabase::new()
}

fn evm_chain_config(name: &str) -> ChainConfig {
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

fn make_invoice(id: &str, network: &str, address: &str, status: InvoiceStatus) -> Invoice {
    Invoice {
        id: id.to_string(),
        address_index: 0,
        address: address.to_string(),
        amount: "1.0".to_string(),
        amount_raw: U256::from(1_000_000_000_000_000_000u128),
        paid: "0.0".to_string(),
        paid_raw: U256::ZERO,
        token: "ETH".to_string(),
        network: network.to_string(),
        decimals: 18,
        webhook_url: Some("https://example.com/webhook".to_string()),
        webhook_secret: Some("secret123".to_string()),
        webhook_max_retries: Some(5),
        created_at: Utc::now(),
        expires_at: Utc::now() + Duration::hours(1),
        status,
    }
}

fn make_uuid_invoice(network: &str, address: &str, status: InvoiceStatus) -> Invoice {
    Invoice {
        id: uuid::Uuid::new_v4().to_string(),
        address_index: 0,
        address: address.to_string(),
        amount: "1.0".to_string(),
        amount_raw: U256::from(1_000_000_000_000_000_000u128),
        paid: "0.0".to_string(),
        paid_raw: U256::ZERO,
        token: "ETH".to_string(),
        network: network.to_string(),
        decimals: 18,
        webhook_url: Some("https://example.com/webhook".to_string()),
        webhook_secret: Some("secret123".to_string()),
        webhook_max_retries: Some(5),
        created_at: Utc::now(),
        expires_at: Utc::now() + Duration::hours(1),
        status,
    }
}

fn make_expired_invoice(id: &str, network: &str, address: &str) -> Invoice {
    Invoice {
        id: id.to_string(),
        address_index: 1,
        address: address.to_string(),
        amount: "2.0".to_string(),
        amount_raw: U256::from(2_000_000_000_000_000_000u128),
        paid: "0.0".to_string(),
        paid_raw: U256::ZERO,
        token: "ETH".to_string(),
        network: network.to_string(),
        decimals: 18,
        webhook_url: None,
        webhook_secret: None,
        webhook_max_retries: None,
        created_at: Utc::now() - Duration::hours(2),
        expires_at: Utc::now() - Duration::hours(1), // already expired
        status: InvoiceStatus::Pending,
    }
}

fn usdt_token() -> TokenConfig {
    TokenConfig {
        symbol: "USDT".to_string(),
        contract: "0xdAC17F958D2ee523a2206206994597C13D831ec7".to_string(),
        decimals: 6,
        logo_url: None,
    }
}

// ===========================================================================
// Chain CRUD
// ===========================================================================

#[tokio::test]
async fn add_and_get_chain() {
    let db = mock_db();
    let cfg = evm_chain_config("ethereum");

    db.add_chain(&cfg).await.unwrap();

    let chain = db.get_chain("ethereum").await.unwrap();
    assert!(chain.is_some());
}

#[tokio::test]
async fn get_nonexistent_chain_returns_none() {
    let db = mock_db();
    let chain = db.get_chain("phantom").await.unwrap();
    assert!(chain.is_none());
}

#[tokio::test]
async fn add_duplicate_chain_fails() {
    let db = mock_db();
    let cfg = evm_chain_config("ethereum");

    db.add_chain(&cfg).await.unwrap();
    let result = db.add_chain(&cfg).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn chain_exists() {
    let db = mock_db();
    assert!(!db.chain_exists("ethereum").await.unwrap());

    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    assert!(db.chain_exists("ethereum").await.unwrap());
}

#[tokio::test]
async fn remove_chain() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    assert!(db.chain_exists("ethereum").await.unwrap());

    db.remove_chain("ethereum").await.unwrap();
    assert!(!db.chain_exists("ethereum").await.unwrap());
}

#[tokio::test]
async fn get_chains_returns_all() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_chain(&evm_chain_config("polygon")).await.unwrap();

    let chains = db.get_chains().await.unwrap();
    assert_eq!(chains.len(), 2);
}

#[tokio::test]
async fn get_chains_map() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_chain(&evm_chain_config("polygon")).await.unwrap();

    let map = db.get_chains_map().await.unwrap();
    assert!(map.contains_key("ethereum"));
    assert!(map.contains_key("polygon"));
}

#[tokio::test]
async fn update_chain_block() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    db.update_chain_block("ethereum", 42000).await.unwrap();

    let block = db.get_latest_block("ethereum").await.unwrap();
    assert_eq!(block, Some(42000));
}

#[tokio::test]
async fn update_chain_block_nonexistent_fails() {
    let db = mock_db();
    let result = db.update_chain_block("ghost", 100).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn set_chain_active() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    db.set_chain_active("ethereum", false).await.unwrap();

    let chain = db.get_chain("ethereum").await.unwrap().unwrap();
    let config_lock = chain.config();
    let cfg = config_lock.read().unwrap();
    assert!(!cfg.active);
}

#[tokio::test]
async fn update_chain_partial() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let update = PartialChainUpdate {
        rpc_urls: Some(vec!["https://new-rpc.com".into()]),
        last_processed_block: Some(5000),
        xpub: None,
        block_lag: Some(5),
        required_confirmations: None,
        active: None,
        logo_url: None,
    };

    db.update_chain_partial("ethereum", &update).await.unwrap();

    let chain = db.get_chain("ethereum").await.unwrap().unwrap();
    let config_lock = chain.config();
    let cfg = config_lock.read().unwrap();
    assert_eq!(cfg.rpc_urls, vec!["https://new-rpc.com".to_string()]);
    assert_eq!(cfg.last_processed_block, 5000);
    assert_eq!(cfg.block_lag, 5);
}

#[tokio::test]
async fn update_chain_partial_nonexistent_fails() {
    let db = mock_db();
    let update = PartialChainUpdate {
        rpc_urls: None,
        last_processed_block: Some(100),
        xpub: None,
        block_lag: None,
        required_confirmations: None,
        active: None,
        logo_url: None,
    };
    let result = db.update_chain_partial("ghost", &update).await;
    assert!(result.is_err());
}

// ===========================================================================
// Token CRUD
// ===========================================================================

#[tokio::test]
async fn add_and_get_token() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    db.add_token("ethereum", &usdt_token()).await.unwrap();

    let token = db.get_token("ethereum", "USDT").await.unwrap();
    assert!(token.is_some());
    assert_eq!(token.unwrap().decimals, 6);
}

#[tokio::test]
async fn get_token_nonexistent_returns_none() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let token = db.get_token("ethereum", "FAKE").await.unwrap();
    assert!(token.is_none());
}

#[tokio::test]
async fn get_token_by_contract() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_token("ethereum", &usdt_token()).await.unwrap();

    let token = db
        .get_token_by_contract("ethereum", &usdt_token().contract)
        .await
        .unwrap();
    assert!(token.is_some());
    assert_eq!(token.unwrap().symbol, "USDT");
}

#[tokio::test]
async fn get_tokens_list() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_token("ethereum", &usdt_token()).await.unwrap();
    db.add_token(
        "ethereum",
        &TokenConfig {
            symbol: "USDC".into(),
            contract: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".into(),
            decimals: 6,
            logo_url: None,
        },
    )
    .await
    .unwrap();

    let tokens = db.get_tokens("ethereum").await.unwrap().unwrap();
    assert_eq!(tokens.len(), 2);
}

#[tokio::test]
async fn get_token_contracts() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_token("ethereum", &usdt_token()).await.unwrap();

    let contracts = db.get_token_contracts("ethereum").await.unwrap().unwrap();
    assert_eq!(contracts.len(), 1);
    assert_eq!(contracts[0], usdt_token().contract);
}

#[tokio::test]
async fn remove_token() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_token("ethereum", &usdt_token()).await.unwrap();

    db.remove_token("ethereum", "USDT").await.unwrap();

    let token = db.get_token("ethereum", "USDT").await.unwrap();
    assert!(token.is_none());
}

#[tokio::test]
async fn get_chains_with_token_native() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let chains = db.get_chains_with_token("ETH").await.unwrap();
    assert_eq!(chains.len(), 1);
}

#[tokio::test]
async fn get_chains_with_token_erc20() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_token("ethereum", &usdt_token()).await.unwrap();

    let chains = db.get_chains_with_token("USDT").await.unwrap();
    assert_eq!(chains.len(), 1);
}

#[tokio::test]
async fn get_chains_with_token_not_found() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let chains = db.get_chains_with_token("DOGE").await.unwrap();
    assert!(chains.is_empty());
}

// ===========================================================================
// Watch addresses
// ===========================================================================

#[tokio::test]
async fn add_and_get_watch_addresses() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    db.add_watch_address("ethereum", "0xABC").await.unwrap();
    db.add_watch_address("ethereum", "0xDEF").await.unwrap();

    let addrs = db.get_watch_addresses("ethereum").await.unwrap().unwrap();
    assert_eq!(addrs.len(), 2);
    assert!(addrs.contains(&"0xABC".to_string()));
    assert!(addrs.contains(&"0xDEF".to_string()));
}

#[tokio::test]
async fn remove_watch_address() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_watch_address("ethereum", "0xABC").await.unwrap();

    db.remove_watch_address("ethereum", "0xABC").await.unwrap();

    let addrs = db.get_watch_addresses("ethereum").await.unwrap().unwrap();
    assert!(addrs.is_empty());
}

#[tokio::test]
async fn remove_watch_addresses_bulk() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_watch_address("ethereum", "0xA").await.unwrap();
    db.add_watch_address("ethereum", "0xB").await.unwrap();
    db.add_watch_address("ethereum", "0xC").await.unwrap();

    db.remove_watch_addresses_bulk("ethereum", &["0xA".into(), "0xC".into()])
        .await
        .unwrap();

    let addrs = db.get_watch_addresses("ethereum").await.unwrap().unwrap();
    assert_eq!(addrs.len(), 1);
    assert!(addrs.contains(&"0xB".to_string()));
}

#[tokio::test]
async fn watch_address_on_nonexistent_chain_fails() {
    let db = mock_db();
    let result = db.add_watch_address("ghost", "0xABC").await;
    assert!(result.is_err());
}

// ===========================================================================
// Chain metadata accessors
// ===========================================================================

#[tokio::test]
async fn get_xpub() {
    let db = mock_db();
    let cfg = evm_chain_config("ethereum");
    let expected_xpub = cfg.xpub.clone();
    db.add_chain(&cfg).await.unwrap();

    let xpub = db.get_xpub("ethereum").await.unwrap();
    assert_eq!(xpub, Some(expected_xpub));
}

#[tokio::test]
async fn get_rpc_urls() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let urls = db.get_rpc_urls("ethereum").await.unwrap();
    assert_eq!(urls, Some(vec!["https://rpc.example.com".to_string()]));
}

#[tokio::test]
async fn get_block_lag() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let lag = db.get_block_lag("ethereum").await.unwrap();
    assert_eq!(lag, Some(3));
}

#[tokio::test]
async fn get_required_confirmations() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let confirmations = db.get_required_confirmations("ethereum").await.unwrap();
    assert_eq!(confirmations, Some(12));
}

// ===========================================================================
// Invoice CRUD
// ===========================================================================

#[tokio::test]
async fn add_and_get_invoice() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0x1234", InvoiceStatus::Pending);

    db.add_invoice(&inv).await.unwrap();

    let fetched = db.get_invoice("inv-1").await.unwrap();
    assert!(fetched.is_some());
    assert_eq!(fetched.unwrap().id, "inv-1");
}

#[tokio::test]
async fn add_duplicate_invoice_fails() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0x1234", InvoiceStatus::Pending);

    db.add_invoice(&inv).await.unwrap();
    let result = db.add_invoice(&inv).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn get_nonexistent_invoice_returns_none() {
    let db = mock_db();
    let fetched = db.get_invoice("ghost").await.unwrap();
    assert!(fetched.is_none());
}

#[tokio::test]
async fn set_invoice_status() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0x1234", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    db.set_invoice_status("inv-1", InvoiceStatus::Paid)
        .await
        .unwrap();

    let fetched = db.get_invoice("inv-1").await.unwrap().unwrap();
    assert_eq!(fetched.status, InvoiceStatus::Paid);
}

#[tokio::test]
async fn set_invoice_status_nonexistent_fails() {
    let db = mock_db();
    let result = db
        .set_invoice_status("ghost", InvoiceStatus::Paid)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn get_pending_invoice_by_address() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0xABC", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    let found = db
        .get_pending_invoice_by_address("ethereum", "0xABC")
        .await
        .unwrap();
    assert!(found.is_some());

    let not_found = db
        .get_pending_invoice_by_address("ethereum", "0xXYZ")
        .await
        .unwrap();
    assert!(not_found.is_none());
}

#[tokio::test]
async fn get_pending_invoice_by_address_excludes_non_pending() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0xABC", InvoiceStatus::Paid);
    db.add_invoice(&inv).await.unwrap();

    let found = db
        .get_pending_invoice_by_address("ethereum", "0xABC")
        .await
        .unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn expire_old_invoices() {
    let db = mock_db();
    let expired_inv = make_expired_invoice("inv-old", "ethereum", "0xOLD");
    let fresh_inv = make_invoice("inv-fresh", "ethereum", "0xFRESH", InvoiceStatus::Pending);

    db.add_invoice(&expired_inv).await.unwrap();
    db.add_invoice(&fresh_inv).await.unwrap();

    let expired = db.expire_old_invoices().await.unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].0, "inv-old");

    let old = db.get_invoice("inv-old").await.unwrap().unwrap();
    assert_eq!(old.status, InvoiceStatus::Expired);

    let fresh = db.get_invoice("inv-fresh").await.unwrap().unwrap();
    assert_eq!(fresh.status, InvoiceStatus::Pending);
}

#[tokio::test]
async fn is_invoice_expired() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0x1", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    assert_eq!(db.is_invoice_expired("inv-1").await.unwrap(), Some(false));

    db.set_invoice_status("inv-1", InvoiceStatus::Expired)
        .await
        .unwrap();
    assert_eq!(db.is_invoice_expired("inv-1").await.unwrap(), Some(true));
}

#[tokio::test]
async fn is_invoice_paid() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0x1", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    assert_eq!(db.is_invoice_paid("inv-1").await.unwrap(), Some(false));

    db.set_invoice_status("inv-1", InvoiceStatus::Paid)
        .await
        .unwrap();
    assert_eq!(db.is_invoice_paid("inv-1").await.unwrap(), Some(true));
}

#[tokio::test]
async fn is_invoice_pending() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0x1", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    assert_eq!(db.is_invoice_pending("inv-1").await.unwrap(), Some(true));
}

#[tokio::test]
async fn cancel_invoice() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0x1", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    db.cancel_invoice("inv-1").await.unwrap();

    let fetched = db.get_invoice("inv-1").await.unwrap().unwrap();
    assert_eq!(fetched.status, InvoiceStatus::Cancelled);
}

#[tokio::test]
async fn get_busy_indexes() {
    let db = mock_db();
    let inv1 = Invoice {
        address_index: 0,
        ..make_invoice("inv-1", "ethereum", "0xA", InvoiceStatus::Pending)
    };
    let inv2 = Invoice {
        address_index: 3,
        ..make_invoice("inv-2", "ethereum", "0xB", InvoiceStatus::Pending)
    };
    let inv3 = Invoice {
        address_index: 5,
        ..make_invoice("inv-3", "ethereum", "0xC", InvoiceStatus::Paid)
    };

    db.add_invoice(&inv1).await.unwrap();
    db.add_invoice(&inv2).await.unwrap();
    db.add_invoice(&inv3).await.unwrap();

    let busy = db.get_busy_indexes("ethereum").await.unwrap();
    assert!(busy.contains(&0));
    assert!(busy.contains(&3));
    assert!(!busy.contains(&5)); // Paid, not Pending
}

// ===========================================================================
// Invoice filtering & pagination
// ===========================================================================

#[tokio::test]
async fn get_invoices_filter_by_status() {
    let db = mock_db();
    db.add_invoice(&make_invoice("inv-1", "eth", "0xA", InvoiceStatus::Pending))
        .await
        .unwrap();
    db.add_invoice(&make_invoice("inv-2", "eth", "0xB", InvoiceStatus::Paid))
        .await
        .unwrap();

    let filter = InvoiceFilter {
        status: Some(InvoiceStatus::Pending),
        pagination: Pagination { limit: 100, offset: 0 },
        ..Default::default()
    };

    let result = db.get_invoices(filter).await.unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.items[0].id, "inv-1");
}

#[tokio::test]
async fn get_invoices_filter_by_network() {
    let db = mock_db();
    db.add_invoice(&make_invoice("inv-1", "ethereum", "0xA", InvoiceStatus::Pending))
        .await
        .unwrap();
    db.add_invoice(&make_invoice("inv-2", "polygon", "0xB", InvoiceStatus::Pending))
        .await
        .unwrap();

    let filter = InvoiceFilter {
        network: Some("polygon".into()),
        pagination: Pagination { limit: 100, offset: 0 },
        ..Default::default()
    };

    let result = db.get_invoices(filter).await.unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.items[0].network, "polygon");
}

#[tokio::test]
async fn get_invoices_pagination() {
    let db = mock_db();
    for i in 0..10 {
        let inv = Invoice {
            created_at: Utc::now() + Duration::seconds(i),
            ..make_invoice(&format!("inv-{}", i), "eth", &format!("0x{}", i), InvoiceStatus::Pending)
        };
        db.add_invoice(&inv).await.unwrap();
    }

    let filter = InvoiceFilter {
        pagination: Pagination { limit: 3, offset: 0 },
        ..Default::default()
    };

    let result = db.get_invoices(filter).await.unwrap();
    assert_eq!(result.items.len(), 3);
    assert_eq!(result.total, 10);
    assert_eq!(result.limit, 3);
    assert_eq!(result.offset, 0);
}

// ===========================================================================
// Payments
// ===========================================================================

#[tokio::test]
async fn add_payment_attempt_and_get_confirming() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0xABC", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    db.add_payment_attempt(
        "inv-1",
        "0xSender",
        "0xABC",
        "0xtxhash123",
        U256::from(500_000_000_000_000_000u128),
        12345,
        "ethereum",
        "ETH",
        None,
    )
    .await
    .unwrap();

    let confirming = db.get_confirming_payments().await.unwrap();
    assert_eq!(confirming.len(), 1);
    assert_eq!(confirming[0].invoice_id, "inv-1");
    assert_eq!(confirming[0].status, PaymentStatus::Confirming);
}

#[tokio::test]
async fn finalize_payment_partial() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0xABC", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    db.add_payment_attempt(
        "inv-1",
        "0xSender",
        "0xABC",
        "0xtx1",
        U256::from(500_000_000_000_000_000u128), // 0.5 ETH, invoice needs 1 ETH
        100,
        "ethereum",
        "ETH",
        None,
    )
    .await
    .unwrap();

    let payments = db.get_confirming_payments().await.unwrap();
    let payment_id = &payments[0].id;

    let is_fully_paid = db.finalize_payment(payment_id).await.unwrap();
    assert!(!is_fully_paid);

    let inv = db.get_invoice("inv-1").await.unwrap().unwrap();
    assert_eq!(inv.status, InvoiceStatus::Pending);
    assert!(inv.paid_raw > U256::ZERO);
}

#[tokio::test]
async fn finalize_payment_full() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0xABC", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    db.add_payment_attempt(
        "inv-1",
        "0xSender",
        "0xABC",
        "0xtx1",
        U256::from(1_000_000_000_000_000_000u128), // exactly 1 ETH
        100,
        "ethereum",
        "ETH",
        None,
    )
    .await
    .unwrap();

    let payments = db.get_confirming_payments().await.unwrap();
    let payment_id = &payments[0].id;

    let is_fully_paid = db.finalize_payment(payment_id).await.unwrap();
    assert!(is_fully_paid);

    let inv = db.get_invoice("inv-1").await.unwrap().unwrap();
    assert_eq!(inv.status, InvoiceStatus::Paid);
}

#[tokio::test]
async fn finalize_payment_overpay() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0xABC", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    db.add_payment_attempt(
        "inv-1",
        "0xSender",
        "0xABC",
        "0xtx1",
        U256::from(2_000_000_000_000_000_000u128), // 2 ETH, invoice is 1 ETH
        100,
        "ethereum",
        "ETH",
        None,
    )
    .await
    .unwrap();

    let payments = db.get_confirming_payments().await.unwrap();
    let payment_id = &payments[0].id;

    let is_fully_paid = db.finalize_payment(payment_id).await.unwrap();
    assert!(is_fully_paid);
}

#[tokio::test]
async fn cancel_payment() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0xABC", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    db.add_payment_attempt(
        "inv-1", "0xS", "0xABC", "0xtx1",
        U256::from(100u64), 50, "ethereum", "ETH", None,
    )
    .await
    .unwrap();

    // NOTE: MockDatabase has bug B-02 -- payments DashMap is keyed by invoice_id,
    // not payment_id. cancel_payment(payment_id) will fail because the lookup key
    // doesn't match. We use the invoice_id (the actual DashMap key) to work around it.
    db.cancel_payment("inv-1").await.unwrap();

    let confirming = db.get_confirming_payments().await.unwrap();
    assert!(confirming.is_empty());
}

// ===========================================================================
// Payment filtering
// ===========================================================================

#[tokio::test]
async fn get_payments_filter_by_network() {
    let db = mock_db();
    let inv = make_invoice("inv-1", "ethereum", "0xA", InvoiceStatus::Pending);
    db.add_invoice(&inv).await.unwrap();

    db.add_payment_attempt(
        "inv-1", "0xS", "0xA", "0xtx1",
        U256::from(100u64), 50, "ethereum", "ETH", None,
    )
    .await
    .unwrap();

    let filter = PaymentFilter {
        network: Some("ethereum".into()),
        pagination: Pagination { limit: 100, offset: 0 },
        ..Default::default()
    };
    let result = db.get_payments(filter).await.unwrap();
    assert_eq!(result.total, 1);

    let filter_empty = PaymentFilter {
        network: Some("polygon".into()),
        pagination: Pagination { limit: 100, offset: 0 },
        ..Default::default()
    };
    let result_empty = db.get_payments(filter_empty).await.unwrap();
    assert_eq!(result_empty.total, 0);
}

// ===========================================================================
// Webhooks
// ===========================================================================

#[tokio::test]
async fn add_and_select_webhook_job() {
    let db = mock_db();
    let inv = make_uuid_invoice("ethereum", "0xA", InvoiceStatus::Pending);
    let inv_id = inv.id.clone();
    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::TxDetected {
        invoice_id: inv_id.clone(),
        tx_hash: "0xtx1".into(),
        amount: "1.0".into(),
        currency: "ETH".into(),
    };

    db.add_webhook_job(&inv_id, &event).await.unwrap();

    let jobs = db.select_webhooks_job().await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].url, "https://example.com/webhook");
}

#[tokio::test]
async fn webhook_job_status_transitions() {
    let db = mock_db();
    let inv = make_uuid_invoice("ethereum", "0xA", InvoiceStatus::Pending);
    let inv_id = inv.id.clone();
    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::InvoiceExpired {
        invoice_id: inv_id.clone(),
    };

    db.add_webhook_job(&inv_id, &event).await.unwrap();

    let jobs = db.select_webhooks_job().await.unwrap();
    let job_id = jobs[0].id.to_string();

    db.set_webhook_status(&job_id, WebhookStatus::Sent)
        .await
        .unwrap();

    let wh = db.get_webhook(&job_id).await.unwrap().unwrap();
    assert_eq!(wh.status, WebhookStatus::Sent);
}

#[tokio::test]
async fn schedule_webhook_retry() {
    let db = mock_db();
    let inv = make_uuid_invoice("ethereum", "0xA", InvoiceStatus::Pending);
    let inv_id = inv.id.clone();
    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::InvoicePaid {
        invoice_id: inv_id.clone(),
        paid_amount: "1.0".into(),
    };

    db.add_webhook_job(&inv_id, &event).await.unwrap();

    let jobs = db.select_webhooks_job().await.unwrap();
    let job_id = jobs[0].id.to_string();

    db.schedule_webhook_retry(&job_id, 2, 4.0).await.unwrap();

    let wh = db.get_webhook(&job_id).await.unwrap().unwrap();
    assert_eq!(wh.status, WebhookStatus::Pending);
    assert_eq!(wh.attempts, 2);
}

#[tokio::test]
async fn cancel_webhook() {
    let db = mock_db();
    let inv = make_uuid_invoice("ethereum", "0xA", InvoiceStatus::Pending);
    let inv_id = inv.id.clone();
    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::InvoiceExpired {
        invoice_id: inv_id.clone(),
    };
    db.add_webhook_job(&inv_id, &event).await.unwrap();

    let jobs = db.select_webhooks_job().await.unwrap();
    let job_id = jobs[0].id.to_string();

    db.cancel_webhook(&job_id).await.unwrap();

    let wh = db.get_webhook(&job_id).await.unwrap().unwrap();
    assert_eq!(wh.status, WebhookStatus::Cancelled);
}

#[tokio::test]
async fn webhook_no_url_silently_succeeds_in_mock() {
    let db = mock_db();
    let inv_id = uuid::Uuid::new_v4().to_string();
    let inv = Invoice {
        id: inv_id.clone(),
        webhook_url: None,
        ..make_uuid_invoice("ethereum", "0xA", InvoiceStatus::Pending)
    };
    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::InvoiceExpired {
        invoice_id: inv_id.clone(),
    };

    // Mock silently returns Ok when no URL is set (differs from Postgres, see audit B-09)
    let result = db.add_webhook_job(&inv_id, &event).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn get_webhooks_filter_by_status() {
    let db = mock_db();
    let inv = make_uuid_invoice("ethereum", "0xA", InvoiceStatus::Pending);
    let inv_id = inv.id.clone();
    db.add_invoice(&inv).await.unwrap();

    let event = WebhookEvent::InvoiceExpired {
        invoice_id: inv_id.clone(),
    };
    db.add_webhook_job(&inv_id, &event).await.unwrap();

    let filter = WebhookFilter {
        status: Some(WebhookStatus::Pending),
        pagination: Pagination { limit: 100, offset: 0 },
        ..Default::default()
    };
    let result = db.get_webhooks(filter).await.unwrap();
    assert_eq!(result.total, 1);

    let filter_sent = WebhookFilter {
        status: Some(WebhookStatus::Sent),
        pagination: Pagination { limit: 100, offset: 0 },
        ..Default::default()
    };
    let result_sent = db.get_webhooks(filter_sent).await.unwrap();
    assert_eq!(result_sent.total, 0);
}

// ===========================================================================
// Token decimals caching
// ===========================================================================

#[tokio::test]
async fn get_token_decimals_native() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let decimals = db.get_token_decimals("ethereum", "ETH").await.unwrap();
    assert_eq!(decimals, Some(18));
}

#[tokio::test]
async fn get_token_decimals_erc20() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();
    db.add_token("ethereum", &usdt_token()).await.unwrap();

    let decimals = db.get_token_decimals("ethereum", "USDT").await.unwrap();
    assert_eq!(decimals, Some(6));
}

#[tokio::test]
async fn get_token_decimals_nonexistent() {
    let db = mock_db();
    db.add_chain(&evm_chain_config("ethereum")).await.unwrap();

    let decimals = db.get_token_decimals("ethereum", "FAKE").await.unwrap();
    assert_eq!(decimals, None);
}

#[tokio::test]
async fn get_token_decimals_nonexistent_chain() {
    let db = mock_db();

    let decimals = db.get_token_decimals("ghost", "ETH").await.unwrap();
    assert_eq!(decimals, None);
}
