<div align="center">
  <a href="https://github.com/necko-moe/necko3-core">
    <img src=".github/static/necko3-3-2-round.png" alt="necko3-3-2-round.png" width="256"/>
  </a>
  <h1>necko3-core</h1>
  <p align="center">
   <a href="https://crates.io/crates/necko3-core">
      <img src="https://img.shields.io/crates/v/necko3-core.svg?style=flat-square" alt="Crates.io">
    </a>
    <a href="https://docs.rs/necko3-core">
      <img src="https://img.shields.io/docsrs/necko3-core?style=flat-square" alt="docs.rs">
    </a>
    <a href="https://github.com/necko-moe/necko3-core/actions">
      <img src="https://img.shields.io/github/actions/workflow/status/necko-moe/necko3-core/ci.yml?branch=main&style=flat-square" alt="CI Status">
    </a>
    <a href="https://opensource.org/licenses/MIT">
      <img src="https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square" alt="License: MIT">
    </a>
    <img src="https://img.shields.io/badge/rustc-1.94.1+-ab6000.svg?style=flat-square&logo=rust" alt="MSRV">
    <a href="https://github.com/necko-moe/necko3-core/stargazers">
       <img src="https://img.shields.io/github/stars/necko-moe/necko3-core?style=social" alt="GitHub stars">
     </a>
  </p>
</div>

***

## About

**necko3-core** is the beating heart of the necko3 project. What started as a crate is now a **modular, event-driven Cargo workspace** containing every ounce of business logic for crypto payment processing, invoice management, deep blockchain listening, and webhook delivery. No binary, no HTTP server, no opinions on how you serve your API. Just the raw, concentrated core.

Think of it as the engine without the chassis. You bring the web framework, plug in a database adapter, hand over your xpub, and `NeckoCore` takes care of the rest. It derives deposit addresses, tracks blocks in real-time with flawless RPC failover, handles complex chain reorgs natively, matches incoming transactions to invoices, and screams at your webhook endpoint until it responds with a `200 OK`.

For a fully assembled, production-ready integration, check out [necko3-backend](https://github.com/necko-moe/necko3-backend) — it wraps this workspace into an Axum web server with REST API, Swagger, and all the bells and whistles.

### Key Features

1. **Core & Architecture**
   * **Framework Agnostic:** Pure indexer with additional business logic. Bring your own web framework (Axum, Actix, etc.).
   * **Event-Driven & Async:** Tokio-powered centralized orchestrator with unbounded event listeners and structured tracing.
   * **Modular Design:** Clean separation of concerns across focused crates (`core`, `database`, `blockchain`, `types`).

2. **Blockchain Engine**
   * **Multi-RPC Failover:** Automatic node rotation on downtime or provider errors.
   * **Advanced Reorg Handling:** Deep block-level reorg detection and phantom transaction protection.
   * **Smart Tracking:** Optimized EVM log processing (ERC-20 & native coins) with zero missed events and built-in rate limit protection.

3. **Data & Storage**
   * **Swappable Backends:** Trait-driven design shipping with `PostgresAdapter` (auto-migrations) and `InMemoryAdapter`.
   * **Smart Decorators:** Lock-free, ultra-fast caching (`CachedDb`) and automatic event dispatching (`NotifyingDb`).
   * **State Recovery:** Indexed block hash persistence for seamless resumption after restarts.

4. **Payments & Billing**
   * **HD Address Derivation:** Generates unique deposit addresses via BIP32 from a single xpub. Zero private keys stored.
   * **Full Invoice Lifecycle:** Tracks detailed payment states (Confirming, Confirmed, Pending, Reorged, Failed).
   * **Automated Cleanup:** Built-in (but optional) Janitor service to expire old invoices and release watched addresses.

5. **Webhooks**
   * **Secure & Resilient:** HMAC-SHA256 signed payloads with exponential backoff retries for failed deliveries.
   * **Safe Concurrency:** Semaphore-controlled dispatcher processes queues efficiently without self-DDoS.
   * **Granular Triggers:** Dedicated events for `TxDetected`, `TxConfirmed`, `InvoicePaid`, and `InvoiceExpired`.

## Architecture

### Workspace Structure

| Crate               | Description                                                                                                                                                               |
|:--------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `necko3-core`       | The interface. Contains `NeckoCore`, `NeckoCoreBuilder`, the central Orchestrator event loops, and background tasks (Janitor & Webhook Dispatcher).                       |
| `necko3-database`   | Database traits (`DatabaseStore`, `DatabaseExt`), implementations (`PostgresAdapter`, `InMemoryAdapter`), and decorators (`CachedDb`, `NotifyingDb`).                     |
| `necko3-blockchain` | Chain interaction (`BlockchainAdapter`, `BlockchainWorker`). Currently powers the `EvmBlockchain` with block tracking, reorg protection, log filtering, and RPC rotation. |
| `necko3-types`      | Pure domain models: `Invoice`, `Payment`, `ChainData`, `TokenData`, `WebhookEvent`, etc.                                                                                  |

### Architecture Breakdown

#### Initialization
To start, call `NeckoCore::builder()`. You can configure it or immediately call `build().await`. By default, it uses an empty `InMemoryAdapter` as storage and only emits core events (`CoreEvent`).

#### Background Services
Calling `build` spawns the following background services:

* **Janitor** (Optional, stupid): Periodically signals the database to update overdue `Pending` invoices to `Expired`.
* **Webhook Dispatcher** (Optional, smart): Handles queued webhook events from the database and automatically schedules retries.
* **Orchestrator**: The core component that ties the entire application together.

#### The Orchestrator
Manages worker lifecycles (initialization, termination) and listens to database events (`DbEvent`) and blockchain worker events (`ChainEvent`). It handles the heavy lifting, saving you from writing massive amounts of boilerplate code and building your own indexer from scratch.

#### Components & Workflow

* **Database Events (`DbEvent`)**: During the build process, the database adapter is wrapped in `NotifyingDb<D>`. This wrapper automatically emits an event upon every database update.
* **Blockchain Workers**: Started during initialization or when a `DbEvent` signals a new chain has been added. They are responsible for sequential block parsing, reorganization (reorg) tracking, reading token transactions/logs, and emitting events such as `TransactionDetected`, `BlockProcessed`, and `BlocksReorged`.
* **Dynamic State Updates**: When a DB event indicates a parameter change, a `StateCommand` is sent to the corresponding worker to update its internal state. This allows applying changes dynamically without restarting the workers.
* **Transaction Tracking**: To handle reorgs and ensure finality, a `TrackTransaction` command (containing the tx hash, block number/hash, and required confirmation block count) is sent to the blockchain worker. The orchestrator usually triggers this immediately after receiving a `TransactionDetected` event from that same worker.

<div align="center">
    <details>
        <summary><i>screaming at your webhook endpoint until it responds with a 200:</i></summary>
        <img src=".github/static/webhook-dispatcher.png" width="263" alt="lion screaming at apple meme"/>
    </details>
</div>


## Usage Example

The reference integration lives at [necko3-backend](https://github.com/necko-moe/necko3-backend).

Add the necko3-core interface crate to your `Cargo.toml`:

```toml
[dependencies]
necko3-core = "0.2"
```

### Creating indexer and Tracking addresses

If you want to set up a standalone blockchain indexer to monitor specific addresses and track transaction lifecycles (detection, confirmation, and chain reorgs), here is how to initialize the core database and listen to the event stream:

```rust
use necko3_core::builder::chain_config::{ChainConfig, EvmBlockchain};
use necko3_core::builder::token_config::TokenConfig;
use necko3_core::core::NeckoCore;
use necko3_core::prelude::db::backends::PostgresAdapter;
use necko3_core::prelude::db::cached::CachedDb;
use necko3_core::prelude::db::traits::{ChainStore, DatabaseAdapter, DatabaseExt};
use necko3_core::types::CoreEvent;
use tracing_subscriber::EnvFilter;

async fn init_db<D: DatabaseExt>(core: NeckoCore<D, CoreEvent>) -> anyhow::Result<()> {
    // Configure and register the Arbitrum chain in the system
    core.add_chain(
        ChainConfig::from(EvmBlockchain)
            .with_name("Arbitrum One") // Internal reference name
            .with_symbol("ETH")        // Native gas token
            // Multiple RPC URLs can be provided for fallback/redundancy
            .with_rpc_urls(vec![
                "https://arbitrum.drpc.org",
                "https://arb1.arbitrum.io/rpc",
                "https://rpc.ankr.com/arbitrum",
                // ...
                "https://arbitrum-one-rpc.publicnode.com",
            ])
            // Number of blocks to lag behind the chain tip to avoid minor reorgs
            .with_block_lag(5)
            // Number of blocks required for a transaction to be considered fully "confirmed"
            .with_required_confirmations(10)
            // Register an ERC-20 token (e.g., USDT) to track its transfers
            .add_token(TokenConfig::new(
                "USDT",
                "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
                6
            ))
            // Statically define which wallet addresses the indexer should monitor
            .with_watch_addresses(vec![
                "0xB38e8c17e38363aF6EbdCb3dAE12e0243582891D",
                "0x3931dAb967C3E2dbb492FE12460a66d0fe4cC857",
                "0x25681Ab599B4E2CEea31F8B498052c53FC2D74db"
            ])
    ).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize standard logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info,necko3-blockchain=warn"))
        .init();

    // Connect to persistent PostgreSQL storage
    let db = PostgresAdapter::new(
        "postgres://necko_user:password@127.0.0.1:5432/necko3",
        20
    ).await?;

    // Build the core, wrapping the DB in a cache layer for performance
    let core = NeckoCore::builder()
        .with_storage(CachedDb::new(db))
        .build()
        .await;

    let db = core.db();

    // Example cleanup: remove the chain state if it already exists
    if db.chain_exists("Arbitrum One").await? {
        db.remove_chain("Arbitrum One").await?;
    }

    // Apply the chain and token configurations
    init_db(core.clone()).await?;

    // Subscribe to the event bus with a channel capacity of 1000
    let (_id, mut event_rx) = core.subscribe(1000);

    // Listen and react to core events from the indexer
    while let Some(event) = event_rx.recv().await {
        match event {
            // Triggered as soon as the transaction is included in a block (0-conf)
            CoreEvent::TransactionDetected(tx_data) => {
                println!("{} | Received transaction from '{}' to '{}'! Asset: {}",
                         tx_data.block_number, tx_data.from, tx_data.to, tx_data.asset);
            }
            // Triggered when the transaction reaches clean `required_confirmations` depth
            CoreEvent::TransactionConfirmed { tx_hash, confirmed_after, block_number, .. } => {
                println!("{} | Transaction {} confirmed after {} blocks.",
                         block_number, tx_hash, confirmed_after);
            }
            // Triggered if the chain experiences a rollback (reorg) affecting a tracked transaction
            CoreEvent::TransactionReorged { tx_hash, new_block_number, .. } => {
                let latest_block = core.db().get_latest_block("Arbitrum One").await?.unwrap();

                println!("{} | Transaction {} just reorged! New block_number: {}",
                         latest_block, tx_hash, new_block_number)
            }
            _ => {}
        }
    }

    Ok(())
}
```

### Creating and Tracking Invoices

If you want to go beyond pure blockchain listening and utilize the built-in billing engine, here is how you generate an invoice, tie it to a dynamically generated address, and wait for the payment event:

```rust
use necko3_core::builder::chain_config::{ChainConfig, EvmBlockchain};
use necko3_core::builder::invoice_config::error::InvoiceCreationError;
use necko3_core::builder::invoice_config::{ExpirationTime, PaymentAddress, PaymentAsset, PaymentSpec};
use necko3_core::builder::token_config::TokenConfig;
use necko3_core::core::NeckoCore;
use necko3_core::prelude::db::DatabaseExt;
use necko3_core::types::core::Invoice;
use necko3_core::types::{CoreEvent, ExternalEvent, NeckoEvent};
use std::time::Duration;
use tracing_subscriber::EnvFilter;

async fn create_new_invoice<D: DatabaseExt>(
   core: NeckoCore<D, NeckoEvent>
) -> Result<Invoice, InvoiceCreationError> {
   core.create_invoice(
      // Generate a new invoice demanding 0.005 USDC on Polygon
      PaymentSpec::new("Polygon Mainnet", "0.005")
              .with_asset(PaymentAsset::Token("USDC".into())),
      // Automatically derive a fresh, unique address for this invoice, using xpub from the chain
      PaymentAddress::GenerateNew,
      // Without webhooks
      None,
      // Set an expiration time for the payment window
      ExpirationTime::Duration(Duration::from_hours(1))
   ).await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
   tracing_subscriber::fmt()
           .with_env_filter(EnvFilter::new("info,necko3-blockchain=warn"))
           .init();

   // Initialize core and configure Polygon Mainnet inline
   let core = NeckoCore::builder()
           .with_event_type::<NeckoEvent>()
           .add_chain(ChainConfig::from(EvmBlockchain)
                   .with_name("Polygon Mainnet")
                   .with_symbol("POL")
                   // Provide a BIP-32 Extended Public Key (xpub). 
                   // The core will use this to derive unique deposit addresses for invoices.
                   .with_xpub("xpub6...NFGcvN")
                   .with_rpc_urls(vec![
                      "https://rpc.satelink.network/rpc/polygon",
                      "https://polygon-bor-rpc.publicnode.com",
                      // ...
                      "https://1rpc.io/matic",
                   ])
                   .with_block_lag(3)
                   .with_required_confirmations(40)
                   .add_token(TokenConfig::new("USDC",
                                               "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
                                               6))
           ).await?
           .build()
           .await;

   // Create the first invoice to start the payment process
   let invoice = create_new_invoice(core.clone()).await?;

   println!("Service started. Pay 0.005 USDC (Polygon Mainnet) on '{}'", invoice.address);

   // Subscribe to the event stream (now expecting combined NeckoEvents)
   let (_id, mut event_rx) = core.subscribe(1000);

   // Listen and react to multiplexed events
   while let Some(event) = event_rx.recv().await {
      match event {
         // Underlying blockchain events are nested inside `NeckoEvent::Core`
         NeckoEvent::Core(CoreEvent::TransactionDetected(tx_data)) => {
            println!("{} | Received transaction from '{}' to '{}'! Asset: {}",
                     tx_data.block_number, tx_data.from, tx_data.to, tx_data.asset);
         }
         NeckoEvent::Core(CoreEvent::TransactionConfirmed { tx_hash, confirmed_after,
                             block_number, .. }) => {
            println!("{} | Transaction {} confirmed after {} blocks.",
                     block_number, tx_hash, confirmed_after);
         }
         // High-level business logic events are nested inside `NeckoEvent::Ext`
         NeckoEvent::Ext(ExternalEvent::InvoicePaid { invoice_id }) => {
            // The invoice was successfully paid in full!
            let new_invoice = create_new_invoice(core.clone()).await?;

            println!("Invoice '{}' got paid! Created new invoice. Address: '{}', amount: {} {} ({})",
                     invoice_id, new_invoice.address, new_invoice.amount, new_invoice.token, new_invoice.network)
         }
         _ => {}
      }
   }

   Ok(())
}
```

## Contributing

I'd be happy to see any feedback.<br />
Found a bug? <a href=https://github.com/necko-moe/necko3-backend/issues/new>Open an Issue</a>.<br />
Want to add a feature? Fork it and send a PR
(or just <a href=https://github.com/necko-moe/necko3-backend/issues/new>Open an Issue</a> and write whatever you want)

## License

The project and all repositories are distributed under the **MIT License**. Feel free to use, modify, and distribute <3

* * *

<div align="center">
  <h1>SUPPORT PROJECT</h1>
  <p>Want to make necko1 employed or donate enough for a Triple Whopper? Contact me -> <a href=https://t.me/everyonehio>Telegram</a> or <a href="mailto:meow@necko.moe">Mail me</a> (I rarely check that)</p>
  <p>I don't accept direct card transfers, but you can feed me some stablecoins:</p>
    <ul style="list-style-type: none; padding: 0;">
      <li><b>USDT (TRC20):</b> <code>THcVNoNu3oaLfssbWbNxXK5rUsLfpPM35D</code></li>
      <li><b>Anything in Ethereum / ERC-20:</b> <code>0x97D596eA81C09aC76a89D495b7bACa7660eb4c73</code></li>
      <li><b>TON:</b> <code>UQDRX9xv1uMxUMe9kkeidWGDkORI4gDx076QIaejtQUjI</code></li>
    </ul>
  <p>
    Broke but still want to help?
    You can just <a href="https://github.com/necko-moe/necko3-backend/stargazers"><b>⭐ Star this repo</b></a> to show your love. It really helps!
  </p>
  <a href="https://github.com/necko-moe">
    <img src=".github/static/necko3-2-200.png" alt="necko3 support banner" width="1024"/>
  </a>
</div>
