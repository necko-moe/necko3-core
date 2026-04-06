<div align="center">
  <a href="https://github.com/necko-moe/necko3-core">
    <img src=".github/static/necko3-3-2-round.png" alt="necko3-3-2-round.png" width="256"/>
  </a>
  <h1>necko3-core</h1>
  <a href="https://github.com/necko-moe/necko3-core/stargazers">
    <img src="https://img.shields.io/github/stars/necko-moe/necko3-core?style=social" alt="GitHub stars">
  </a>
</div>

***

## About

**necko3-core** is the beating heart of the necko3 project — a pure Rust library containing every ounce of business logic: payment processing, blockchain listening, invoice management, transaction confirmation, and webhook delivery. No binary, no HTTP server, no opinions on how you serve your API. Just the raw, concentrated core.

Think of it as the engine without the chassis. You bring the web framework (or don't — nobody's judging), plug in a database, hand over your xpub, and she takes care of the rest: deriving deposit addresses, watching blocks in real-time, matching incoming transactions to invoices, counting confirmations, handling chain reorgs, and screaming at your webhook endpoint until it responds with a 200.

For a fully assembled, production-ready integration, check out [necko3-backend](https://github.com/necko-moe/necko3-backend) — it wraps this crate into an Axum web server with REST API, Swagger, and all the bells and whistles.

### Key Features

- **Trait-based architecture** — `DatabaseAdapter` and `BlockchainAdapter` traits let you swap implementations or build your own without touching core logic.
- **EVM blockchain support** with multi-RPC provider rotation and automatic failover *(your favorite node went down? she already switched to the next one)*.
- **HD address derivation** from xpub via `coins-bip32` — billions of unique deposit addresses, zero stored private keys.
- **0 missed blocks** and proper chain reorg detection with configurable confirmation thresholds and block lag.
- **Full invoice lifecycle** — Pending → Paid / Expired / Cancelled, with automatic expiration cleanup.
- **HMAC-SHA256 signed webhooks** with exponential backoff retries (`2^n` seconds), so your server can take a nap and still get the memo.
- **Postgres** for production, **MockDatabase** for testing — same trait, zero friction.
- **Fully asynchronous** on Tokio. Every background service runs as a spawned task with structured `tracing` instrumentation.

## Architecture

```mermaid
flowchart TB
    subgraph init ["AppState::init"]
        direction TB
        DB["Database (Postgres | Mock)"]
        AS["AppState"]
    end

    AS -->|"spawns"| W["Watcher"]
    AS -->|"spawns"| J["Janitor"]
    AS -->|"spawns"| C["Confirmator"]
    AS -->|"spawns"| WH["Webhook Dispatcher"]
    AS -->|"spawns per chain"| EVM["EvmBlockchain::listen"]

    EVM -->|"PaymentEvent (mpsc)"| W
    W -->|"add_payment_attempt"| DB
    W -->|"TxDetected webhook"| DB

    J -->|"expire_old_invoices"| DB
    J -->|"InvoiceExpired webhook"| DB
    J -->|"remove_watch_addresses"| DB

    C -->|"get_confirming_payments"| DB
    C -->|"verify on-chain"| EVM
    C -->|"finalize_payment"| DB
    C -->|"InvoicePaid / TxConfirmed webhook"| DB

    WH -->|"select_webhooks_job"| DB
    WH -->|"HTTP POST (HMAC-SHA256)"| ExtServer["Your Server"]
```



**How it flows:**

1. `AppState::init` holds the database, creates an mpsc channel (capacity 100), and spawns five categories of background tasks.
2. **EvmBlockchain::listen** polls blocks one-by-one via RPC. For each block it scans native coin transfers and ERC-20 `Transfer` logs to watched addresses, emitting `PaymentEvent` into the channel.
3. **Watcher** receives events, resolves the destination address to a pending invoice, records the payment attempt, and queues a `TxDetected` webhook.
4. **Confirmator** periodically checks all `Confirming` payments. Once the required confirmations are reached, it verifies the transaction receipt on-chain (detecting reorgs if the block changed), finalizes the payment, and fires `TxConfirmed` / `InvoicePaid` webhooks.
5. **Janitor** sweeps expired invoices on a timer, removes their watch addresses, and queues `InvoiceExpired` webhooks.
6. **Webhook Dispatcher** pulls pending jobs from the database and POSTs them with an `X-Webhook-Signature` (HMAC-SHA256) header. Failed deliveries are retried with exponential backoff up to a configurable max.

<div align="center">
    <details>
        <summary><i>screaming at your webhook endpoint until it responds with a 200:</i></summary>
        <img src=".github/static/webhook-dispatcher.png" width="263" alt="lion screaming at apple meme"/>
    </details>
</div>


## Modules


| Module  | Description                                                                                                                                                                                                                 |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `model` | All domain types: `Invoice`, `Payment`, `ChainConfig`, `TokenConfig`, `WebhookEvent`, status enums, filters, and `PaginatedVec<T>`                                                                                          |
| `db`    | `DatabaseAdapter` trait (~30 methods covering chains, tokens, invoices, payments, webhooks) and the `Database` enum dispatching to `Postgres` or `MockDatabase`. Postgres runs embedded SQLx migrations on init             |
| `chain` | `BlockchainAdapter` trait (derive addresses, listen to blocks, verify tx receipts) and the `Blockchain` enum. Currently only `EvmBlockchain` — multi-provider, rotating on failure, with native + ERC-20 transfer detection |
| `state` | `AppState` holds the DB handle, mpsc sender, and active chain listener handles. Spawns `watcher`, `janitor`, `confirmator`, and `webhook` dispatcher as independent Tokio tasks                                             |
| `deps`  | Convenience re-exports from `alloy`: `U256`, `B256`, `parse_units`, `format_units`. So downstream crates don't need to depend on `alloy` directly for common primitives                                                     |


## Usage Example

The reference integration lives at [necko3-backend](https://github.com/necko-moe/necko3-backend). Here's the gist of how to wire it up:

```rust
use necko3_core::db::Database;
use necko3_core::AppState;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Database::init(
        "postgres://user:pass@localhost/necko3",
        5,       // max connections
        "postgres"
    ).await?;

    let state = match AppState::init(
        db,
        "your-api-key",
        Duration::from_secs(30),  // janitor interval
        Duration::from_secs(10),  // confirmator interval
    ).await {
        Ok(state) => state,
        Err(e) => {
            error!(error = %e, "Failed to init AppState");
        },
    };

    // state is Arc<AppState> — pass it to your web framework, do whatever you want
    // all background services are already running

    Ok(())
}
```

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
necko3-core = { git = "https://github.com/necko-moe/necko3-core" }
```

## Contributing

I'd be happy to see any feedback.  

Found a bug? [Open an Issue](https://github.com/necko-moe/necko3-core/issues/new).  

Want to add a feature? Fork it and send a PR.

## License

The project and all repositories are distributed under the **MIT License**. Feel free to use, modify, and distribute <3

***

<div align="center">
  <h1>SUPPORT PROJECT</h1>
  <p>Want to make necko1 employed or donate enough for a Triple Whopper? Contact me -> <a href=https://t.me/everyonehio>Telegram</a> or <a href="mailto:meow@necko.moe">Mail me</a> (I rarely check that). I don't accept direct card transfers, just so you know</p>
  <p>
    Broke but still want to help?
    You can just <a href="https://github.com/necko-moe/necko3-core/stargazers"><b>⭐ Star this repo</b></a> to show your love. It really helps!
  </p>
  <a href="https://github.com/necko-moe">
    <img src=".github/static/necko3-2-200.png" alt="necko3 support banner" width="1024"/>
  </a>
</div>