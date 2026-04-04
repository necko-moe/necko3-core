# necko3-core Security & Logic Audit Report

**Date:** 2026-04-04
**Scope:** All 13 Rust source files, 14 SQL migrations, `Cargo.toml`
**Commit:** HEAD of `master`

---

## Table of Contents

- [Severity Legend](#severity-legend)
- [CRITICAL -- Data / Money Loss Risk](#critical----data--money-loss-risk)
- [SECURITY](#security)
- [BUGS](#bugs)
- [DESIGN / PERFORMANCE](#design--performance)
- [Summary Matrix](#summary-matrix)

---

## Severity Legend

| Level | Meaning |
|-------|---------|
| **CRITICAL** | Can cause loss of funds, data corruption, or complete service failure in production |
| **SECURITY** | Exploitable weakness that may be leveraged by an attacker |
| **BUG** | Incorrect behavior that will surface under normal or edge-case operation |
| **DESIGN** | Architectural or performance concern that should be addressed but is not immediately dangerous |

---

## CRITICAL -- Data / Money Loss Risk

### C-01: SQL Typo `&1` Instead of `$1` in `remove_token_by_id`

**File:** `src/db/postgres.rs`, line 589
**Impact:** The function **always fails** at runtime. The SQL parameter placeholder is `&1` instead of `$1`.

```rust
let symbol_opt: Option<String> = sqlx::query_scalar(
    "DELETE FROM tokens WHERE id = &1 RETURNING symbol"  // BUG: &1 should be $1
)
```

Postgres will reject this query with a syntax error. Any attempt to delete a token by ID will return an error to the caller.

**Fix:** Replace `&1` with `$1`.

---

### C-02: `finalize_payment` Does Not Check Invoice Status -- Expired Invoice Can Become Paid

**File:** `src/db/postgres.rs`, lines 908-954
**Impact:** **Money-loss scenario.** If the janitor expires an invoice (sets status to `Expired`) while a payment is still in `Confirming` state, the confirmator will later call `finalize_payment`, which:

1. Sets the payment to `Confirmed`
2. Adds the amount to the invoice's `paid_raw`
3. If `paid_raw >= amount_raw`, sets invoice status to `Paid`

Neither the `UPDATE invoices SET paid_raw = paid_raw + $1` nor the `UPDATE invoices SET status = 'Paid'` query checks the current invoice status. An invoice that was already expired (and whose watch address was removed) can silently transition to `Paid`.

The same race exists for `Cancelled` invoices.

```sql
-- No WHERE status = 'Pending' guard:
UPDATE invoices SET paid_raw = paid_raw + $1 WHERE id = $2
  RETURNING paid_raw::TEXT, amount_raw::TEXT

UPDATE invoices SET status = 'Paid' WHERE id = $1
```

**Fix:** Add `AND status IN ('Pending')` to the finalization queries, or at minimum check the invoice status before finalizing and skip/cancel payments for non-pending invoices.

---

### C-03: `set_chain_active` Never Persists to Database

**File:** `src/db/postgres.rs`, lines 489-498
**Impact:** The method only updates the in-memory cache. After a server restart, the chain reverts to its previous `active` state from the database. An operator who disables a chain through the API will find it re-enabled after restart.

```rust
async fn set_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
    match self.chains_cache.read().unwrap().get(chain_name) {
        Some(c) => {
            c.config().write().unwrap().active = active;
        }
        None => anyhow::bail!("chain '{}' does not exist", chain_name),
    }
    Ok(())
    // Missing: UPDATE chains SET active = $1 WHERE name = $2
}
```

**Fix:** Add an `sqlx::query("UPDATE chains SET active = $1 WHERE name = $2")` call.

---

### C-04: `required_confirmations` Truncated from `u64` to `i16`

**File:** `src/db/postgres.rs`, line 400
**Impact:** In `update_chain_partial`, the `required_confirmations` value is cast to `i16` before binding to the SQL query. The database column is `BIGINT`. Any value above 32,767 will be silently truncated, potentially reducing confirmations to a dangerously low number.

```rust
.bind(chain_update.required_confirmations.map(|x| x as i16))  // u64 -> i16 truncation
```

For context, other places correctly cast to `i64`:
- `add_chain` at line 287: `.bind(chain_config.required_confirmations as i64)` -- correct

**Fix:** Change `as i16` to `as i64`.

---

### C-05: Cascading Panic Risk from `RwLock::unwrap()` on Lock Poisoning

**Files:** All files using `std::sync::RwLock`
**Impact:** Every `RwLock` access in the codebase uses `.unwrap()`. If any thread panics while holding a lock (which is possible since the code runs in Tokio tasks), the lock becomes poisoned. Every subsequent `.unwrap()` on that lock will also panic, creating a cascading failure across all services (watcher, janitor, confirmator, webhook dispatcher, and all chain listeners).

Key locations (non-exhaustive):
- `src/db/postgres.rs`: `chains_cache.read().unwrap()` / `.write().unwrap()` (~40 occurrences)
- `src/chain/evm.rs`: `chain_config.read().unwrap()` (~8 occurrences)
- `src/db/mock.rs`: `chains.read().unwrap()` (~20 occurrences)

**Fix:** Use `.read().unwrap_or_else(|e| e.into_inner())` to recover from poisoned locks, or switch to `tokio::sync::RwLock` where appropriate, or use `parking_lot::RwLock` which is unpoisonable.

---

## SECURITY

### S-01: Hardcoded Fallback Webhook Secret `'default_secret'`

**File:** `src/db/postgres.rs`, line 1110
**Impact:** When an invoice has no `webhook_secret` set, the SQL query uses `COALESCE(i.webhook_secret, 'default_secret')`. Any attacker who knows this (it's in the open-source code) can forge valid HMAC-SHA256 webhook signatures for invoices without a configured secret.

```sql
COALESCE(i.webhook_secret, 'default_secret') as secret_key
```

The same behavior is in `src/db/mock.rs`, line 552:
```rust
.unwrap_or_else(|| "default_secret".to_owned());
```

**Fix:** Either require a webhook secret when a webhook URL is set, or refuse to send webhooks when no secret is configured.

---

### S-02: No SSRF Protection on Webhook URLs

**File:** `src/state/webhook.rs`, line 98
**Impact:** The webhook dispatcher sends HTTP POST requests to whatever URL is stored in the invoice. There is no validation against:
- Private IP ranges (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`)
- Loopback (`127.0.0.0/8`, `::1`)
- Link-local addresses (`169.254.0.0/16`)
- Cloud metadata endpoints (`169.254.169.254`)

An attacker who can create invoices with arbitrary webhook URLs can use the payment gateway as an SSRF proxy to scan or interact with internal services.

**Fix:** Validate webhook URLs against a deny-list of private/reserved IP ranges before sending. Consider using a DNS resolution check as well.

---

### S-03: API Key Stored as Plain `String` in Memory

**File:** `src/state/mod.rs`, line 19
**Impact:** `AppState.api_key` is a `pub` field of type `String`. The key remains in memory in cleartext and is not zeroed on drop. Memory dumps, core dumps, or swap files could expose the key.

```rust
pub struct AppState {
    pub api_key: String,  // plaintext, pub, no zeroing
    ...
}
```

**Fix:** Use `secrecy::SecretString` (from the `secrecy` crate) or `zeroize::Zeroizing<String>` to ensure the key is zeroed from memory on drop. Also consider making the field private with accessor methods.

---

### S-04: No Input Validation on Chain Names, Token Symbols, Contract Addresses

**Files:** `src/db/postgres.rs` (add_chain, add_token), `src/db/mock.rs`
**Impact:** Chain names and token symbols are used as HashMap keys, in SQL queries (parameterized, so no SQL injection), and in log messages. However, there is no validation that:
- Chain names are alphanumeric / reasonable length
- Token symbols are uppercase, reasonable length
- Contract addresses are valid hex addresses
- xpub values are valid extended public keys

Malformed input could cause subtle downstream bugs (e.g., a chain name containing whitespace or special characters could prevent lookup).

**Fix:** Add validation functions for each input type, called at the entry points (`add_chain`, `add_token`, `add_invoice`).

---

### S-05: Webhook Secret Exposed via Invoice API

**File:** `src/db/postgres.rs`, lines 174, 794
**Impact:** The `map_row_to_invoice` function includes `webhook_secret` in the returned `Invoice` struct. If the backend exposes invoices through a REST API (as `necko3-backend` does), the webhook secret is sent to the client. This defeats the purpose of HMAC signing -- the secret should be write-only.

```rust
webhook_secret: row.get("webhook_secret"),
```

**Fix:** Either exclude `webhook_secret` from API responses in `necko3-backend`, or add a `#[serde(skip_serializing)]` attribute on the `webhook_secret` field.

---

## BUGS

### B-01: `block_lag` Captured Once, Never Re-read on Config Update

**File:** `src/chain/evm.rs`, line 110
**Impact:** `block_lag` is read once before the main listener loop starts and stored in a local variable. If an operator updates `block_lag` via `update_chain_partial`, the running listener will not pick up the change. Only a restart would apply the new value.

```rust
let block_lag = self.chain_config.read().unwrap().block_lag;  // read once

loop {
    // ...
    let current_block_num = /* ... */.saturating_sub(block_lag as u64);  // uses stale value
}
```

Contrast with `active`, which IS re-read inside the loop (line 113).

**Fix:** Move the `block_lag` read inside the loop, similar to the `active` check.

---

### B-02: MockDatabase `payments` Keyed by `invoice_id`, Not `payment_id`

**File:** `src/db/mock.rs`, line 17
**Impact:** The `payments` DashMap uses `invoice_id` as the key:

```rust
payments: DashMap<String, Payment>, // key = invoice_id
```

But multiple methods try to look up payments by `payment_id`:
- `get_payment(payment_id)` at line 524 -- will never find a payment
- `update_payment_block(payment_id)` at line 467 -- will panic with `unwrap()`
- `cancel_payment(payment_id)` at line 473 -- will always bail

This also means only one payment per invoice is stored, contradicting the Postgres behavior.

**Fix:** Use `payment.id` as the key and store payments in a way that supports multiple per invoice.

---

### B-03: Mock Only Allows One Payment Per Invoice

**File:** `src/db/mock.rs`, lines 410-437
**Impact:** Since `payments` is keyed by `invoice_id`, `add_payment_attempt` either updates the existing payment or inserts a new one, but there can only be one entry per invoice. This means partial payments (multiple transactions to the same invoice) cannot be tested with the mock database.

**Fix:** Change the key to `payment.id` and use a separate index for invoice lookups.

---

### B-04: `unwrap_or_default()` on TxHash / Address Parsing

**File:** `src/chain/evm.rs`, lines 196, 411, 472
**Impact:** When parsing addresses or transaction hashes fails, the code silently substitutes zero values:

```rust
// Line 196: Bad address becomes 0x0000...0000
.map(|s| Address::from_str(&s).unwrap_or_default())

// Line 411: Missing tx hash becomes 0x0000...0000
tx_hash: log.transaction_hash.unwrap_or_default(),

// Line 472: Unparseable hash becomes 0x0000...0000
tx_hash: tx_hash.parse().unwrap_or_default(),
```

A payment record with a zero tx_hash could collide with another zero-hash record, or be impossible to verify on-chain during confirmation.

**Fix:** Log and skip entries that fail to parse instead of using default values.

---

### B-05: `block_number: u64::MAX` Fallback for Missing Log Block Number

**File:** `src/chain/evm.rs`, lines 418-419
**Impact:** If a log entry has no block number (which shouldn't happen in practice but is an `Option`), the payment is recorded with `block_number = u64::MAX`. The confirmator checks `last_processed >= block_number + required_confirmations`, which will overflow or never be satisfied. The payment will be stuck in `Confirming` state forever.

```rust
block_number: log.block_number.unwrap_or(u64::MAX),
```

**Fix:** Skip the log entry and log a warning if `block_number` is `None`.

---

### B-06: No Retry Limit on `get_logs` RPC Errors

**File:** `src/chain/evm.rs`, lines 370-375
**Impact:** When `get_logs` returns an error, the code retries indefinitely (rotates provider and sleeps 1s). If all RPC providers consistently fail for a specific block's logs, the listener is stuck forever on that block, halting all payment detection for the chain.

```rust
Err(e) => {
    warn!(error = %e, "Failed to get logs. Retrying in 1s...");
    self.rotate_provider();
    tokio::time::sleep(Duration::from_secs(1)).await;
    // No retry counter, no bail condition
}
```

Contrast with the "suspicious block" retry which does have a `max_retries = 15` limit.

**Fix:** Add a retry counter with a configurable maximum. After max retries, skip the block's logs (with an error log) and continue.

---

### B-07: `cancel_invoice` Does Not Cancel Associated Confirming Payments

**File:** `src/db/postgres.rs`, lines 869-878; `src/db/mock.rs`, lines 403-405
**Impact:** When an invoice is cancelled, only the invoice status is updated. Any payments in `Confirming` state continue to exist and will be processed by the confirmator, potentially finalizing them and overriding the `Cancelled` status (same race as C-02).

```rust
async fn cancel_invoice(&self, uuid: &str) -> anyhow::Result<()> {
    // Only updates invoice status, no payment cleanup
    sqlx::query("UPDATE invoices SET status = 'Cancelled' WHERE id = $1")
        .bind(uuid_parsed)
        .execute(&self.pool)
        .await?;
    Ok(())
}
```

**Fix:** Also cancel all `Confirming` payments for the invoice and remove the watch address.

---

### B-08: `update_chain_block` Does Not Update `chains_cache`

**File:** `src/db/postgres.rs`, lines 303-311
**Impact:** The method updates the database but not the in-memory `chains_cache`. While the blockchain listener's own `chain_config` is updated separately (in `evm.rs:214`), the cache read by other components via `get_latest_block` may return stale data.

```rust
async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
    sqlx::query("UPDATE chains SET last_processed_block = $1 WHERE name = $2")
        .bind(block_num as i64)
        .bind(chain_name)
        .execute(&self.pool)
        .await?;
    // Missing: update chains_cache
    Ok(())
}
```

However, because the `Blockchain` adapter itself holds an `Arc<RwLock<ChainConfig>>` that the listener updates directly (line 214 of `evm.rs`), and because `chains_cache` stores `Arc<Blockchain>` pointing to the same config, the in-memory state IS actually updated transitively through the shared `Arc`. The DB and cache stay in sync by accident. This is fragile but currently not broken.

**Recommendation:** Either document this coupling clearly or update the cache explicitly for clarity.

---

### B-09: Mock vs Postgres Behavioral Differences

**Files:** `src/db/mock.rs`, `src/db/postgres.rs`
**Impact:** Several methods behave differently between the two implementations:

| Method | Postgres | Mock |
|--------|----------|------|
| `add_webhook_job` with `webhook_url = None` | `bail!("URL is not set")` | `return Ok(())` silently |
| `get_chain_by_id` | Works via SQL | `unimplemented!()` panic |
| `remove_token_by_id` | Attempts SQL (buggy, see C-01) | `unimplemented!()` panic |
| `remove_chain_by_id` | Works via SQL | `unimplemented!()` panic |
| Multiple payments per invoice | Supported | Only one stored |

Tests using `MockDatabase` may pass while Postgres behavior differs, creating false confidence.

**Fix:** Align mock behavior with Postgres, or at minimum document the differences and add Postgres-specific integration tests.

---

### B-10: `rotate_provider` Index Overflow is Handled but Modular Arithmetic is Racy

**File:** `src/chain/evm.rs`, lines 254-266
**Impact:** The `provider()` method uses `idx % self.providers.len()`, and `rotate_provider` uses `fetch_add(1, SeqCst)`. The `current_idx` grows without bound. While the modulo prevents out-of-bounds access, the `AtomicUsize` will eventually wrap around (after `usize::MAX` rotations). In practice this takes an astronomically long time, but it's worth noting.

More importantly, `provider()` uses `Ordering::Relaxed` while `rotate_provider` uses `Ordering::SeqCst`. This mismatch means `provider()` might not immediately see a rotation done by another thread.

**Fix:** Use consistent memory ordering. Consider using `fetch_add` with wrapping or resetting the counter.

---

### B-11: Transactions Array Parse Error Retries Infinitely

**File:** `src/chain/evm.rs`, lines 181-190
**Impact:** When `bj["transactions"].as_array()` returns `None` (block exists but `transactions` field is not an array), the code retries forever with a provider rotation. The comment `"THERE IS NO FUCKING WAY THAT THERE ARE NO TRANSACTIONS"` suggests this is unexpected, but it creates an infinite loop if it does happen (e.g., due to a non-standard RPC response).

```rust
None => {
    error!("Failed to parse transactions. Retrying in 1s...");
    self.rotate_provider();
    tokio::time::sleep(Duration::from_secs(1)).await;
    continue;  // infinite retry
}
```

**Fix:** Add a retry limit. After N attempts, treat the block as having zero transactions and move on.

---

## DESIGN / PERFORMANCE

### D-01: `get_free_slot` is O(n^2)

**File:** `src/state/mod.rs`, lines 84-89
**Impact:** The method iterates `0..=busy_indexes.len()` and for each index calls `busy_indexes.contains()` which is O(n) on a `Vec`. Total complexity: O(n^2). With thousands of pending invoices per chain, this becomes noticeably slow.

```rust
for i in 0..=busy_indexes.len() as u32 {
    if !busy_indexes.contains(&(i)) {
        return Some(i);
    }
}
```

**Fix:** Convert `busy_indexes` to a `HashSet<u32>` before the loop, making lookup O(1) and total O(n).

---

### D-02: No Graceful Shutdown for Background Tasks

**File:** `src/state/mod.rs`
**Impact:** There is no shutdown mechanism for the watcher, janitor, confirmator, or webhook dispatcher. When the process stops:
- In-flight webhook deliveries are lost
- The payment event channel may have unprocessed events
- `stop_listening` uses `handle.abort()` which is an immediate kill

**Fix:** Use `tokio::sync::watch` or `CancellationToken` for cooperative shutdown. Drain channels before exiting.

---

### D-03: No Idempotency for Webhook Job Creation

**File:** `src/db/postgres.rs`, lines 1153-1187
**Impact:** `add_webhook_job` has no deduplication. If it's called twice for the same event (e.g., due to a retry in the calling code), two webhook jobs are created and the recipient gets duplicate deliveries.

**Fix:** Add a unique constraint on `(invoice_id, event_type, payload_hash)` or implement upsert logic.

---

### D-04: `ChainConfig::patch` Cannot Clear `logo_url` to `None`

**File:** `src/model.rs`, line 46
**Impact:** The `patch` method wraps the value in `Some()`:
```rust
if let Some(x) = &update.logo_url { self.logo_url = Some(x.to_owned()); }
```

There is no way to set `logo_url` back to `None` once it has been set. The `COALESCE` in the SQL update (`postgres.rs:393`) has the same limitation.

**Fix:** Use a two-layer Option pattern (e.g., `Option<Option<String>>`) or a separate `clear_logo_url: Option<bool>` field.

---

### D-05: Hardcoded Channel Capacity 100

**File:** `src/state/mod.rs`, line 30
**Impact:** The mpsc channel between blockchain listeners and the watcher has a fixed capacity of 100. Under heavy load (many chains, many transactions per block), the channel can fill up and `sender.send()` will block the listener, creating backpressure that stalls block processing.

```rust
let (tx, rx): (Sender<PaymentEvent>, Receiver<PaymentEvent>) = mpsc::channel(100);
```

**Fix:** Make the capacity configurable. Consider using `try_send` with overflow handling or a larger default.

---

### D-06: Webhook Dispatcher Busy-Polls with 500ms Sleep

**File:** `src/state/webhook.rs`, lines 36-39
**Impact:** When there are no pending webhooks, the dispatcher sleeps for 500ms and tries again. This creates unnecessary database queries.

```rust
if jobs.is_empty() {
    tokio::time::sleep(Duration::from_millis(500)).await;
    continue;
}
```

**Fix:** Use PostgreSQL `LISTEN/NOTIFY` to wake the dispatcher when new webhook jobs are inserted, or use exponential backoff for the polling interval.

---

### D-07: No Database Connection Pool Health Checks

**File:** `src/db/mod.rs`, lines 104-126
**Impact:** The database pool is created with `PgPoolOptions::new().max_connections(max_connections).connect(...)`. There are no idle timeout, connection health check, or min_connections settings. Long-idle connections may be killed by the database or network intermediaries.

**Fix:** Configure `idle_timeout`, `max_lifetime`, and optionally `min_connections` on the pool.

---

### D-08: `process_transactions` Trusts Raw JSON Structure

**File:** `src/chain/evm.rs`, lines 433-491
**Impact:** The method parses transactions from raw JSON (`serde_json::Value`). It trusts the structure completely: `tx["to"].as_str()`, `tx["value"].as_str()`, `tx["hash"].as_str()`, `tx["from"].as_str()`. If any RPC returns a slightly different format (or a null field), the `unwrap_or_default()` calls produce garbage data silently.

**Fix:** Deserialize into a proper typed struct rather than relying on dynamic JSON access.

---

### D-09: Background Tasks Ignore Join Handles

**File:** `src/state/mod.rs`, lines 55-64
**Impact:** The `JoinHandle` returned by `start_invoice_watcher`, `start_janitor`, `start_confirmator`, and `start_webhook_dispatcher` are all dropped (not stored). If any of these tasks panic, there is no way to detect or restart them.

```rust
watcher::start_invoice_watcher(state_arc.clone(), rx);  // handle dropped
janitor::start_janitor(state_arc.clone(), janitor_timeout);  // handle dropped
confirmator::start_confirmator(state_arc.clone(), confirmator_timeout);  // handle dropped
webhook::start_webhook_dispatcher(state_arc.clone());  // handle dropped
```

Only chain listener handles are stored in `active_chains`.

**Fix:** Store all task handles and implement a supervisor that restarts failed tasks.

---

### D-10: `Pagination` Default Has `limit: 0`

**File:** `src/model.rs`, lines 196-200
**Impact:** `Pagination` derives `Default`, which sets `limit` to `0`. Any filter with default pagination will return zero results, which could be confusing.

```rust
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub struct Pagination {
    pub limit: u32,   // Default: 0
    pub offset: u64,  // Default: 0
}
```

**Fix:** Implement `Default` manually with a sensible default limit (e.g., 50).

---

## Summary Matrix

| ID | Severity | Module | Description |
|----|----------|--------|-------------|
| C-01 | CRITICAL | `db/postgres` | SQL typo `&1` in `remove_token_by_id` |
| C-02 | CRITICAL | `db/postgres` | `finalize_payment` ignores invoice status |
| C-03 | CRITICAL | `db/postgres` | `set_chain_active` not persisted to DB |
| C-04 | CRITICAL | `db/postgres` | `required_confirmations` truncated u64->i16 |
| C-05 | CRITICAL | all | `RwLock::unwrap()` cascading panic on poison |
| S-01 | SECURITY | `db/postgres`, `db/mock` | Hardcoded `'default_secret'` fallback |
| S-02 | SECURITY | `state/webhook` | No SSRF protection on webhook URLs |
| S-03 | SECURITY | `state/mod` | API key in plain String |
| S-04 | SECURITY | `db/postgres`, `db/mock` | No input validation |
| S-05 | SECURITY | `db/postgres` | Webhook secret exposed in API responses |
| B-01 | BUG | `chain/evm` | `block_lag` read once, never refreshed |
| B-02 | BUG | `db/mock` | `payments` keyed by wrong field |
| B-03 | BUG | `db/mock` | Only one payment per invoice |
| B-04 | BUG | `chain/evm` | `unwrap_or_default()` on TxHash/Address |
| B-05 | BUG | `chain/evm` | `u64::MAX` fallback for block number |
| B-06 | BUG | `chain/evm` | No retry limit on `get_logs` errors |
| B-07 | BUG | `db/postgres`, `db/mock` | `cancel_invoice` ignores payments |
| B-08 | BUG | `db/postgres` | `update_chain_block` cache inconsistency |
| B-09 | BUG | `db/mock` | Mock/Postgres behavior mismatch |
| B-10 | BUG | `chain/evm` | Provider rotation ordering mismatch |
| B-11 | BUG | `chain/evm` | Infinite retry on transactions parse failure |
| D-01 | DESIGN | `state/mod` | `get_free_slot` O(n^2) |
| D-02 | DESIGN | `state/mod` | No graceful shutdown |
| D-03 | DESIGN | `db/postgres` | No webhook job idempotency |
| D-04 | DESIGN | `model` | `patch` can't clear `logo_url` |
| D-05 | DESIGN | `state/mod` | Hardcoded channel capacity 100 |
| D-06 | DESIGN | `state/webhook` | Busy-polling webhook dispatcher |
| D-07 | DESIGN | `db/mod` | No pool health check config |
| D-08 | DESIGN | `chain/evm` | Raw JSON parsing without typed structs |
| D-09 | DESIGN | `state/mod` | Background task handles dropped |
| D-10 | DESIGN | `model` | `Pagination` default limit is 0 |

**Total: 31 findings** -- 5 Critical, 5 Security, 11 Bugs, 10 Design/Performance
