# Testing Guide for necko3-core

## Running Tests

```bash
# Run all tests
cargo test

# Run a specific test file
cargo test --test model_tests
cargo test --test mock_db_tests
cargo test --test webhook_tests
cargo test --test state_tests

# Run a single test by name
cargo test get_free_slot_with_gap

# Run with output visible (for debugging)
cargo test -- --nocapture

# Run the existing in-crate webhook test
cargo test --lib state::webhook::tests
```

## Test Structure

```
tests/
  model_tests.rs       - Domain model types, enums, serde, ChainConfig::patch
  mock_db_tests.rs     - MockDatabase CRUD for chains, tokens, invoices, payments, webhooks
  webhook_tests.rs     - HMAC-SHA256 signing, HTTP delivery via wiremock, retry logic
  state_tests.rs       - AppState construction, channel behavior, get_free_slot
src/
  state/webhook.rs     - Contains one in-crate test (test_webhook_delivery_with_signature)
```

Integration tests live in the `tests/` directory and test the crate through its public API. The single in-crate test inside `src/state/webhook.rs` tests the private `process_webhook` function directly.

## What Each File Covers

| File | Tests | What it validates |
|------|-------|-------------------|
| `model_tests.rs` | 34 | `ChainConfig::patch` field updates, enum `Display`/`FromStr` round-trips, `WebhookEvent` tagged serde, `PaginatedVec`, `Pagination` defaults, filter defaults, `TokenConfig` in `HashSet`, serde for `Invoice`/`Payment`/`Webhook` |
| `mock_db_tests.rs` | 61 | Full `DatabaseAdapter` trait coverage via `MockDatabase`: chain CRUD, token CRUD, watch addresses, invoice lifecycle (add/get/expire/cancel/status), payment lifecycle (attempt/finalize/cancel), webhook lifecycle (add/select/retry/cancel), token decimals, filtering, pagination |
| `webhook_tests.rs` | 22 | HMAC-SHA256 determinism, signature verification, tamper detection, webhook JSON wire format, HTTP delivery with wiremock (200/500), exponential backoff formula, full lifecycle through `MockDatabase` and `Database` enum |
| `state_tests.rs` | 14 | `AppState::new` construction, API key storage, mpsc channel send/receive, channel capacity (100), `get_free_slot` with various busy patterns (empty/gap/contiguous/multi-chain), DB access through `AppState` |

## When to Write New Tests

**Always add tests when:**
- Fixing a bug: write a test that reproduces the bug first, then fix the code
- Adding a new `DatabaseAdapter` method: add a corresponding test in `mock_db_tests.rs`
- Adding a new model type or enum variant: add serde and `Display`/`FromStr` tests in `model_tests.rs`
- Changing webhook signing or delivery logic: update/add tests in `webhook_tests.rs`
- Modifying `AppState` or background service spawning logic: update `state_tests.rs`

**Consider adding tests when:**
- Refactoring internal logic that could break existing behavior
- Changing SQL queries in `postgres.rs` (add integration tests with a real Postgres if possible)
- Adding a new blockchain adapter (e.g., non-EVM chain)

## When to Update Existing Tests

Tests must be updated when:
- A public API signature changes (method renamed, parameters added/removed)
- Enum variants are added or removed
- Serde format changes (e.g., tag names, field names)
- Business logic changes (e.g., new invoice statuses, different confirmation rules)
- `MockDatabase` behavior is fixed to match Postgres (see known issues below)

Tests should NOT need updating for:
- Internal refactors that don't change public behavior
- Performance optimizations
- Log message changes
- Adding new independent features

## Known Limitations

1. **MockDatabase bugs**: The mock has known issues (payments keyed by `invoice_id` instead of `payment_id`, one payment per invoice limit). Tests work around these. When the mock is fixed, update the corresponding tests to use correct payment IDs.

2. **No Postgres integration tests**: All database tests use `MockDatabase`. For full confidence, set up a Postgres test database and add integration tests that run against real SQL.

3. **No blockchain integration tests**: EVM blockchain operations (address derivation, block listening, tx verification) require a real RPC endpoint. Consider using a local Anvil/Hardhat node for these.

4. **Private module access**: The `webhook` module is private (`mod webhook`), so `process_webhook` and `generate_signature` cannot be called from integration tests. The HMAC logic is reimplemented in `webhook_tests.rs` to verify correctness.

## Dependencies

Test-only dependencies in `Cargo.toml`:

```toml
[dev-dependencies]
wiremock = "0.6"    # HTTP mock server for webhook delivery tests
```

The `uuid` crate (already a runtime dependency) is used in tests to generate valid invoice IDs.
