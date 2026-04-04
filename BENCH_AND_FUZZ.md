# Benchmarks and property-based tests

## `Cargo.toml` dependencies

Already present:

```toml
[dev-dependencies]
criterion = "0.8.2"
proptest = "1.11.0"

[[bench]]
name = "gateway"
path = "benches/gateway.rs"
harness = false
```

Criterion 0.8 targets **Rust 1.86+** (crate MSRV). Check with `rustc --version`.

## How to run

Compile benchmarks without executing them:

```bash
cargo bench --no-run
```

Run all benchmarks:

```bash
cargo bench
```

Only the `gateway` target:

```bash
cargo bench --bench gateway
```

Criterion HTML reports: `target/criterion/`.

Integration property tests:

```bash
cargo test --test prop_tests
```

Increase proptest case count (environment variable):

```bash
export PROPTEST_CASES=10000
cargo test --test prop_tests
```

Windows CMD: `set PROPTEST_CASES=10000`. PowerShell: `$env:PROPTEST_CASES=10000`.

## `gateway` benchmark groups

| Criterion group | What it measures |
|-----------------|------------------|
| `webhook_hmac` | HMAC-SHA256 over webhook body (same shape as `state/webhook.rs`), short and large JSON. |
| `webhook_json` | `serde_json::to_string` for each `WebhookEvent` variant. |
| `model_serde` | JSON round-trip for `Payment`, `Invoice`, `Webhook`, filters, `TokenConfig`, `PartialChainUpdate`, `ChainConfig`, `PaginatedVec`. |
| `amount_format` | `format_units` / `parse_units` (alloy) on representative values. |
| `xpub_derive` | `XPub::from_str` (once) + `derive_child` + `Address::from_public_key` with a fixed xpub. |
| `ids_and_decimal` | `Uuid::parse_str`, `BigDecimal::from_str` from `U256::MAX`, `Url::parse`. |

Filter by benchmark name (substring):

```bash
cargo bench --bench gateway webhook_hmac
```

## Criterion cheat sheet

- Wrap hot-path inputs and outputs in `std::hint::black_box` so the compiler does not eliminate “dead” work.
- For before/after comparisons, store a baseline (`cargo bench` with Criterion’s baseline flags; see the `criterion` docs).
- Results are noisy on a busy machine; prefer a quiet box or CI without CPU turbo for regression tracking.

## Proptest cheat sheet

- The `proptest! { ... }` macro defines properties; `#![proptest_config(...)]` inside the first `proptest!` sets module-wide case counts.
- `prop_compose!` builds reusable strategies from primitives (`any::<u32>()`, `prop::collection::vec`, `prop_oneof!`, `prop::sample::select`).
- On failure, proptest prints a **minimal counterexample**; `PROPTEST_CASES` increases how many inputs are tried.
- Optional regressions file: `tests/prop_tests.proptest-regressions` — lines like `cc <hash>` pin past failures (see proptest docs).

## Known limitations surfaced by tests

- **`alloy::primitives::utils::parse_units`**: some generated Unicode strings triggered panics inside alloy (invalid scalar indexing). In `prop_tests`, `parse_units` is only fed **ASCII** strings (bytes 0–127).
- **`coins_bip32::XPub::from_str`**: arbitrary garbage can **panic** (e.g. subtraction overflow in the decoder). There is no property test for “any random string”; there is a property for derivation from a **known valid** xpub with indices `0..4096`.

Coverage-guided fuzzing (`cargo fuzz` / libFuzzer) is out of scope for this document.
