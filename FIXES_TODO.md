# OpenPool Manager API — Pipeline Fixes TODO

Comprehensive review of the Dagster pipeline identifying defects, data-loss vectors, performance issues, and code quality concerns. Organized by severity.

---

## CRITICAL — Data Loss / Financial Risk

### 1. ~~`process_inbound_events_job` uses `AssetSelection.all()` — triggers payments on every S3 ingest~~ FIXED

**Status:** FIXED — `jobs.py` now excludes `worker_payments`, `worker_summary_analytics`, and `worker_summary_s3` from `process_inbound_events_job`. Payments only trigger via `worker_payment_sensor` → `worker_payment_job`.

**File:** `openpool_management/jobs.py`

---

### 2. ~~Sensor cursor advances before the run succeeds~~ FIXED

**Status:** FIXED — `sensors.py` rewritten with a two-phase cursor. Phase 1 reconciles pending runs (promotes cursor only on SUCCESS, resets on FAILURE). Phase 2 scans for new files and sets pending state without advancing the cursor. S3 archival (Fix 5) also integrated into Phase 1 on success.

**File:** `openpool_management/sensors.py`

---

### 3. ~~`worker_fees` has no event deduplication — re-processes all events from `processed_jobs`~~ FIXED

**Status:** FIXED — `worker.py` now loads `_processed_event_ids` from previous state, filters out already-processed events before accumulating fees, and persists the set in the returned state. Same pattern as `worker_performance_analytics`.

**File:** `openpool_management/assets/worker.py`

---

### 4. ~~No idempotency on blockchain payments~~ FIXED

**Status:** FIXED — `payment.py` now implements a write-ahead log (WAL). Each payment writes a "pending" entry before `send_eth()` and a result entry after. On startup, `_reconcile_wal()` credits completed payments and flags ambiguous pending entries (crash during send) as `_wal_needs_investigation`, skipping those workers with a CRITICAL log.

**File:** `openpool_management/assets/payment.py`

---

### 5. ~~`worker_payments` self-loads via raw `InputContext` — fragile and incorrect context~~ FIXED

**Status:** FIXED — Added `load_previous_output(context)` method to `PartitionedFilesystemIOManager` that accepts `AssetExecutionContext` directly (which already has `.asset_key`, `.partition_key`, `.has_partition_key`). All 5 assets (`worker_fees`, `worker_connections`, `worker_payments`, `worker_performance_analytics`, `worker_summary_analytics`) now use this method instead of constructing a bare `InputContext`. Includes `.bak` recovery. Returns `None` when no previous state exists.

**Files:** `openpool_management/io_manager.py`, `openpool_management/assets/worker.py`, `openpool_management/assets/payment.py`, `openpool_management/assets/metrics.py`

---

## HIGH — Correctness Issues

### 6. ~~`processed_jobs` loads previous state inconsistently~~ FIXED

**Status:** FIXED — `processed_jobs` now uses `context.resources.io_manager.load_previous_output(context)` instead of the fragile `get_latest_materialization_event()` + `load_input(context)` chain. The `load_previous_output` method handles file-not-found and corruption gracefully.

**File:** `openpool_management/assets/events.py`

---

### 7. ~~`pd.DataFrame()` reference in IO manager without pandas import~~ FIXED

**Status:** FIXED — `io_manager.py` now returns `None` for `transcode_performance` and `ai_performance` in the fallback, with a new `_get_empty_fallback` method. `metrics.py` updated with `is not None` guards at all `.empty` call sites.

**Files:** `openpool_management/io_manager.py`, `openpool_management/assets/metrics.py`

---

### 8. ~~Payment sensor reads pickle files directly from disk~~ FIXED

**Status:** FIXED — Added `load_for_sensor(asset_key_path, partition_key)` method to `PartitionedFilesystemIOManager` that handles path construction, pickle loading, and `.bak` recovery. The `worker_payment_sensor` now calls `io_mgr.load_for_sensor(...)` instead of constructing paths and reading pickles directly. Direct `pickle`/`os` imports removed from `sensors.py`.

**Files:** `openpool_management/io_manager.py`, `openpool_management/sensors.py`

---

### 9. ~~`worker_connections` discards disconnected workers~~ FIXED

**Status:** FIXED — Disconnected workers now stay in state with `active: False`, `is_active: False`, and a `last_seen` timestamp. The purge block has been replaced with active/inactive counting and richer metadata (`total_worker_count`, `active_worker_count`, `inactive_worker_count`).

**File:** `openpool_management/assets/worker.py`

---

### 10. ~~Hardcoded `valid_combinations` duplicated across sensors~~ FIXED

**Status:** FIXED — `VALID_COMBINATIONS` is now defined as a constant in `partitions.py` with a comment explaining the 3 excluded cells. Both `s3_event_sensor` and `worker_payment_sensor` import this constant instead of maintaining local copies.

**Files:** `openpool_management/partitions.py`, `openpool_management/sensors.py`

---

## MEDIUM — Resilience / Performance

### 11. ~~`raw_events` is not persisted by the IO manager~~ FIXED

**Status:** FIXED — Added `io_manager_key="io_manager"` to the `raw_events` asset. Parsed events are now persisted in the same partitioned pickle structure as all other assets, eliminating S3 re-fetch on retry.

**File:** `openpool_management/assets/events.py`

---

### 12. ~~No atomic writes in IO manager~~ FIXED

**Status:** FIXED — `io_manager.py` `handle_output` now uses `tempfile.mkstemp` + `fsync` + atomic `os.rename`. Existing file is backed up to `.bak` before rename. `load_input` recovers from `.bak` on corrupt primary. Duplicate `os.makedirs` also removed.

**File:** `openpool_management/io_manager.py`

---

### 13. ~~`S3Resource` creates a new boto3 client on every call~~ FIXED

**Status:** FIXED — `S3Resource` now caches the boto3 client via `PrivateAttr(default=None)`. The client is created once on first call and reused for all subsequent S3 operations within the same resource instance.

**File:** `openpool_management/resources.py`

---

### 14. `worker_performance_analytics` accumulates `processed_event_ids` forever

**File:** `openpool_management/assets/metrics.py:77-88`

The `processed_event_ids` set grows unboundedly across materializations. Over months of operation with high event volume, this set becomes very large, increasing pickle file size and deserialization time.

**Impact:** Gradual performance degradation. Eventually, loading the pickle file may take significant time or memory.

**Fix:** Implement a rotation strategy:
- Keep only event IDs from the last N materializations or last N days.
- Or use a Bloom filter for approximate deduplication with bounded memory.
- Or, if events are guaranteed to arrive in chronological order, track only the latest timestamp and deduplicate by timestamp comparison.

---

### 15. ~~No retry/backoff on blockchain RPC calls~~ FIXED

**Status:** FIXED — Added `_retry_rpc()` helper (stdlib, no extra dependency) with 3 attempts and exponential backoff (1s→2s→4s, capped at 10s). Wrapped `get_balance`, `get_transaction_count`, `gas_price`, `send_raw_transaction`, and `wait_for_transaction_receipt` in `send_eth()`. Receipt wait uses longer backoff (2s→4s→8s, capped at 15s).

**File:** `openpool_management/resources.py`

---

### 16. Gas price check race condition

**File:** `openpool_management/resources.py:192-200, 203-210`

Gas price is fetched and checked against `max_gas_price_wei`, then the same value is used in the transaction dict. Between the check and the actual `send_raw_transaction`, network conditions could change.

**Impact:** Low risk on Arbitrum (stable low fees), but the pattern is fragile for chains with volatile gas markets. A spike between check and send could result in a stuck transaction.

**Fix:** On Arbitrum this is acceptable. For robustness, consider using EIP-1559 fields (`maxFeePerGas`, `maxPriorityFeePerGas`) which let the protocol handle gas price fluctuation, or re-check gas price immediately before signing.

---

## LOW — Code Quality / Maintenance

### 17. Unused Pydantic models

**File:** `openpool_management/models.py`

The following models are defined but never imported or used by any asset, resource, or sensor:
- `PaymentRecord`
- `FeeRecord`
- `WorkerFeeState`
- `WorkerState`
- `WorkerPaymentState`
- `PaymentEvent`

All assets work with plain dicts instead.

**Impact:** Confusion about canonical data formats. Maintenance burden keeping unused models in sync.

**Fix:** Either adopt these models in the assets (preferred — gives you validation and IDE support) or remove them. If keeping them as documentation, add a clear comment noting they are reference schemas.

---

### 18. Unused config classes

**File:** `openpool_management/configs.py`

The following config classes are defined but never imported or used:
- `WorkerPaymentConfig`
- `RawEventsConfig`
- `IOManagerConfig`

**Impact:** Dead code, potential confusion.

**Fix:** Remove or integrate into the pipeline.

---

### 19. ~~Payment sensor passes `run_config` that `worker_payments` ignores~~ FIXED

**Status:** FIXED — Removed the unused `run_config` from `worker_payment_sensor`'s `RunRequest`. The asset reads the threshold from `context.resources.payment_processor.get_payment_threshold()` which is the single source of truth.

**File:** `openpool_management/sensors.py`

---

### 20. ~~`worker_summary_analytics` active_workers count is inconsistent on incremental runs~~ FIXED

**Status:** FIXED — Removed the `active_workers` / `total_connections` accumulation from inside the connection processing loop. Both are now computed from the final merged `worker_summary["workers"]` dict alongside `total_workers` and all other aggregates.

**File:** `openpool_management/assets/metrics.py`

---

### 21. ~~`payments/` directory writes are not durable~~ FIXED

**Status:** FIXED — Payment audit logs now write to `{IO_MANAGER_BASE_DIR}/payments/` instead of a relative `payments/` path. Since the IO manager base dir is on a mounted volume, audit logs inherit the same durability as all pipeline state.

**File:** `openpool_management/assets/payment.py`

---

### 22. ~~Duplicate `os.makedirs` call in IO manager~~ FIXED

**Status:** FIXED — Removed as part of the atomic writes rewrite in `io_manager.py`.

**File:** `openpool_management/io_manager.py`

---

## NEW — Added via Pipeline Durability Fixes

### 23. S3 archival on successful processing (ADDED)

**Status:** FIXED — Processed S3 files are archived to a configurable `S3_ARCHIVE_BUCKET` with prefix after the sensor confirms run success. Archival failure is non-blocking. Config wired via `S3Config.archive_bucket` / `S3Config.archive_prefix` and Docker env vars.

**Files:** `openpool_management/configs.py`, `openpool_management/resources.py`, `openpool_management/sensors.py`, `openpool_management/definitions.py`, `docker/dagster.yaml-pre-variables`

---

## Summary

| Priority | # | Status | Key Theme |
|----------|---|--------|-----------|
| **Critical** | 1 | FIXED | `AssetSelection.all()` triggering payments |
| **Critical** | 2 | FIXED | Cursor advances before run succeeds |
| **Critical** | 3 | FIXED | `worker_fees` event deduplication |
| **Critical** | 4 | FIXED | Payment idempotency (WAL) |
| **Critical** | 5 | FIXED | Fragile `InputContext` self-loading |
| **High** | 6 | FIXED | `processed_jobs` inconsistent state loading |
| **High** | 7 | FIXED | `pd.DataFrame()` NameError in IO manager |
| **High** | 8 | FIXED | Payment sensor uses `load_for_sensor` via IO manager |
| **High** | 9 | FIXED | `worker_connections` keeps disconnected workers |
| **High** | 10 | FIXED | `VALID_COMBINATIONS` constant in `partitions.py` |
| **Medium** | 11 | FIXED | `raw_events` persisted via IO manager |
| **Medium** | 12 | FIXED | Atomic writes in IO manager |
| **Medium** | 13 | FIXED | S3 client cached via PrivateAttr |
| **Medium** | 14 | OPEN | `processed_event_ids` unbounded growth |
| **Medium** | 15 | FIXED | RPC retry/backoff with exponential backoff |
| **Medium** | 16 | OPEN | Gas price race condition (low risk on Arbitrum) |
| **Low** | 17 | OPEN | Unused Pydantic models |
| **Low** | 18 | OPEN | Unused config classes |
| **Low** | 19 | FIXED | Removed unused `run_config` from payment sensor |
| **Low** | 20 | FIXED | `active_workers` computed from final merged dict |
| **Low** | 21 | FIXED | `payments/` writes to IO_MANAGER_BASE_DIR |
| **Low** | 22 | FIXED | Duplicate `os.makedirs` |
| **New** | 23 | FIXED | S3 archival on success |

### Progress: 19 FIXED, 4 OPEN

### Remaining Fixes

1. **#14** — Bound `processed_event_ids` growth (MEDIUM — ticking time bomb, affects both `worker_fees` and `worker_performance_analytics`)
2. **#16** — Gas price race condition (MEDIUM — low risk on Arbitrum, consider EIP-1559 for other chains)
3. **#17** — Remove or adopt unused Pydantic models (LOW — dead code cleanup)
4. **#18** — Remove or integrate unused config classes (LOW — dead code cleanup)
