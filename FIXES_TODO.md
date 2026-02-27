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

### 8. ~~Payment sensor reads pickle files directly from disk~~ PARTIALLY FIXED

**Status:** PARTIALLY FIXED — The hardcoded `"data"` path is now read from `IO_MANAGER_BASE_DIR` env var, and `.bak` fallback is added for corrupt pickle recovery. The sensor still reads pickle files directly rather than going through Dagster's materialization API.

**File:** `openpool_management/sensors.py`

---

### 9. `worker_connections` discards disconnected workers

**File:** `openpool_management/assets/worker.py:282-287`

After processing events, workers with zero active connections are removed from state entirely. If a worker disconnects temporarily, their entire record is purged.

**Impact:**
- A worker who disconnects then reconnects appears as a brand-new worker.
- The summary analytics loses visibility into workers that are between connections.
- No historical record of workers who were once active.

**Fix:** Keep disconnected workers in state with `active: False` rather than removing them. Add a separate "last_seen" timestamp for cleanup of truly stale workers after a configurable retention period.

---

### 10. Hardcoded `valid_combinations` duplicated across sensors

**File:** `openpool_management/sensors.py:29-35` and `sensors.py:199-205`

Both sensors hardcode the same 5 partition combinations. The partition definition (`partitions.py`) defines an 8-cell grid (4 regions x 2 node types), but only 5 are active.

**Impact:**
- Adding a new region or node type requires updating both sensors manually.
- The 3 unused partition cells (`ai|us-west`, `ai|eu-central`, `ai|oceania`) can be manually triggered in the Dagster UI but will never be triggered by sensors — a source of confusion.

**Fix:** Define `VALID_COMBINATIONS` as a constant in `partitions.py` and import it in both sensors. Consider also reducing the `MultiPartitionsDefinition` to only the valid combinations, or adding a comment explaining why 3 cells are intentionally excluded.

---

## MEDIUM — Resilience / Performance

### 11. `raw_events` is not persisted by the IO manager

**File:** `openpool_management/assets/events.py:10-16`

The `raw_events` asset does not specify `io_manager_key`. It returns `List[RawEvent]` which Dagster stores via the default IO manager (filesystem pickle in the Dagster storage directory, not the custom partitioned IO manager). If downstream assets fail and the run is retried, `raw_events` must re-fetch everything from S3.

**Impact:** For 250 files per batch, re-fetching on retry is expensive and depends on S3 availability. If S3 has an outage during retry, the entire run fails again.

**Fix:** Add `io_manager_key="io_manager"` to the `raw_events` asset so parsed events are persisted in the same partitioned structure. Alternatively, accept the re-fetch cost but add retry logic to the S3 calls.

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

### 15. No retry/backoff on blockchain RPC calls

**File:** `openpool_management/resources.py:109, 169-264`

`Web3Client` makes RPC calls (`is_connected`, `get_balance`, `get_transaction_count`, `gas_price`, `send_raw_transaction`, `wait_for_transaction_receipt`) with no retry logic. Arbitrum RPC endpoints have rate limits and intermittent failures.

**Impact:** A single RPC timeout during a payment run causes the entire materialization to fail, potentially after some payments have already been sent successfully (compounding issue #4). Transient network issues become permanent failures.

**Fix:** Add retry with exponential backoff around RPC calls, at minimum around `send_raw_transaction` and `wait_for_transaction_receipt`. Consider using `web3.py`'s built-in retry middleware or a library like `tenacity`:

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def _send_with_retry(self, raw_tx):
    return self.w3.eth.send_raw_transaction(raw_tx)
```

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

### 19. Payment sensor passes `run_config` that `worker_payments` ignores

**File:** `openpool_management/sensors.py:248-249`

The sensor sends:
```python
{"ops": {"worker__worker_payments": {"config": {"payment_threshold_wei": payment_threshold_wei}}}}
```

But `worker_payments` reads the threshold from:
```python
context.resources.payment_processor.get_payment_threshold()  # payment.py:31
```

The `op_config` value is never read.

**Impact:** Misleading — suggests the threshold is configurable per-run but it isn't.

**Fix:** Either read from `op_config` in the asset (making it truly configurable per-run) or remove the `run_config` from the sensor.

---

### 20. `worker_summary_analytics` active_workers count is inconsistent on incremental runs

**File:** `openpool_management/assets/metrics.py:512-520, 580-582`

Aggregates (`active_workers`, `total_connections`) are initialized to zero each run (line 512-520), then only populated from the current `worker_connection_states` (lines 580-582). Workers from a previous summary who still have connections but had no new connection events in this batch are carried over in the `workers` dict but their connections are **not** counted in the aggregates.

**Impact:** `active_workers` and `total_connections` undercount on incremental runs when some workers had no new events.

**Fix:** Compute aggregates from the final `worker_summary["workers"]` dict after all merging is complete, rather than accumulating during the connection processing loop.

---

### 21. `payments/` directory writes are not durable

**File:** `openpool_management/assets/payment.py:246-266`

Payment event JSON files are written to a local `payments/` directory relative to CWD. In a Docker deployment with ephemeral containers (as configured in `dagster-template.yaml`), these files may be lost between runs unless the volume is explicitly mounted.

These files are also never read back by any part of the pipeline — they are write-only audit logs with no guaranteed persistence.

**Impact:** Audit trail is unreliable. Payment records may be lost on container restart.

**Fix:** Write payment audit logs to S3 (durable) instead of or in addition to local disk. Or ensure the Docker volume mount includes the `payments/` directory.

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
| **High** | 8 | PARTIAL | Payment sensor hardcoded path (env var fixed, still direct file read) |
| **High** | 9 | OPEN | `worker_connections` discards disconnected workers |
| **High** | 10 | OPEN | Hardcoded `valid_combinations` duplication |
| **Medium** | 11 | OPEN | `raw_events` not persisted by IO manager |
| **Medium** | 12 | FIXED | Atomic writes in IO manager |
| **Medium** | 13 | FIXED | S3 client cached via PrivateAttr |
| **Medium** | 14 | OPEN | `processed_event_ids` unbounded growth |
| **Medium** | 15 | OPEN | No RPC retry/backoff |
| **Medium** | 16 | OPEN | Gas price race condition (low risk on Arbitrum) |
| **Low** | 17 | OPEN | Unused Pydantic models |
| **Low** | 18 | OPEN | Unused config classes |
| **Low** | 19 | OPEN | Payment sensor unused `run_config` |
| **Low** | 20 | OPEN | `active_workers` count inconsistency |
| **Low** | 21 | OPEN | `payments/` directory not durable |
| **Low** | 22 | FIXED | Duplicate `os.makedirs` |
| **New** | 23 | FIXED | S3 archival on success |

### Progress: 11 FIXED, 1 PARTIAL, 11 OPEN

### Recommended Next Fixes

1. **#15** — RPC retry/backoff (MEDIUM — transient failures become permanent; WAL mitigates but doesn't prevent)
2. **#9** — Keep disconnected workers in state (HIGH — data continuity)
3. **#10** — Extract `VALID_COMBINATIONS` constant (HIGH — maintenance hygiene, quick fix)
4. **#14** — Bound `processed_event_ids` growth (MEDIUM — ticking time bomb, now affects both `worker_fees` and `worker_performance_analytics`)
