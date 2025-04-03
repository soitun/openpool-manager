# OpenPool Manager API — Pipeline Fixes TODO

Comprehensive review of the Dagster pipeline identifying defects, data-loss vectors, performance issues, and code quality concerns. Organized by severity.

---

## CRITICAL — Data Loss / Financial Risk

### 1. `process_inbound_events_job` uses `AssetSelection.all()` — triggers payments on every S3 ingest

**File:** `openpool_management/jobs.py:5-11`

The S3 sensor triggers `process_inbound_events`, which selects **all** assets. This means every 5-minute S3 batch materializes `worker_payments`, potentially sending real ETH transactions on every ingest cycle — not just when the payment sensor fires every 4 hours. The payment sensor (`worker_payment_sensor`) is effectively redundant when this job path is active.

**Impact:** Unintended payment execution on every ingest run. Payments should only be triggered deliberately.

**Fix:** Change `process_inbound_events_job` to explicitly select only the ingest/transform/tracking/analytics assets, excluding `worker_payments`:

```python
process_inbound_events_job = define_asset_job(
    name="process_inbound_events",
    selection=AssetSelection.all()
        - AssetSelection.keys(AssetKey(["worker", "worker_payments"])),
    ...
)
```

Or enumerate the intended assets explicitly.

---

### 2. Sensor cursor advances before the run succeeds

**File:** `openpool_management/sensors.py:170-171, 180-181`

The `s3_event_sensor` updates `cursor_data` immediately after yielding each `RunRequest` (line 171), then persists the cursor at line 181. If the triggered run **fails** (S3 timeout, processing error, OOM, etc.), the cursor has already moved past those S3 keys. Those files will never be retried.

**Impact:** Permanent data loss — S3 event files are skipped silently on run failure.

**Fix:** Do not advance the cursor optimistically. Instead:
- Option A: Use Dagster's built-in cursor semantics — only update the cursor **after** confirming the run completed successfully (requires a separate sensor tick or callback).
- Option B: Track "in-flight" keys separately from "confirmed" keys. Only promote in-flight keys to confirmed after the run succeeds.
- Option C: Use `context.instance.get_runs()` to check if the *previous* run for a partition succeeded before advancing the cursor for that partition.

---

### 3. `worker_fees` has no event deduplication — re-processes all events from `processed_jobs`

**File:** `openpool_management/assets/worker.py:34, 72-100`

`worker_fees` loads its previous cumulative state (lines 37-55), then iterates over **all** `processed_events` from its input (line 72), adding fees to the cumulative totals. This is correct only if `processed_jobs` returns exclusively *new* events each time.

However, if `process_inbound_events_job` re-materializes `processed_jobs` and the auto-discovery fallback (`events.py:37-53`) picks the same latest S3 file, the same events are re-processed and fees are double-counted. The deduplication logic present in `worker_performance_analytics` (via `processed_event_ids` set) is completely absent from `worker_fees`.

**Impact:** Fee inflation leading to overpayment.

**Fix:** Add a `processed_event_ids` set to `worker_fees` state (same pattern as `worker_performance_analytics`). Filter out already-processed events before accumulating fees:

```python
processed_event_ids = set(existing_states.get("_processed_event_ids", []))
new_events = [e for e in job_events if e.id not in processed_event_ids]
for event in new_events:
    processed_event_ids.add(event.id)
    # ... accumulate fees ...
```

---

### 4. No idempotency on blockchain payments

**File:** `openpool_management/assets/payment.py:159-167`

If `send_eth()` succeeds on-chain but the process crashes before `handle_output` persists the updated `payment_states`, the next run will see the same `unpaid_fees` and send the payment **again**. There is no transaction log or idempotency key checked before sending.

The `payments/*.json` files written at line 265 are local-only, write-only, and not consulted before paying.

**Impact:** Double (or multiple) payments to workers. Real ETH lost.

**Fix:** Implement a pre-payment idempotency check:
1. Before each `send_eth()`, write a pending payment record to a durable store (S3 or a local WAL file) keyed by `(eth_address, partition, payment_id)`.
2. On startup, check for pending records. If a pending record exists, query the chain for the transaction hash before re-sending.
3. Alternatively, persist payment state incrementally after each successful `send_eth()` rather than batching all at the end.

---

### 5. `worker_payments` self-loads via raw `InputContext` — fragile and incorrect context

**File:** `openpool_management/assets/payment.py:41-48`

The asset manually constructs a bare `InputContext` to load its own previous output. This bypasses Dagster's dependency graph. The `InputContext` is constructed with minimal metadata (`metadata={}`), and the IO manager's `_get_path` depends on `context.asset_key` and `context.partition_key` — but a manually constructed `InputContext` may not wire these identically to a real Dagster-provided context.

If the path resolution diverges, the asset silently starts from empty state, **losing all payment history**.

The same fragile pattern appears in:
- `worker_fees` — `worker.py:40-48`
- `worker_connections` — `worker.py:180-188`
- `worker_performance_analytics` — `metrics.py:59-66`
- `worker_summary_analytics` — `metrics.py:484-491`

**Impact:** Silent state loss across any of these assets if Dagster's `InputContext` API changes or if the context attributes don't match what the IO manager expects.

**Fix:** Use Dagster's `self_dependent_asset` pattern or explicitly declare each asset as depending on its own previous partition output via `AssetIn`. Alternatively, use the IO manager directly with a manually constructed path that mirrors `_get_path` logic — but encapsulate this in a shared helper to keep it in sync.

---

## HIGH — Correctness Issues

### 6. `processed_jobs` loads previous state inconsistently

**File:** `openpool_management/assets/events.py:154-161`

For AI jobs, `processed_jobs` tries to load its own previous output to recover `pending_jobs`. It uses `context.instance.get_latest_materialization_event()` to check existence, then `context.resources.io_manager.load_input(context)` — passing the *output execution context* as an *input context*. This is semantically wrong. The asset execution context is not an `InputContext`.

It works only because the IO manager reads just `.asset_key` and `.partition_key` from whatever object it receives. If Dagster tightens this API or adds type checks, this breaks.

**Impact:** Potential loss of pending AI job correlation state, leading to AI `job-processed` events missing their ETH addresses (unpayable jobs).

**Fix:** Same as #5 — use a proper self-dependency or a dedicated helper for loading previous state.

---

### 7. `pd.DataFrame()` reference in IO manager without pandas import

**File:** `openpool_management/io_manager.py:93-98`

The `load_input` fallback for `worker_performance_analytics` calls `pd.DataFrame()`, but `pandas` is not imported in `io_manager.py`. This will raise `NameError` if this code path is reached (first-ever materialization of performance analytics, or corrupted pickle file).

**Impact:** Runtime crash on first performance analytics materialization or after data corruption.

**Fix:** Either add `import pandas as pd` to `io_manager.py`, or return a plain dict fallback and let the asset handle the conversion:

```python
elif "worker_performance_analytics" in asset_key_str:
    return {
        "transcode_performance": None,
        "ai_performance": None,
        "ai_model_pipeline_rankings": {}
    }
```

---

### 8. Payment sensor reads pickle files directly from disk

**File:** `openpool_management/sensors.py:216-219`

`worker_payment_sensor` hardcodes the path `data/worker/worker_payments/{node_type}/{region}/data.pkl` and reads with `pickle.load()`. This is tightly coupled to the IO manager's directory layout.

**Impact:**
- If `IO_MANAGER_BASE_DIR` env var changes, the sensor silently reads nothing and never triggers payments.
- If the asset key path changes, same result.
- Security: unpickling untrusted data (low risk since files are self-written, but violates best practice).

**Fix:** Use Dagster's `AssetSelection` or materialization event API to load the latest materialized value through the IO manager, rather than reading the filesystem directly. Or at minimum, read `IO_MANAGER_BASE_DIR` from the same env var the IO manager uses.

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

### 12. No atomic writes in IO manager

**File:** `openpool_management/io_manager.py:65-67`

`handle_output` writes directly to the final path with `pickle.dump()`. If the process is killed mid-write (OOM, SIGKILL, disk full), the pickle file is corrupted. The next `load_input` will hit the `except` block (line 115) and return an empty fallback — **silently losing all accumulated state** for that partition.

**Impact:** Complete state loss for any asset/partition on interrupted writes. For `worker_fees` or `worker_payments`, this means losing all fee/payment history.

**Fix:** Write to a temporary file in the same directory, then `os.rename()` (atomic on POSIX):

```python
import tempfile

def handle_output(self, context, obj):
    path = self._get_path(context)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Write to temp file first, then atomically rename
    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            pickle.dump(obj, f)
        os.rename(tmp_path, path)
    except:
        os.unlink(tmp_path)
        raise
```

---

### 13. `S3Resource` creates a new boto3 client on every call

**File:** `openpool_management/resources.py:25-44`

`S3Resource.client()` creates a fresh `boto3.client` each invocation. During `raw_events` processing of 250 files, `get_object()` is called 250 times, each creating a new client with new TCP connections and auth negotiation.

**Impact:** Wasted connection setup time. Potential rate-limiting from the S3 endpoint. Slower ingest.

**Fix:** Cache the client instance using `PrivateAttr`, same pattern as the `PaymentResource` fix:

```python
from pydantic import PrivateAttr

class S3Resource(ConfigurableResource):
    config: S3Config
    _client: Optional[Any] = PrivateAttr(default=None)

    def client(self):
        if self._client is None:
            self._client = boto3.client(...)
        return self._client
```

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

### 22. Duplicate `os.makedirs` call in IO manager

**File:** `openpool_management/io_manager.py:60, 63`

```python
os.makedirs(os.path.dirname(path), exist_ok=True)  # line 60
# ...
os.makedirs(os.path.dirname(path), exist_ok=True)  # line 63
```

**Impact:** Harmless but indicates copy-paste artifact.

**Fix:** Remove the duplicate call.

---

## Summary

| Priority | # | Key Theme |
|----------|---|-----------|
| **Critical** | 5 | Data loss, double payments, fee inflation, cursor advancement |
| **High** | 5 | Correctness bugs, missing imports, fragile state loading |
| **Medium** | 6 | Resilience, performance, unbounded growth |
| **Low** | 6 | Dead code, config mismatches, minor bugs |

### Recommended Fix Order

1. **#2** — Cursor advancement (most direct data-loss path)
2. **#4** — Payment idempotency (financial risk)
3. **#12** — Atomic writes in IO manager (state corruption risk)
4. **#1** — `AssetSelection.all()` triggering payments (unintended payments)
5. **#3** — Event deduplication in `worker_fees` (fee inflation)
6. **#5/#6** — Fragile self-loading pattern (silent state loss)
7. **#7** — Missing pandas import (runtime crash)
8. **#15** — RPC retry logic (resilience)
9. Everything else
