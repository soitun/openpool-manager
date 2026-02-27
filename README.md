# OpenPool Manager API

A production-grade event processing and worker payment management system for the [Livepeer](https://livepeer.org/) OpenPool orchestrator network. Built on [Dagster](https://dagster.io/), it ingests orchestrator events from S3, tracks worker performance and fees, and executes automated ETH payments on Arbitrum.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Assets](#assets)
- [Sensors](#sensors)
- [Jobs](#jobs)
- [Payment System](#payment-system)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [go-livepeer Integration](#go-livepeer-integration)
- [References](#references)

## Overview

OpenPool Manager API manages the lifecycle of a Livepeer orchestrator pool:

1. **Event Ingestion** — Polls S3 for orchestrator events (job completions, worker connections, resets)
2. **Fee Tracking** — Calculates per-worker fees earned with pool commission deductions
3. **Performance Analytics** — Ranks workers by real-time ratio (transcode) or response time (AI)
4. **Automated Payments** — Executes threshold-based ETH payments on Arbitrum with full idempotency
5. **Metrics Export** — Publishes worker summaries and performance data back to S3

The system processes events from both **AI inference** and **video transcoding** workloads across multiple geographic regions.

## Architecture

```
                          ┌──────────────────────────┐
                          │    S3 Event Source        │
                          │  (partitioned by region   │
                          │   and node type)          │
                          └────────────┬─────────────┘
                                       │
                          ┌────────────▼─────────────┐
                          │   S3 Event Sensor         │
                          │  (two-phase cursor,       │
                          │   5-min polling interval)  │
                          └────────────┬─────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                   │
           ┌────────▼──────┐  ┌───────▼───────┐  ┌───────▼────────┐
           │  raw_events   │  │processed_jobs │  │                │
           │  (S3 fetch)   │  │(AI correlate) │  │                │
           └────────┬──────┘  └───────┬───────┘  │                │
                    │                 │           │                │
         ┌──────────┼─────────────────┤           │                │
         │          │                 │           │                │
  ┌──────▼──────┐ ┌─▼──────────┐ ┌───▼───────────▼──┐             │
  │  worker_    │ │ worker_    │ │   worker_         │             │
  │  connections│ │ fees       │ │   performance_    │             │
  │             │ │            │ │   analytics       │             │
  └──────┬──────┘ └─┬──────────┘ └───────────────────┘             │
         │          │                                              │
         │   ┌──────▼──────────┐                                   │
         │   │ worker_payments │◄──── Payment Sensor (4h interval) │
         │   │ (WAL-backed)    │                                   │
         │   └──────┬──────────┘                                   │
         │          │                                              │
         └────┬─────┘                                              │
              │                                                    │
    ┌─────────▼──────────────┐                                     │
    │ worker_summary_        │                                     │
    │ analytics              │                                     │
    └─────────┬──────────────┘                                     │
              │                                                    │
    ┌─────────▼──────────────┐    ┌────────────────────────────────┘
    │ worker_summary_s3      │    │ worker_performance_s3
    │ (metrics export)       │    │ (metrics export)
    └────────────────────────┘    └─────────────────────────
```

### Multi-Partition Strategy

Data is partitioned across two dimensions:

| Dimension | Values |
|-----------|--------|
| **Node Type** | `ai`, `transcode` |
| **Region** | `us-central`, `us-west`, `eu-central`, `oceania` |

**Active partition combinations (5 of 8):**

| | us-central | us-west | eu-central | oceania |
|---|:---:|:---:|:---:|:---:|
| **AI** | active | — | — | — |
| **Transcode** | active | active | active | active |

Partition keys use the format `"{node_type}|{region}"` (e.g., `"transcode|eu-central"`).

## Features

### Fault Tolerance
- **Two-phase sensor cursor** — Cursor advances only after run succeeds; resets on failure
- **Atomic file persistence** — Tempfile + fsync + os.rename prevents corruption on crash
- **Backup recovery** — `.bak` files enable automatic fallback on corrupt primary state
- **Write-Ahead Log (WAL)** for payments — 3-phase idempotency prevents double-payments even through daemon crashes

### Event Deduplication
- **High-water-mark timestamps** with a 30-minute overlap window
- Memory usage is O(events-in-30-minutes) instead of O(all-events-ever)
- Events older than the cutoff are skipped by timestamp; events within the overlap window are deduped by ID

### Blockchain Payments
- **EIP-1559 (type 2) transactions** with capped `maxFeePerGas`
- **Exponential backoff** — 3 attempts with 1s/2s/4s for RPC calls, 2s/4s/8s for receipt wait
- **Keystore support** — JSON keystore files or raw private keys
- **Audit logging** — Every payment recorded to durable JSON audit files

### Performance Analytics
- **Transcode workers** — Ranked by median real-time ratio, response time, throughput
- **AI workers** — Ranked per model/pipeline by response time and compute units/second
- **Metrics export** — Summary and performance data published to S3 as JSON

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Dagster 1.10+ |
| Runtime | Python 3.12 |
| Metadata Storage | PostgreSQL |
| Data Processing | Pandas, NumPy |
| Blockchain | Web3.py 7.10+ (Arbitrum One, chain 42161) |
| Object Storage | S3-compatible (MinIO / AWS S3) |
| Containerization | Docker, Docker Compose |
| Configuration | Pydantic, python-dotenv |

## Project Structure

```
openpool-manager-api/
├── openpool_management/
│   ├── definitions.py          # Dagster definitions: wires assets, jobs, sensors, resources
│   ├── configs.py              # Pydantic config: S3Config, PaymentConfig, PoolConfig
│   ├── models.py               # Domain model: RawEvent with parsing/correlation methods
│   ├── partitions.py           # MultiPartitionsDefinition (region x node_type)
│   ├── resources.py            # S3Resource, Web3Client, PaymentResource, PoolResource
│   ├── io_manager.py           # PartitionedFilesystemIOManager (atomic pickle persistence)
│   ├── sensors.py              # s3_event_sensor, worker_payment_sensor
│   ├── jobs.py                 # Job definitions with asset selections
│   └── assets/
│       ├── events.py           # raw_events, processed_jobs (AI job correlation)
│       ├── worker.py           # worker_fees, worker_connections
│       ├── payment.py          # worker_payments (WAL-backed blockchain payments)
│       └── metrics.py          # Performance analytics, summaries, S3 export
├── docker/
│   ├── Dockerfile.dagster      # Dagster webserver/daemon image
│   ├── Dockerfile.user_code    # User code server image (gRPC)
│   ├── dagster-template.yaml   # Dagster instance config (templated)
│   ├── entrypoint.sh           # Template variable substitution
│   ├── docker-compose.yml      # Local development stack
│   └── docker-compose-prod.yml # Production deployment
├── workspace.yaml              # Dagster workspace (gRPC code location)
├── setup.py                    # Package definition and dependencies
├── FIXES_TODO.md               # Tracked pipeline fixes (23/23 completed)
└── docs/
    └── assets/
        └── LivepeerOrchestratorRemoteSetup.png
```

## Data Pipeline

### Event Flow

1. **Livepeer orchestrators** emit events (job completions, worker connections) to S3 as JSON files
2. **S3 Event Sensor** discovers new files, batches them (max 250), and triggers pipeline runs
3. **raw_events** fetches and parses events from S3, filtering by partition
4. **processed_jobs** correlates AI events (links `job-received` to `job-processed` via request ID) and passes through transcode events directly
5. Downstream assets consume processed events to track fees, connections, and performance

### State Persistence

All asset state is persisted via a custom `PartitionedFilesystemIOManager`:

```
{IO_MANAGER_BASE_DIR}/
├── raw_events/{node_type}/{region}/data.pkl
├── processed_jobs/{node_type}/{region}/data.pkl
├── worker_fees/{node_type}/{region}/data.pkl
├── worker_connections/{node_type}/{region}/data.pkl
├── worker_payments/{node_type}/{region}/data.pkl
├── worker_performance_analytics/{node_type}/{region}/data.pkl
├── worker_summary_analytics/{node_type}/{region}/data.pkl
├── wal/worker_payments/{node_type}/{region}/payment_wal.jsonl
└── payments/   (audit logs)
```

## Assets

### Event Processing

| Asset | Description |
|-------|-------------|
| **raw_events** | Fetches JSON events from S3, validates format, filters by partition |
| **processed_jobs** | For AI: correlates received+processed events via requestID. For transcode: pass-through |

### Worker Tracking

| Asset | Description |
|-------|-------------|
| **worker_fees** | Tracks total fees, pool commission (default 25%), and net worker earnings per ETH address |
| **worker_connections** | Tracks active/inactive worker connections; handles orchestrator-reset events |

### Payments

| Asset | Description |
|-------|-------------|
| **worker_payments** | WAL-backed blockchain payment execution. Calculates unpaid fees, sends ETH on Arbitrum, records full payment history |

### Analytics

| Asset | Description |
|-------|-------------|
| **worker_performance_analytics** | Per-worker performance metrics: real-time ratio (transcode), response time (AI), rankings by percentile |
| **worker_summary_analytics** | Merges fees + connections + payments into comprehensive per-worker and aggregate summaries |
| **worker_summary_s3** | Exports summary analytics to S3 metrics bucket as JSON |
| **worker_performance_s3** | Exports performance rankings to S3 metrics bucket as JSON |

## Sensors

### S3 Event Sensor
- **Interval:** 5 minutes (configurable)
- **Two-phase cursor design:**
  - **Phase 1 (Reconcile):** Checks pending runs — promotes cursor on SUCCESS, resets on FAILURE
  - **Phase 2 (Scan):** Discovers new S3 files, sets pending state without advancing cursor
- **Batch size:** Max 250 files per run (configurable)
- **Archival:** Processed files are copied to an archive bucket on success

### Worker Payment Sensor
- **Interval:** 4 hours (configurable)
- Scans all partitions for workers with unpaid fees exceeding the payment threshold
- Prevents overlapping payment runs for the same partition

## Jobs

| Job | Trigger | Description |
|-----|---------|-------------|
| **process_inbound_events** | S3 Event Sensor | Processes new events through fees, connections, and performance analytics. Excludes payment assets |
| **worker_payment_job** | Payment Sensor | Executes blockchain payments for eligible workers |
| **worker_summary_job** | Manual / schedule | Generates comprehensive summaries and exports to S3 |
| **worker_performance_job** | Manual / schedule | Runs performance analytics and exports to S3 |

## Payment System

### Flow

1. Workers earn fees through job completions
2. Pool commission is deducted (default 25%)
3. When a worker's unpaid fees reach the threshold (default 0.01 ETH), the payment sensor triggers
4. The payment asset executes the following:
   - Load previous payment state
   - Reconcile WAL entries (handle crash recovery)
   - Calculate unpaid fees per worker
   - Execute EIP-1559 transactions on Arbitrum
   - Record payment history and audit logs

### Idempotency (Write-Ahead Log)

Each payment follows a 3-phase WAL protocol:

1. **Write "pending"** entry to WAL before sending
2. **Send ETH** via Web3Client
3. **Write result** entry to WAL after confirmation

On startup, `_reconcile_wal()` handles crash recovery:
- Completed payments are credited
- Ambiguous "pending" entries (crash during send) are flagged for manual investigation

## Getting Started

### Prerequisites

- Python 3.12+
- PostgreSQL (for Dagster metadata)
- S3-compatible object storage (MinIO for local development)

### Local Development

Create and activate a virtual environment:

```bash
python3 -m venv openpool-venv
source openpool-venv/bin/activate
```

Install dependencies:

```bash
pip install dagster dagster-webserver dagster-aws
pip install -e ".[dev]"
```

Start the Dagster development server:

```bash
dagster dev
```

Open http://localhost:3000 to access the Dagster UI.

### Adding Dependencies

Add new Python dependencies to `setup.py`:

```python
install_requires=[
    "dagster",
    "dagster-aws",
    "dagster-postgres",
    "dagster-webserver",
    "pandas",
    "dotenv",
    "pyarrow",
    "web3"
],
```

## Configuration

### Environment Variables

#### S3 / Object Storage

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_BUCKET` | Source bucket for orchestrator events | `open-pool-events` |
| `S3_METRICS_BUCKET` | Destination bucket for analytics exports | `open-pool-metrics` |
| `S3_ENDPOINT` | S3-compatible endpoint URL | — |
| `S3_ARCHIVE_BUCKET` | Bucket for archiving processed files | — |
| `S3_ARCHIVE_PREFIX` | Key prefix for archived files | `archive/` |
| `AWS_ACCESS_KEY_ID` | S3 access key | — |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key | — |

#### Blockchain / Payments

| Variable | Description | Default |
|----------|-------------|---------|
| `ETH_RPC_ENDPOINT` | Arbitrum RPC URL | `https://arb1.arbitrum.io/rpc` |
| `ETH_PRIVATE_KEY` | Private key or path to JSON keystore | — |
| `ETH_KEYSTORE_PASSWORD` | Keystore password or path to password file | — |
| `PAYMENT_THRESHOLD_WEI` | Minimum unpaid fees to trigger payment | `10000000000000000` (0.01 ETH) |
| `MAX_GAS_PRICE_WEI` | Gas price ceiling | `5000000000000` (5000 gwei) |
| `GAS_LIMIT` | Transaction gas limit | `1000000` |
| `CHAIN_ID` | Blockchain chain ID | `42161` (Arbitrum One) |

#### Pool

| Variable | Description | Default |
|----------|-------------|---------|
| `POOL_COMMISSION_RATE` | Pool's fee percentage (0.0 - 1.0) | `0.25` (25%) |

#### Pipeline Tuning

| Variable | Description | Default |
|----------|-------------|---------|
| `MAX_FILES_PER_BATCH` | Max S3 files per sensor batch | `250` |
| `S3_SENSOR_INTERVAL_SECONDS` | S3 sensor polling interval | `30` |
| `PAYMENT_SENSOR_INTERVAL_SECONDS` | Payment sensor interval | `600` |
| `IO_MANAGER_BASE_DIR` | Base directory for pickle state files | `data/` |

#### Dagster / PostgreSQL

| Variable | Description | Default |
|----------|-------------|---------|
| `DAGSTER_HOME` | Dagster metadata directory | `/opt/dagster/dagster_home` |
| `DAGSTER_POSTGRES_USER` | PostgreSQL username | `postgres` |
| `DAGSTER_POSTGRES_PASSWORD` | PostgreSQL password | `postgres` |
| `DAGSTER_POSTGRES_DB` | PostgreSQL database name | `postgres` |
| `DAGSTER_POSTGRES_HOST` | PostgreSQL hostname | `dagster-postgres` |

## Deployment

### Docker Images

Build the three required images:

```bash
# Dagster webserver and daemon
docker build -t open-pool-dagster-webserver:latest -f docker/Dockerfile.dagster .
docker build -t open-pool-dagster-daemon:latest -f docker/Dockerfile.dagster .

# User code server (gRPC)
docker build -t open-pool-mgmt:latest -f docker/Dockerfile.user_code .
```

### Docker Compose

The system runs as three containers plus PostgreSQL:

| Service | Image | Port | Role |
|---------|-------|------|------|
| **dagster-webserver** | open-pool-dagster-webserver | 3000 | Dagster UI |
| **dagster-daemon** | open-pool-dagster-daemon | — | Runs sensors and schedules |
| **openpool_management-code-server** | open-pool-mgmt | 4000 (gRPC) | Hosts pipeline code |
| **dagster-postgres** | postgres:14 | 5432 | Dagster metadata store |

#### Local Development

```bash
docker compose -f docker/docker-compose.yml up
```

#### Production

```bash
docker compose -f docker/docker-compose-prod.yml up -d
```

Production uses `DockerRunLauncher` which spawns isolated containers for each pipeline run, mounting the shared data volume at `/pool`.

### Volume Mounts

| Volume | Mount Point | Purpose |
|--------|-------------|---------|
| `open-pool-mgmt-data` | `/pool` | Pipeline state (pickle files, WAL, audit logs) |
| Docker socket | `/var/run/docker.sock` | DockerRunLauncher spawns run containers |

### Network

All services communicate on the `open-pool-network` (local) or `ingress` (production) Docker network.

## go-livepeer Integration

This system consumes events from a modified [go-livepeer](https://github.com/livepeer/go-livepeer) orchestrator with the following additions:

- Supports ETH address forwarding from remote workers
- Global Event Tracker publishes events to S3
- **Events emitted:**
  - `orchestrator-reset` — Clears all worker connection state
  - `worker-connected` — New worker joins the pool
  - `worker-disconnected` — Worker leaves the pool
  - `job-processed` (AI & Transcode) — Contains fees, price data, and worker ETH address (transcode) or request ID (AI)
  - `job-received` (AI only) — Links request ID to worker ETH address for AI job correlation
- All events use `computeUnits` and `pricePerComputeUnit` in wei (underlying unit: pixels)

### Orchestrator Network Topology

<img src="docs/assets/LivepeerOrchestratorRemoteSetup.png">

## References

- [Dagster Documentation](https://docs.dagster.io/)
- [Livepeer Protocol](https://livepeer.org/)
- [Arbitrum One](https://arbitrum.io/)
- [EIP-1559 Transaction Format](https://eips.ethereum.org/EIPS/eip-1559)
