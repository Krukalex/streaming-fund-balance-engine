# Streaming Fund Balance Engine

## Introduction

The Streaming Fund Balance Engine is an event-driven finance pipeline designed to showcase modern data engineering capabilities using Kafka, Airflow, and Apache Spark. It simulates transaction event flow across funds, maintains an append-only event log, and ultimately computes canonical fund balances through batch processing.

This project demonstrates end-to-end data engineering skills—from event generation through idempotent state derivation—and is structured for senior-level portfolio and interview discussion.

## Technologies Used

- **Apache Kafka**: Event streaming and durable messaging backbone
- **Apache Spark**: Batch processing and stateful aggregation
- **Apache Airflow**: Workflow orchestration and scheduling
- **Python**: Emitter and consumer applications
- **Docker**: Containerized services for local development
- **PostgreSQL**: Ledger, balances, run metrics, audit logs, and Kafka offset checkpoints

## Architecture

The system is built around four core components:

1. **Event Emitter**

- Produces transaction events for multiple funds
- Sends events into Kafka topics for downstream processing

2. **Kafka Stream**

- Serves as the event log and durable messaging backbone
- Maintains an ordered, replayable history of all transactions

3. **Airflow Service**

- Orchestrates and schedules batch Spark jobs on a fixed interval
- Supplies a stable `run_id` per DAG run for retries and cross-table correlation
- Provides replay and backfill capabilities

4. **Spark Processor**

- Consumes transaction events from Kafka in **offset-based batches** (not Structured Streaming)
- Resumes each run from the last successful checkpoint stored in PostgreSQL (`kafka_offsets`)
- Writes a transaction ledger, audit logs, run metrics, and derived fund/deal balances in PostgreSQL

## Key Features

- Multi-fund transaction simulation with realistic fund-deal relationships
- Append-only event stream storage with guaranteed ordering
- Offset-based Spark batch reads with PostgreSQL checkpointing (`kafka_offsets`)
- Batch orchestration with Airflow DAGs and task dependencies
- **Airflow-run configuration** for incremental processing or bounded Kafka backfills (`run_mode`, `starting_offset`, `ending_offset`)
- Spark-based balance computation with idempotency and reconciliation
- Canonical state management for fund balances
- Modular design for easy extension and testing

## Key Engineering Decisions

### Batch Kafka reads vs Structured Streaming

- The Spark job uses the **batch** Kafka source (`spark.read.format("kafka")`) with **PostgreSQL offset checkpoints**, not Structured Streaming.
- **Why for this project:** bounded, on-demand runs are easier to demo, replay, and explain in interviews; the same idempotency patterns (ledger, audit logs, offsets) apply whether the job is triggered manually or on a schedule.
- **Production nuance:** sub-minute latency would push you toward Structured Streaming (Databricks, Flink, or Spark SS with checkpointing). Many fund-accounting and balance-derivation workloads still run as **scheduled micro-batches** (hourly/daily) because correctness and auditability matter more than real-time updates. The offset-table pattern here maps cleanly to that model.
- **What would change at scale:** replace the JDBC offset table with Spark SS checkpoints or Kafka consumer groups; add watermarks for event-time lateness in streaming mode; keep the same ledger idempotency rules on the sink.

### Airflow → Spark via `docker exec` (local only)

- The DAG task uses the Python **Docker SDK** to `exec` `spark-submit` inside the long-running `spark-master-fund-balance` container, rather than `DockerOperator` spinning up a one-off container per run.
- **Why:** faster local iteration, code and JARs are already bind-mounted, and avoids fragile host-path mounts (especially on Windows). `DockerOperator` is the closer analog to production job submission but adds startup latency and compose wiring complexity for a portfolio demo.
- **How it works:** Astro Airflow (`astro dev start`) runs the control plane; `docker/docker-compose.yml` runs the data plane (Kafka, Postgres, Spark). `airflow/docker-compose.override.yml` joins Airflow workers to the external `fund-pipeline` network and mounts `/var/run/docker.sock` so the task can reach the Spark container by name.

### Idempotency and replay-first design

- **`transaction_ledger`:** `ON CONFLICT (transaction_id) DO NOTHING` — replays do not double-insert movements.
- **`transaction_balance`:** full `SUM` over the ledger each run — identical ledger → identical balances.
- **Audit logs:** deterministic SHA-256 `surrogate_pk` keys with `ON CONFLICT DO NOTHING` — first write wins across retries.
- **`kafka_offsets`:** only rows with `status = 'SUCCESS'` advance `MAX(end_offset)` — failed runs do not skip data.
- **`run_id`:** passed from Airflow (`dag_run.run_id`) so retries of the same DAG run correlate metrics and audit rows under one identifier.

Together, these choices make **Airflow retries safe** and support **manual replay** for demos without corrupting canonical state.

### Production deployment (how this would evolve)

Local Docker choices are intentional shortcuts. A production deployment would typically:

| Layer             | Local (this repo)                    | Production direction                                                                     |
| ----------------- | ------------------------------------ | ---------------------------------------------------------------------------------------- |
| **Kafka**         | Single-broker Compose                | MSK, Confluent Cloud, or on-prem cluster with replication                                |
| **Spark**         | Standalone master/worker in Docker   | Databricks jobs, EMR on EKS, or Dataproc — submitted via API/operator, not `docker exec` |
| **Orchestration** | Astro CLI + `docker exec` task       | Astronomer, MWAA, or Dagster with a proper Spark/K8s operator                            |
| **State DB**      | Postgres in Compose                  | RDS/Aurora or warehouse tables (e.g. Snowflake) for ledger and balances                  |
| **Secrets**       | Plain env vars in compose            | Vault, AWS Secrets Manager, or Airflow connections                                       |
| **Observability** | Postgres audit tables + Airflow logs | Metrics (Datadog/Prometheus), alerting on `run_metrics`, data quality monitors           |

For **low-latency** fund P&L or risk, Structured Streaming on Databricks with idempotent foreachBatch sinks to the same ledger schema would be the natural upgrade. For **batch SLAs** (e.g. end-of-day balances), the current micro-batch + offset checkpoint design is already production-shaped; only the runtime and ops layer would change.

### Known limitations (local demo)

- **Empty batches:** if a run reads zero Kafka messages, no `kafka_offsets` row is written and the read pointer does not advance (acceptable for demos; production would still record a heartbeat or empty-run checkpoint).
- **Money as `DOUBLE PRECISION`:** fine for a portfolio; production would use `NUMERIC`/`DECIMAL`.
- **Single partition / topic:** offset logic assumes partition `0` on first run; extend `get_starting_offsets_json()` when adding partitions.

## Spark Processing Logic

The Spark job processes Kafka transaction events into a durable **transaction ledger**, then derives deterministic, auditable balances per fund/deal pair.

1. **Read data from Kafka (offset-based batch)**

- Before reading, load the last successful checkpoint from `kafka_offsets` and build Spark’s `startingOffsets` JSON (see [Offset-based batch processing](#offset-based-batch-processing)).
- If the checkpoint table is empty, start at offset **0** for partition **0** (first run).
- Consume only new messages from that position through the current log end (or an explicit `endingOffsets` bound during development).
- Retain metadata (`topic`, `partition`, `offset`, `kafka_timestamp`) for traceability and deterministic tie-breaking.

2. **Flatten into a DataFrame**

- Parse JSON payloads into a typed schema.
- Flatten nested `fund` and `deal` objects into top-level columns.
- Apply fallback handling for missing fields to support schema evolution.
- **Type note:** emitters send `fund_id` and `deal_id` as JSON integers; the Spark schema reads them as **strings** for consistent JDBC/Postgres handling (values coerce cleanly to `VARCHAR` keys in the database).

3. **Detect late arriving events**

- Define **late** as **ingest lag**: how long after the business-time `transaction_timestamp` the record appeared in Kafka, using epoch seconds: `unix_timestamp(kafka_timestamp) - unix_timestamp(transaction_timestamp)`.
- Flag rows where ingest lag exceeds a fixed threshold (e.g. **900 seconds**); the same message always yields the same lag on replay, unlike comparisons to `current_timestamp()`.
- Rows that qualify are written to `late_arriving_event_log`; each pipeline run also records how many late rows were observed in that run’s batch in `run_metrics` (see [Late arriving events (design)](#late-arriving-events-design)).

4. **Identify duplicate events**

- Treat any repeated `transaction_id` as a duplicate candidate.
- Resolve duplicates deterministically using descending `transaction_timestamp`, then descending `kafka_timestamp`, then descending `offset`.

5. **Determine winning duplicate records**

- Rank records per `transaction_id` and keep the highest-ranked row as the winner.
- Persist winner metadata to an audit table to document why specific rows were retained.

6. **Build duplicate summary diagnostics**

- Compute per-ID duplicate stats (`min transaction_timestamp`, `max transaction_timestamp`, `duplicate_count`).
- Join duplicate stats with winner metadata to create a complete duplicate summary.
- Persist this summary to a metrics/audit table for debugging and replay analysis.

7. **Apply business signing logic**

- Convert transaction types into signed cash flow values:
  - `DEBIT` → negative amount
  - `CREDIT` → positive amount

8. **Upsert into the transaction ledger**

- After deduplication, write one row per winning transaction to `transaction_ledger` with signed amount and Kafka lineage.
- Use `ON CONFLICT (transaction_id) DO NOTHING`: each `transaction_id` is stored at most once. Replaying the **same** input does not add a second ledger row.

9. **Derive canonical balances from the ledger**

- Recompute `transaction_balance` as `SUM(transaction_amount)` grouped by `(fund_id, deal_id)` over the **entire** ledger (not a batch-only incremental add in SQL).
- Upsert into `transaction_balance` with `ON CONFLICT DO UPDATE`, setting `balance` to the new aggregate. If the ledger is unchanged (same inputs replayed), the sums are unchanged and balances stay the same (**idempotent**). If new transactions were added to the ledger since the last run, affected fund/deal balances update to reflect the new totals.

10. **Aggregate and persist run metrics**

- Build one row per `run_id` with `record_count` (rows in the batch), `duplicate_count` (duplicate groups observed in the batch), and `late_arrival_count` (rows matching the ingest-lag rule in the batch).
- Upsert into `run_metrics` on `run_id` with `ON CONFLICT DO UPDATE`, so a **retry/replay of the same run** replaces metrics with the latest computed values for that run.

## Offset-based batch processing

Spark uses the **batch** Kafka source (`spark.read.format("kafka")`), not Structured Streaming. Each job run processes a **slice** of the topic instead of re-reading the full history every time.

### Checkpoint table: `kafka_offsets`

PostgreSQL table `kafka_offsets` records **per-run consumption metadata** and acts as the read pointer for the next batch:

| Column          | Description                                                                                                                              |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `run_id`        | Correlates with the pipeline run (from Airflow `dag_run.run_id` when orchestrated, or a UUID fallback in `spark/job.py` for manual runs) |
| `topic`         | Kafka topic name (e.g. `transactions`)                                                                                                   |
| `partition`     | Topic partition id                                                                                                                       |
| `start_offset`  | First offset read in this run (inclusive; matches Spark `startingOffsets`)                                                               |
| `end_offset`    | **Next** offset to read after this run (exclusive end; becomes the next run’s `startingOffsets`)                                         |
| `status`        | Run outcome (e.g. `SUCCESS`); only successful runs advance the read pointer                                                              |
| `run_timestamp` | When the checkpoint row was recorded                                                                                                     |

DDL is in `db/init.sql`.

### How the read pointer is resolved

`get_starting_offsets_json()` in `spark/job.py`:

1. Queries `MAX(end_offset)` per `(topic, partition)` from `kafka_offsets` where `status = 'SUCCESS'`.
2. Builds a JSON map for Spark, e.g. `{"transactions": {"0": 5}}`.
3. If no successful checkpoints exist, defaults to `{"transactions": {"0": 0}}` (read from the beginning).

That value is passed to `.option("startingOffsets", ...)`.

**Offset semantics (Spark batch Kafka):**

- `startingOffsets` — **inclusive** (first offset to read).
- `endingOffsets` — **exclusive** (stop before this offset).
- Persist `end_offset` as the exclusive end so the following run does not reprocess the same messages.

### Run lifecycle

```text
  ┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
  │ kafka_offsets   │────▶│ Spark batch  │────▶│ ledger, balances│
  │ (last SUCCESS)  │     │ read + transform│   │ run_metrics, …  │
  └─────────────────┘     └──────────────┘     └────────┬────────┘
         ▲                                                │
         └──────── write row (start/end, SUCCESS) ───────┘
```

After sinks succeed, insert a row into `kafka_offsets` for the current `run_id` with the processed range and `status = 'SUCCESS'`. Failed runs should not advance the pointer (or should record a non-`SUCCESS` status so `MAX(end_offset)` ignores them).

## Late arriving events (design)

This section records the product and replay semantics for **late** detection and logging.

**Batch scope:** late detection and `run_metrics` counts apply to messages in the **current offset batch** (from `startingOffsets` through the log end for that run), not the entire topic history on every execution.

### Definition of “late”

A record is treated as **late for observability** when **ingest lag** exceeds a configured threshold:

- **Ingest lag (seconds)** = `unix_timestamp(kafka_timestamp) - unix_timestamp(transaction_timestamp)`
- **`kafka_timestamp`** is the timestamp from the Spark Kafka source for that message (broker metadata).
- **`transaction_timestamp`** is the business event time from the payload.

This definition is **stable across replays**: reprocessing the same message recomputes the **same** lag, so a batch replay does not reinterpret “late” using wall-clock time at replay.

### Surrogate primary key and idempotent log writes

Each row in `late_arriving_event_log` is keyed by a **deterministic `surrogate_pk`**: a SHA-256 hex string over a fixed UTF-8 literal that includes the **physical Kafka identity** — `topic`, `partition`, and `offset` — in a single documented order (e.g. `LATE|{topic}|{partition}|{offset}` with `|` separators). That identity is stable for the life of the message in the log.

Inserts use **`ON CONFLICT (surrogate_pk) DO NOTHING`**: the **first** successful write wins; replays and retries do **not** replace or duplicate the observability row. That preserves “what we thought when we first persisted this late message.”

### Run metrics vs log cardinality

**`run_metrics.late_arrival_count`** is the count of messages **in the current batch slice** that satisfy the late predicate **in Spark**, computed every time the job runs. That number answers: _“this execution observed n late messages in its input.”_ It is **not** required to equal the number of **new** rows appended to `late_arriving_event_log` on that attempt (replays often append **zero** new rows because of `DO NOTHING`).

### Why not `current_timestamp()` − `transaction_timestamp`?

Comparing business time to **job wall clock** changes every time you run or replay a job, so old batches can incorrectly inflate “late” over time. Ingest lag ties late-ness to **when the message actually landed in Kafka** relative to the business event, which is the usual operational reading for pipeline delay.

## PostgreSQL schema

PostgreSQL holds the canonical **balance state**, **per-run rollup metrics**, and **drill-down logs**. Each run is tagged with a **`run_id`** for correlation across tables (see [Airflow orchestration](#airflow-orchestration)).

### Tables and grain

| Table                     | Grain / identity                                                         | Purpose                                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `transaction_ledger`      | `transaction_id` (+ unique Kafka coordinates)                            | Append-style ledger of signed movements; idempotent per `transaction_id` on replay.                                           |
| `transaction_balance`     | `(fund_id, deal_id)`                                                     | Current net balance per fund/deal pair, derived as `SUM` over the ledger.                                                     |
| `run_metrics`             | `run_id`                                                                 | One row per pipeline run: volumes and quality counts; updated on retry of the same `run_id`.                                  |
| `duplicate_records_log`   | **Surrogate PK** + unique `(run_id, transaction_id)`                     | Duplicate groups for that run: counts, min/max business time, and full Kafka pointer for the retained winner.                 |
| `late_arriving_event_log` | **Surrogate PK** + unique `(kafka_topic, kafka_partition, kafka_offset)` | One durable row per **physical Kafka message** flagged late; survives retries, backfills, and replays without double inserts. |
| `kafka_offsets`           | `run_id`                                                                 | Per-run Kafka consumption checkpoint: topic, partition, offset range, and status for offset-based batch resume.               |

### Idempotent surrogate keys (SHA-256, 64-char lowercase hex)

Both log tables use a **`surrogate_pk`** primary key computed in Spark as **`sha2(..., 256)`** (hex) over a UTF-8 string with fixed prefixes so key spaces never collide:

- **`duplicate_records_log.surrogate_pk`**: hash the UTF-8 literal string `DUP|{run_id}|{transaction_id}` (exact `|` separators). Same `run_id` + same `transaction_id` always maps to one row, so retries of the **same** run are idempotent.
- **`late_arriving_event_log.surrogate_pk`**: hash the UTF-8 literal string `LATE|{kafka_topic}|{partition}|{offset}`, with `partition` and `offset` as decimal integers (no zero-padding). Identity is the Kafka coordinate, not `transaction_id`.

**Database constraints:** DDL adds **unique indexes** on the natural tuples above as well (`duplicate_records_identity_uq`, `late_arriving_event_kafka_uq`) so ingestion bugs cannot silently diverge surrogate vs physical identity.

**`transaction_ledger`**

- One row per `transaction_id` from the deduplicated (winning) stream, with signed `transaction_amount`, fund/deal keys, and Kafka metadata.
- `ON CONFLICT (transaction_id) DO NOTHING` on insert: replays with the same winning rows do not create duplicate ledger entries.

**`transaction_balance`**

- `fund_id`, `deal_id` — natural key (identifiers only; names live on events / dimensions, not on state).
- `balance` — `SUM(transaction_amount)` from `transaction_ledger` for that fund/deal (full snapshot recompute each run).
- **Idempotency:** identical ledger contents produce identical sums; `ON CONFLICT DO UPDATE` refreshes balances only when the recomputed total changes (e.g. new `transaction_id`s landed in the ledger).

**`run_metrics`**

- `record_count` — rows considered in the run after parsing (aligned with Spark’s batch scope).
- `duplicate_count` — number of distinct `transaction_id` values that had duplicates in that run (batch observation); on first load for a `run_id`, should match `SELECT COUNT(*) FROM duplicate_records_log WHERE run_id = ?`.
- `late_arrival_count` — number of messages in **this run’s batch** that satisfied the ingest-lag late rule in Spark (**observed in batch**). It does **not** necessarily equal new rows inserted into `late_arriving_event_log` on replay (`ON CONFLICT DO NOTHING` on `surrogate_pk`).
- `run_timestamp` — when the run was recorded (updated on `ON CONFLICT DO UPDATE` when the same `run_id` is retried).

**`kafka_offsets`**

- One row per pipeline run (`run_id` primary key) recording which Kafka slice was consumed.
- `start_offset` / `end_offset` — inclusive start and **exclusive** end of the batch (see [Offset-based batch processing](#offset-based-batch-processing)).
- `status` — only rows with `SUCCESS` participate in `MAX(end_offset)` when computing the next `startingOffsets`.
- Ties batch boundaries to the same `run_id` used in `run_metrics` and audit logs.

**`duplicate_records_log`**

Winner metadata includes **`winner_partition`** and **`winner_offset`** (with **`winner_topic`**) so the chosen message can be relocated in Kafka independently of other ties.

**`late_arriving_event_log`**

- **`txn_age_sec`** — persisted **ingest lag** in seconds: `unix_timestamp(kafka_timestamp) - unix_timestamp(transaction_timestamp)`, matching the Spark late rule. See [Late arriving events (design)](#late-arriving-events-design).

DDL for these objects lives in **`db/init.sql`**.

## Airflow orchestration

Airflow lives in **`airflow/`** (Astronomer Astro project) and orchestrates the Spark batch on a schedule. The data plane (Kafka, Postgres, Spark) runs separately via **`docker/docker-compose.yml`**.

### DAG: `fund_balance_dag`

- **File:** `airflow/dags/fund_balance_dag.py`
- **Schedule:** `@daily` (demo cadence; adjust as needed)
- **Retries:** 2, with exponential backoff starting at 2 seconds (`default_args` + task-level `retry_exponential_backoff`)
- **Failure handling:** `on_failure_callback` in `airflow/include/fund_balance_dag/callbacks.py` logs task context and classifies failures (timeout vs application error)

### How the Spark job is triggered

The task does **not** use `DockerOperator`. It:

1. Reads `dag_run.run_id` from Airflow context and normalizes it (`-` → `_`) for use as `RUN_ID`.
2. Uses the Docker Python SDK (`docker.from_env()`) to `exec` into **`spark-master-fund-balance`**.
3. Runs `spark-submit` against `spark://spark-master-fund-balance:7077` with comma-separated JARs from `/opt/spark/jars-extra` and `/opt/spark-apps/job.py`.
4. Raises if `spark-submit` exits non-zero so Airflow can retry.

`RUN_ID` is passed as an environment variable on the `spark-submit` command so `spark/job.py` correlates `run_metrics`, audit logs, and (in incremental mode) `kafka_offsets` under the same identifier across retries of one DAG run.

### Run configuration: incremental vs backfill

`fund_balance_dag` exposes three **DAG params** (defaults for scheduled runs). Override them when triggering manually via the Airflow UI form or `dag_run.conf`:

| Param | Default | Description |
| ----- | ------- | ----------- |
| `run_mode` | `incremental` | `incremental` — resume from the last `SUCCESS` row in `kafka_offsets`. `backfill` — reprocess a bounded Kafka offset range you specify. |
| `starting_offset` | `null` | **Required for backfill.** Inclusive Kafka offset for partition `0` (Spark `startingOffsets`). |
| `ending_offset` | `null` | **Required for backfill.** Exclusive Kafka offset for partition `0` (Spark `endingOffsets`). |

The DAG task reads `dag_run.conf` first, then falls back to DAG params, and passes values to Spark as environment variables: `RUN_ID`, `RUN_MODE`, and (in backfill mode) `STARTING_OFFSET` / `ENDING_OFFSET`.

**Incremental (default):** Spark calls `get_starting_offsets_json()` and reads through the current log end. On success, a new `kafka_offsets` row is written with `status = SUCCESS`.

**Backfill:** Spark reads only the offset range `[starting_offset, ending_offset)` and still applies the same idempotent ledger, balance, and audit logic. **`kafka_offsets` is not updated** on backfill runs, so reprocessing history does not move the incremental read pointer.

If `run_mode` is `backfill` but either offset is missing, the DAG task fails fast with a clear validation error before `spark-submit` runs.

### Two-stack local setup

```text
  docker/docker-compose.yml          airflow/ (astro dev start)
  ┌─────────────────────────┐        ┌─────────────────────────┐
  │ Kafka, Postgres :5433   │        │ Scheduler, API :8080    │
  │ Spark master :8081 UI   │◀─exec──│ docker.sock mounted     │
  └───────────┬─────────────┘        └───────────┬─────────────┘
              │         fund-pipeline network     │
              └───────────────────────────────────┘
```

1. Start the data plane: `docker compose -f docker/docker-compose.yml up -d`
2. Copy `airflow/docker-compose.override.yml` behavior is applied automatically by Astro when present — it attaches Airflow containers to **`fund-pipeline`** and mounts the Docker socket.
3. From `airflow/`: `astro dev start` (UI at http://localhost:8080)
4. Trigger **`fund_balance_dag`** manually or wait for the schedule.

**Port note:** fund Postgres is exposed on host **`5433`** (not 5432) to avoid conflicting with Astro’s metadata Postgres.

### Offset checkpoints and DAG success

`kafka_offsets` rows are written at the **end** of a successful Spark run inside `spark/job.py`. If the DAG task fails before `spark-submit` completes, no new `SUCCESS` checkpoint is recorded and the next run resumes from the last successful offset. Combined with idempotent ledger writes, this makes Airflow retries safe.

## Event Schema

The event schema represents a financial transaction event in an investment fund context. Each event captures a cash flow between investors and the fund, where the fund holds investments in specific deals.

### Design Principles

- **Fund-Deal Relationship**: Transactions are tied to a specific fund and one of its associated deals, reflecting real-world investment structures.
- **Cash Flow Direction**:
  - `CREDIT`: Capital calls (money flowing into the fund from investors)
  - `DEBIT`: Distributions (money flowing out to investors)
  - `REVERSAL`: Corrections or reversals of previous transactions
- **Status Lifecycle**: Indicates processing state:
  - `PENDING`: Transaction initiated but not yet posted
  - `COMPLETED`: Successfully posted
  - `FAILED`: Processing failed
- **Ordering**: Events are ordered by `transaction_timestamp` (ISO 8601 format) to maintain chronological sequence in the event log.
- **Uniqueness**: Each transaction has a unique `transaction_id` (UUID) for idempotency and deduplication.

### JSON Structure

```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "transaction_timestamp": "2026-05-02T14:30:00.000Z",
  "event_timestamp": "2026-05-02T14:30:05.123Z",
  "transaction_amount": 125000.5,
  "currency": "USD",
  "transaction_type": "CREDIT",
  "status": "COMPLETED",
  "fund": {
    "fund_id": 1,
    "fund_name": "Fund A"
  },
  "deal": {
    "deal_id": 2,
    "deal_name": "Deal B"
  },
  "metadata": {
    "source": "simulated-emitter",
    "strategy": "growth"
  }
}
```

### Field Definitions

| Field                   | Type              | Description                                                                              | Required |
| ----------------------- | ----------------- | ---------------------------------------------------------------------------------------- | -------- |
| `transaction_id`        | string (UUID)     | Unique identifier for the transaction                                                    | Yes      |
| `transaction_timestamp` | string (ISO 8601) | When the transaction occurred (used for event ordering)                                  | Yes      |
| `event_timestamp`       | string (ISO 8601) | When the event was produced / ingested (emitter wall clock)                              | Yes      |
| `transaction_amount`    | number (float)    | Transaction amount in specified currency                                                 | Yes      |
| `currency`              | string            | Currency code (e.g., USD, EUR)                                                           | Yes      |
| `transaction_type`      | string            | Direction of cash flow: `DEBIT`, `CREDIT`, `REVERSAL`                                    | Yes      |
| `status`                | string            | Processing status: `PENDING`, `COMPLETED`, `FAILED`                                      | Yes      |
| `fund`                  | object            | Fund context: `fund_id` (integer in JSON; read as string in Spark), `fund_name` (string) | Yes      |
| `deal`                  | object            | Deal context: `deal_id` (integer in JSON; read as string in Spark), `deal_name` (string) | Yes      |
| `metadata`              | object            | Optional enrichment data (e.g., source, strategy)                                        | No       |

### Schema Evolution

New fields are added as optional to maintain backward compatibility. Consumers apply defaults for missing fields (e.g., `metadata` defaults to an empty object).

**Spark Processing Strategy**: When reading events in PySpark, missing fields are handled with default values using functions like `coalesce()` or `.get()` with fallbacks. For example, a new `risk_factor` field defaults to 0.0 if absent.

**State Table Design**: `transaction_balance` stores only `fund_id`, `deal_id`, `balance`, and `last_modified` so the ledger state stays narrow and authoritative; enrichment (names, metadata) stays on raw events or drill-down logs.

**Identifier types**: JSON events use integer `fund_id` / `deal_id`; Spark declares them as strings in the parse schema and persists them as `VARCHAR` in PostgreSQL for consistent keys across sinks.

## Project Structure

```
streaming-fund-balance-engine/
├── emitter/
│   ├── emitter.py              # Event producer for transaction events
│   └── insert_duplicate.py     # Publishes the same transaction twice (duplicate testing)
├── spark/
│   ├── job.py                  # Spark batch processing job
│   ├── upsert_functions.py     # JDBC upsert helpers (ledger, balances, audit logs)
│   └── query_wrappers.py       # Postgres read/execute utilities
├── airflow/
│   ├── dags/
│   │   └── fund_balance_dag.py # Airflow DAG (docker exec → spark-submit)
│   ├── include/fund_balance_dag/
│   │   └── callbacks.py        # Task failure logging
│   ├── docker-compose.override.yml  # Join fund-pipeline network + docker.sock
│   └── requirements.txt        # docker SDK for DAG task
├── docker/
│   └── docker-compose.yml      # Kafka, Postgres, Spark (data plane)
├── db/
│   └── init.sql                # Database initialization script
├── jars/                       # Kafka + PostgreSQL JARs (not committed; see Dependencies)
└── README.md
```

### Component Details

- **emitter/** — Generates transaction events and publishes them to Kafka; `insert_duplicate.py` supports dedup testing
- **spark/** — Batch Kafka consumer, transformations, and JDBC upserts to Postgres
- **airflow/** — Astro project with `fund_balance_dag` and failure callbacks; orchestrates Spark via Docker exec
- **docker/** — Data-plane Compose stack (Kafka, Postgres on host port 5433, Spark standalone cluster)
- **db/** — Postgres DDL for ledger, balances, metrics, audit logs, and offset checkpoints

## What This Project Demonstrates

- Design of a scalable, loosely coupled pipeline for financial events
- Use of Kafka for durable event streaming and replayability
- Orchestration patterns using Airflow DAGs and task dependencies
- Spark batch processing and stateful aggregation logic
- How event-driven systems can support transactional state updates
- Ability to build a real-world data engineering solution end to end

## Dependencies

Download these JARs into `./jars` before running Docker Compose:

- [spark-sql-kafka-0-10_2.12-3.5.1.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar)
- [spark-token-provider-kafka-0-10_2.12-3.5.1.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar)
- [kafka-clients-3.4.0.jar](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar)
- [commons-pool2-2.11.1.jar](https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar)
- [postgresql-42.7.3.jar](https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar) (JDBC driver for writing to PostgreSQL from Spark)

### Connecting Spark to PostgreSQL

`docker-compose.yml` starts PostgreSQL with database **`fund_balance`**, user **`funduser`**, password **`fundpass`**, and applies **`db/init.sql`** on first data volume creation.

- **Spark runs inside the Compose network** (e.g. `spark-submit` from `spark-master-fund-balance`): use host **`postgres`**, port **`5432`**: `jdbc:postgresql://postgres:5432/fund_balance`
- **Spark runs on your host machine** (IDE / local `spark-submit`): use **`localhost:5433`** (host-mapped port; same credentials).

Put the PostgreSQL JAR in `./jars` (on Spark’s extra classpath via Compose). PySpark uses `format("jdbc")` with upsert logic implemented via staging tables and SQL `ON CONFLICT` in `spark/upsert_functions.py`.

## Usage

### 1. Start the data plane

From the repo root:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Download JARs into `./jars` first (see [Dependencies](#dependencies)). Spark UI: http://localhost:8081 — Postgres on host port **5433**.

### 2. Publish events

```bash
python emitter/emitter.py
```

### 3. Run the pipeline

**Option A — Airflow (recommended for scheduled / correlated `run_id`):**

```bash
cd airflow && astro dev start
```

Open http://localhost:8080 and trigger **`fund_balance_dag`**.

- **Incremental (default):** click **Trigger DAG** with no changes — `run_mode` stays `incremental` and Spark resumes from `kafka_offsets`.
- **Backfill:** click **Trigger DAG**, set `run_mode` to `backfill`, and enter integer `starting_offset` and `ending_offset` (e.g. `0` and `20` to process offsets 0–19 on partition `0`). Confirm in task logs: `run mode: backfill`.

Alternatively, trigger with JSON config (Linux / macOS, or `docker exec` into the scheduler on Windows — see Option B).

**Option B — Manual Airflow DAG run from the terminal**

Incremental test (no custom config):

```bash
astro dev run dags test fund_balance_dag 2025-01-01
```

Backfill via **scheduler container** (recommended on Windows — `astro dev run ... --conf` often strips JSON quotes on PowerShell):

```bash
# Replace <scheduler> with your Astro scheduler container name (docker ps | grep scheduler)
docker exec -it <scheduler> bash -lc \
  'airflow dags trigger fund_balance_dag --conf '"'"'{"run_mode":"backfill","starting_offset":0,"ending_offset":20}'"'"''
```

On **PowerShell**, use a here-string:

```powershell
docker exec -it <scheduler> bash -lc @'
airflow dags trigger fund_balance_dag --conf '{"run_mode":"backfill","starting_offset":0,"ending_offset":20}'
'@
```

**Option C — Manual Spark submit (development):**

**CMD / bash**

```bash
docker exec -it spark-master-fund-balance bash -lc \
  'JARS=$(find /opt/spark/jars-extra -maxdepth 1 -name "*.jar" | paste -sd, -) && \
   RUN_ID=manual_incr RUN_MODE=incremental \
   /opt/spark/bin/spark-submit --master spark://spark-master-fund-balance:7077 \
   --jars "${JARS}" /opt/spark-apps/job.py'
```

**PowerShell — incremental**

```powershell
docker exec -it spark-master-fund-balance bash -lc @'
JARS=$(find /opt/spark/jars-extra -maxdepth 1 -name "*.jar" | paste -sd, -) && \
RUN_ID=manual_incr RUN_MODE=incremental \
/opt/spark/bin/spark-submit --master spark://spark-master-fund-balance:7077 \
  --jars "${JARS}" /opt/spark-apps/job.py
'@
```

**PowerShell — backfill** (env vars must be on the `spark-submit` line, not before `JARS=...`)

```powershell
docker exec -it spark-master-fund-balance bash -lc @'
JARS=$(find /opt/spark/jars-extra -maxdepth 1 -name "*.jar" | paste -sd, -) && \
RUN_ID=manual_backfill_01 RUN_MODE=backfill STARTING_OFFSET=0 ENDING_OFFSET=20 \
/opt/spark/bin/spark-submit --master spark://spark-master-fund-balance:7077 \
  --jars "${JARS}" /opt/spark-apps/job.py
'@
```

Without `RUN_ID` set, `spark/job.py` generates a UUID for the run. Without `RUN_MODE`, Spark defaults to `incremental`.

### 4. Verify results

```bash
docker exec -it postgres-fund-balance psql -U funduser -d fund_balance
```

```sql
SELECT * FROM transaction_balance LIMIT 10;
SELECT run_id, record_count, duplicate_count, late_arrival_count FROM run_metrics ORDER BY run_timestamp DESC LIMIT 5;
SELECT run_id, topic, partition, start_offset, end_offset, status FROM kafka_offsets ORDER BY run_timestamp DESC LIMIT 5;
```

### 5. Demo replay, backfill, and idempotency

1. Run the pipeline once after emitting events — note `kafka_offsets.end_offset` and balances.
2. Trigger the **same** DAG run retry or re-run Spark manually — ledger row count for existing `transaction_id`s should not grow; balances should be unchanged.
3. Emit **new** events and run again — `end_offset` advances; balances update for affected fund/deal pairs.
4. **Backfill demo:** trigger `fund_balance_dag` with `run_mode=backfill` and a prior offset range (e.g. `0` / `20`). Ledger and balances should remain consistent on repeat; `kafka_offsets` should not change for that backfill run (check task logs for `skipping Kafka offset update`).

### Testing duplicate detection

With Kafka running, publish two messages that share the same `transaction_id` (different Kafka offsets):

```bash
python emitter/insert_duplicate.py
```

Then run the Spark job and inspect `duplicate_records_log` (and `run_metrics.duplicate_count`) to confirm deduplication and per-run duplicate summary logging.

## Why This Project Matters

This repo is built to demonstrate strong data engineering skills in both architecture and implementation. It shows how to:

- Build reliable event-driven systems
- Connect streaming and batch processing patterns
- Manage orchestration and workflow dependencies
- Produce a strong technical story for data engineering interviews or portfolio presentations

## Notes

- **Two Docker contexts:** data plane (`docker/`) and Airflow (`airflow/` via Astro) are started separately and joined on the `fund-pipeline` network.
- **DAG `execution_timeout`:** the Spark task currently uses a short timeout suitable for small demos; increase it in `fund_balance_dag.py` if batch runs exceed the limit.
- When presenting the project, walk through **emit → incremental run → backfill → replay → duplicate inject** to show idempotency and audit trails end to end.
