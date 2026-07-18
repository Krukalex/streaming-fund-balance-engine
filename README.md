# Streaming Fund Balance Engine

## Overview

The Streaming Fund Balance Engine is a local event-driven data pipeline built with Kafka, Spark, Airflow, PostgreSQL, and Docker. A Python producer emits fund transactions to Kafka; Airflow submits bounded Spark batch jobs that transform those events, write an idempotent transaction ledger, and recompute current balances by fund and deal.

The design emphasizes replay safety, deterministic deduplication, offset-based incremental reads, bounded backfills, audit logs, and testable DataFrame transformations.

## 30-second read

- **Flow:** Python producer → Kafka → Airflow-triggered Spark batch → PostgreSQL ledger, fund/deal balances, audit logs, metrics, and checkpoints.
- **Processing model:** scheduled incremental Kafka reads, not Structured Streaming. Successful runs resume from PostgreSQL offsets; operators can replay a bounded offset range without advancing the incremental pointer.
- **Correctness:** Spark deduplicates each batch deterministically, signs credits/debits, writes a first-write-wins ledger, and recomputes balances from the full ledger so retries do not double-count money.
- **Auditability:** every event retains Kafka coordinates; duplicate groups, late arrivals, run metrics, and checkpoints are queryable in PostgreSQL and correlated with Airflow `run_id`.
- **Testing:** pytest covers Airflow configuration/failure paths and production Spark DataFrame transforms using a local shared `SparkSession`.
- **Scope:** this is a single-partition local reference implementation. The README explicitly documents production upgrades and current limitations.

**Stack:** Kafka · Spark · Airflow/Astro · PostgreSQL · Docker · Python · pytest

**Start here:** [run the pipeline](#usage) · [run the tests](#automated-testing) · [review trade-offs](#key-engineering-decisions)

## Contents

- [Architecture](#architecture)
- [Key engineering decisions](#key-engineering-decisions)
- [Spark processing logic](#spark-processing-logic)
- [Offset-based batch processing](#offset-based-batch-processing)
- [PostgreSQL schema](#postgresql-schema)
- [Airflow orchestration](#airflow-orchestration)
- [Event schema](#event-schema)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Automated testing](#automated-testing)

## Architecture

```text
Python producer
      │
      ▼
Kafka topic: transactions
      │  topic / partition / offset / broker timestamp
      ▼
Spark bounded batch ───────────────┐
      │                            │
      ├─ transaction ledger        ├─ duplicate and late-arrival logs
      ├─ fund/deal balances        ├─ run metrics
      └─ incremental checkpoints ──┘
             ▲
             │ spark-submit
          Airflow
```

- **Producer:** emits simulated transactions for valid fund/deal combinations.
- **Kafka:** provides a replayable in-container log for the local demo. Compose does not currently persist Kafka data across `docker compose down`, and ordering is guaranteed only within a partition.
- **Airflow:** runs `fund_balance_dag` daily or on demand, supplies a stable `run_id`, and supports incremental and bounded backfill modes.
- **Spark:** reads Kafka with the batch API, applies the pure transformations in `spark/transforms.py`, and invokes JDBC sink helpers.
- **PostgreSQL:** stores the ledger, current balances, quality metrics, audit records, and successful incremental checkpoints.

## Key Engineering Decisions

### Batch Kafka reads vs Structured Streaming

- The Spark job uses the **batch** Kafka source (`spark.read.format("kafka")`) with **PostgreSQL offset checkpoints**, not Structured Streaming.
- **Why for this project:** bounded, on-demand runs are easier to demo, replay, and explain in interviews; the same idempotency patterns (ledger, audit logs, offsets) apply whether the job is triggered manually or on a schedule.
- **Production nuance:** sub-minute latency would push you toward Structured Streaming (Databricks, Flink, or Spark SS with checkpointing). Many fund-accounting and balance-derivation workloads still run as scheduled bounded batches because correctness and auditability matter more than low latency. The offset-table pattern here maps to that model.
- **What would change at scale:** replace the JDBC offset table with Spark SS checkpoints or Kafka consumer groups; add watermarks for event-time lateness in streaming mode; keep the same ledger idempotency rules on the sink.

### Airflow → Spark via `docker exec` (local only)

- The DAG task uses the Python **Docker SDK** to `exec` `spark-submit` inside the long-running `spark-master-fund-balance` container, rather than `DockerOperator` spinning up a one-off container per run.
- **Why:** faster local iteration, code and JARs are already bind-mounted, and avoids fragile host-path mounts (especially on Windows). `DockerOperator` is the closer analog to production job submission but adds startup latency and compose wiring complexity for a portfolio demo.
- **How it works:** Astro Airflow (`astro dev start`) runs the control plane; `docker/docker-compose.yml` runs the data plane (Kafka, Postgres, Spark). `airflow/docker-compose.override.yml` joins Airflow services to the external `fund-pipeline` network and mounts `/var/run/docker.sock` so the task can reach the Spark container by name.

### Idempotency and replay-first design

- **`transaction_ledger`:** `ON CONFLICT (transaction_id) DO NOTHING` — the first persisted version of a transaction wins across batches and replays.
- **`transaction_balance`:** full `SUM` over the ledger each run — identical ledger → identical balances.
- **Duplicate audit:** `DUP|{run_id}|{transaction_id}` is stable for retries of the same run; a new replay `run_id` produces a separate run-scoped observation.
- **Late-arrival audit:** `LATE|{topic}|{partition}|{offset}` identifies one physical Kafka message across retries, backfills, and replays.
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

For **low-latency** fund P&L or risk, Structured Streaming with durable checkpoints and idempotent `foreachBatch` sinks would be a natural upgrade. For scheduled batch SLAs, the current bounded-read and replay-safe sink patterns remain applicable, but production hardening would also require stronger data validation, secrets management, monitoring, and failure recovery.

### Known limitations (local demo)

- **Empty batches:** if a run reads zero Kafka messages, no `kafka_offsets` row is written and the read pointer does not advance (acceptable for demos; production would still record a heartbeat or empty-run checkpoint).
- **Money as `DOUBLE PRECISION`:** fine for a portfolio; production would use `NUMERIC`/`DECIMAL`.
- **Single-partition design:** the Compose stack relies on Kafka auto-creating `transactions`, typically with one partition. Backfill JSON and first-run fallback assume partition `0`. Checkpoint upserts also use `run_id` as the sole primary key, so multi-partition checkpoint writes are not supported without a composite key such as `(run_id, topic, partition)`.
- **Batch-scoped deduplication:** duplicates are ranked only within the current Kafka slice, using `transaction_timestamp`, then `kafka_timestamp`, then `offset`. Across separate runs, `transaction_ledger` is first-write-wins by `transaction_id`; later corrections with the same id are not applied.
- **Status handling:** `status` is retained for audit output but is not currently used to exclude `FAILED` events from the ledger.
- **Checkpoint source:** checkpoint bounds are currently derived from the post-deduplication ledger DataFrame rather than directly from the raw Kafka batch. A production version should checkpoint the raw consumed partition ranges so duplicate-only edge cases cannot leave the pointer behind the actual read boundary.
- **Demo timeout:** the Airflow Spark task has a 10-minute execution timeout; increase it for larger batches or slower machines.
- **Concurrent runs:** JDBC helpers use shared staging-table names, so overlapping Spark runs are not isolated. Production sinks should use run-scoped staging tables or transactional merge patterns and enforce appropriate Airflow concurrency limits.

## Spark Processing Logic

The Spark job processes Kafka transaction events into a durable **transaction ledger**, then derives auditable balances per fund/deal pair.

1. **Read data from Kafka (offset-based batch)**

- Before reading, load the last successful checkpoint from `kafka_offsets` and build Spark’s `startingOffsets` JSON (see [Offset-based batch processing](#offset-based-batch-processing)).
- If the checkpoint table is empty, start at offset **0** for partition **0** (first run).
- Consume only new messages from that position through the current log end, or through an explicit exclusive `endingOffsets` bound in backfill mode.
- Retain metadata (`topic`, `partition`, `offset`, `kafka_timestamp`) for traceability and deterministic tie-breaking.

2. **Flatten into a DataFrame**

- Parse JSON payloads into a typed schema.
- Flatten nested `fund` and `deal` objects into top-level columns.
- Parse `event_timestamp` and `transaction_timestamp` as timestamps; missing timestamp values currently fall back to `1900-01-01T00:00:00`.
- **Type note:** emitters send `fund_id` and `deal_id` as JSON integers; the Spark schema reads them as **strings** for consistent JDBC/Postgres handling (values coerce cleanly to `VARCHAR` keys in the database).

3. **Detect late arriving events**

- Define **late** as **ingest lag**: how long after the business-time `transaction_timestamp` the record appeared in Kafka, using epoch seconds: `unix_timestamp(kafka_timestamp) - unix_timestamp(transaction_timestamp)`.
- Flag rows where ingest lag exceeds a hard-coded **900-second** threshold; the same message always yields the same lag on replay, unlike comparisons to `current_timestamp()`.
- Rows that qualify are written to `late_arriving_event_log`; each pipeline run also records how many late rows were observed in that run’s batch in `run_metrics` (see [Late arriving events (design)](#late-arriving-events-design)).

4. **Identify duplicate events**

- Treat any repeated `transaction_id` as a duplicate candidate.
- Resolve duplicates deterministically using descending `transaction_timestamp`, then descending `kafka_timestamp`, then descending `offset`.

5. **Determine winning duplicate records**

- Rank records per `transaction_id` and keep the highest-ranked row as the winner.
- For ids that are duplicated within the batch, persist winner metadata in `duplicate_records_log` to document which row was retained.

6. **Build duplicate summary diagnostics**

- Compute per-ID duplicate stats (`min transaction_timestamp`, `max transaction_timestamp`, `duplicate_count`).
- Join duplicate stats with winner metadata to create a complete duplicate summary.
- Persist this summary to a metrics/audit table for debugging and replay analysis.

7. **Apply business signing logic**

- Convert transaction types into signed cash flow values:
  - `DEBIT` → negative amount
  - `CREDIT` → positive amount
  - any other value, including `REVERSAL` → zero in the current implementation

8. **Upsert into the transaction ledger**

- After deduplication, write one row per winning transaction to `transaction_ledger` with signed amount and Kafka lineage.
- Use `ON CONFLICT (transaction_id) DO NOTHING`: each `transaction_id` is stored at most once. Replaying the **same** input does not add a second ledger row.

9. **Derive canonical balances from the ledger**

- Recompute `transaction_balance` as `SUM(transaction_amount)` grouped by `(fund_id, deal_id)` over the **entire** ledger (not a batch-only incremental add in SQL).
- Upsert into `transaction_balance` with `ON CONFLICT DO UPDATE`, setting `balance` to the recomputed aggregate. If the ledger is unchanged, the values remain the same even though `last_modified` is refreshed. If new transactions were added, affected fund/deal balances reflect the new totals.

10. **Aggregate and persist run metrics**

- Build one row per `run_id` with `record_count` (Kafka rows after the parse attempt, including rows with null fields when JSON is malformed), `duplicate_count` (duplicate groups observed in the batch), and `late_arrival_count` (rows matching the ingest-lag rule in the batch).
- Upsert into `run_metrics` on `run_id` with `ON CONFLICT DO UPDATE`, so a **retry/replay of the same run** replaces metrics with the latest computed values for that run.

## Offset-based batch processing

Spark uses the **batch** Kafka source (`spark.read.format("kafka")`), not Structured Streaming. Each job run processes a **slice** of the topic instead of re-reading the full history every time.

### Checkpoint table: `kafka_offsets`

PostgreSQL table `kafka_offsets` records **per-run checkpoint metadata** and acts as the read pointer for the next incremental batch:

| Column          | Description                                                                                                                              |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `run_id`        | Correlates with the pipeline run (from Airflow `dag_run.run_id` when orchestrated, or a UUID fallback in `spark/job.py` for manual runs) |
| `topic`         | Kafka topic name (e.g. `transactions`)                                                                                                   |
| `partition`     | Topic partition id                                                                                                                       |
| `start_offset`  | Inclusive lower bound recorded for this run from the post-dedup ledger offsets                                                           |
| `end_offset`    | Exclusive upper bound recorded for this run (`max(offset) + 1` from that ledger slice); intended as the next incremental start           |
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

After the sinks succeed, a **non-empty incremental** run inserts or updates a `kafka_offsets` row for the current `run_id` with `status = 'SUCCESS'`. Empty batches and backfill runs deliberately skip this write. If the Spark job fails first, no new successful checkpoint is recorded.

## Late arriving events (design)

This section records the product and replay semantics for **late** detection and logging.

**Batch scope:** late detection and `run_metrics` counts apply to messages in the **current offset batch** (from `startingOffsets` through the log end for that run), not the entire topic history on every execution.

### Definition of “late”

A record is treated as **late for observability** when **ingest lag** exceeds the hard-coded 900-second threshold:

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

PostgreSQL holds the canonical **balance state**, **per-run rollup metrics**, and **drill-down logs**. Run metrics, checkpoints, and audit observations carry a **`run_id`** for correlation (see [Airflow orchestration](#airflow-orchestration)); the ledger and balance tables do not.

### Tables and grain

| Table                     | Grain / identity                                                         | Purpose                                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `transaction_ledger`      | `transaction_id` (+ unique Kafka coordinates)                            | Append-style ledger of signed movements; idempotent per `transaction_id` on replay.                                           |
| `transaction_balance`     | `(fund_id, deal_id)`                                                     | Current net balance per fund/deal pair, derived as `SUM` over the ledger.                                                     |
| `run_metrics`             | `run_id`                                                                 | One row per pipeline run: volumes and quality counts; updated on retry of the same `run_id`.                                  |
| `duplicate_records_log`   | **Surrogate PK** + unique `(run_id, transaction_id)`                     | Duplicate groups for that run: counts, min/max business time, and full Kafka pointer for the retained winner.                 |
| `late_arriving_event_log` | **Surrogate PK** + unique `(kafka_topic, kafka_partition, kafka_offset)` | One durable row per **physical Kafka message** flagged late; survives retries, backfills, and replays without double inserts. |
| `kafka_offsets`           | `run_id`                                                                 | Per-run incremental checkpoint: topic, partition, offset range, and status for offset-based batch resume.                     |

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
- **Idempotency:** identical ledger contents produce identical sums; `ON CONFLICT DO UPDATE` writes the recomputed balance and refreshes `last_modified`.

**`run_metrics`**

- `record_count` — Kafka rows in the batch after the parse attempt. Malformed JSON can still produce a counted row with null fields.
- `duplicate_count` — number of distinct `transaction_id` values that had duplicates in that run (batch observation); on first load for a `run_id`, should match `SELECT COUNT(*) FROM duplicate_records_log WHERE run_id = ?`.
- `late_arrival_count` — number of messages in **this run’s batch** that satisfied the ingest-lag late rule in Spark (**observed in batch**). It does **not** necessarily equal new rows inserted into `late_arriving_event_log` on replay (`ON CONFLICT DO NOTHING` on `surrogate_pk`).
- `run_timestamp` — when the run was recorded (updated on `ON CONFLICT DO UPDATE` when the same `run_id` is retried).

**`kafka_offsets`**

- One row per incremental pipeline run (`run_id` primary key) recording the checkpointed Kafka slice. The current primary key and backfill configuration are designed for one partition.
- `start_offset` / `end_offset` — inclusive start and **exclusive** end derived from the post-deduplication ledger DataFrame for that run, not always the raw consumed Kafka range (see [Offset-based batch processing](#offset-based-batch-processing)).
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
- **Failure handling:** `on_failure_callback` in `airflow/include/fund_balance_dag/callbacks.py` logs task context and classifies timeout, manual-failure, and application errors.

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

**Incremental (default):** Spark calls `get_starting_offsets_json()` and reads through the current log end. On a non-empty successful run, a new `kafka_offsets` row is written with `status = SUCCESS`.

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
2. Astro automatically applies `airflow/docker-compose.override.yml`, attaching Airflow services to **`fund-pipeline`** and mounting the Docker socket.
3. From `airflow/`: `astro dev start` (UI at http://localhost:8080)
4. Trigger **`fund_balance_dag`** manually or wait for the schedule.

**Port note:** fund Postgres is exposed on host **`5433`** (not 5432) to avoid conflicting with Astro’s metadata Postgres.

### Offset checkpoints and DAG success

`kafka_offsets` rows are written at the **end** of a successful non-empty incremental Spark run inside `spark/job.py`. If the DAG task fails before `spark-submit` completes, no new `SUCCESS` checkpoint is recorded and the next run resumes from the last successful offset. Combined with idempotent ledger writes, this makes Airflow retries safe.

## Event Schema

The event schema represents a financial transaction event in an investment fund context. Each event captures a fund/deal cash-flow movement; the current schema does not model individual investors.

### Current producer behavior

- **Fund-Deal Relationship**: Transactions are tied to a specific fund and one of its associated deals, reflecting real-world investment structures.
- **Cash-flow direction:** the producer emits `CREDIT` and `DEBIT`. Spark maps credits positive, debits negative, and any other value to zero.
- **Status:** the producer currently emits `COMPLETED` or `FAILED`; `PENDING` is present only as a commented example. Status does not yet control ledger eligibility.
- **Event time:** `transaction_timestamp` is randomized up to 30 minutes before production time, which creates late-arrival test data. Kafka ordering remains partition/offset ordering, not business-time ordering.
- **Identity:** normal producer events use UUID transaction ids. `emitter/insert_duplicate.py` intentionally publishes the same event twice to exercise deduplication.

### JSON Structure

```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "transaction_timestamp": "2026-05-02T14:30:00.000000",
  "event_timestamp": "2026-05-02T14:30:05.123000",
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
    "source": "simulator",
    "strategy": "growth"
  }
}
```

### Field Definitions

| Field                   | Type              | Description                                                                              | Required |
| ----------------------- | ----------------- | ---------------------------------------------------------------------------------------- | -------- |
| `transaction_id`        | string (UUID)     | Unique identifier for the transaction                                                    | Yes      |
| `transaction_timestamp` | string (ISO 8601) | Business time used as the primary deduplication winner field                             | Yes      |
| `event_timestamp`       | string (ISO 8601) | Producer wall-clock creation time; late detection uses Kafka broker timestamp instead    | Yes      |
| `transaction_amount`    | number (float)    | Transaction amount in the event currency                                                 | Yes      |
| `currency`              | string            | Currency code present on the event; balances currently ignore currency and assume USD    | Yes      |
| `transaction_type`      | string            | Direction of cash flow; the current producer emits `DEBIT` or `CREDIT`                   | Yes      |
| `status`                | string            | Producer status; currently `COMPLETED` or `FAILED`                                       | Yes      |
| `fund`                  | object            | Fund context: `fund_id` (integer in JSON; read as string in Spark), `fund_name` (string) | Yes      |
| `deal`                  | object            | Deal context: `deal_id` (integer in JSON; read as string in Spark), `deal_name` (string) | Yes      |
| `metadata`              | object            | Optional enrichment data (e.g., source, strategy)                                        | No       |

### Schema and state notes

- `metadata` is optional in the Spark schema. The job does not currently default missing metadata to an empty object or persist it to the ledger.
- Missing event or transaction timestamps receive a sentinel `1900-01-01` value during flattening. Other malformed or missing required fields do not yet have a dead-letter path.
- `currency` is retained on late-event audit rows but is dropped before ledger and balance writes, so multi-currency amounts would currently be summed together.
- `transaction_balance` stays narrow: `fund_id`, `deal_id`, `balance`, and `last_modified`. Names and event attributes remain outside canonical balance state.
- JSON events use integer `fund_id` / `deal_id`; Spark parses them as strings and PostgreSQL stores them as `VARCHAR`.

## Project Structure

```
streaming-fund-balance-engine/
├── emitter/
│   ├── emitter.py              # Event producer for transaction events
│   ├── consumer.py             # Simple console consumer for inspection
│   └── insert_duplicate.py     # Publishes the same transaction twice (duplicate testing)
├── spark/
│   ├── job.py                  # Spark batch processing job
│   ├── transforms.py           # Pure DataFrame transformations
│   ├── upsert_functions.py     # JDBC upsert helpers (ledger, balances, audit logs)
│   ├── query_wrappers.py       # Postgres read/execute utilities
│   └── tests/
│       ├── conftest.py         # Shared local SparkSession fixture
│       └── test_transforms_example.py
├── airflow/
│   ├── dags/
│   │   └── fund_balance_dag.py # Airflow DAG (docker exec → spark-submit)
│   ├── include/fund_balance_dag/
│   │   └── callbacks.py        # Task failure logging
│   ├── tests/dags/
│   │   └── test_fund_balance_dag.py
│   ├── docker-compose.override.yml  # Join fund-pipeline network + docker.sock
│   └── requirements.txt        # docker SDK for DAG task
├── docker/
│   └── docker-compose.yml      # Kafka, Postgres, Spark (data plane)
├── db/
│   └── init.sql                # Database initialization script
├── jars/                       # Kafka + PostgreSQL JARs (not committed; see Prerequisites)
├── requirements.txt            # Local producer and PySpark dependencies
└── README.md
```

## Prerequisites

- Docker Desktop with Docker Compose
- Python and `pip` for the producer and local Spark tests
- Java 17 for the locally pinned PySpark 4.1 test runtime
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli) for the Airflow environment

Install the local Python dependencies and pytest from the repository root:

```bash
python -m pip install -r requirements.txt pytest
```

The Compose data plane uses Spark **3.5.1**. The root requirements currently pin PySpark **4.1.1** for local transform tests; the tested APIs are compatible, but production projects should align test and runtime versions exactly.

### Spark connector JARs

Download these JARs into `./jars` before starting the data plane:

- [spark-sql-kafka-0-10_2.12-3.5.1.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar)
- [spark-token-provider-kafka-0-10_2.12-3.5.1.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar)
- [kafka-clients-3.4.0.jar](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar)
- [commons-pool2-2.11.1.jar](https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar)
- [postgresql-42.7.3.jar](https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar) (JDBC driver for writing to PostgreSQL from Spark)

### PostgreSQL connection details

`docker/docker-compose.yml` starts PostgreSQL with database **`fund_balance`**, user **`funduser`**, password **`fundpass`**, and applies **`db/init.sql`** on first data volume creation.

- The Spark job currently hard-codes `jdbc:postgresql://postgres:5432/fund_balance`, so `spark-submit` is intended to run inside the Compose network (for example from `spark-master-fund-balance`).
- Postgres is also published on host port **`5433`** for local inspection with `psql` or a SQL client. Host-local Spark would need a JDBC URL override that is not implemented yet.

Put the PostgreSQL JAR in `./jars` (on Spark’s extra classpath via Compose). PySpark uses `format("jdbc")` with upsert logic implemented via staging tables and SQL `ON CONFLICT` in `spark/upsert_functions.py`.

## Usage

### 1. Start the data plane

From the repo root:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Download JARs into `./jars` first (see [Spark connector JARs](#spark-connector-jars)). Spark UI: http://localhost:8081 — Postgres on host port **5433**.

### 2. Publish events

```bash
python emitter/emitter.py
```

The producer emits one event every 10 seconds until stopped.

### 3. Run the pipeline

**Option A — Airflow (recommended for scheduled / correlated `run_id`):**

```bash
cd airflow
astro dev start
```

Open http://localhost:8080 and trigger **`fund_balance_dag`**.

- **Incremental (default):** click **Trigger DAG** with no changes — `run_mode` stays `incremental` and Spark resumes from `kafka_offsets`.
- **Backfill:** click **Trigger DAG**, set `run_mode` to `backfill`, and enter integer `starting_offset` and `ending_offset` (e.g. `0` and `20` to process offsets 0–19 on partition `0`). Confirm in task logs: `run mode: backfill`.

Alternatively, trigger with JSON config (Linux / macOS, or `docker exec` into the scheduler on Windows — see Option B).

**Option B — Manual Airflow DAG run from the terminal**

Manual incremental DAG execution (no custom config):

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

## Automated Testing

The project has two classes of automated tests:

1. **Airflow tests** validate DAG configuration, runtime parameter handling, command construction, and failure classification.
2. **Spark tests** run the production DataFrame transformations against small in-memory datasets using a local Spark session.

These test suites are intentionally separate because they run in different environments. Airflow tests run inside the Astro test environment, while Spark tests run with local PySpark from the repository root.

### Airflow tests

Navigate to the `airflow` directory and run:

```bash
cd airflow
astro dev pytest tests/dags/test_fund_balance_dag.py -v
```

The Airflow test suite verifies that:

1. A DAG run with no custom configuration defaults to `incremental` mode.
2. A backfill run passes the configured starting and ending offsets to Spark.
3. Backfill mode raises an error when required offsets are missing.
4. Values supplied through `dag_run.conf` take precedence over DAG `params`.
5. The DAG contract remains intact, including params, tags, retries, execution timeout, and task failure callback wiring.
6. The failure callback maps timeout, manual-failure, and application exceptions to stable reason labels.

The Docker SDK call is mocked in these tests, so they validate Airflow orchestration logic without launching a real Spark job.

### Spark tests

From the repository root, run:

```bash
python -m pytest spark/tests -v
```

The Spark test suite creates one local `SparkSession` for the pytest session and runs the production functions from `spark/transforms.py` against small in-memory DataFrames. It verifies that:

1. Credits are signed positive, debits negative, and unsupported types such as `REVERSAL` become zero.
2. Deduplication retains only the most recent row when a transaction appears more than once.
3. Kafka offset is used as the final tie-breaker when duplicate rows have the same transaction timestamp and Kafka timestamp.
4. Events older than the hard-coded 900-second lateness threshold are selected and shaped correctly for the late-arrival audit log.
5. The offset transform produces an exclusive ending offset, calculated as the maximum input offset plus one for the DataFrame passed into the function.

The Spark unit tests do not require Kafka or PostgreSQL. They test deterministic DataFrame transformations independently from external I/O. End-to-end behavior is manually demonstrable through the Usage and replay checks above, but is not currently covered by automated integration tests.
