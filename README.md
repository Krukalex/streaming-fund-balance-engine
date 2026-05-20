# Streaming Fund Balance Engine

## Introduction

The Streaming Fund Balance Engine is an event-driven finance pipeline designed to showcase modern data engineering capabilities using Kafka, Airflow, and Apache Spark. It simulates transaction event flow across funds, maintains an append-only event log, and ultimately computes canonical fund balances through batch processing.

This project demonstrates end-to-end data engineering skills, from event generation to stateful processing, and is built to impress in senior-level interviews and portfolio reviews.

## Technologies Used

- **Apache Kafka**: Event streaming and durable messaging backbone
- **Apache Spark**: Batch processing and stateful aggregation
- **Apache Airflow**: Workflow orchestration and scheduling
- **Python**: Emitter and consumer applications
- **Docker**: Containerized services for local development
- **PostgreSQL**: Canonical balance table storage (optional)

## Architecture

The system is built around four core components:

1. **Event Emitter**
  - Produces transaction events for multiple funds
  - Sends events into Kafka topics for downstream processing
2. **Kafka Stream**
  - Serves as the event log and durable messaging backbone
  - Maintains an ordered, replayable history of all transactions
3. **Airflow Service** (planned)
  - Will orchestrate and schedule batch Spark jobs on a fixed interval
  - Will supply a stable `run_id` per DAG run for retries and cross-table correlation
4. **Spark Processor**
  - Consumes transaction events from Kafka in **offset-based batches** (not Structured Streaming)
  - Resumes each run from the last successful checkpoint stored in PostgreSQL (`kafka_offsets`)
  - Writes a transaction ledger, audit logs, run metrics, and derived fund/deal balances in PostgreSQL

## Key Features

- Multi-fund transaction simulation with realistic fund-deal relationships
- Append-only event stream storage with guaranteed ordering
- Offset-based Spark batch reads with PostgreSQL checkpointing (`kafka_offsets`)
- Batch orchestration with Airflow DAGs and task dependencies
- Spark-based balance computation with idempotency and reconciliation
- Canonical state management for fund balances
- Modular design for easy extension and testing

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

| Column | Description |
| ------ | ----------- |
| `run_id` | Correlates with the pipeline run (same identifier family as `run_metrics`; today generated in `spark/job.py`, later from Airflow) |
| `topic` | Kafka topic name (e.g. `transactions`) |
| `partition` | Topic partition id |
| `start_offset` | First offset read in this run (inclusive; matches Spark `startingOffsets`) |
| `end_offset` | **Next** offset to read after this run (exclusive end; becomes the next run’s `startingOffsets`) |
| `status` | Run outcome (e.g. `SUCCESS`); only successful runs advance the read pointer |
| `run_timestamp` | When the checkpoint row was recorded |

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
- `**kafka_timestamp`** is the timestamp from the Spark Kafka source for that message (broker metadata).
- `**transaction_timestamp`** is the business event time from the payload.

This definition is **stable across replays**: reprocessing the same message recomputes the **same** lag, so a batch replay does not reinterpret “late” using wall-clock time at replay.

### Surrogate primary key and idempotent log writes

Each row in `late_arriving_event_log` is keyed by a **deterministic `surrogate_pk`**: a SHA-256 hex string over a fixed UTF-8 literal that includes the **physical Kafka identity** — `topic`, `partition`, and `offset` — in a single documented order (e.g. `LATE|{topic}|{partition}|{offset}` with `|` separators). That identity is stable for the life of the message in the log.

Inserts use `**ON CONFLICT (surrogate_pk) DO NOTHING`**: the **first** successful write wins; replays and retries do **not** replace or duplicate the observability row. That preserves “what we thought when we first persisted this late message.”

### Run metrics vs log cardinality

`**run_metrics.late_arrival_count`** is the count of messages **in the current batch slice** that satisfy the late predicate **in Spark**, computed every time the job runs. That number answers: *“this execution observed n late messages in its input.”* It is **not** required to equal the number of **new** rows appended to `late_arriving_event_log` on that attempt (replays often append **zero** new rows because of `DO NOTHING`).

### Why not `current_timestamp()` − `transaction_timestamp`?

Comparing business time to **job wall clock** changes every time you run or replay a job, so old batches can incorrectly inflate “late” over time. Ingest lag ties late-ness to **when the message actually landed in Kafka** relative to the business event, which is the usual operational reading for pipeline delay.

## PostgreSQL schema

PostgreSQL holds the canonical **balance state**, **per-run rollup metrics**, and **drill-down logs**. Each run is tagged with a `**run_id`** for correlation across tables (see [Airflow orchestration (planned)](#airflow-orchestration-planned)).

### Tables and grain


| Table                     | Grain / identity                                                         | Purpose                                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `transaction_ledger`      | `transaction_id` (+ unique Kafka coordinates)                            | Append-style ledger of signed movements; idempotent per `transaction_id` on replay.                                           |
| `transaction_balance`     | `(fund_id, deal_id)`                                                     | Current net balance per fund/deal pair, derived as `SUM` over the ledger.                                                     |
| `run_metrics`             | `run_id`                                                                 | One row per pipeline run: volumes and quality counts; updated on retry of the same `run_id`.                                  |
| `duplicate_records_log`   | **Surrogate PK** + unique `(run_id, transaction_id)`                     | Duplicate groups for that run: counts, min/max business time, and full Kafka pointer for the retained winner.                 |
| `late_arriving_event_log` | **Surrogate PK** + unique `(kafka_topic, kafka_partition, kafka_offset)` | One durable row per **physical Kafka message** flagged late; survives retries, backfills, and replays without double inserts. |
| `kafka_offsets`             | `run_id`                                                                 | Per-run Kafka consumption checkpoint: topic, partition, offset range, and status for offset-based batch resume. |


### Idempotent surrogate keys (SHA-256, 64-char lowercase hex)

Both log tables use a `**surrogate_pk`** primary key computed in Spark as `**sha256(..., 256)`** (hex) over a UTF-8 string with fixed prefixes so key spaces never collide:

- `**duplicate_records_log.surrogate_pk`**: hash the UTF-8 literal string `DUP|{run_id}|{transaction_id}` (exact `|` separators). Same `run_id` + same `transaction_id` always maps to one row, so retries of the **same** run are idempotent.
- `**late_arriving_event_log.surrogate_pk`**: hash the UTF-8 literal string `LATE|{kafka_topic}|{partition}|{offset}`, with `partition` and `offset` as decimal integers (no zero-padding). Identity is the Kafka coordinate, not `transaction_id`.

**Database constraints:** DDL adds **unique indexes** on the natural tuples above as well (`duplicate_records_identity_uq`, `late_arriving_event_kafka_uq`) so ingestion bugs cannot silently diverge surrogate vs physical identity.

`**transaction_ledger`**

- One row per `transaction_id` from the deduplicated (winning) stream, with signed `transaction_amount`, fund/deal keys, and Kafka metadata.
- `ON CONFLICT (transaction_id) DO NOTHING` on insert: replays with the same winning rows do not create duplicate ledger entries.

`**transaction_balance`**

- `fund_id`, `deal_id` — natural key (identifiers only; names live on events / dimensions, not on state).
- `balance` — `SUM(transaction_amount)` from `transaction_ledger` for that fund/deal (full snapshot recompute each run).
- **Idempotency:** identical ledger contents produce identical sums; `ON CONFLICT DO UPDATE` refreshes balances only when the recomputed total changes (e.g. new `transaction_id`s landed in the ledger).

`**run_metrics`**

- `record_count` — rows considered in the run after parsing (aligned with Spark’s batch scope).
- `duplicate_count` — number of distinct `transaction_id` values that had duplicates in that run (batch observation); on first load for a `run_id`, should match `SELECT COUNT(*) FROM duplicate_records_log WHERE run_id = ?`.
- `late_arrival_count` — number of messages in **this run’s batch** that satisfied the ingest-lag late rule in Spark (**observed in batch**). It does **not** necessarily equal new rows inserted into `late_arriving_event_log` on replay (`ON CONFLICT DO NOTHING` on `surrogate_pk`).
- `run_timestamp` — when the run was recorded (updated on `ON CONFLICT DO UPDATE` when the same `run_id` is retried).

`**kafka_offsets`**

- One row per pipeline run (`run_id` primary key) recording which Kafka slice was consumed.
- `start_offset` / `end_offset` — inclusive start and **exclusive** end of the batch (see [Offset-based batch processing](#offset-based-batch-processing)).
- `status` — only rows with `SUCCESS` participate in `MAX(end_offset)` when computing the next `startingOffsets`.
- Ties batch boundaries to the same `run_id` used in `run_metrics` and audit logs.

`**duplicate_records_log`**

Winner metadata includes `**winner_partition`** and `**winner_offset`** (with `**winner_topic**`) so the chosen message can be relocated in Kafka independently of other ties.

`**late_arriving_event_log**`

- `**txn_age_sec**` — persisted **ingest lag** in seconds: `unix_timestamp(kafka_timestamp) - unix_timestamp(transaction_timestamp)`, matching the Spark late rule. See [Late arriving events (design)](#late-arriving-events-design).

DDL for these objects lives in `**db/init.sql`**.

## Airflow orchestration (planned)

Airflow is not required to run the pipeline today; the Spark job can be executed manually while the core logic matures.

**Planned integration:**

- A DAG under `airflow/dags/` will schedule the Spark batch job (e.g. every 5 minutes).
- `**run_id`** will be passed from Airflow (e.g. `dag_run.run_id` or a templated execution id) into the Spark job so retries of the **same** DAG run reuse one `run_id` across `run_metrics`, `duplicate_records_log`, and correlated audit rows.
- **Today:** `spark/job.py` generates a local UUID `run_id` at startup for development and testing.
- Task dependencies will enforce ordering: infrastructure health → Spark processing → optional validation queries against PostgreSQL.
- Offset checkpoints in `kafka_offsets` will be written only after successful DAG completion, using the same `run_id` as `run_metrics` (see [Offset-based batch processing](#offset-based-batch-processing)).

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
│   └── job.py                  # Spark batch processing job for balance computation
├── airflow/
│   └── dags/
│       └── pipeline_dag.py     # Airflow DAG for orchestrating batch jobs
├── docker/
│   └── docker-compose.yml      # Docker Compose configuration for services
├── db/
│   └── init.sql                # Database initialization script
├── README.md                   # This file
└── .gitignore                  # Git ignore file
```

### Component Details

- **emitter/** - Contains the event emitter application that generates transaction events for multiple funds and publishes them to Kafka
- **spark/** - Contains the Spark job that processes batches of events from Kafka and updates the canonical balance table
- **airflow/** - Contains the Airflow DAG definitions for orchestrating the Spark jobs on a schedule
- **docker/** - Contains Docker Compose configuration to spin up all required services (Kafka, Airflow, Spark, database)
- **db/** - Contains database schema and initialization scripts for the canonical balance table

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

`docker-compose.yml` starts PostgreSQL with database `**fund_balance`**, user `**funduser`**, password `**fundpass**`, and applies `**db/init.sql**` on first data volume creation.

- **Spark runs inside the Compose network** (e.g. `spark-submit` from `spark-master`): use host `**postgres`**, port `**5432`**: `jdbc:postgresql://postgres:5432/fund_balance`
- **Spark runs on your host machine** (IDE / local `spark-submit`): use `**localhost:5432`** (same URL path and credentials).

Put the PostgreSQL JAR in `./jars` (already on Spark’s extra classpath via Compose). In PySpark, use `format("jdbc")` with `.option("url", url)`, `.option("dbtable", table)`, `.option("user", "funduser")`, `.option("password", "fundpass")`, and for upserts use `foreachBatch` with JDBC or stage via temp table + SQL `ON CONFLICT` (implementation detail for a later change).

## Usage

1. From the repo root: `docker compose -f docker/docker-compose.yml up -d` (starts Kafka, PostgreSQL, Spark master/worker, and related services)
2. Run the event emitter to publish transaction messages (`python emitter/emitter.py`)
3. Execute the Spark job to process the next Kafka batch (resumes from `kafka_offsets`; ledger, balances, audit logs, and `run_metrics`) — Airflow scheduling is [planned](#airflow-orchestration-planned)
4. (Future) Enable the Airflow DAG for scheduled runs with a stable `run_id`
5. Review `transaction_balance`, `run_metrics`, and drill-down logs to verify correctness

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

- The exact stack and deployment details may vary based on the repository's implementation files.
- If Docker, Kubernetes, or cloud deployment are available, this project can be extended to show infrastructure orchestration as well.
- Focus on the end-to-end flow from event generation to canonical balance update when presenting the project.

