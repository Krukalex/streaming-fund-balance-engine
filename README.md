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
3. **Airflow Service**
  - Orchestrates and schedules batch Spark jobs
  - Demonstrates dependency management, retries, and monitoring
4. **Spark Processor**
  - Consumes transaction event batches
  - Processes events to update a canonical fund balance table
  - Maintains state and supports incremental reconciliation

## Key Features

- Multi-fund transaction simulation with realistic fund-deal relationships
- Append-only event stream storage with guaranteed ordering
- Batch orchestration with Airflow DAGs and task dependencies
- Spark-based balance computation with idempotency and reconciliation
- Canonical state management for fund balances
- Modular design for easy extension and testing

## Spark Processing Logic

The Spark job processes Kafka transaction events into deterministic, auditable balance deltas for each fund/deal combination.

1. **Read data from Kafka stream**
  - Consume transaction events from Kafka and retain metadata (`topic`, `partition`, `offset`, `kafka_timestamp`) for traceability and deterministic tie-breaking.
2. **Flatten into a DataFrame**
  - Parse JSON payloads into a typed schema.
  - Flatten nested `fund` and `deal` objects into top-level columns.
  - Apply fallback handling for missing fields to support schema evolution.
3. **Detect late arriving events**
  - Compute transaction age using epoch-second comparison against `current_timestamp()`.
  - Flag events older than 5 minutes into a dedicated late-arrival DataFrame.
  - Persist those rows to the **late arriving event log** table; rollup counts go to `**run_metrics`**.
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
    - `DEBIT` -> positive amount
    - `CREDIT` -> negative amount
8. **Aggregate by fund and deal**
  - Group by `fund_id` and `deal_id` to produce per-batch incremental net balance changes.
9. **Upsert into canonical state table**
  - Perform an idempotent upsert so retries do not double count and current balances remain correct.

## PostgreSQL schema

PostgreSQL holds the canonical **balance state**, **per-run rollup metrics**, and **drill-down logs**. Pipeline runs should carry a stable **`run_id`** (typically one Airflow/Spark invocation) for correlation across tables.

### Tables and grain

| Table                     | Grain / identity | Purpose |
| ------------------------- | ---------------- | ------- |
| `transaction_balance`     | `(fund_id, deal_id)` | Current net balance per fund/deal pair. |
| `run_metrics`             | `run_id` | One row per pipeline run: volumes and quality counts. |
| `duplicate_records_log` | **Surrogate PK** + unique `(run_id, transaction_id)` | Duplicate groups for that run: counts, min/max business time, and full Kafka pointer for the retained winner. |
| `late_arriving_event_log` | **Surrogate PK** + unique `(kafka_topic, kafka_partition, kafka_offset)` | One durable row per **physical Kafka message** flagged late; survives retries, backfills, and replays without double inserts. |

### Idempotent surrogate keys (SHA-256, 64-char lowercase hex)

Both log tables use a **`surrogate_pk`** primary key computed in Spark as **`sha256(..., 256)`** (hex) over a UTF-8 string with fixed prefixes so key spaces never collide:

- **`duplicate_records_log.surrogate_pk`**: hash the UTF-8 literal string ``DUP|{run_id}|{transaction_id}`` (exact `|` separators; keep `run_id` / `transaction_id` consistent with what you write to the row). Same **`run_id`** + same **`transaction_id`** always maps to one row, so retries of the **same** run are idempotent.

- **`late_arriving_event_log.surrogate_pk`**: hash the UTF-8 literal string ``LATE|{kafka_topic}|{partition}|{offset}``, with **`partition`** and **`offset`** as decimal integers (no zero-padding). Identity is **the Kafka coordinate**, not **`transaction_id`**, so replaying the same message always hits the same key even if **`transaction_id` appears across multiple runs**.

**Database constraints:** DDL adds **unique indexes** on the natural tuples above as well (`duplicate_records_identity_uq`, `late_arriving_event_kafka_uq`) so ingestion bugs cannot silently diverge surrogate vs physical identity.

**`transaction_balance`**

- `fund_id`, `deal_id` — natural key (identifiers only; names live on events / dimensions, not on state).
- `balance` — current net balance after applied signed movements.
- `last_modified` — when this row was last updated by the job.

**`run_metrics`**

- `record_count` — rows considered in the run after parsing (aligned with Spark’s batch scope).
- `duplicate_count` — number of **`transaction_id`s that had duplicates** in that run; should match **`SELECT COUNT(*) FROM duplicate_records_log WHERE run_id = ?`** after a successful load.
- `late_arrival_count` — number of events flagged late **in this run’s batch** (Spark count). It may not equal `COUNT(*) FROM late_arriving_event_log WHERE run_id = ?` because late rows are keyed by **Kafka coordinates** and may have been inserted under an earlier replay; use this field as the run’s **observed** late volume, not as a strict join key to the log.
- `run_timestamp` — when the run was recorded (e.g. job start or completion).

**`duplicate_records_log`**

Winner metadata includes **`winner_partition`** and **`winner_offset`** (with **`winner_topic`**) so the chosen message can be relocated in Kafka independently of other ties.

**`late_arriving_event_log`**

- **`txn_age_sec`** — arrival delay / staleness metric: same definition as Spark’s **`txn_age_sec`**, namely `unix_timestamp(current_timestamp) - unix_timestamp(transaction_timestamp)` at processing time for that run (seconds). This column is persisted for SLA reporting without recomputing from timestamps.

DDL for these objects lives in **`db/init.sql`**.

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


| Field                   | Type              | Description                                             | Required |
| ----------------------- | ----------------- | ------------------------------------------------------- | -------- |
| `transaction_id`        | string (UUID)     | Unique identifier for the transaction                   | Yes      |
| `transaction_timestamp` | string (ISO 8601) | When the transaction occurred (used for event ordering) | Yes      |
| `transaction_amount`    | number (float)    | Transaction amount in specified currency                | Yes      |
| `currency`              | string            | Currency code (e.g., USD, EUR)                          | Yes      |
| `transaction_type`      | string            | Direction of cash flow: `DEBIT`, `CREDIT`, `REVERSAL`   | Yes      |
| `status`                | string            | Processing status: `PENDING`, `COMPLETED`, `FAILED`     | Yes      |
| `fund`                  | object            | Fund context: `fund_id` (int), `fund_name` (string)     | Yes      |
| `deal`                  | object            | Deal context: `deal_id` (int), `deal_name` (string)     | Yes      |
| `metadata`              | object            | Optional enrichment data (e.g., source, strategy)       | No       |


### Schema Evolution

New fields are added as optional to maintain backward compatibility. Consumers apply defaults for missing fields (e.g., `metadata` defaults to an empty object).

**Spark Processing Strategy**: When reading events in PySpark, missing fields are handled with default values using functions like `coalesce()` or `.get()` with fallbacks. For example, a new `risk_factor` field defaults to 0.0 if absent.

**State Table Design**: `transaction_balance` stores only `**fund_id`**, `**deal_id**`, `**balance**`, and `**last_modified**` so the ledger state stays narrow and authoritative; enrichment (names, metadata) stays on raw events or drill-down logs.

## Project Structure

```
streaming-fund-balance-engine/
├── emitter/
│   └── emitter.py              # Event producer for transaction events
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

## Usage

1. Start Kafka and any required services
2. Run the event emitter to publish transaction messages
3. Start Airflow and enable the DAG for batch processing
4. Execute the Spark job to process events and update balances
5. Review the canonical balance table to verify correctness

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

