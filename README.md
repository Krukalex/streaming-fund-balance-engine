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

**State Table Design**: The canonical balance table includes columns for all possible event fields. Events without certain fields insert default values (e.g., NULL or 0) into the corresponding columns, ensuring consistent schema across all records.

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
