# Streaming Fund Balance Engine

## Project Overview

The Streaming Fund Balance Engine is an event-driven finance pipeline designed to showcase modern data engineering capabilities using Kafka, Airflow, and Apache Spark. It simulates transaction event flow across funds, maintains an append-only event log, and ultimately computes canonical fund balances through batch processing.

This project is ideal for demonstrating:

- event-driven architecture for transaction processing
- stream ingestion and event logging with Kafka
- workflow orchestration with Apache Airflow
- batch data processing with Apache Spark
- state management via a canonical balance table
- end-to-end data engineering design and implementation

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

- design of a scalable, loosely coupled pipeline for financial events
- use of Kafka for durable event streaming and replayability
- orchestration patterns using Airflow DAGs and task dependencies
- Spark batch processing and stateful aggregation logic
- how event-driven systems can support transactional state updates
- ability to build a real-world data engineering solution end to end

## Key Features

- multi-fund transaction simulation
- append-only event stream storage
- batch orchestration with Airflow
- Spark-based balance computation
- canonical state management for fund balances
- modular design for easy extension

## Recommended Technologies

- Apache Kafka
- Apache Spark
- Apache Airflow
- Python (for emitter + orchestration)
- Docker (optional for local deployment)
- PostgreSQL / Delta Lake / parquet files for balance storage (optional)

## Usage

1. Start Kafka and any required services
2. Run the event emitter to publish transaction messages
3. Start Airflow and enable the DAG for batch processing
4. Execute the Spark job to process events and update balances
5. Review the canonical balance table to verify correctness

## Why This Project Matters

This repo is built to demonstrate strong data engineering skills in both architecture and implementation. It shows how to:

- build reliable event-driven systems
- connect streaming and batch processing patterns
- manage orchestration and workflow dependencies
- produce a strong technical story for data engineering interviews or portfolio presentations

## Notes

- The exact stack and deployment details may vary based on the repository’s implementation files.
- If Docker, Kubernetes, or cloud deployment are available, this project can be extended to show infrastructure orchestration as well.
- Focus on the end-to-end flow from event generation to canonical balance update when presenting the project.
