-- Streaming Fund Balance Engine — PostgreSQL schema
-- Canonical state, run rollups, and drill-down logs for Spark sinks.
-- Log tables use SHA-256 (64-char lowercase hex) surrogate PKs for idempotent upserts across retries, backfills, and replays.

CREATE TABLE IF NOT EXISTS transaction_balance (
    fund_id VARCHAR(64) NOT NULL,
    deal_id VARCHAR(64) NOT NULL,
    balance DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fund_id, deal_id)
);

CREATE TABLE IF NOT EXISTS transaction_ledger (
    transaction_id VARCHAR(128) PRIMARY KEY,
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    transaction_amount DOUBLE PRECISION NOT NULL,
    fund_id VARCHAR(64) NOT NULL,
    deal_id VARCHAR(64) NOT NULL,
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_topic VARCHAR(256) NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT transaction_ledger_kafka_uq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);


CREATE TABLE IF NOT EXISTS run_metrics (
    run_id VARCHAR(128) PRIMARY KEY,
    record_count INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    late_arrival_count INTEGER NOT NULL DEFAULT 0,
    run_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per duplicate group observed in a run; PK is deterministic from (run_id, transaction_id).
CREATE TABLE IF NOT EXISTS duplicate_records_log (
    surrogate_pk CHAR(64) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    transaction_id VARCHAR(128) NOT NULL,
    duplicate_count INTEGER NOT NULL,
    earliest_transaction_timestamp TIMESTAMPTZ,
    latest_transaction_timestamp TIMESTAMPTZ,
    winner_transaction_timestamp TIMESTAMPTZ,
    winner_kafka_timestamp TIMESTAMPTZ,
    winner_partition INTEGER NOT NULL,
    winner_offset BIGINT NOT NULL,
    winner_topic VARCHAR(256) NOT NULL,
    CONSTRAINT duplicate_records_pk PRIMARY KEY (surrogate_pk),
    CONSTRAINT duplicate_records_identity_uq UNIQUE (run_id, transaction_id)
);

CREATE TABLE IF NOT EXISTS late_arriving_event_log (
    surrogate_pk CHAR(64) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    transaction_id VARCHAR(128) NOT NULL,
    txn_age_sec INTEGER NOT NULL,
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    event_timestamp TIMESTAMPTZ,
    transaction_amount DOUBLE PRECISION NOT NULL,
    currency VARCHAR(16) NOT NULL,
    transaction_type VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    fund_id VARCHAR(64),
    fund_name VARCHAR(256),
    deal_id VARCHAR(64),
    deal_name VARCHAR(256),
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_topic VARCHAR(256) NOT NULL,
    CONSTRAINT late_arriving_event_pk PRIMARY KEY (surrogate_pk),
    CONSTRAINT late_arriving_event_kafka_uq UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS kafka_offsets (
    run_id VARCHAR(128) PRIMARY KEY NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition INTEGER NOT NULL,
    start_offset INTEGER,
    end_offset INTEGER,
    run_timestamp TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_duplicate_records_log_run_id ON duplicate_records_log (run_id);
CREATE INDEX IF NOT EXISTS idx_late_arriving_event_log_run_id ON late_arriving_event_log (run_id);
