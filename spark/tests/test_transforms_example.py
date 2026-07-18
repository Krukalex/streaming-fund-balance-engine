"""
Unit tests for pure Spark transforms in transforms.py.
"""
from datetime import datetime

from transforms import (
    add_signed_amount,
    build_late_arriving_events,
    deduplicate_transactions,
    get_df_with_offsets,
)


def test_add_signed_amount_signs_by_transaction_type(spark):
    input_df = spark.createDataFrame(
        [
            ("t1", "CREDIT", 100.0),
            ("t2", "DEBIT", 40.0),
            ("t3", "REVERSAL", 25.0),
        ],
        ["transaction_id", "transaction_type", "transaction_amount"],
    )

    result = {
        row["transaction_id"]: row["signed_amount"]
        for row in add_signed_amount(input_df).collect()
    }

    assert result["t1"] == 100.0
    assert result["t2"] == -40.0
    assert result["t3"] == 0


def test_deduplicate_transactions_keeps_latest_row(spark):
    """
    Winner order: transaction_timestamp DESC, kafka_timestamp DESC, offset DESC.
    """
    input_df = spark.createDataFrame(
        [
            # older business time — should lose
            ("txn-1", datetime(2025, 1, 1, 10, 0, 0), datetime(2025, 1, 1, 12, 0, 0), 1),
            # newer business time — should win
            ("txn-1", datetime(2025, 1, 1, 11, 0, 0), datetime(2025, 1, 1, 11, 0, 0), 2),
            # unique id — always kept
            ("txn-2", datetime(2025, 1, 1, 9, 0, 0), datetime(2025, 1, 1, 9, 0, 0), 3),
        ],
        ["transaction_id", "transaction_timestamp", "kafka_timestamp", "offset"],
    )

    rows = deduplicate_transactions(input_df).collect()
    by_id = {row["transaction_id"]: row for row in rows}

    assert len(rows) == 2
    assert by_id["txn-1"]["offset"] == 2
    assert by_id["txn-2"]["offset"] == 3


def test_deduplicate_transactions_uses_offset_as_tiebreaker(spark):
    """
    Same transaction_timestamp and kafka_timestamp → higher offset wins.
    """
    ts = datetime(2025, 1, 1, 10, 0, 0)
    input_df = spark.createDataFrame(
        [
            ("txn-1", ts, ts, 10),
            ("txn-1", ts, ts, 20),
        ],
        ["transaction_id", "transaction_timestamp", "kafka_timestamp", "offset"],
    )

    rows = deduplicate_transactions(input_df).collect()

    assert len(rows) == 1
    assert rows[0]["offset"] == 20


def test_build_late_arriving_events_filters_and_shapes(spark):
    """
    Only rows with txn_age_sec > threshold are kept; run_id and aliases are set.
    """
    input_df = spark.createDataFrame(
        [
            (
                "late-1",
                901,  # late (> 900)
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 1, 9, 0, 0),
                50.0,
                "USD",
                "CREDIT",
                "SETTLED",
                "F1",
                "Fund One",
                "D1",
                "Deal One",
                datetime(2025, 1, 1, 10, 0, 0),
                0,
                5,
                "transactions",
            ),
            (
                "on-time-1",
                900,  # not late (threshold is strict >)
                datetime(2025, 1, 1, 9, 45, 0),
                datetime(2025, 1, 1, 9, 45, 0),
                25.0,
                "USD",
                "DEBIT",
                "SETTLED",
                "F1",
                "Fund One",
                "D1",
                "Deal One",
                datetime(2025, 1, 1, 10, 0, 0),
                0,
                6,
                "transactions",
            ),
        ],
        [
            "transaction_id",
            "txn_age_sec",
            "transaction_timestamp",
            "event_timestamp",
            "transaction_amount",
            "currency",
            "transaction_type",
            "status",
            "fund_id",
            "fund_name",
            "deal_id",
            "deal_name",
            "kafka_timestamp",
            "partition",
            "offset",
            "topic",
        ],
    )

    result = build_late_arriving_events(
        input_df, run_id="run_abc", lag_threshold_sec=900
    ).collect()

    assert len(result) == 1
    row = result[0]
    assert row["transaction_id"] == "late-1"
    assert row["run_id"] == "run_abc"
    assert row["txn_age_sec"] == 901
    assert row["kafka_partition"] == 0
    assert row["kafka_offset"] == 5
    assert row["kafka_topic"] == "transactions"
    assert row["surrogate_pk"] is not None


def test_get_df_with_offsets_computes_exclusive_end(spark):
    """
    Checkpoint end_offset is exclusive: max(kafka_offset) + 1.
    """
    run_ts = datetime(2025, 1, 1, 12, 0, 0)
    input_df = spark.createDataFrame(
        [
            ("transactions", 0, 10),
            ("transactions", 0, 15),
            ("transactions", 0, 12),
        ],
        ["kafka_topic", "kafka_partition", "kafka_offset"],
    )

    rows = get_df_with_offsets(input_df, run_id="run_xyz", run_ts=run_ts).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["run_id"] == "run_xyz"
    assert row["topic"] == "transactions"
    assert row["partition"] == 0
    assert row["start_offset"] == 10
    assert row["end_offset"] == 16  # 15 + 1
    assert row["status"] == "SUCCESS"
