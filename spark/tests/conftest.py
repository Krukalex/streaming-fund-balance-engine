"""
Shared pytest fixtures for Spark unit tests.

A SparkSession is expensive to create, so we build one local session and
reuse it across the whole test session (scope="session").
"""
import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Allow `from transforms import ...` when pytest is run from the repo root.
SPARK_DIR = Path(__file__).resolve().parents[1]
if str(SPARK_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_DIR))

# Spark launches "python" for its workers. On Windows that name is often
# hijacked by the Microsoft Store app-execution alias, so point Spark at the
# exact interpreter running the tests.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("fund-balance-tests")
        .master("local[*]")
        # Small shuffle partition count keeps tiny test DataFrames fast.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
