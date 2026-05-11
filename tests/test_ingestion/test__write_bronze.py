import pytest
from unittest.mock import MagicMock
import retail_analysis.databricks.notebooks.ingestion as ingestion
from retail_analysis.databricks.utils.custom_exceptions import WriteError

@pytest.mark.ingestion
@pytest.fixture()
def write_setup(monkeypatch):
    spark = MagicMock(name="spark")
    df = MagicMock(name="df")

    mock_write = MagicMock(name="write_delta_table")

    monkeypatch.setattr(ingestion, "write_delta_table", mock_write)

    return {
        "spark": spark,
        "df": df,
        "mock_write": mock_write,
    }
def test_write_bronze_default_overwrite(write_setup):
    s = write_setup

    ingestion._write_bronze(
        s["spark"],
        s["df"],
        "orders.csv",
        "/tmp/bronze",
    )

    s["mock_write"].assert_called_once_with(
        s["spark"],
        s["df"],
        file_name="orders",
        base_path="/tmp/bronze",
        mode="overwrite",
        partition_by=None,
        overwriteSchema="true",
    )

@pytest.mark.ingestion
def test_write_bronze_non_overwrite_mode(write_setup):
    s = write_setup

    ingestion._write_bronze(
        s["spark"],
        s["df"],
        "orders.csv",
        "/tmp/bronze",
        mode="append",
    )

    s["mock_write"].assert_called_once_with(
        s["spark"],
        s["df"],
        file_name="orders",
        base_path="/tmp/bronze",
        mode="append",
        partition_by=None,
    )

@pytest.mark.ingestion
def test_write_bronze_with_partitions(write_setup):
    s = write_setup

    ingestion._write_bronze(
        s["spark"],
        s["df"],
        "orders.csv",
        "/tmp/bronze",
        partition_cols=["year"],
    )

    s["mock_write"].assert_called_once_with(
        s["spark"],
        s["df"],
        file_name="orders",
        base_path="/tmp/bronze",
        mode="overwrite",
        partition_by=["year"],
        overwriteSchema="true",
    )

@pytest.mark.ingestion
def test_write_bronze_failure_raises_write_error(write_setup):
    s = write_setup

    s["mock_write"].side_effect = Exception("disk full")

    with pytest.raises(WriteError, match="Writing bronze file orders.csv failed"):
        ingestion._write_bronze(
            s["spark"],
            s["df"],
            "orders.csv",
            "/tmp/bronze",
        )