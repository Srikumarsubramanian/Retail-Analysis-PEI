import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock
from retail_analysis.databricks.utils.custom_exceptions import ReadError, TransformError

import retail_analysis.databricks.notebooks.create_aggregate as agg


@pytest.fixture()
def read_master_orders_setup(monkeypatch):
    spark = MagicMock(name="spark")
    df = MagicMock(name="df")

    read = MagicMock(name="read_data", return_value=df)
    log = MagicMock(name="log")

    monkeypatch.setattr(agg, "read_data", read)
    monkeypatch.setattr(agg, "log", log)

    return SimpleNamespace(
        spark=spark,
        df=df,
        read=read,
        log=log,
    )

#--------------------------read_master_orders----------------------------------------#

@pytest.mark.gold
def test_read_master_orders_success(read_master_orders_setup):
    """
    Ensure master orders are read successfully.
    """
    s = read_master_orders_setup

    result = agg._read_master_orders(s.spark)

    assert result == s.df

    s.read.assert_called_once_with(
        s.spark,
        agg.GOLD_DELTA_PATH,
        "master_orders",
        "delta",
    )

    s.log.info.assert_called_once()
    s.log.exception.assert_not_called()

@pytest.mark.gold
def test_read_master_orders_failure(read_master_orders_setup):
    """
    Ensure read failures are wrapped as ReadError.
    """
    s = read_master_orders_setup

    s.read.side_effect = Exception("File not found")

    with pytest.raises(agg.ReadError, match="Failed to read master orders"):
        agg._read_master_orders(s.spark)

    s.log.exception.assert_called_once_with("Failed to read master orders")
