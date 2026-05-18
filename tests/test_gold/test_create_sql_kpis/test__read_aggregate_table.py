import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from retail_analysis.databricks.notebooks import create_sql_kpis as rpt
from retail_analysis.databricks.utils.custom_exceptions import ReadError


@pytest.fixture()
def read_agg_setup(monkeypatch):
    spark = MagicMock(name="spark")
    df = MagicMock(name="df")
    enforced_df = MagicMock(name="enforced_df")

    read_data = MagicMock(return_value=df)
    enforce_schema = MagicMock(return_value=enforced_df)
    log = MagicMock()

    monkeypatch.setattr(rpt, "read_data", read_data)
    monkeypatch.setattr(rpt, "enforce_schema", enforce_schema)
    monkeypatch.setattr(rpt, "log", log)

    return SimpleNamespace(
        spark=spark,
        df=df,
        enforced_df=enforced_df,
        read_data=read_data,
        enforce_schema=enforce_schema,
        log=log,
    )


@pytest.mark.reporting
def test_read_aggregate_table_success(read_agg_setup):
    s = read_agg_setup

    result = rpt._read_aggregate_table(s.spark)

    assert result == s.enforced_df

    s.read_data.assert_called_once_with(
        s.spark,
        rpt.GOLD_DELTA_PATH,
        "profit_by_year_category_customer",
        "delta",
    )

    s.enforce_schema.assert_called_once_with(
        s.df,
        rpt.PROFIT_AGGREGATE_SCHEMA,
        "Profit Aggregate Read",
    )

    


@pytest.mark.reporting
def test_read_aggregate_table_failure(read_agg_setup):
    s = read_agg_setup

    s.read_data.side_effect = Exception("Error reading aggregate table")

    with pytest.raises(rpt.ReadError, match="Error reading aggregate table"):
        rpt._read_aggregate_table(s.spark)
    
    s.log.exception.assert_called_once_with("Error reading aggregate table")

