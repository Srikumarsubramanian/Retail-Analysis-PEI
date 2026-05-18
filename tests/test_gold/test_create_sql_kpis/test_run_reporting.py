import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock
from pyspark.sql import SparkSession

from retail_analysis.databricks.notebooks import create_sql_kpis as rpt


@pytest.fixture()
def run_reporting_setup(monkeypatch):
    spark = MagicMock(name="spark")

    df = MagicMock(name="df")
    df.cache.return_value = df

    read_aggregate_table = MagicMock(return_value=df)
    build_kpis = MagicMock()
    log = MagicMock()

    monkeypatch.setattr(rpt, "_read_aggregate_table", read_aggregate_table)
    monkeypatch.setattr(rpt, "build_kpis", build_kpis)
    monkeypatch.setattr(rpt, "log", log)

    return SimpleNamespace(
        spark=spark,
        df=df,
        read_aggregate_table=read_aggregate_table,
        build_kpis=build_kpis,
        log=log,
    )


@pytest.mark.reporting
def test_run_reporting_success(run_reporting_setup):
    """
    Validate successful reporting pipeline execution.

    Ensures:
    - aggregate is read and cached
    - temp view created
    - KPIs executed
    - cleanup (unpersist) happens
    """
    s = run_reporting_setup

    result = rpt.run_reporting(s.spark)


    assert result is True

    # read + cache
    s.read_aggregate_table.assert_called_once_with(s.spark)
    s.df.cache.assert_called_once()

    # temp view
    s.df.createOrReplaceTempView.assert_called_once_with("profit_aggregate")

    # KPI execution
    s.build_kpis.assert_called_once_with(s.spark)

    # cleanup should happen
    s.df.unpersist.assert_called_once()

    # logs
    s.log.info.assert_any_call("Running KPI calculations")
    s.log.info.assert_any_call("Loaded aggregate source table")
    s.log.info.assert_any_call("Reporting pipeline completed successfully")

    s.log.exception.assert_not_called()

@pytest.mark.reporting
@pytest.mark.parametrize(
    "failure_target",
    ["read", "kpi"],
)
def test_run_reporting_failure(run_reporting_setup, failure_target):
    """
    Ensure failures are wrapped as ReportingError
    and cleanup still executes when applicable.
    """
    s = run_reporting_setup

    if failure_target == "read":
        s.read_aggregate_table.side_effect = Exception("Error reading aggregate table")

    elif failure_target == "kpi":
        s.build_kpis.side_effect = Exception("Error calculating KPIs")

    with pytest.raises(rpt.ReportingError, match="Reporting pipeline failed"):
        rpt.run_reporting(s.spark)

    # cleanup behavior
    if failure_target == "read":
        # agg never created → no unpersist
        s.df.unpersist.assert_not_called()
    else:
        # agg exists → cleanup attempted
        s.df.unpersist.assert_called_once()

    # exception logged
    s.log.exception.assert_called_once_with("Reporting pipeline failed")