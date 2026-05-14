import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from retail_analysis.databricks.notebooks import run_master_pipeline as pipe


@pytest.fixture()
def pipeline_setup(monkeypatch):
    spark = MagicMock(name="spark")

    # stage mocks
    bronze = MagicMock(return_value="bronze_df")
    silver = MagicMock(return_value="silver_df")
    master = MagicMock(return_value="master_df")
    aggregate = MagicMock(return_value="agg_df")
    reporting = MagicMock(return_value=None)

    log = MagicMock()

    # patch imports
    monkeypatch.setattr(pipe, "log", log)

    monkeypatch.setattr(
        "retail_analysis.databricks.notebooks.create_raw_tables.run_bronze", bronze
    )
    monkeypatch.setattr(
        "retail_analysis.databricks.notebooks.create_enriched_customers_products.run_silver", silver
    )
    monkeypatch.setattr(
        "retail_analysis.databricks.notebooks.create_enriched_orders.run_master_orders", master
    )
    monkeypatch.setattr(
        "retail_analysis.databricks.notebooks.create_aggregate.run_profit_aggregate", aggregate
    )
    monkeypatch.setattr(
        "retail_analysis.databricks.notebooks.create_sql_kpis.run_reporting", reporting
    )

    return SimpleNamespace(
        spark=spark,
        bronze=bronze,
        silver=silver,
        master=master,
        aggregate=aggregate,
        reporting=reporting,
        log=log,
    )



@pytest.mark.master_pipeline
@pytest.mark.parametrize(
    "failure_stage",
    ["bronze", "silver", "master", "aggregate", "reporting"],
)
def test_run_pipeline_failure(pipeline_setup, failure_stage):
    """
    Ensure pipeline stops and wraps failure correctly.
    """
    s = pipeline_setup

    getattr(s, failure_stage).side_effect = Exception("Simulated pipeline failure")

    with pytest.raises(pipe.PipelineError):
        pipe.run_pipeline(s.spark)

    s.log.exception.assert_called()




@pytest.mark.master_pipeline
def test_run_pipeline_success(pipeline_setup):
    """
    Validate full pipeline execution.
    """
    s = pipeline_setup

    result = pipe.run_pipeline(s.spark, "config.yml")

    assert result == {
        "bronze": "bronze_df",
        "silver": "silver_df",
        "gold_master": "master_df",
        "gold_aggregate": "agg_df",
        "reporting": None,
    }

    s.bronze.assert_called_once_with(s.spark, "config.yml")
    s.silver.assert_called_once_with(s.spark)
    s.master.assert_called_once_with(s.spark)
    s.aggregate.assert_called_once_with(s.spark)
    s.reporting.assert_called_once_with(s.spark)


