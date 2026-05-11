import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from retail_analysis.databricks.notebooks import aggregate as agg


@pytest.fixture()
def run_profit_aggregate_setup(monkeypatch):
    """
    Shared setup for run_profit_aggregate tests.
    """
    spark = MagicMock(name="spark")

    read_df = MagicMock(name="read_df")
    enforced_df = MagicMock(name="enforced_df")
    agg_df = MagicMock(name="agg_df")
    repartitioned_df = MagicMock(name="repartitioned_df")
    
    agg_df.repartition.return_value = repartitioned_df

    read = MagicMock(name="_read_master_orders", return_value=read_df)
    enforce = MagicMock(name="enforce_schema", return_value=enforced_df)
    build = MagicMock(name="build_profit_aggregate", return_value=agg_df)
    
    merge = MagicMock(name="merge_delta_upsert")
    optimize = MagicMock(name="optimize_delta_zorder")
    log = MagicMock(name="log")

    monkeypatch.setattr(agg, "_read_master_orders", read)
    monkeypatch.setattr(agg, "enforce_schema", enforce)
    monkeypatch.setattr(agg, "build_profit_aggregate", build)
    monkeypatch.setattr(agg, "merge_delta_upsert", merge)
    monkeypatch.setattr(agg, "optimize_delta_zorder", optimize)
    monkeypatch.setattr(agg, "log", log)

    return SimpleNamespace(
        spark=spark,
        read=read,
        enforce=enforce,
        build=build,
        merge=merge,
        optimize=optimize,
        log=log,
        read_df=read_df,
        enforced_df=enforced_df,
        agg_df=agg_df,
        repartitioned_df=repartitioned_df,
    )


@pytest.mark.silver
def test_run_profit_aggregate_success(run_profit_aggregate_setup):
    """
    Validate the happy path for the profit aggregation pipeline.
    """
    s = run_profit_aggregate_setup

    result = agg.run_profit_aggregate(s.spark)

    assert result == s.agg_df

    s.read.assert_called_once_with(s.spark)
    s.enforce.assert_called_once_with(
        s.read_df,
        agg.MASTER_ORDERS_SCHEMA,
        "Master Orders Read",
    )
    s.build.assert_called_once_with(s.enforced_df)
    s.agg_df.repartition.assert_called_once_with("order_year")


    s.merge.assert_called_once_with(
        s.spark,
        s.repartitioned_df,
        table="profit_by_year_category_customer",
        base_path=agg.GOLD_DELTA_PATH,
        merge_keys=["order_year", "category", "sub_category", "customer_id"],
        partition_cols=["order_year"],
    )

    s.optimize.assert_called_once_with(
        s.spark,
        agg.GOLD_DELTA_PATH,
        "profit_by_year_category_customer",
        ["customer_id", "category"],
        s.log,
    )

    assert s.log.info.call_count > 0

    s.log.exception.assert_not_called()


@pytest.mark.silver
@pytest.mark.parametrize(
    "failure_target",
    [
        "read",
        "schema",

        "aggregate",
        "merge",
    ],
    ids=[
        "read-failure",
        "schema-failure",

        "aggregate-failure",
        "merge-failure",
    ],
)
def test_run_profit_aggregate_failures(run_profit_aggregate_setup, failure_target):
    """
    Validate failures are propagated from every stage in the pipeline.
    """
    s = run_profit_aggregate_setup

    if failure_target == "read":
        s.read.side_effect = Exception("Failed to read master orders")

    elif failure_target == "schema":
        s.enforce.side_effect = Exception("Failed to enforce schema")

    elif failure_target == "aggregate":
        s.build.side_effect = Exception("Profit aggregate pipeline failed ")

    elif failure_target == "merge":
        s.merge.side_effect = Exception("Failed to merge delta upsert")


    with pytest.raises(Exception):
        agg.run_profit_aggregate(s.spark)

    if failure_target == "read":
        s.read.assert_called_once_with(s.spark)
        s.enforce.assert_not_called()

        s.build.assert_not_called()
        s.merge.assert_not_called()
        s.optimize.assert_not_called()

    elif failure_target == "schema":
        s.read.assert_called_once_with(s.spark)
        s.enforce.assert_called_once_with(
            s.read_df,
            agg.MASTER_ORDERS_SCHEMA,
            "Master Orders Read",
        )

        s.build.assert_not_called()
        s.merge.assert_not_called()
        s.optimize.assert_not_called()


    elif failure_target == "aggregate":
        s.read.assert_called_once_with(s.spark)
        s.enforce.assert_called_once_with(
            s.read_df,
            agg.MASTER_ORDERS_SCHEMA,
            "Master Orders Read",
        )
        s.build.assert_called_once_with(s.enforced_df)
        s.merge.assert_not_called()
        s.optimize.assert_not_called()

    elif failure_target == "merge":
        s.read.assert_called_once_with(s.spark)
        s.enforce.assert_called_once_with(
            s.read_df,
            agg.MASTER_ORDERS_SCHEMA,
            "Master Orders Read",
        )
        s.build.assert_called_once_with(s.enforced_df)
        s.agg_df.repartition.assert_called_once_with("order_year")
        s.merge.assert_called_once_with(
            s.spark,
            s.repartitioned_df,
            table="profit_by_year_category_customer",
            base_path=agg.GOLD_DELTA_PATH,
            merge_keys=["order_year", "category", "sub_category", "customer_id"],
            partition_cols=["order_year"],
        )
        s.optimize.assert_not_called()