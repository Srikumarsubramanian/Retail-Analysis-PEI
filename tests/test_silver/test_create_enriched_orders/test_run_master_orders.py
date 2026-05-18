import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

import retail_analysis.databricks.notebooks.silver.create_enriched_orders as silver
from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import PipelineError , ReadError , TransformError , WriteError

@pytest.fixture()
def run_master_orders_setup(monkeypatch):
    spark = MagicMock(name="spark")

    orders = MagicMock(name="orders_df")
    customers = MagicMock(name="customers_df")
    products = MagicMock(name="products_df")

    master_df = MagicMock(name="master_orders_df")

    # chainable repartition
    master_df.repartition.return_value = master_df

    # mocks
    read_sources = MagicMock(return_value=(orders, customers, products))
    build_master_orders = MagicMock(return_value=master_df)
    merge = MagicMock()

    monkeypatch.setattr(silver, "_read_sources", read_sources)
    monkeypatch.setattr(silver, "build_master_orders", build_master_orders)
    monkeypatch.setattr(silver, "merge_delta_upsert", merge)

    return SimpleNamespace(
        spark=spark,
        orders=orders,
        customers=customers,
        products=products,
        master_df=master_df,
        read_sources=read_sources,
        build_master_orders=build_master_orders,
        merge=merge,
    )


@pytest.mark.silver
def test_run_master_orders_success(run_master_orders_setup):
    """
    Validate orchestration flow for building master orders.

    Ensures:
    - sources are read
    - build_master_orders is called
    - repartition is applied
    - merge is executed
    - result is returned
    """
    s = run_master_orders_setup

    result = silver.run_master_orders(s.spark)

    assert result == s.master_df

    s.read_sources.assert_called_once_with(s.spark)

    s.build_master_orders.assert_called_once_with(
        s.spark,
        s.orders,
        s.customers,
        s.products,
    )

    s.master_df.repartition.assert_called_once_with("order_year")

    s.merge.assert_called_once_with(
        s.spark,
        s.master_df,
        "master_orders",
        silver.SILVER_DELTA_PATH,
        merge_keys=["order_id", "product_id"],
        partition_cols=["order_year"],
    )


@pytest.mark.silver
@pytest.mark.parametrize(
    "failure_target",
    [
        "read",
        "build",
        "repartition",
        "merge",
    ],
)
def test_run_master_orders_failure(run_master_orders_setup, failure_target):
    """
    Ensure failures at different stages are propagated correctly.
    """
    s = run_master_orders_setup

    if failure_target == "read":
        s.read_sources.side_effect = ReadError("Error reading bronze orders and silver dimension tables")

    elif failure_target == "build":
        s.build_master_orders.side_effect = TransformError("Error building master orders table")

    elif failure_target == "repartition":
        s.master_df.repartition.side_effect = TransformError("Error repartitioning master orders table")

    elif failure_target == "merge":
        s.merge.side_effect = WriteError("Error merging master orders table")

    with pytest.raises(PipelineError, match="Error building master orders table"):
        silver.run_master_orders(s.spark)