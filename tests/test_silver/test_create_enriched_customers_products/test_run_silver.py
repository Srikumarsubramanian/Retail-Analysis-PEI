import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from retail_analysis.databricks.notebooks import create_enriched_customers_products as silver

from retail_analysis.databricks.utils.custom_exceptions import DataQualityError, WriteError ,ReadError , TransformError,PipelineError


# ─────────────────────────────────────────────
# Fixture
# ─────────────────────────────────────────────
@pytest.fixture()
def silver_setup(monkeypatch):
    spark = MagicMock(name="spark")

    build_products = MagicMock(name="build_products")
    build_customers = MagicMock(name="build_customers")
    merge = MagicMock(name="merge_delta_upsert")

    monkeypatch.setattr(silver, "build_products", build_products)
    monkeypatch.setattr(silver, "build_customers", build_customers)
    monkeypatch.setattr(silver, "merge_delta_upsert", merge)

    return SimpleNamespace(
        spark=spark,
        build_products=build_products,
        build_customers=build_customers,
        merge=merge,
    )


# ─────────────────────────────────────────────
# Success Case
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_run_silver_success(silver_setup):
    s = silver_setup

    products_df = MagicMock(name="products_df")
    customers_df = MagicMock(name="customers_df")

    s.build_products.return_value = products_df
    s.build_customers.return_value = customers_df

    result = silver.run_silver(s.spark)

    assert result == {
        "products": products_df,
        "customers": customers_df,
    }

    assert s.merge.call_count == 2


# ─────────────────────────────────────────────
# Failure: products stage
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_run_silver_products_failure(silver_setup):
    s = silver_setup

    s.build_products.side_effect = Exception("boom")

    with pytest.raises(PipelineError, match="Pipeline failed at stage: products"):
        silver.run_silver(s.spark)

    s.build_customers.assert_not_called()
    s.merge.assert_not_called()


# ─────────────────────────────────────────────
# Failure: customers stage
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_run_silver_customers_failure(silver_setup):
    s = silver_setup

    s.build_products.return_value = MagicMock(name="products_df")
    s.build_customers.side_effect = Exception("boom")

    with pytest.raises(PipelineError, match="Pipeline failed at stage: customers"):
        silver.run_silver(s.spark)

    # merge should have been called only once (for products)
    assert s.merge.call_count == 1


# ─────────────────────────────────────────────
# Failure: merge during products
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_run_silver_merge_failure_products(silver_setup):
    s = silver_setup

    s.build_products.return_value = MagicMock(name="products_df")
    s.merge.side_effect = Exception("merge failed")

    with pytest.raises(PipelineError, match="Pipeline failed at stage: products"):
        silver.run_silver(s.spark)

    s.build_customers.assert_not_called()


# ─────────────────────────────────────────────
# Failure: merge during customers
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_run_silver_merge_failure_customers(silver_setup):
    s = silver_setup

    s.build_products.return_value = MagicMock(name="products_df")
    s.build_customers.return_value = MagicMock(name="customers_df")

    # First merge succeeds, second fails
    s.merge.side_effect = [None, Exception("merge failed")]

    with pytest.raises(PipelineError, match="Pipeline failed at stage: customers"):
        silver.run_silver(s.spark)

    assert s.merge.call_count == 2