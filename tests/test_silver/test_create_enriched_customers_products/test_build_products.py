import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock


from retail_analysis.databricks.notebooks.silver import create_enriched_customers_products as silver
from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import DataQualityError, WriteError ,ReadError , TransformError,PipelineError


@pytest.fixture()
def build_products_setup(monkeypatch):
    """
    Provides isolated setup for build_products.

    Mocks:
    - read_data
    - enforce_schema
    - transform_products
    """
    spark = MagicMock(name="spark")

    read_data = MagicMock(name="read_data")
    enforce_schema = MagicMock(name="enforce_schema")
    transform_products = MagicMock(name="transform_products")

    monkeypatch.setattr(silver, "read_data", read_data)
    monkeypatch.setattr(silver, "enforce_schema", enforce_schema)
    monkeypatch.setattr(silver, "transform_products", transform_products)

    return SimpleNamespace(
        spark=spark,
        read_data=read_data,
        enforce_schema=enforce_schema,
        transform_products=transform_products,
    )





@pytest.mark.silver
def test_build_products_success(build_products_setup):
    """
    Validate successful execution of build_products.

    Ensures:
    - read_data is called correctly
    - enforce_schema is applied
    - transform_products is called
    - function returns None (current behavior)
    """
    s = build_products_setup

    raw_df = MagicMock(name="raw_df")
    enforced_df = MagicMock(name="enforced_df")
    final_df = MagicMock(name="final_df")

    s.read_data.return_value = raw_df
    s.enforce_schema.return_value = enforced_df
    s.transform_products.return_value = final_df

    result = silver.build_products(s.spark)

    assert result is final_df

    s.read_data.assert_called_once_with(
        s.spark,
        silver.BRONZE_DELTA_PATH,
        "products",
        "delta",
    )

    s.enforce_schema.assert_called_once_with(
        raw_df,
        silver.PRODUCTS_SCHEMA,
        "Products Bronze",
    )

    s.transform_products.assert_called_once_with(
        s.spark,
        enforced_df,
    )


@pytest.mark.silver
def test_build_products_read_failure(build_products_setup):
    """
    Validate that read_data failure raises RuntimeError.

    Ensures:
    - exception is wrapped
    - downstream steps are not executed
    """
    s = build_products_setup

    s.read_data.side_effect = Exception("read failed")

    with pytest.raises(PipelineError, match="Error in build_products"):
        silver.build_products(s.spark)

    s.enforce_schema.assert_not_called()
    s.transform_products.assert_not_called()


@pytest.mark.silver
def test_build_products_schema_failure(build_products_setup):
    """
    Validate that enforce_schema failure raises RuntimeError.

    Ensures:
    - transform step is not executed
    """
    s = build_products_setup

    s.read_data.return_value = MagicMock()
    s.enforce_schema.side_effect = Exception("schema failed")

    with pytest.raises(PipelineError, match="Error in build_products"):
        silver.build_products(s.spark)

    s.transform_products.assert_not_called()


@pytest.mark.silver
def test_build_products_transform_failure(build_products_setup):
    """
    Validate that transform_products failure raises RuntimeError.

    Ensures:
    - upstream steps executed before failure
    """
    s = build_products_setup

    s.read_data.return_value = MagicMock()
    s.enforce_schema.return_value = MagicMock()
    s.transform_products.side_effect = Exception("transform failed")

    with pytest.raises(PipelineError, match="Error in build_products"):
        silver.build_products(s.spark)