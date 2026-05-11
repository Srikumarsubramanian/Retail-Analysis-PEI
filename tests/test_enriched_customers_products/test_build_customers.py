import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from retail_analysis.databricks.notebooks import enrich_customers_products as silver
from retail_analysis.databricks.utils.custom_exceptions import DataQualityError, WriteError ,ReadError , TransformError,PipelineError


# ─────────────────────────────────────────────
# Fixture
# ─────────────────────────────────────────────
@pytest.fixture()
def build_customers_setup(monkeypatch):
    """
    Provides a fully isolated test setup for build_customers.

    Mocks:
    - read_data
    - enforce_schema
    - transform_customers

    Returns
    -------
    SimpleNamespace
        Contains spark and all mocked dependencies.
    """
    spark = MagicMock(name="spark")

    read_data = MagicMock(name="read_data")
    enforce_schema = MagicMock(name="enforce_schema")
    transform_customers = MagicMock(name="transform_customers")

    monkeypatch.setattr(silver, "read_data", read_data)
    monkeypatch.setattr(silver, "enforce_schema", enforce_schema)
    monkeypatch.setattr(silver, "transform_customers", transform_customers)

    return SimpleNamespace(
        spark=spark,
        read_data=read_data,
        enforce_schema=enforce_schema,
        transform_customers=transform_customers,
    )


# ─────────────────────────────────────────────
# Success case
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_build_customers_success(build_customers_setup):
    """
    Validate successful execution of build_customers.

    Ensures:
    - read_data is called with correct arguments
    - enforce_schema is applied on raw dataframe
    - transform_customers is called with enforced dataframe
    - final transformed dataframe is returned
    """
    s = build_customers_setup

    raw_df = MagicMock(name="raw_df")
    enforced_df = MagicMock(name="enforced_df")
    final_df = MagicMock(name="final_df")

    s.read_data.return_value = raw_df
    s.enforce_schema.return_value = enforced_df
    s.transform_customers.return_value = final_df

    result = silver.build_customers(s.spark)

    assert result is final_df

    s.read_data.assert_called_once_with(
        s.spark,
        silver.BRONZE_DELTA_PATH,
        "customer",
        "delta",
    )

    s.enforce_schema.assert_called_once_with(
        raw_df,
        silver.CUSTOMER_SCHEMA,
        "Customers Bronze",
    )

    s.transform_customers.assert_called_once_with(
        s.spark,
        enforced_df,
    )


# ─────────────────────────────────────────────
# Failure: read_data
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_build_customers_read_failure(build_customers_setup):
    """
    Validate that failure in read_data raises RuntimeError.

    Ensures:
    - exception is wrapped as Pipeline Error
    - downstream steps are not executed
    """
    s = build_customers_setup

    s.read_data.side_effect = Exception("read failed")

    with pytest.raises(PipelineError, match="Error in build_customers"):
        silver.build_customers(s.spark)

    s.enforce_schema.assert_not_called()
    s.transform_customers.assert_not_called()


# ─────────────────────────────────────────────
# Failure: enforce_schema
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_build_customers_schema_failure(build_customers_setup):
    """
    Validate that failure in enforce_schema raises RuntimeError.

    Ensures:
    - exception is wrapped as RuntimeError
    - transform step is not executed
    """
    s = build_customers_setup

    s.read_data.return_value = MagicMock(name="raw_df")
    s.enforce_schema.side_effect = Exception("schema failed")

    with pytest.raises(PipelineError, match="Error in build_customers"):
        silver.build_customers(s.spark)

    s.transform_customers.assert_not_called()


# ─────────────────────────────────────────────
# Failure: transform_customers
# ─────────────────────────────────────────────
@pytest.mark.silver
def test_build_customers_transform_failure(build_customers_setup):
    """
    Validate that failure in transform_customers raises RuntimeError.

    Ensures:
    - exception is wrapped as RuntimeError
    - upstream steps are executed before failure
    """
    s = build_customers_setup

    s.read_data.return_value = MagicMock(name="raw_df")
    s.enforce_schema.return_value = MagicMock(name="enforced_df")
    s.transform_customers.side_effect = Exception("transform failed")

    with pytest.raises(PipelineError, match="Error in build_customers"):
        silver.build_customers(s.spark)