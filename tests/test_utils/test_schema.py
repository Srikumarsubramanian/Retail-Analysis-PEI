import pytest
from pyspark.sql.types import StructType
from retail_analysis.templates.schema import (
    PRODUCTS_SCHEMA,
    ORDERS_SCHEMA,
    CUSTOMER_SCHEMA,
    MASTER_ORDERS_SCHEMA,
    PROFIT_AGGREGATE_SCHEMA,
)



@pytest.fixture
def schemas():
    """Fixture that returns all the defined schemas."""
    return {
        "products": PRODUCTS_SCHEMA,
        "orders": ORDERS_SCHEMA,
        "customers": CUSTOMER_SCHEMA,
        "master_orders": MASTER_ORDERS_SCHEMA,
        "profit_aggregate": PROFIT_AGGREGATE_SCHEMA,
    }


@pytest.mark.schema
@pytest.mark.parametrize(
    "schema_name",
    [
        "products",
        "orders",
        "customers",
        "master_orders",
        "profit_aggregate",
    ],
)
def test_schema_types(schemas, schema_name):
    """Test if exported schemas are valid PySpark StructType."""
    assert isinstance(schemas[schema_name], StructType)
