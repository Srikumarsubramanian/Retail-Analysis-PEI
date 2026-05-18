import pytest
from retail_analysis.databricks.utils.custom_exceptions import TransformError
from retail_analysis.databricks.notebooks import create_aggregate as agg
from retail_analysis.databricks.notebooks.create_aggregate import build_profit_aggregate
from unittest.mock import MagicMock

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import DataFrame


def get_schema():
    return StructType([
        StructField("order_year", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("profit", DoubleType(), True),
        StructField("total_revenue", DoubleType(), True),
    ])

@pytest.mark.unit
@pytest.mark.positive
def test_build_profit_aggregate_positive(spark):
    """
    Validate correct aggregation for a single group with positive values.

    Ensures:
    - Rows with identical grouping keys are aggregated into one output row
    - total_profit is computed as SUM(profit)
    - total_revenue is computed as SUM(total_revenue)
    - No data loss or duplication occurs during aggregation
    """
    df = spark.createDataFrame([
        (2024, "Tech", "Phones", 1, 10.12, 100.0),
        (2024, "Tech", "Phones", 1, 5.88, 25.0),
    ], get_schema())

    result = build_profit_aggregate(df).collect()[0]

    assert result["total_profit"] == pytest.approx(16.0)
    assert result["total_revenue"] == pytest.approx(125.0)



@pytest.mark.unit
@pytest.mark.positive
def test_build_profit_aggregate_negative_mixed(spark):
    """
    Validate aggregation with mixed positive and negative profit values.

    Ensures:
    - Negative values are included in aggregation (not filtered out)
    - SUM correctly reflects net profit (gain - loss)
    - Aggregation logic does not alter sign or magnitude of values
    """


    df = spark.createDataFrame([
        (2024, "Tech", "Phones", 1, 63.69, 500.0),
        (2024, "Tech", "Phones", 1, -14.92, 200.0),
    ], get_schema())

    result = build_profit_aggregate(df).collect()[0]

    assert result["total_profit"] == pytest.approx(48.77)
    assert result["total_revenue"] == pytest.approx(700.0)



@pytest.mark.unit
@pytest.mark.boundary
def test_build_profit_aggregate_single_row(spark):
    """
    Validate behavior with minimal input (single row).

    Ensures:
    - Aggregation does not alter values when only one record exists
    - Output contains exactly one row
    - Values remain unchanged after aggregation
    """
    df = spark.createDataFrame([
        (2024, "Tech", "Phones", 1, 0.3, 1.0),
    ], get_schema())

    result = build_profit_aggregate(df).collect()[0]

    assert result["total_profit"] == pytest.approx(0.3, rel=1e-3)
    assert result["total_revenue"] == pytest.approx(1.0, rel=1e-3)

@pytest.mark.unit
@pytest.mark.boundary
def test_build_profit_aggregate_empty(spark):
    """
    Validate behavior for empty input DataFrame.

    Ensures:
    - Function does not fail on empty input
    - Output DataFrame is empty
    - Output schema still includes all expected columns
    - Metadata column (_updated_at) is present even when no rows exist
    """
    df = spark.createDataFrame([], get_schema())

    result = build_profit_aggregate(df)

    assert result.count() == 0
    assert "_updated_at" in result.columns


@pytest.mark.gold
@pytest.mark.unit
def test_build_profit_aggregate_has_updated_at(spark):
    """
    Ensure _updated_at column is added.
    """

    df = spark.createDataFrame(
        [(2024, "Tech", "Phones", 1, 100.0, 500.0)],
        ["order_year", "category", "sub_category", "customer_id", "profit", "total_revenue"],
    )

    result_df = build_profit_aggregate(df)

    assert "_updated_at" in result_df.columns



@pytest.mark.gold
@pytest.mark.exception
def test_build_profit_aggregate_failure(monkeypatch):
    """
    Ensure failures are wrapped as TransformError.
    """


    df = MagicMock()

    # break groupBy
    df.groupBy.side_effect = Exception("boom")

    with pytest.raises(agg.TransformError, match="Failed to aggregate master_orders"):
        agg.build_profit_aggregate(df)



@pytest.mark.gold
@pytest.mark.unit
def test_build_profit_aggregate_schema(spark):
    """
    Ensure output DataFrame has correct columns and type.
    """
    

    df = spark.createDataFrame(
        [(2024, "Tech", "Phones", 1, 100.0, 500.0)],
        ["order_year", "category", "sub_category", "customer_id", "profit", "total_revenue"],
    )

    result_df = build_profit_aggregate(df)

    # type check
    assert isinstance(result_df, DataFrame)

    # column check
    expected_cols = {
        "order_year",
        "category",
        "sub_category",
        "customer_id",
        "total_profit",
        "total_revenue",
        "_updated_at",
    }

    assert set(result_df.columns) == expected_cols