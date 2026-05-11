import pytest
from retail_analysis.databricks.utils.custom_exceptions import TransformError
from retail_analysis.databricks.notebooks import aggregate as agg

@pytest.mark.gold
@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        # Case 1: single group
        (
            [
                (2024, "Tech", "Phones", 1, 100.0, 500.0),
            ],
            [
                {
                    "order_year": 2024,
                    "category": "Tech",
                    "sub_category": "Phones",
                    "customer_id": 1,
                    "total_profit": 100.0,
                    "total_revenue": 500.0,
                }
            ],
        ),

        # Case 2: aggregation across rows
        (
            [
                (2024, "Tech", "Phones", 1, 100.0, 500.0),
                (2024, "Tech", "Phones", 1, 50.0, 200.0),
            ],
            [
                {
                    "order_year": 2024,
                    "category": "Tech",
                    "sub_category": "Phones",
                    "customer_id": 1,
                    "total_profit": 150.0,
                    "total_revenue": 700.0,
                }
            ],
        ),

        # Case 3: multiple groups
        (
            [
                (2024, "Tech", "Phones", 1, 100.0, 500.0),
                (2024, "Furniture", "Chairs", 2, 200.0, 800.0),
            ],
            [
                {
                    "order_year": 2024,
                    "category": "Tech",
                    "sub_category": "Phones",
                    "customer_id": 1,
                    "total_profit": 100.0,
                    "total_revenue": 500.0,
                },
                {
                    "order_year": 2024,
                    "category": "Furniture",
                    "sub_category": "Chairs",
                    "customer_id": 2,
                    "total_profit": 200.0,
                    "total_revenue": 800.0,
                },
            ],
        ),
    ],
)
def test_build_profit_aggregate_values(spark, input_data, expected_output):
    """
    Validate aggregation logic using real Spark DataFrame.

    Ensures:
    - correct grouping
    - correct sum aggregation
    - multiple groups handled
    """
    from retail_analysis.databricks.notebooks.aggregate import build_profit_aggregate

    columns = [
        "order_year",
        "category",
        "sub_category",
        "customer_id",
        "profit",
        "total_revenue",
    ]

    df = spark.createDataFrame(input_data, columns)

    result_df = build_profit_aggregate(df)

    result = result_df.select(
        "order_year",
        "category",
        "sub_category",
        "customer_id",
        "total_profit",
        "total_revenue",
    ).collect()

    result_dicts = [row.asDict() for row in result]

    assert sorted(result_dicts, key=lambda x: (x["customer_id"], x["category"])) == \
           sorted(expected_output, key=lambda x: (x["customer_id"], x["category"]))


@pytest.mark.gold
def test_build_profit_aggregate_has_updated_at(spark):
    """
    Ensure _updated_at column is added.
    """
    from retail_analysis.databricks.notebooks.aggregate import build_profit_aggregate

    df = spark.createDataFrame(
        [(2024, "Tech", "Phones", 1, 100.0, 500.0)],
        ["order_year", "category", "sub_category", "customer_id", "profit", "total_revenue"],
    )

    result_df = build_profit_aggregate(df)

    assert "_updated_at" in result_df.columns



@pytest.mark.gold
def test_build_profit_aggregate_failure(monkeypatch):
    """
    Ensure failures are wrapped as TransformError.
    """
    from unittest.mock import MagicMock
    import retail_analysis.databricks.notebooks.aggregate as agg

    df = MagicMock()

    # break groupBy
    df.groupBy.side_effect = Exception("boom")

    with pytest.raises(agg.TransformError, match="Failed to aggregate master_orders"):
        agg.build_profit_aggregate(df)



@pytest.mark.gold
def test_build_profit_aggregate_schema(spark):
    """
    Ensure output DataFrame has correct columns and type.
    """
    from retail_analysis.databricks.notebooks.aggregate import build_profit_aggregate
    from pyspark.sql import DataFrame

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