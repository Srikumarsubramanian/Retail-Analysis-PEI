import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

import retail_analysis.databricks.notebooks.create_enriched_customers_products as silver


@pytest.fixture()
def transform_products_setup(monkeypatch):
    df = MagicMock(name="df")

    # chainable behavior
    df.filter.return_value = df
    df.dropDuplicates.return_value = df
    df.withColumn.return_value = df

    # mock Spark functions (VERY IMPORTANT)
    fake_col = MagicMock(name="Column")

    monkeypatch.setattr(silver.F, "col", lambda x: fake_col)
    monkeypatch.setattr(silver.F, "trim", lambda x: fake_col)
    monkeypatch.setattr(silver.F, "initcap", lambda x: fake_col)
    monkeypatch.setattr(silver.F, "current_timestamp", lambda: fake_col)

    # mock when/otherwise chain
    fake_when = MagicMock(name="when")
    fake_when.otherwise.return_value = fake_col

    monkeypatch.setattr(silver.F, "when", lambda *args, **kwargs: fake_when)

    return SimpleNamespace(
        df=df,
        fake_col=fake_col,
    )
    
    

import pytest


@pytest.mark.transforms
@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        # Case 1: basic cleanup + negative price
        (
            [
                ("1", " electronics ", " phones ", " iphone ", -100.0),
                ("1", " electronics ", " phones ", " iphone ", -100.0),  # duplicate
                (None, "bad", "bad", "bad", 10.0),  # null id → drop
            ],
            [
                {
                    "product_id": "1",
                    "category": "Electronics",
                    "sub_category": "Phones",
                    "product_name": "iphone",
                    "price_per_product": None,
                }
            ],
        ),
        # Case 2: valid data
        (
            [
                ("2", " furniture ", " chairs ", " office chair ", 250.5),
            ],
            [
                {
                    "product_id": "2",
                    "category": "Furniture",
                    "sub_category": "Chairs",
                    "product_name": "office chair",
                    "price_per_product": 250.5,
                }
            ],
        ),
    ],
)
def test_transform_products_values(spark, input_data, expected_output):
    """
    Validate actual transformation results using real Spark DataFrame.

    Ensures:
    - null product_id dropped
    - duplicates removed
    - trimming + initcap applied
    - negative prices set to NULL
    """
    from retail_analysis.databricks.notebooks.create_enriched_customers_products import transform_products

    columns = [
        "product_id",
        "category",
        "sub_category",
        "product_name",
        "price_per_product",
    ]

    df = spark.createDataFrame(input_data, columns)

    result_df = transform_products(spark, df)

    result = result_df.select(
        "product_id",
        "category",
        "sub_category",
        "product_name",
        "price_per_product",
    ).collect()

    result_dicts = [row.asDict() for row in result]

    # order-safe comparison
    assert sorted(result_dicts, key=lambda x: x["product_id"]) == \
           sorted(expected_output, key=lambda x: x["product_id"])

@pytest.mark.transforms
def test_transform_products_failure_wrapped(transform_products_setup):
    """
    Ensure any internal failure is wrapped as TransformError.
    """
    s = transform_products_setup

    s.df.filter.side_effect = Exception("boom")

    with pytest.raises(silver.TransformError, match="Error in transform_products"):
        silver.transform_products(None, s.df)

@pytest.mark.transforms
def test_transform_products_withcolumn_failure(transform_products_setup):
    """
    Ensure transformation failures are wrapped.
    """
    s = transform_products_setup

    s.df.withColumn.side_effect = Exception("withColumn failed")

    with pytest.raises(silver.TransformError, match="Error in transform_products"):
        silver.transform_products(None, s.df)


