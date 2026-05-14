import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from retail_analysis.databricks.utils.transforms import round_currency
from retail_analysis.databricks.utils.custom_exceptions import TransformError


def run_round(df, col="value"):
    return df.select(round_currency(F.col(col)).alias("r")).first()["r"]

@pytest.mark.unit
@pytest.mark.positive
@pytest.mark.parametrize(
    "value, expected",
    [
        (10.123, 10.12),
        (10.125, 10.13),
        (63.69, 63.69),
        (0.3, 0.30),
    ],
    ids=[
        "truncate",
        "round-up",
        "no-change",
        "pad-decimals",
    ],
)
def test_round_currency_positive(spark, value, expected):
    df = spark.createDataFrame([(value,)], ["value"])

    result = run_round(df)

    assert float(result) == pytest.approx(expected, rel=1e-3)


@pytest.mark.unit
@pytest.mark.edge
@pytest.mark.parametrize(
    "value, expected",
    [
        (-10.123, -10.12),
        (-10.125, -10.13),
        (None, None),
        (0.0, 0.00),
    ],
    ids=[
        "negative-truncate",
        "negative-round-up",
        "null",
        "zero",
    ],
)
def test_round_currency_edge(spark, value, expected):
    from pyspark.sql.types import StructType, StructField, DoubleType

    df = spark.createDataFrame(
        [(value,)],
        StructType([StructField("value", DoubleType(), True)])
    )

    result = run_round(df)

    if expected is None:
        assert result is None
    else:
        assert float(result) == pytest.approx(expected, rel=1e-3)


@pytest.mark.unit
@pytest.mark.negative
@pytest.mark.parametrize(
    "value, expected_error",
    [
        ("not_a_number", None),
        (float("nan"), None),
    ],
    ids=[
        "invalid-type",
        "not-a-number",
    ],
)
def test_round_currency_invalid(spark, value, expected_error):
    df = spark.createDataFrame([(value,)], ["value"])
    result = run_round(df)
    assert result is None



@pytest.mark.unit
@pytest.mark.exception
def test_round_currency_invalid_input():
    with pytest.raises(TypeError):
        round_currency("not_a_column")



