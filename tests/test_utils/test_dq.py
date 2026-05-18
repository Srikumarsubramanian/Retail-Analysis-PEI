import pytest
from types import SimpleNamespace

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from retail_analysis.databricks.utils.util_funcs.dq import clean_customer_name, clean_email, clean_phone


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────
@pytest.fixture()
def scalar_df_factory(spark):
    """
    Factory for creating a one-row DataFrame with a single string column.
    """
    def _create(column_name: str, value):
        schema = StructType([StructField(column_name, StringType(), True)])
        return spark.createDataFrame([(value,)], schema)

    return _create


@pytest.fixture(scope="session")
def phone_schema():
    return StructType([StructField("phone", StringType(), True)])


# ─────────────────────────────────────────────
# clean_customer_name
# ─────────────────────────────────────────────
@pytest.mark.dq
@pytest.mark.parametrize(
    "input_value, expected",
    [
        pytest.param("john doe", "John Doe", id="basic-lowercase"),
        pytest.param("  john doe  ", "John Doe", id="trim-spaces"),
        pytest.param("JOHN DOE", "John Doe", id="uppercase"),
        pytest.param("jOhN dOe", "John Doe", id="mixed-case"),
        pytest.param("john123", "John", id="numbers-suffix"),
        pytest.param("123john", "John", id="numbers-prefix"),
        pytest.param("jo123hn", "John", id="numbers-middle"),
        pytest.param(None, None, id="null"),
        pytest.param("", None, id="empty-string"),
        pytest.param("   ", None, id="only-spaces"),
        pytest.param("123", None, id="only-numbers"),
    ],
)
def test_clean_customer_name(spark, scalar_df_factory, input_value, expected):
    """
    Validate customer name cleaning across normal and edge inputs.
    """
    df = scalar_df_factory("name", input_value)

    result = df.select(
        clean_customer_name(F.col("name")).alias("cleaned")
    ).first()["cleaned"]

    assert result == expected


# ─────────────────────────────────────────────
# clean_email
# ─────────────────────────────────────────────
@pytest.mark.dq
@pytest.mark.parametrize(
    "input_value, expected",
    [
        pytest.param("test@example.com", "test@example.com", id="simple-valid"),
        pytest.param("TEST@EXAMPLE.COM", "test@example.com", id="uppercase"),
        pytest.param("  test@example.com  ", "test@example.com", id="trim-spaces"),
        pytest.param("user@mail.example.com", "user@mail.example.com", id="subdomain"),
        pytest.param("user+tag@example.com", "user+tag@example.com", id="plus-alias"),
        pytest.param("first.last@example.com", "first.last@example.com", id="dot-local"),
        pytest.param("user%test@example.com", "user%test@example.com", id="percent-local"),
        pytest.param("testexample.com", None, id="missing-at"),
        pytest.param("test@", None, id="missing-domain"),
        pytest.param("@example.com", None, id="missing-local"),
        pytest.param("test@.example.com", None, id="leading-dot-domain"),
        pytest.param("test@example..com", None, id="double-dot-domain"),
        pytest.param("test@example", None, id="no-tld"),
        pytest.param("test @example.com", None, id="space-inside"),
        pytest.param("test!email@example.com", None, id="invalid-char"),
        pytest.param(None, None, id="null"),
        pytest.param("", None, id="empty"),
        pytest.param("   ", None, id="only-spaces"),
    ],
)
def test_clean_email(spark, scalar_df_factory, input_value, expected):
    """
    Validate email cleaning across valid, invalid, and edge inputs.
    """
    df = scalar_df_factory("email", input_value)

    result = df.select(
        clean_email(F.col("email")).alias("cleaned")
    ).first()["cleaned"]

    assert result == expected


# ─────────────────────────────────────────────
# clean_phone helpers
# ─────────────────────────────────────────────
@pytest.mark.dq
def run_clean_phone(spark, schema, value):
    df = spark.createDataFrame([(value,)], schema)
    return df.select(clean_phone(F.col("phone")).alias("result")).first()["result"]


# ─────────────────────────────────────────────
# clean_phone - valid path
# ─────────────────────────────────────────────
@pytest.mark.dq
@pytest.mark.parametrize(
    "raw, expected",
    [
        pytest.param("2025551234", "2025551234", id="bare-us-number"),
        pytest.param("202-555-1234", "2025551234", id="hyphen-separated"),
        pytest.param("202.555.1234", "2025551234", id="dot-separated"),
        pytest.param("202 555 1234", "2025551234", id="space-separated"),
        pytest.param("(202) 555-1234", "2025551234", id="parentheses-and-hyphen"),
        pytest.param("(202) 555 1234", "2025551234", id="parentheses-and-spaces"),
        pytest.param("+12025551234", "+12025551234", id="compact-e164"),
        pytest.param("+1 202 555 1234", "+12025551234", id="plus-spaces"),
        pytest.param("+1 (202) 555-1234", "+12025551234", id="plus-full-format"),
        pytest.param("  2025551234  ", "2025551234", id="trimmed"),
        pytest.param("44 20 7946 0958", "442079460958", id="uk-local"),
        pytest.param("+442079460958", "+442079460958", id="uk-e164"),
        pytest.param("+33 1 42 68 53 00", "+33142685300", id="fr-e164"),
    ],
)
def test_valid_phone(spark, phone_schema, raw, expected):
    """
    Validate valid phone formats are normalized correctly.
    """
    assert run_clean_phone(spark, phone_schema, raw) == expected


# ─────────────────────────────────────────────
# clean_phone - returns NULL
# ─────────────────────────────────────────────
@pytest.mark.dq
@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(None, id="null"),
        pytest.param("", id="empty"),
        pytest.param("   ", id="whitespace"),
        pytest.param("123456", id="too-short"),
        pytest.param("12345", id="much-too-short"),
        pytest.param("1234567890123456", id="too-long"),
        pytest.param("abcdefg", id="letters-only"),
        pytest.param("N/A", id="placeholder"),
        pytest.param("--------", id="separators-only"),
        pytest.param("()", id="brackets-only"),
        pytest.param("+++", id="pluses-only"),
        pytest.param("+", id="lone-plus"),
    ],
)
def test_returns_null(spark, phone_schema, raw):
    """
    Validate invalid phone inputs return NULL.
    """
    assert run_clean_phone(spark, phone_schema, raw) is None


# ─────────────────────────────────────────────
# clean_phone - extension stripping
# ─────────────────────────────────────────────
@pytest.mark.dq
@pytest.mark.parametrize(
    "raw, expected",
    [
        pytest.param("202-555-1234 ext 567", "2025551234", id="ext-space"),
        pytest.param("202-555-1234 ext. 567", "2025551234", id="ext-dot"),
        pytest.param("202-555-1234 EXT 567", "2025551234", id="ext-upper"),
        pytest.param("202-555-1234 Ext.567", "2025551234", id="ext-mixed"),
        pytest.param("202-555-1234 x567", "2025551234", id="x-short"),
        pytest.param("202-555-1234 X567", "2025551234", id="x-upper"),
        pytest.param("+12025551234 ext 99", "+12025551234", id="e164-extension"),
        pytest.param("(202) 555-1234x1000", "2025551234", id="x-no-space"),
        pytest.param("text123x456", None, id="garbage"),
    ],
)
def test_extension_stripped(spark, phone_schema, raw, expected):
    """
    Validate extensions are removed and the base number is preserved.
    """
    assert run_clean_phone(spark, phone_schema, raw) == expected


# ─────────────────────────────────────────────
# clean_phone - plus prefix behavior
# ─────────────────────────────────────────────
@pytest.mark.dq
@pytest.mark.parametrize(
    "raw, expected",
    [
        pytest.param("+12025551234", "+12025551234", id="compact-e164"),
        pytest.param("+1-202-555-1234", "+12025551234", id="e164-dashes"),
        pytest.param("+1 (202) 555-1234", "+12025551234", id="e164-full"),
        pytest.param("+447911123456", "+447911123456", id="uk-e164"),
        pytest.param("12025551234", "12025551234", id="no-plus"),
        pytest.param("2025551234", "2025551234", id="domestic"),
        pytest.param("002025551234", "002025551234", id="idd-prefix"),
    ],
)
def test_plus_prefix_preservation(spark, phone_schema, raw, expected):
    """
    Validate plus-prefix behavior is preserved correctly.
    """
    assert run_clean_phone(spark, phone_schema, raw) == expected


# ─────────────────────────────────────────────
# clean_phone - boundaries
# ─────────────────────────────────────────────
@pytest.mark.dq
@pytest.mark.parametrize(
    "raw, expected",
    [
        pytest.param("1234567", "1234567", id="min-valid"),
        pytest.param("12345678", "12345678", id="just-above-min"),
        pytest.param("12345678901234", "12345678901234", id="just-below-max"),
        pytest.param("123456789012345", "123456789012345", id="max-valid"),
        pytest.param("123456", None, id="below-min"),
        pytest.param("1234567890123456", None, id="above-max"),
    ],
)
def test_digit_count_boundaries(spark, phone_schema, raw, expected):
    """
    Validate digit-count boundaries for phone normalization.
    """
    assert run_clean_phone(spark, phone_schema, raw) == expected