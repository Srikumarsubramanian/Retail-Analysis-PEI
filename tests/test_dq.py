import pytest
from pyspark.sql import functions as F
from src.databricks.utils.dq import clean_customer_name, clean_email
from pyspark.sql.types import StructType, StructField, StringType

@pytest.mark.parametrize(
    "input_value, expected",
    [
        # --- basic cleaning ---
        pytest.param("john doe", "John Doe", id="basic_lowercase"),
        pytest.param("  john doe  ", "John Doe", id="trim_spaces"),
        pytest.param("JOHN DOE", "John Doe", id="uppercase"),
        pytest.param("jOhN dOe", "John Doe", id="mixed_case"),

        # --- numbers ---
        pytest.param("john123", "John", id="numbers_suffix"),
        pytest.param("123john", "John", id="numbers_prefix"),
        pytest.param("jo123hn", "John", id="numbers_middle"),
        pytest.param("123", None, id="only_numbers"),

        # --- special characters ---
        pytest.param("john@doe", "John Doe", id="special_at"),
        pytest.param("john#doe!", "John Doe", id="special_hash_exclaim"),
        pytest.param("john_doe", "John Doe", id="underscore"),
        pytest.param("john.doe", "John Doe", id="dot"),

        # --- allowed characters ---
        pytest.param("d'arcy-smith", "D'arcy-smith", id="apostrophe_hyphen"),

        # --- edge cases ---
        pytest.param(None, None, id="null"),
        pytest.param("", None, id="empty_string"),
        pytest.param("   ", None, id="only_spaces"),
        pytest.param("!!!", None, id="only_special_chars"),
        pytest.param("123!!!", None, id="numbers_and_specials"),
    ],
)
@pytest.mark.dq
def test_clean_customer_name(spark, input_value, expected):
    schema = StructType([
    StructField("name", StringType(), True)
])
    df = spark.createDataFrame([(input_value,)], schema)
    result = df.select(
        clean_customer_name(F.col("name")).alias("cleaned")
    ).collect()[0]["cleaned"]

    assert result == expected



#######tests for email


import pytest
from pyspark.sql import functions as F

@pytest.mark.parametrize(
    "case_id, input_value, expected",
    [
        # --- valid emails ---
        pytest.param("simple_valid", "test@example.com", "test@example.com", id="simple_valid"),
        pytest.param("uppercase", "TEST@EXAMPLE.COM", "test@example.com", id="uppercase"),
        pytest.param("trim_spaces", "  test@example.com  ", "test@example.com", id="trim_spaces"),
        pytest.param("subdomain", "user@mail.example.com", "user@mail.example.com", id="subdomain"),

        # --- special characters allowed ---
        pytest.param("plus_alias", "user+tag@example.com", "user+tag@example.com", id="plus_alias"),
        pytest.param("dot_local", "first.last@example.com", "first.last@example.com", id="dot_local"),
        pytest.param("percent_local", "user%test@example.com", "user%test@example.com", id="percent_local"),

        # --- invalid formats ---
        pytest.param("missing_at", "testexample.com", None, id="missing_at"''),
        pytest.param("missing_domain", "test@", None, id="missing_domain"''),
        pytest.param("missing_local", "@example.com", None, id="missing_local"''),

        # --- domain issues (important bug catchers) ---
        pytest.param("leading_dot_domain", "test@.example.com", None, id="leading_dot_domain"''),
        pytest.param("double_dot_domain", "test@example..com", None, id="double_dot_domain"''),
        pytest.param("no_tld", "test@example", None, id="no_tld"''),

        # --- invalid characters ---
        pytest.param("space_inside", "test @example.com", None, id="space_inside"''),
        pytest.param("invalid_char", "test!email@example.com", None, id="invalid_char"''),

        # --- edge cases ---
        pytest.param("null", None, None, id="null"''),
        pytest.param("empty", "", None, id="empty"''),
        pytest.param("only_spaces", "   ", None, id="only_spaces"''),
    ],
)
@pytest.mark.dq
def test_clean_email(spark, case_id, input_value, expected):
    schema = StructType([
    StructField("email", StringType(), True)
])
    df = spark.createDataFrame([(input_value,)], schema)

    result = df.select(
        clean_email(F.col("email")).alias("cleaned")
    ).collect()[0]["cleaned"]

    assert result == expected, f"Failed case: {case_id}"