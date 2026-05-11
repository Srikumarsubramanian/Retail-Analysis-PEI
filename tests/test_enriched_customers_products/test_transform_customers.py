import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

import retail_analysis.databricks.notebooks.enrich_customers_products as silver
from pyspark.sql.column import Column
from pyspark.sql import DataFrame , functions as F
from pyspark.sql import Row
from retail_analysis.databricks.utils.schema import CUSTOMER_SCHEMA

@pytest.fixture()
def transform_customers_setup(monkeypatch):
    df = MagicMock(name="df")

    # chainable DataFrame behavior
    df.filter.return_value = df
    df.dropDuplicates.return_value = df
    df.withColumn.return_value = df
    df.select.return_value = df

    # mock cleaning functions
    clean_customer_name = MagicMock(name="clean_customer_name")
    clean_customer_name.side_effect = lambda col: col

    clean_email = MagicMock(name="clean_email")
    clean_email.side_effect = lambda col: col

    clean_phone = MagicMock(name="clean_phone")
    clean_phone.side_effect = lambda col: col

    monkeypatch.setattr(silver, "clean_customer_name", clean_customer_name)
    monkeypatch.setattr(silver, "clean_email", clean_email)
    monkeypatch.setattr(silver, "clean_phone", clean_phone)

    return SimpleNamespace(
        df=df,
        clean_customer_name=clean_customer_name,
        clean_email=clean_email,
        clean_phone=clean_phone,
    )


@pytest.mark.silver
def test_transform_customers_failure_wrapped(transform_customers_setup):
    """
    Ensure any internal failure is wrapped as TransformError.
    """
    s = transform_customers_setup

    s.df.columns = ["customer_id"]

    s.df.filter.side_effect = Exception("Something went wrong while transforming customers")

    with pytest.raises(silver.TransformError, match="Error in transform_customers"):
        silver.transform_customers(None, s.df)

@pytest.mark.silver
def test_transform_customers_cleaning_failure(transform_customers_setup):
    """
    Ensure cleaning function failures are wrapped.
    """
    s = transform_customers_setup

    s.df.columns = ["customer_id", "customer_name"]

    s.clean_customer_name.side_effect = Exception("Something went wrong while cleaning the customer name")

    with pytest.raises(silver.TransformError, match="Error in transform_customers"):
        silver.transform_customers(None, s.df)


@pytest.mark.silver
def test_transform_customers_clean_functions_receive_column(spark, transform_customers_setup):
    s = transform_customers_setup
    df = spark.createDataFrame([
        Row(
            customer_id=1,
            customer_name="john",
            email="a@test.com",
            phone="123",
            segment="consumer",
            city="ny",
            address="123 main st",
            state="ny",
            region="east",
            country="us",
            postal_code="12345",
            _ingested_at=None
        )
    ],schema=CUSTOMER_SCHEMA)

    silver.transform_customers(spark, df)

    name_arg = s.clean_customer_name.call_args[0][0]
    email_arg = s.clean_email.call_args[0][0]
    phone_arg = s.clean_phone.call_args[0][0]

    assert isinstance(name_arg, Column)
    assert isinstance(email_arg, Column)
    assert isinstance(phone_arg, Column)