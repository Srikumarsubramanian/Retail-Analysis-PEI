import pytest
from unittest.mock import MagicMock
from pyspark.sql import DataFrame

from retail_analysis.databricks.notebooks import ingestion
from retail_analysis.databricks.utils.custom_exceptions import ReadError


@pytest.mark.ingestion
@pytest.mark.parametrize(
    "file_name, expected_format, expected_options",
    [
        pytest.param(
            "customers.csv",
            "csv",
            {"header": "true", "escape": '"'},
            id="csv-file",
        ),
        pytest.param(
            "orders.json",
            "json",
            {"multiLine": "true", "dateFormat": "d/M/yyyy"},
            id="json-file",
        ),
        pytest.param(
            "inventory.parquet",
            "parquet",
            {},
            id="default-file-type",
        ),
        pytest.param(
            "UPPER.CSV",
            "csv",
            {"header": "true", "escape": '"'},
            id="uppercase-extension",
        ),
    ],
)
def test_read_bronze_source_call_contract_and_return_value(
    monkeypatch,
    spark,
    file_name,
    expected_format,
    expected_options,
):
    """
    Verify that _read_bronze_source sends the correct format-specific options
    to read_data and returns the exact DataFrame produced by read_data.
    """
    mock_read_data = MagicMock(name="read_data")
    df_returned = spark.createDataFrame([(1, "alice")], ["id", "name"])
    mock_read_data.return_value = df_returned

    monkeypatch.setattr(ingestion, "read_data", mock_read_data)

    source_path = "/tmp/source"

    result = ingestion._read_bronze_source(spark, source_path, file_name)

    mock_read_data.assert_called_once_with(
        spark,
        source_path,
        file_name,
        format=expected_format,
        **expected_options,
    )
    assert result == df_returned
    assert isinstance(result, DataFrame)


@pytest.mark.ingestion
def test_read_bronze_source_propagates_exceptions(monkeypatch, spark):
    """
    Verify that _read_bronze_source does not swallow exceptions raised by read_data.
    """
    mock_read_data = MagicMock(name="read_data")
    mock_read_data.side_effect = ReadError("read failed")

    monkeypatch.setattr(ingestion, "read_data", mock_read_data)

    with pytest.raises(ReadError, match="Reading raw bronze file customers.csv failed"):
        ingestion._read_bronze_source(
            spark,
            "/tmp/source",
            "customers.csv",
        )


@pytest.mark.ingestion
@pytest.mark.parametrize(
    "file_name, expected_format, expected_options",
    [
        pytest.param("sample.CSV", "csv", {"header": "true", "escape": '"'}, id="csv-upper"),
        pytest.param("sample.Json", "json", {"multiLine": "true", "dateFormat": "d/M/yyyy"}, id="json-mixed-case"),
        pytest.param("sample.txt", "txt", {}, id="unknown-extension"),
    ],
)
def test_read_bronze_source_handles_file_extension_normalisation(
    monkeypatch,
    spark,
    file_name,
    expected_format,
    expected_options,
):
    """
    Verify that file extensions are normalised to lowercase and that unknown
    extensions are passed through with no extra options.
    """
    mock_read_data = MagicMock(name="read_data")
    df_returned = spark.createDataFrame([(1,)], ["value"])
    mock_read_data.return_value = df_returned

    monkeypatch.setattr(ingestion, "read_data", mock_read_data)

    result = ingestion._read_bronze_source(
        spark,
        "/tmp/source",
        file_name,
    )

    mock_read_data.assert_called_once_with(
        spark,
        "/tmp/source",
        file_name,
        format=expected_format,
        **expected_options,
    )
    assert result is df_returned
