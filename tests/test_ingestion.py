import pytest
from unittest.mock import patch, MagicMock

from src.databricks.notebooks.ingestion import (
    _read_bronze_source,
    _transform_bronze,
    _write_bronze,
    run_bronze,
)
from src.databricks.utils.custom_exceptions import ConfigError


# ─────────────────────────────────────────────
# _read_bronze_source
# ─────────────────────────────────────────────
@pytest.mark.unit
@pytest.mark.parametrize("file_name, expected_format, expected_kwargs", [
    ("test_data.csv", "csv", {"header": "true", "escape": '"'}),
    ("orders.json", "json", {"multiLine": "true", "dateFormat": "d/M/yyyy"}),
    ("products.xlsx", "xlsx", {}),
    ("data.parquet", "parquet", {}),
])
@patch('src.databricks.notebooks.ingestion.read_data')
def test_read_bronze_source(mock_read_data, spark, file_name, expected_format, expected_kwargs):
    """Test reading different file formats passes correct options to read_data."""
    source_path = "/tmp/bronze"

    result = _read_bronze_source(spark, source_path, file_name)

    mock_read_data.assert_called_once_with(
        spark,
        source_path,
        file_name,
        format=expected_format,
        **expected_kwargs,
    )
    assert result == mock_read_data.return_value


@pytest.mark.unit
@patch('src.databricks.notebooks.ingestion.read_data')
def test_read_bronze_source_unknown_extension(mock_read_data, spark):
    """Unknown file extensions pass through with no special options."""
    _read_bronze_source(spark, "/data", "file.avro")

    mock_read_data.assert_called_once_with(
        spark, "/data", "file.avro", format="avro",
    )


# ─────────────────────────────────────────────
# _transform_bronze
# ─────────────────────────────────────────────
@pytest.mark.unit
@patch('src.databricks.notebooks.ingestion.normalise_columns')
@patch('src.databricks.notebooks.ingestion.add_ingestion_metadata')
def test_transform_bronze(mock_add_metadata, mock_normalise):
    """Transformation calls normalise_columns then add_ingestion_metadata."""
    mock_df = MagicMock()
    mock_normalise.return_value = MagicMock()
    mock_add_metadata.return_value = MagicMock()

    result = _transform_bronze(mock_df)

    mock_normalise.assert_called_once_with(mock_df)
    mock_add_metadata.assert_called_once_with(mock_normalise.return_value)
    assert result == mock_add_metadata.return_value


# ─────────────────────────────────────────────
# _write_bronze
# ─────────────────────────────────────────────
@pytest.mark.unit
@patch('src.databricks.notebooks.ingestion.write_delta_table')
def test_write_bronze_default_mode(mock_write, spark):
    """Default mode is 'error', strips file extension for table name."""
    mock_df = MagicMock()

    _write_bronze(spark, mock_df, "products_2026.csv", "/tmp/delta/bronze")

    mock_write.assert_called_once_with(
        spark,
        mock_df,
        file_name="products_2026",
        base_path="/tmp/delta/bronze",
        mode="error",
        partition_by=None,
    )


@pytest.mark.unit
@patch('src.databricks.notebooks.ingestion.write_delta_table')
def test_write_bronze_overwrite_mode(mock_write, spark):
    """Overwrite mode adds overwriteSchema option."""
    mock_df = MagicMock()

    _write_bronze(spark, mock_df, "orders.json", "/tmp/bronze", mode="overwrite")

    mock_write.assert_called_once_with(
        spark,
        mock_df,
        file_name="orders",
        base_path="/tmp/bronze",
        mode="overwrite",
        partition_by=None,
        overwriteSchema="true",
    )


@pytest.mark.unit
@patch('src.databricks.notebooks.ingestion.write_delta_table')
def test_write_bronze_with_partition_cols(mock_write, spark):
    """Partition columns are forwarded to write_delta_table."""
    mock_df = MagicMock()

    _write_bronze(spark, mock_df, "data.csv", "/tmp/bronze", partition_cols=["year"])

    mock_write.assert_called_once_with(
        spark,
        mock_df,
        file_name="data",
        base_path="/tmp/bronze",
        mode="error",
        partition_by=["year"],
    )


# ─────────────────────────────────────────────
# run_bronze
# ─────────────────────────────────────────────
@pytest.mark.unit
@pytest.mark.edge
@patch('src.databricks.notebooks.ingestion.load_and_validate_config')
def test_run_bronze_missing_files_key(mock_load_config, spark):
    """ConfigError when 'files' key is missing."""
    mock_load_config.return_value = {"not_files": []}

    with pytest.raises(ConfigError, match="The 'files' key is missing or empty"):
        run_bronze(spark, config_path="dummy.yml")


@pytest.mark.unit
@pytest.mark.edge
@patch('src.databricks.notebooks.ingestion.load_and_validate_config')
def test_run_bronze_files_key_none(mock_load_config, spark):
    """ConfigError when 'files' is None."""
    mock_load_config.return_value = {"files": None}

    with pytest.raises(ConfigError, match="The 'files' key is missing or empty"):
        run_bronze(spark, config_path="dummy.yml")


@pytest.mark.unit
@pytest.mark.edge
@patch('src.databricks.notebooks.ingestion.load_and_validate_config')
def test_run_bronze_files_empty_dict(mock_load_config, spark):
    """ConfigError when 'files' is an empty dict."""
    mock_load_config.return_value = {"files": {}}

    with pytest.raises(ConfigError, match="The 'files' key is missing or empty"):
        run_bronze(spark, config_path="dummy.yml")


@pytest.mark.unit
@pytest.mark.edge
@patch('src.databricks.notebooks.ingestion.ensure_files_exist')
@patch('src.databricks.notebooks.ingestion.load_and_validate_config')
def test_run_bronze_missing_files_on_disk(mock_load_config, mock_ensure_files, spark):
    """FileNotFoundError when source files are absent."""
    mock_load_config.return_value = {"files": {"file1.csv": {}}}
    mock_ensure_files.return_value = ([], ["file1.csv"])

    with pytest.raises(FileNotFoundError, match="Missing 1 file"):
        run_bronze(spark, config_path="dummy.yml")


@pytest.mark.unit
@patch('src.databricks.notebooks.ingestion._write_bronze')
@patch('src.databricks.notebooks.ingestion._transform_bronze')
@patch('src.databricks.notebooks.ingestion._read_bronze_source')
@patch('src.databricks.notebooks.ingestion.ensure_files_exist')
@patch('src.databricks.notebooks.ingestion.load_and_validate_config')
def test_run_bronze_success(mock_load_config, mock_ensure_files, mock_read, mock_transform, mock_write, spark):
    """Happy path: all files ingested, results returned."""
    mock_load_config.return_value = {"files": {"f1.csv": {}, "f2.json": {}}}
    mock_ensure_files.return_value = (["f1.csv", "f2.json"], [])

    mock_read.return_value = MagicMock()
    mock_transform.return_value = MagicMock()

    results = run_bronze(spark, config_path="dummy.yml")

    assert len(results) == 2
    assert "f1.csv" in results
    assert "f2.json" in results
    assert mock_read.call_count == 2
    assert mock_transform.call_count == 2
    assert mock_write.call_count == 2


@pytest.mark.unit
@pytest.mark.edge
@patch('src.databricks.notebooks.ingestion._read_bronze_source')
@patch('src.databricks.notebooks.ingestion.ensure_files_exist')
@patch('src.databricks.notebooks.ingestion.load_and_validate_config')
def test_run_bronze_read_failure_wraps_runtime_error(mock_load_config, mock_ensure_files, mock_read, spark):
    """RuntimeError wraps any exception during ingestion with the file name."""
    mock_load_config.return_value = {"files": {"bad.csv": {}}}
    mock_ensure_files.return_value = (["bad.csv"], [])
    mock_read.side_effect = Exception("Simulated read failure")

    with pytest.raises(RuntimeError, match="Pipeline failed on file: bad.csv"):
        run_bronze(spark, config_path="dummy.yml")


@pytest.mark.unit
@pytest.mark.edge
@patch('src.databricks.notebooks.ingestion._write_bronze')
@patch('src.databricks.notebooks.ingestion._transform_bronze')
@patch('src.databricks.notebooks.ingestion._read_bronze_source')
@patch('src.databricks.notebooks.ingestion.ensure_files_exist')
@patch('src.databricks.notebooks.ingestion.load_and_validate_config')
def test_run_bronze_write_failure_on_second_file(
    mock_load_config, mock_ensure_files, mock_read, mock_transform, mock_write, spark
):
    """If the second file fails to write, RuntimeError is raised with that file name."""
    mock_load_config.return_value = {"files": {"ok.csv": {}, "bad.json": {}}}
    mock_ensure_files.return_value = (["ok.csv", "bad.json"], [])

    mock_read.return_value = MagicMock()
    mock_transform.return_value = MagicMock()
    # First call succeeds, second raises
    mock_write.side_effect = [None, Exception("disk full")]

    with pytest.raises(RuntimeError, match="Pipeline failed on file: bad.json"):
        run_bronze(spark, config_path="dummy.yml")
