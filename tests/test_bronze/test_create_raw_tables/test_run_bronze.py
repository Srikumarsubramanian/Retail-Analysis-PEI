import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from types import SimpleNamespace

from retail_analysis.databricks.notebooks.bronze import create_raw_tables as ingestion
from retail_analysis.databricks.notebooks.bronze.create_raw_tables import run_bronze

from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import ConfigError, TransformError , WriteError, ReadError, PipelineError 

#-----------------------
# fixtures
#-----------------------

@pytest.fixture(scope = 'function')
def mocks(monkeypatch):
    m = SimpleNamespace(
        _read_bronze_source=MagicMock(name="_read_bronze_source"),
        _transform_bronze=MagicMock(name="_transform_bronze"),
        _write_bronze=MagicMock(name="_write_bronze"),
        load_and_validate_config=MagicMock(name="load_and_validate_config"),
        ensure_files_exist=MagicMock(name="ensure_files_exist"),
        spark = MagicMock(name = 'spark')
    )

    monkeypatch.setattr(ingestion, "_read_bronze_source", m._read_bronze_source)
    monkeypatch.setattr(ingestion, "_transform_bronze", m._transform_bronze)
    monkeypatch.setattr(ingestion, "_write_bronze", m._write_bronze)
    monkeypatch.setattr(ingestion, "load_and_validate_config", m.load_and_validate_config)
    monkeypatch.setattr(ingestion, "ensure_files_exist", m.ensure_files_exist)

    return m




#==========================Call contracts===================#

@pytest.mark.ingestion
@pytest.mark.unit
@pytest.mark.call_contracts
def test_run_bronze_call_contracts(mocks):
    config_path = "test_config.yml"
    files = ["customers.csv", "orders.csv"]
    spark = mocks.spark
    # - Arrange
    mocks.load_and_validate_config.return_value = {"files": files}
    mocks.ensure_files_exist.return_value = (files, [])

    # Create one DF per file
    df_mocks = [
        MagicMock(spec=DataFrame, name=f"df_{f}") for f in files
    ]

    mocks._read_bronze_source.side_effect = df_mocks
    mocks._transform_bronze.side_effect = lambda df: df 

    # - Run
    result = ingestion.run_bronze(spark, config_path)

    # - Assert: config calls
    mocks.load_and_validate_config.assert_called_once_with(path=config_path)
    mocks.ensure_files_exist.assert_called_once_with(
        ingestion.SOURCE_BASE_PATH, files
    )

    # - Assert: call counts
    assert mocks._read_bronze_source.call_count == len(files)
    assert mocks._transform_bronze.call_count == len(files)
    assert mocks._write_bronze.call_count == len(files)

    # - Assert: per-file calls
    for i, file_name in enumerate(files):
        mocks._read_bronze_source.assert_any_call(
            spark,
            ingestion.SOURCE_BASE_PATH,
            file_name,
        )

        mocks._transform_bronze.assert_any_call(
            df_mocks[i],
        )

        mocks._write_bronze.assert_any_call(
            spark,
            df_mocks[i],
            file_name,
            ingestion.BRONZE_DELTA_PATH,
            mode="overwrite",
        )

    # - Assert: transform input = read output
    for call, df in zip(mocks._transform_bronze.call_args_list, df_mocks):
        assert call.args[0] == df

    # - Assert: return contract
    assert set(result.keys()) == set(files)
    for i, file_name in enumerate(files):
        assert result[file_name] == df_mocks[i]


#========================== Exceptions ===================#

#------------------------------------------------
# Config + file validation errors
# -------------------------------------------------
# -------------------------------------------------
# Config errors
# -------------------------------------------------
@pytest.mark.ingestion
@pytest.mark.exceptions
def test_run_bronze_config_errors_missing_files(mocks):
    spark = mocks.spark
    # ConfigError (missing files key)
    mocks.load_and_validate_config.return_value = {}

    with pytest.raises(ingestion.ConfigError):
        run_bronze(spark)

@pytest.mark.ingestion
@pytest.mark.exceptions
def test_run_bronze_config_error_empty_files( mocks):
    spark = mocks.spark
    # ConfigError (empty files)
    mocks.load_and_validate_config.return_value = {"files": []}

    with pytest.raises(ingestion.ConfigError):
        run_bronze(spark)


# -------------------------------------------------
# File validation error
# -------------------------------------------------
@pytest.mark.ingestion
@pytest.mark.exceptions
def test_run_bronze_file_validation_error( mocks):
    spark = mocks.spark
    mocks.load_and_validate_config.return_value = {"files": ["sales.csv"]}
    mocks.ensure_files_exist.return_value = ([], ["sales.csv"])

    with pytest.raises(ingestion.ConfigError):
        run_bronze(spark)

    

# -------------------------------------------------
# Pipeline stage failures
# -------------------------------------------------

@pytest.mark.ingestion
@pytest.mark.exceptions
def test_run_bronze_read_failure( mocks):
    spark = mocks.spark
    mocks.load_and_validate_config.return_value = {"files": ["sales.csv"]}
    mocks.ensure_files_exist.return_value = (["sales.csv"], [])

    mocks._read_bronze_source.side_effect = RuntimeError("read failed")

    with pytest.raises(ingestion.PipelineError) as exc:
        run_bronze(spark)



@pytest.mark.ingestion
@pytest.mark.exceptions
def test_run_bronze_transform_failure( mocks):
    spark = mocks.spark
    mocks.load_and_validate_config.return_value = {"files": ["sales.csv"]}
    mocks.ensure_files_exist.return_value = (["sales.csv"], [])

    # Read must succeed
    mocks._read_bronze_source.return_value = MagicMock(name="df")

    # Transform fails
    mocks._transform_bronze.side_effect = ValueError("transform failed")

    with pytest.raises(ingestion.PipelineError) as exc:
        run_bronze(spark)


@pytest.mark.ingestion
@pytest.mark.exceptions
def test_run_bronze_write_failure( mocks):
    spark = mocks.spark
    mocks.load_and_validate_config.return_value = {"files": ["sales.csv"]}
    mocks.ensure_files_exist.return_value = (["sales.csv"], [])

    # Read + transform succeed
    mocks._read_bronze_source.return_value = MagicMock(name="df")
    mocks._transform_bronze.return_value = MagicMock(name="df")

    # Write fails
    mocks._write_bronze.side_effect = IOError("write failed")

    with pytest.raises(ingestion.PipelineError) as exc:
        run_bronze(spark)

