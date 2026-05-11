import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from types import SimpleNamespace

import retail_analysis.databricks.notebooks.ingestion as ingestion

from retail_analysis.databricks.notebooks.ingestion import (
    _read_bronze_source,
    _transform_bronze,
    _write_bronze,
    run_bronze,
)
from retail_analysis.databricks.utils.custom_exceptions import ConfigError, TransformError , WriteError, ReadError, PipelineError

#─────────────────────────────────────────────
# fixtures
#─────────────────────────────────────────────

@pytest.fixture
def mocks(monkeypatch):
    m = SimpleNamespace(
        _read_bronze_source=MagicMock(name="_read_bronze_source"),
        _transform_bronze=MagicMock(name="_transform_bronze"),
        _write_bronze=MagicMock(name="_write_bronze"),
        load_and_validate_config=MagicMock(name="load_and_validate_config"),
        ensure_files_exist=MagicMock(name="ensure_files_exist"),
    )

    monkeypatch.setattr(ingestion, "_read_bronze_source", m._read_bronze_source)
    monkeypatch.setattr(ingestion, "_transform_bronze", m._transform_bronze)
    monkeypatch.setattr(ingestion, "_write_bronze", m._write_bronze)
    monkeypatch.setattr(ingestion, "load_and_validate_config", m.load_and_validate_config)
    monkeypatch.setattr(ingestion, "ensure_files_exist", m.ensure_files_exist)

    return m



@pytest.mark.ingestion
def test_run_bronze_call_contracts(mocks, spark):
    config_path = "test_config.yml"
    files = ["customers.csv", "orders.csv"]

    # ── Arrange
    mocks.load_and_validate_config.return_value = {"files": files}
    mocks.ensure_files_exist.return_value = (files, [])

    # Create one DF per file (\\\)
    df_mocks = [
        MagicMock(spec=DataFrame, name=f"df_{f}") for f in files
    ]

    mocks._read_bronze_source.side_effect = df_mocks
    mocks._transform_bronze.side_effect = lambda df: df 

    # ── Act
    result = ingestion.run_bronze(spark, config_path)

    # ── Assert: config calls
    mocks.load_and_validate_config.assert_called_once_with(path=config_path)
    mocks.ensure_files_exist.assert_called_once_with(
        ingestion.SOURCE_BASE_PATH, files
    )

    # ── Assert: call counts
    assert mocks._read_bronze_source.call_count == len(files)
    assert mocks._transform_bronze.call_count == len(files)
    assert mocks._write_bronze.call_count == len(files)

    # ── Assert: per-file calls
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

    # ── Assert: transform input = read output
    for call, df in zip(mocks._transform_bronze.call_args_list, df_mocks):
        assert call.args[0] == df

    # ── Assert: return contract
    assert set(result.keys()) == set(files)
    for i, file_name in enumerate(files):
        assert result[file_name] == df_mocks[i]




