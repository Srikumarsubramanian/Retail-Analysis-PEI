import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock
from pyspark.sql import DataFrame
from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import TransformError
from retail_analysis.databricks.notebooks.bronze import create_raw_tables as ingestion


@pytest.fixture
def transform_setup(monkeypatch):
    """
    Common fixture to setup mocks and input/output DataFrames
    for _transform_bronze tests.
    """
    m = SimpleNamespace(
        normalise_columns=MagicMock(name="normalise_columns"),
        add_ingestion_metadata=MagicMock(name="add_ingestion_metadata"),
    )

    monkeypatch.setattr(ingestion, "normalise_columns", m.normalise_columns)
    monkeypatch.setattr(ingestion, "add_ingestion_metadata", m.add_ingestion_metadata)

    df_input = MagicMock(spec=DataFrame, name="df_input")
    df_norm = MagicMock(spec=DataFrame, name="df_norm")
    df_final = MagicMock(spec=DataFrame, name="df_final")

    return SimpleNamespace(
        mocks=m,
        df_input=df_input,
        df_norm=df_norm,
        df_final=df_final,
    )


@pytest.mark.ingestion
def test_transform_bronze_call_contract(transform_setup):
    """
    Verify that:
    - normalise_columns is called with input DataFrame
    - add_ingestion_metadata is called with transformed DataFrame
    """
    s = transform_setup

    s.mocks.normalise_columns.return_value = s.df_norm
    s.mocks.add_ingestion_metadata.return_value = s.df_final

    ingestion._transform_bronze(s.df_input)

    s.mocks.normalise_columns.assert_called_once_with(s.df_input)
    s.mocks.add_ingestion_metadata.assert_called_once_with(s.df_norm)

@pytest.mark.ingestion
def test_transform_bronze_execution_and_return(transform_setup):
    """
    Verify that:
    - functions are executed in correct order
    - data flows correctly between steps
    - final output is returned
    - return type is DataFrame
    """
    s = transform_setup

    s.mocks.normalise_columns.return_value = s.df_norm
    s.mocks.add_ingestion_metadata.return_value = s.df_final

    result = ingestion._transform_bronze(s.df_input)

    # ensure chaining correctness
    called_arg = s.mocks.add_ingestion_metadata.call_args.args[0]
    assert called_arg is s.df_norm

    # Return checks
    assert result is s.df_final
    assert isinstance(result, DataFrame)
 

@pytest.mark.ingestion
def test_transform_bronze_normalise_exception(transform_setup):
    """
    Ensure that if normalise_columns raises an exception,
    it propagates and metadata step is not called.
    """
    s = transform_setup

    s.mocks.normalise_columns.side_effect = TransformError("Failed to transform data in Bronze layer")

    with pytest.raises(TransformError, match="Failed to transform data in Bronze layer"):
        ingestion._transform_bronze(MagicMock(spec=DataFrame))

    s.mocks.add_ingestion_metadata.assert_not_called()


@pytest.mark.ingestion
def test_transform_bronze_metadata_exception(transform_setup):
    """
    Ensure that if add_ingestion_metadata raises an exception,
    it propagates and normalise_columns step is not called.
    """
    s = transform_setup

    s.mocks.add_ingestion_metadata.side_effect = TransformError("Failed to transform data in Bronze layer")

    with pytest.raises(TransformError, match="Failed to transform data in Bronze layer"):
        ingestion._transform_bronze(MagicMock(spec=DataFrame))

    s.mocks.normalise_columns.assert_called_once()