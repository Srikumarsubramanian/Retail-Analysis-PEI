import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from retail_analysis.databricks.notebooks.orchestrators import run_master_pipeline as pipe


@pytest.mark.master_pipeline
def test_run_stage_success(monkeypatch):
    """
    Test successful execution of a  stage.
    """

    fn = MagicMock(return_value="result")
    log = MagicMock()

    monkeypatch.setattr(pipe, "log", log)

    result = pipe._run_stage("test_stage", fn, 1, 2)

    assert result == "result"
    fn.assert_called_once_with(1, 2)

    log.info.assert_any_call("Starting: test_stage")

@pytest.mark.master_pipeline
def test_run_stage_failure(monkeypatch):
    """
    Test failure handling within a stage.
    """
    fn = MagicMock(side_effect=Exception("Simulated failure"))
    log = MagicMock()

    monkeypatch.setattr(pipe, "log", log)

    with pytest.raises(pipe.PipelineError, match="Pipeline failed at: test_stage"):
        pipe._run_stage("test_stage", fn)

    log.exception.assert_called()
