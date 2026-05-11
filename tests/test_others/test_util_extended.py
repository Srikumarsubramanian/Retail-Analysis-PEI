import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from retail_analysis.databricks.utils.util import load_and_validate_config, write_delta_table
from retail_analysis.databricks.utils.custom_exceptions import ConfigError, WriteError
import retail_analysis.databricks.utils.util as util


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────
@pytest.fixture()
def config_factory(tmp_path):
    """
    Create temporary YAML config files for load_and_validate_config tests.
    """
    def _create(filename: str, content: str):
        file_path = tmp_path / filename
        file_path.write_text(content, encoding="utf-8")
        return file_path

    return _create


@pytest.fixture()
def write_setup(monkeypatch):
    """
    Shared setup for write_delta_table tests.
    """
    

    spark = MagicMock(name="spark")
    df = MagicMock(name="df")
    writer = MagicMock(name="writer")
    log = MagicMock(name="log")

    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.options.return_value = writer
    writer.partitionBy.return_value = writer

    df.write = writer

    monkeypatch.setattr(util, "log", log)

    return SimpleNamespace(
        spark=spark,
        df=df,
        writer=writer,
        log=log,
    )


# ─────────────────────────────────────────────
# load_and_validate_config
# ─────────────────────────────────────────────
@pytest.mark.util
@pytest.mark.parametrize(
    "content, expected",
    [
        pytest.param(
            "a: 1",
            {"a": 1},
            id="single-key",
        ),
        pytest.param(
            "name: test\nvalue: 123",
            {"name": "test", "value": 123},
            id="multi-key",
        ),
        pytest.param(
            "nested:\n  enabled: true\n  count: 2",
            {"nested": {"enabled": True, "count": 2}},
            id="nested-dict",
        ),
    ],
)
def test_load_valid_configs(config_factory, content, expected):
    file_path = config_factory("config.yaml", content)

    result = load_and_validate_config(str(file_path))

    assert result == expected


@pytest.mark.util
@pytest.mark.parametrize(
    "filename, content, expected_message",
    [
        pytest.param(
            "missing.yaml",
            None,
            "Config file not found",
            id="file-not-found",
        ),
        pytest.param(
            "invalid.yaml",
            "key: [unclosed_list",
            "Invalid YAML",
            id="invalid-yaml",
        ),
        pytest.param(
            "empty.yaml",
            "",
            "Config is empty",
            id="empty-yaml",
        ),
        pytest.param(
            "list.yaml",
            "- item1\n- item2",
            "expected dict: found list",
            id="non-dict-yaml",
        ),
    ],
)
def test_load_and_validate_config_failures(config_factory, filename, content, expected_message):
    if content is None:
        file_path = config_factory("placeholder.yaml", "a: 1").parent / filename
    else:
        file_path = config_factory(filename, content)

    with pytest.raises(ConfigError) as exc:
        load_and_validate_config(str(file_path))

    assert expected_message in str(exc.value)


# ─────────────────────────────────────────────
# write_delta_table
# ─────────────────────────────────────────────
@pytest.mark.util
@pytest.mark.parametrize(
    "file_name, base_path, mode, partition_by, options",
    [
        pytest.param(
            "orders",
            "/tmp/delta",
            "error",
            None,
            {},
            id="no-partition-no-options",
        ),
        pytest.param(
            "orders",
            "/tmp/delta",
            "append",
            [],
            {},
            id="empty-partition-list",
        ),
        pytest.param(
            "customers",
            "/mnt/layer",
            "overwrite",
            ["country"],
            {"mergeSchema": "true"},
            id="single-partition-with-option",
        ),
        pytest.param(
            "events",
            "s3://bucket/layer",
            "append",
            ["event_date", "region"],
            {"compression": "zstd", "overwriteSchema": True},
            id="multi-partition-with-multiple-options",
        ),
    ],
)
def test_write_delta_table(write_setup, file_name, base_path, mode, partition_by, options):
    write_delta_table(
        spark=write_setup.spark,
        df=write_setup.df,
        file_name=file_name,
        base_path=base_path,
        mode=mode,
        partition_by=partition_by,
        **options,
    )

    target_path = f"{base_path}/{file_name}"

    write_setup.writer.format.assert_called_once_with("delta")
    write_setup.writer.mode.assert_called_once_with(mode)
    write_setup.writer.options.assert_called_once_with(**options)
    write_setup.writer.save.assert_called_once_with(target_path)

    if partition_by:
        write_setup.writer.partitionBy.assert_called_once_with(*partition_by)
    else:
        write_setup.writer.partitionBy.assert_not_called()

    write_setup.log.info.assert_called_once_with(
        f"Delta write of {file_name} completed to {target_path} with mode {mode}",
    )
    write_setup.log.exception.assert_not_called()
    assert write_setup.spark.mock_calls == []


@pytest.mark.util
@pytest.mark.parametrize(
    "failure_target",
    [
        "format",
        "mode",
        "options",
        "partitionBy",
        "save",
    ],
    ids=[
        "format-failure",
        "mode-failure",
        "options-failure",
        "partition-failure",
        "save-failure",
    ],
)
def test_write_delta_table_failures(write_setup, failure_target):
    err = WriteError("Delta upsert failed for table")

    if failure_target == "format":
        write_setup.writer.format.side_effect = err
    elif failure_target == "mode":
        write_setup.writer.mode.side_effect = err
    elif failure_target == "options":
        write_setup.writer.options.side_effect = err
    elif failure_target == "partitionBy":
        write_setup.writer.partitionBy.side_effect = err
    elif failure_target == "save":
        write_setup.writer.save.side_effect = err

    with pytest.raises(WriteError):
        write_delta_table(
            spark=write_setup.spark,
            df=write_setup.df,
            file_name="orders",
            base_path="/tmp/delta",
            mode="overwrite",
            partition_by=["country"],
        )

    write_setup.log.exception.assert_called()