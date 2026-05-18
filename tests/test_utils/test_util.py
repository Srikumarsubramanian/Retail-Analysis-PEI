import pytest
from types import SimpleNamespace

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from retail_analysis.databricks.utils.util_funcs.util import read_data, enforce_schema
from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import ReadError, DataQualityError, WriteError
from retail_analysis.templates.constants import QUARANTINE_PATH


# ─────────────────────────────────────────────
# Shared schemas
# ─────────────────────────────────────────────
@pytest.fixture()
def base_schema():
    return StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
        ]
    )


@pytest.fixture()
def read_schema():
    return StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )


# ─────────────────────────────────────────────
# read_data fixtures
# ─────────────────────────────────────────────
@pytest.fixture()
def test_files(spark, tmp_path):
    """
    Create small real files for read_data tests.
    Returns a namespace with stable file names and base path.
    """
    df = spark.createDataFrame(
        [("Alice", 30), ("Bob", 40)],
        ["name", "age"],
    )

    csv_name = "valid.csv"
    json_name = "valid.json"

    csv_dir = tmp_path / "valid_csv"
    json_dir = tmp_path / "valid_json"

    df.write.mode("overwrite").option("header", "true").csv(str(csv_dir))
    df.write.mode("overwrite").json(str(json_dir))

    return SimpleNamespace(
        base_path=str(tmp_path),
        csv=csv_name,
        json=json_name,
        csv_dir=str(csv_dir),
        json_dir=str(json_dir),
    )


# ─────────────────────────────────────────────
# read_data tests
# ─────────────────────────────────────────────
@pytest.mark.util
@pytest.mark.parametrize(
    "fmt,file_name,options,mode,schema,expected_cols,expected_count",
    [
        pytest.param(
            "csv",
            "valid_csv",
            {"header": "true"},
            "PERMISSIVE",
            None,
            ["name", "age"],
            2,
            id="valid-csv",
        ),
        pytest.param(
            "json",
            "valid_json",
            {},
            "PERMISSIVE",
            None,
            ["name", "age"],
            2,
            id="valid-json",
        ),
    ],
)
def test_read_data_valid(
    spark,
    test_files,
    fmt,
    file_name,
    options,
    mode,
    schema,
    expected_cols,
    expected_count,
):
    df = read_data(
        spark=spark,
        path=test_files.base_path,
        file_name=file_name,
        format=fmt,
        schema=schema,
        mode=mode,
        **options,
    )

    assert set(df.columns) == set(expected_cols)
    assert df.count() == expected_count


@pytest.mark.util
@pytest.mark.parametrize(
    "fmt,file_name,options,mode,schema",
    [
        pytest.param(
            "csv",
            "missing.csv",
            {},
            "PERMISSIVE",
            None,
            id="missing-file",
        ),
        pytest.param(
            "invalid",
            "file.xxx",
            {},
            "PERMISSIVE",
            None,
            id="invalid-format",
        ),
    ],
)
def test_read_data_failure(spark, test_files, fmt, file_name, options, mode, schema):
    with pytest.raises(ReadError):
        read_data(
            spark=spark,
            path=test_files.base_path,
            file_name=file_name,
            format=fmt,
            schema=schema,
            mode=mode,
            **options,
        )


# ─────────────────────────────────────────────
# enforce_schema helpers
# ─────────────────────────────────────────────
class DummyWriter:
    def __init__(self):
        self.saved = False
        self.path = None
        self.df = None

    def format(self, *_):
        return self

    def mode(self, *_):
        return self

    def option(self, *_):
        return self

    def options(self, *_):
        return self

    def partitionBy(self, *_):
        return self

    def csv(self, path):
        self.saved = True
        self.path = path
        return self

    def json(self, path):
        self.saved = True
        self.path = path
        return self

    def parquet(self, path):
        self.saved = True
        self.path = path
        return self

    def save(self, path):
        self.saved = True
        self.path = path
        return self


@pytest.fixture()
def dummy_writer(monkeypatch):
    writer = DummyWriter()

    def capture_write(df):
        writer.df = df
        return writer

    monkeypatch.setattr(
        "pyspark.sql.dataframe.DataFrame.write",
        property(capture_write),
    )

    return writer


# ─────────────────────────────────────────────
# enforce_schema tests
# ─────────────────────────────────────────────
@pytest.mark.util
def test_missing_required_column_raises(spark, base_schema):
    df = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(DataQualityError):
        enforce_schema(df, base_schema, "test_label")


@pytest.mark.util
def test_extra_columns_are_ignored(spark, base_schema):
    df = spark.createDataFrame([(1, "Alice", "junk")], ["id", "name", "junk"])

    result = enforce_schema(df, base_schema, "test_label")

    assert set(result.columns) == {"id", "name"}


@pytest.mark.util
@pytest.mark.parametrize(
    "input_data,expected_good,expected_bad",
    [
        pytest.param(
            [(1, "Alice"), (None, "Bob")],
            [(1, "Alice")],
            [(None, "Bob")],
            id="mixed-good-and-bad-rows",
        ),
        pytest.param(
            [(None, "X"), (None, "Y")],
            [],
            [(None, "X"), (None, "Y")],
            id="all-rows-quarantined",
        ),
        pytest.param(
            [(1, "A"), (2, "B")],
            [(1, "A"), (2, "B")],
            [],
            id="all-rows-valid",
        ),
        pytest.param(
            [],
            [],
            [],
            id="empty-df",
        ),
    ],
)
def test_quarantine_behavior(
    spark,
    base_schema,
    input_data,
    expected_good,
    expected_bad,
    dummy_writer,
):
    test_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )

    df = spark.createDataFrame(input_data, test_schema)

    result = enforce_schema(df, base_schema, "Test Label")

    actual_good = [tuple(row) for row in result.collect()]
    assert actual_good == expected_good

    if expected_bad:
        actual_bad = [tuple(row[:2]) for row in dummy_writer.df.collect()]
        assert actual_bad == expected_bad


@pytest.mark.util
@pytest.mark.parametrize(
    "input_data,expected_good",
    [
        pytest.param(
            [("1",), ("2",)],
            [(1,), (2,)],
            id="valid-string-to-int-cast",
        ),
        pytest.param(
            [("1",), ("bad",)],
            [(1,)],
            id="mixed-valid-and-invalid-cast",
        ),
        pytest.param(
            [("x",), ("y",)],
            [],
            id="all-invalid-casts",
        ),
        pytest.param(
            [(1,), (2,)],
            [(1,), (2,)],
            id="correct-type",
        ),
    ],
)
def test_type_casting(spark, input_data, expected_good, dummy_writer):
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
        ]
    )

    df = spark.createDataFrame(input_data, ["id"])

    result = enforce_schema(df, schema, "test_label")

    actual = [tuple(row) for row in result.collect()]
    assert actual == expected_good