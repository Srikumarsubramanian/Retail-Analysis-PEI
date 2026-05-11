import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.databricks.utils.util import read_data
from src.databricks.utils.custom_exceptions import IngestionError

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])



@pytest.fixture
def test_files(spark, tmp_path):
    # Create valid  CSV
    csv_path = tmp_path / "valid.csv"
    df = spark.createDataFrame(
        [("Alice", 30), ("Bob", 40)],
        ["name", "age"]
    )
    df.write.mode("overwrite").option("header", "true").csv(str(csv_path))

    # Spark writes folder, not file → adjust path
    csv_dir = str(csv_path)

    # Create valid JSON
    json_path = tmp_path / "valid.json"
    df.write.mode("overwrite").json(str(json_path))


    
    
    return {
        "base_path": str(tmp_path),
        "csv": "valid.csv",
        "json": "valid.json",
        "corrupt_csv": "corrupt.csv"
    }

valid_cases = [
    pytest.param(
        "csv", "valid.csv", {"header": "true"}, "PERMISSIVE", schema,
        ["name", "age"], 2,
        id="valid csv schema"
    ),
    pytest.param(
        "json", "valid.json", {}, "PERMISSIVE", None,
        ["name", "age"], 2,
        id="valid json"
    ),
]

@pytest.mark.parametrize(
    "fmt, file_name, options, mode, schema, expected_cols, expected_count",
    valid_cases
)
def test_read_data_valid(spark, fmt,test_files, file_name, options, mode, schema,
                         expected_cols, expected_count):

    df = read_data(
        spark=spark,
        path=test_files["base_path"],
        file_name=file_name,
        format=fmt,
        schema=schema,
        mode=mode,
        **options
    )

    assert set(df.columns) == set(expected_cols)
    assert df.count() == expected_count

    if schema:
        assert df.schema == schema




# edge_cases = [
#     pytest.param(
#         "csv", "corrupt.csv", {"header": "true"}, "PERMISSIVE", None,
#         ["name", "age"], 1,
#         marks=pytest.mark.edge,
#         id="csv quarantine"
#     ),
#     pytest.param(
#         "csv", "corrupt.csv", {"header": "true"}, "FAILFAST", None,
#         None, None,
#         marks=pytest.mark.edge,
#         id="failfast mode"
#     ),
# ]

# @pytest.mark.parametrize(
#     "fmt, file_name, options, mode, schema, expected_cols, expected_count",
#     edge_cases
# )
# @pytest.mark.edge
# def test_read_data_edge(spark, fmt, test_files,file_name, options, mode, schema,
#                         expected_cols, expected_count):

#     if mode == "FAILFAST":
 
#         with pytest.raises(IngestionError):
#             read_data(
#                 spark=spark,
#                 path=test_files["base_path"],
#                 file_name=file_name,
#                 format=fmt,
#                 schema=schema,
#                 mode=mode,
#                 **options
#             )
#         return

#     df = read_data(
#         spark=spark,
#         path=test_files["base_path"],
#         file_name=file_name,
#         format=fmt,
#         schema=schema,
#         mode=mode,
#         **options
#     )

#     assert "_corrupt_record" not in df.columns
#     assert set(df.columns) == set(expected_cols)
#     assert df.count() == expected_count





failure_cases = [
    pytest.param(
        "csv", "missing.csv", {}, "PERMISSIVE", None,
        "missing file",
        marks=pytest.mark.failure,
        id="missing file"
    ),
    pytest.param(
        "invalid", "file.xxx", {}, "PERMISSIVE", None,
        "invalid format",
        marks=pytest.mark.failure,
        id="invalid format"
    ),
]

@pytest.mark.parametrize(
    "fmt, file_name, options, mode, schema, _",
    failure_cases
)
@pytest.mark.failure
def test_read_data_failure(spark, fmt, test_files, file_name, options, mode, schema, _):

    with pytest.raises(IngestionError):
        read_data(
            spark=spark,
            path=test_files["base_path"],
            file_name=file_name,
            format=fmt,
            schema=schema,
            mode=mode,
            **options
        )
