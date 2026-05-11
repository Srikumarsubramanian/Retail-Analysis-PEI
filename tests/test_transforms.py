import pytest
from pyspark.sql import SparkSession
from src.databricks.utils.transforms import normalise_columns


@pytest.fixture
def df_factory(spark):
    def _create(columns):
        data = [tuple(range(len(columns)))]
        return spark.createDataFrame(data, columns)
    return _create


import pytest

valid_cases = [
    pytest.param(
        ["Name", "Age"],
        ["name", "age"],
        id="basic lowercase"
    ),
    pytest.param(
        [" First Name ", "Last Name"],
        ["first_name", "last_name"],
        id="trim + spaces"
    ),
    pytest.param(
        ["A-B", "C&D"],
        ["a_b", "c_d"],
        id="special chars replaced"
    ),
    pytest.param(
        ["Col__1", "Col@@2"],
        ["col_1", "col_2"],
        id="collapse underscores"
    ),
    pytest.param(
        ["Mixed CASE Column"],
        ["mixed_case_column"],
        id="mixed casing"
    ),
]

edge_cases = [
    pytest.param(
        ["!!!", "@@@"],
        ["col", "col_1"],
        id="only special characters",
        marks=pytest.mark.edge
    ),
    pytest.param(
        ["A B", "A@B", "a_b"],
        ["a_b", "a_b_1", "a_b_2"],
        id="duplicate collision",
        marks=pytest.mark.edge
    ),
    pytest.param(
        ["   ", ""],
        ["col", "col_1"],
        id="empty and whitespace columns",
        marks=pytest.mark.edge
    ),
    pytest.param(
        None,
        [],
        id="empty dataframe no columns",
        marks=pytest.mark.edge
    ),
]
@pytest.mark.parametrize(
    "input_cols, expected_cols",
    valid_cases
)
def test_normalise_columns_valid(df_factory, spark, input_cols, expected_cols):
    df = df_factory(input_cols)
    result = normalise_columns(df)

    assert result.columns == expected_cols

@pytest.mark.parametrize(
    "input_cols, expected_cols",
    edge_cases
)
@pytest.mark.edge
def test_normalise_columns_edge(df_factory, spark, input_cols, expected_cols):

    if input_cols is None:
        from pyspark.sql.types import StructType
        df = spark.createDataFrame([], StructType())
    else:
        df = df_factory(input_cols)

    result = normalise_columns(df)

    assert result.columns == expected_cols



