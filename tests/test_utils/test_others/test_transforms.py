import pytest
from types import SimpleNamespace

from retail_analysis.databricks.utils.transforms import normalise_columns


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────
@pytest.fixture()
def df_factory(spark):
    """
    Factory to create small DataFrames with given columns.
    """
    def _create(columns):
        data = [tuple(range(len(columns)))]
        return spark.createDataFrame(data, columns)
    return _create


# ─────────────────────────────────────────────
# Valid Cases
# ─────────────────────────────────────────────
@pytest.mark.transforms
@pytest.mark.parametrize(
    "input_cols, expected_cols",
    [
        pytest.param(
            ["Name", "Age"],
            ["name", "age"],
            id="basic-lowercase",
        ),
        pytest.param(
            [" First Name ", "Last Name"],
            ["first_name", "last_name"],
            id="trim-and-spaces",
        ),
        pytest.param(
            ["A-B", "C&D"],
            ["a_b", "c_d"],
            id="special-chars",
        ),
        pytest.param(
            ["Col__1", "Col@@2"],
            ["col_1", "col_2"],
            id="collapse-underscores",
        ),
        pytest.param(
            ["Mixed CASE Column"],
            ["mixed_case_column"],
            id="mixed-case",
        ),
    ],
)
def test_normalise_columns_valid(df_factory, input_cols, expected_cols):
    """
    Validate column normalization for standard inputs.
    """
    df = df_factory(input_cols)

    result = normalise_columns(df)

    assert result.columns == expected_cols


# ─────────────────────────────────────────────
# Edge Cases
# ─────────────────────────────────────────────
@pytest.mark.transforms
@pytest.mark.parametrize(
    "input_cols, expected_cols",
    [
        pytest.param(
            ["!!!", "@@@"],
            ["col", "col_1"],
            id="only-special-chars",
        ),
        pytest.param(
            ["A B", "A@B", "a_b"],
            ["a_b", "a_b_1", "a_b_2"],
            id="duplicate-collision",
        ),
        pytest.param(
            ["   ", ""],
            ["col", "col_1"],
            id="empty-and-whitespace",
        ),
        pytest.param(
            None,
            [],
            id="no-columns",
        ),
    ],
)
def test_normalise_columns_edge(df_factory, spark, input_cols, expected_cols):
    """
    Validate edge cases in column normalization.
    """
    if input_cols is None:
        from pyspark.sql.types import StructType
        df = spark.createDataFrame([], StructType())
    else:
        df = df_factory(input_cols)

    result = normalise_columns(df)

    assert result.columns == expected_cols