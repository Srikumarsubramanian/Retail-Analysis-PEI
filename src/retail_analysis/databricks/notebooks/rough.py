from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import DecimalType
import pytest

from src.retail_analysis.databricks.utils.spark_session import get_spark
spark = get_spark()
# # def round_currency(
# #     column: Column,
# #     scale: int = 2,
# #     precision: int = 18,
# #     rounding_mode: str = "HALF_UP",
# # ) -> Column:
# #     """
# #     Enterprise-grade rounding for financial columns.

# #     Parameters
# #     ----------
# #     column : Column
# #         Input numeric column
# #     scale : int
# #         Number of decimal places (default: 2)
# #     precision : int
# #         Total digits (default: 18)
# #     rounding_mode : str
# #         Rounding mode:
# #             - HALF_UP (default, financial standard)
# #             - HALF_EVEN (banker's rounding)

# #     Returns
# #     -------
# #     Column
# #         Rounded column with Decimal precision

# #     Notes
# #     -----
# #     - Handles NULL safely
# #     - Avoids floating point precision issues
# #     - Ensures deterministic rounding for aggregates
# #     """

# #     if rounding_mode not in {"HALF_UP", "HALF_EVEN"}:
# #         raise ValueError(f"Unsupported rounding mode: {rounding_mode}")

# #     # Cast to Decimal for precision safety
# #     decimal_col = column.cast(DecimalType(precision, scale + 4))

# #     # Apply rounding
# #     if rounding_mode == "HALF_UP":
# #         rounded = F.round(decimal_col, scale)
# #     else:
# #         rounded = F.bround(decimal_col, scale)

# #     return F.when(column.isNull(), F.lit(None)).otherwise(rounded)








# # import pytest
# # from pyspark.sql import functions as F


# # @pytest.mark.unit
# # def test_float_precision_breaks(spark):
# #     """
# #     Demonstrates floating point precision error in aggregation.
# #     """
# #     # 0.1 repeated 1000 times → should be 100.0
# #     data = [(0.1,) for _ in range(1000)]

# #     df = spark.createDataFrame(data, ["profit"])  # default → DoubleType

# #     result = df.select(F.sum("profit").alias("total")).collect()[0]["total"]
# #     print(result)
# #     # ❌ This will FAIL
# #     # assert result == 100.0

# # test_float_precision_breaks(spark)

# # from pyspark.sql.types import DecimalType


# # @pytest.mark.unit
# # def test_decimal_precision_correct(spark):
# #     """
# #     Decimal ensures exact aggregation.
# #     """
# #     data = [(0.1,) for _ in range(1000)]

# #     df = spark.createDataFrame(data, ["profit"])

# #     df = df.withColumn(
# #         "profit",
# #         F.col("profit").cast(DecimalType(18, 4))
# #     )

# #     result = df.select(F.sum("profit").alias("total")).collect()[0]["total"]
# #     print(result)

# #     # assert float(result) == pytest.approx(100.0)

# # test_decimal_precision_correct(spark)


# # @pytest.mark.unit
# def test_decimal_rounding_is_correct(spark):
#     data = [(0.1,) for _ in range(1000)]

#     df = spark.createDataFrame(['asdasd'], ["profit"])

#     df = df.withColumn(
#         "profit",
#         F.col("profit").cast(DecimalType(18, 4))
#     )

#     result = df.select(
#         F.round(F.sum("profit"), 2).alias("total")
#     ).collect()[0]["total"]
#     print(result)
#     assert float(result) == pytest.approx(100.00)

# # test_decimal_rounding_is_correct(spark)


# from pyspark.sql import functions as F
# from pyspark.sql.column import Column
# from pyspark.sql.types import DecimalType


# def round_currency(
#     column: Column,
#     scale: int = 2,
#     precision: int = 18,
# ) -> Column:
#     """
#     Enterprise-grade rounding for financial columns.

#     Parameters
#     ----------
#     column : Column
#         Input numeric column
#     scale : int
#         Number of decimal places (default: 2)
#     precision : int
#         Total digits (default: 18)


#     Returns
#     -------
#     Column
#         Rounded column with Decimal precision

#     Notes
#     -----
#     - Handles NULL safely
#     - Avoids floating point precision issues
#     - Applies higher intermediate precision (scale + 4) for safe aggregation
#     """
#     # Cast to Decimal for precision safety
#     decimal_col = column.cast(DecimalType(precision, scale + 4))

#     # Apply rounding
#     rounded = F.round(decimal_col, scale)

#     return rounded

# import os
# from pyspark.sql.types import StructType, StructField, StringType


# sample_json_path = "d:/PEI/retail-analysis/src/retail_analysis/databricks/notebooks/sample_profit.json"


# df_sample = spark.read.option('mode', 'PERMISSIVE').option("multiline", "true").json('temp\source\Orders.json')
# # df_cast = df_sample.withColumn(
# #     "profit_decimal",
# #     type(F.round(F.col("profit").cast(DecimalType(18, 4)), 2))
# # )

# df_cast = df_sample.withColumn(
#     "profit_decimal",
#     round_currency(F.col("profit"))
# )
# df_cast.printSchema()
# df_cast.show(truncate=False)


# spark


df = spark.read.format('delta').load('temp\quarantine\master_orders_read')
df.show(truncate=False)