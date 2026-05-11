"""
SparkSession factory — single responsibility module.

Handles Delta Lake configuration.
Every entry point should call ``get_spark()`` from here.

Usage:
    from retail_analysis.databricks.utils.spark_session import get_spark
    spark = get_spark()
"""


import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "RetailAnalysis") -> SparkSession:
    """Create or retrieve a configured SparkSession.

    Parameters
    ----------
    app_name : str
        Application name visible in the Spark UI.

    Returns
    -------
    SparkSession
        Configured session with Delta Lake extensions enabled.
    """

    # ── Windows: point to local Hadoop binaries ──────────
    if sys.platform == "win32":
        hadoop_home = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "hadoop")
        )
        os.environ.setdefault("HADOOP_HOME", hadoop_home)

        hadoop_bin = os.path.join(hadoop_home, "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

        os.environ["HADOOP_OPTS"] = "-Dhadoop.native.lib=false"
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # ── Build session ────────────────────────────────────
    from delta import configure_spark_with_delta_pip

    # Combine Java options into a single string to avoid silent overwrites
    java_opts = " ".join([
        "-Dhadoop.native.lib=false",
        f"-Dlog4j.configuration=file:{_log4j_path()}",
    ])

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Windows socket-timeout fixes
        .config("spark.python.worker.timeout", "120")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Optimization Configs
        # .config("spark.sql.adaptive.enabled", "true")
        # .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        # .config("spark.databricks.delta.autoCompact.enabled", "true")
        # .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Increase default partitions (200 is too small for > 1TB)
        # Note: If running on a Databricks cluster, you can use "auto" instead of 800.
        # .config("spark.sql.shuffle.partitions", "800")
    )

    extra_packages = [
        "com.crealytics:spark-excel_2.12:3.3.1_0.18.7"
    ]
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()



def _log4j_path() -> str:
    """Resolve the log4j properties path relative to the project."""
    candidate = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "scripts", "log4j.properties")
    )
    return candidate.replace("\\", "/")
