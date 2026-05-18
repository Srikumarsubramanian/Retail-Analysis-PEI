DEFAULT_SPARK_CONFIG = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "auto",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
}