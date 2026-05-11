# schemas/sales_schema.py
from pyspark.sql.types import *

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",        StringType(),  False),
    StructField("category",          StringType(),  True),
    StructField("sub_category",      StringType(),  True),
    StructField("product_name",      StringType(),  True),
    StructField("state",             StringType(),  True),
    StructField("price_per_product", DoubleType(),  True),
    StructField("_ingested_at",      TimestampType(), True),
])

# orders.json has no pre-defined schema — inferred from JSON, then cast
ORDERS_SCHEMA = StructType([
    StructField("row_id",     StringType(),  True),
    StructField("order_id",   StringType(), False),
    StructField("order_date", StringType(), True),   # kept as string; parsed in enriched layer
    StructField("ship_date",  StringType(), True),
    StructField("ship_mode",  StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id",  StringType(), True),
    StructField("quantity",    IntegerType(), True),
    StructField("price",       DoubleType(), True),
    StructField("discount",    DoubleType(), True),
    StructField("profit",      DoubleType(), True),
    StructField("_ingested_at",      TimestampType(), True),
])


CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",   StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email",         StringType(), True),
    StructField("phone",         StringType(), True),
    StructField("address",       StringType(), True),
    StructField("segment",       StringType(), True),
    StructField("country",       StringType(), True),
    StructField("city",          StringType(), True),
    StructField("state",         StringType(), True),
    StructField("postal_code",   StringType(), True),
    StructField("region",        StringType(), True),
    StructField("_ingested_at",      TimestampType(), True),
])

MASTER_ORDERS_SCHEMA = StructType([
    StructField("order_id",          StringType(), False),
    StructField("order_date",        DateType(),   True),
    StructField("ship_date",         DateType(),   True),
    StructField("ship_mode",         StringType(), True),
    StructField("order_year",        IntegerType(), True),
    StructField("order_month",       IntegerType(), True),
    StructField("days_to_ship",      IntegerType(), True),
    
    StructField("customer_id",       StringType(), True),
    StructField("customer_name",     StringType(), True),
    StructField("country",           StringType(), True),
    StructField("segment",           StringType(), True),
    StructField("customer_city",     StringType(), True),
    StructField("customer_state",    StringType(), True),
    StructField("region",            StringType(), True),
    
    StructField("product_id",        StringType(), True),
    StructField("product_name",      StringType(), True),
    StructField("category",          StringType(), True),
    StructField("sub_category",      StringType(), True),
    StructField("price_per_product", DoubleType(), True),
    
    StructField("quantity",          IntegerType(), True),
    StructField("order_price",       DoubleType(), True),
    StructField("discount",          DoubleType(), True),
    StructField("profit",            DoubleType(), True),
    StructField("total_revenue",DoubleType(), True),
    StructField("_updated_at",      TimestampType(), True),
])

PROFIT_AGGREGATE_SCHEMA = StructType([
    StructField("order_year",        IntegerType(), False),
    StructField("category",          StringType(),  False),
    StructField("customer_id",       StringType(),  False),
    StructField("customer_name",     StringType(),  True),
    StructField("country",           StringType(),  True),
    StructField("region",            StringType(),  True),
    StructField("total_profit",      DoubleType(),  True),
    StructField("total_revenue",     DoubleType(),  True),
    StructField("_updated_at",       TimestampType(), True),
])
