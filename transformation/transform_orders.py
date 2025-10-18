# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DateType

def parse_order_dates(df: DataFrame)-> DataFrame:
    # Converting to date format
    bronze_orders_df = df.withColumn("order_date", F.to_date(F.col("order_date"), "d/M/yyyy"))
    # Extract year
    parsed_year_bronze_df = bronze_orders_df.withColumn("year", F.year(F.col("order_date")))
    return parsed_year_bronze_df

def round_profits(df:DataFrame)-> DataFrame:
    parsed_df = parse_order_dates(df)
    # Round profit to 2 decimal places
    round_profit_orders_df = parsed_df.withColumn("profit", F.round(F.col("profit").cast("double"), 2))
    return round_profit_orders_df

def silver_order_write(df: DataFrame):
    round_profits(orders_df).select('customer_id','product_id','year','profit').write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_processing.silver.orders")

    if spark.catalog.tableExists('sales_processing.silver.orders'):
            if not spark.table('sales_processing.silver.orders').isEmpty():
                print(f'Silver Orders table written successfully as delta table! ')
