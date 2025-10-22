# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DateType

def parse_order_dates(df: DataFrame)-> DataFrame:
    """
    Parses the 'order_date' column into proper date format and extracts 'year'.
    """
    try:
        #Converting to date format
        bronze_orders_df = df.withColumn("order_date", F.to_date(F.col("order_date"), "d/M/yyyy"))
        #Extracting year for later aggregation
        parsed_year_bronze_df = bronze_orders_df.withColumn("year", F.year(F.col("order_date")))
        return parsed_year_bronze_df
    except Exception as e:
        print(f"Error while parsing order dates: {e}")
        raise

def round_profits(df:DataFrame)-> DataFrame:
    """
    Rounds the profit column to 2 decimal places.
    """
    try:
        parsed_df = parse_order_dates(df)
        # Round profit to 2 decimal places
        round_profit_orders_df = parsed_df.withColumn("profit", F.round(F.col("profit").cast("double"), 2))
        return round_profit_orders_df
    except Exception as e:
        print(f"Error while rounding profits: {e}")
        raise

def silver_order_write(df: DataFrame):
    """
    Writes the transformed orders DataFrame to a Delta table in the Silver layer.
    Performs validation after write.
    """
    table_name = "sales_processing.silver.orders"
    try:
        round_profits(orders_df).select('customer_id','product_id','year','profit').write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)
    
    #Post-Write Valdations
    if spark.catalog.tableExists('sales_processing.silver.orders'):
            if not spark.table('sales_processing.silver.orders').isEmpty():
                print(f'Silver Orders table written successfully as delta table! ')
            else:
                print(f"Silver Orders table '{table_name}' is empty after write.")
    else:
        print(f"Failed to create Silver Orders table '{table_name}'.")

    except AnalysisException as ae:
        print(f"Spark AnalysisException during table write: {ae}")
        raise
    except Exception as e:
        print(f"Unexpected error while writing Silver Orders table: {e}")
        raise
