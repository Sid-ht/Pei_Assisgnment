# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

def write_orders(df:DataFrame):
    """
    Writes orders DataFrame to the Delta table in overwrite mode.
    """
    try:
        df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true")\
        .saveAsTable("sales_processing.bronze.orders")
        
        print("Wrote data to sales_processing.bronze.orders")
    
    except Exception as e:
        print(f"Failed to write orders data to Delta table: {e}")
        raise
    

def write_products(df:DataFrame):
    """
    Writes products DataFrame to the Delta table in overwrite mode.
    """
    try:
        df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true")\
        .saveAsTable("sales_processing.bronze.products")

        print("Wrote data to sales_processing.bronze.products")
    except Exception as e:
        print(f"Failed to write products data to Delta table: {e}")
        raise
    

def write_customers(df:DataFrame):
    """
    Writes customers DataFrame to the Delta table in overwrite mode.
    """
    try:
        df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true")\
        .saveAsTable("sales_processing.bronze.customers")

        print("Wrote data to sales_processing.bronze.customers")
    except Exception as e:
        print(f"Failed to write customers data to Delta table: {e}")
        raise
