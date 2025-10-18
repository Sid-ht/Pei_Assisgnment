# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

def write_orders(df:DataFrame, path: str):
    df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_processing.bronze.orders")
    

def write_products(df:DataFrame, path: str):
    df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_processing.bronze.products")
    

def write_customers(df:DataFrame, path: str):
    df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_processing.bronze.customers")
