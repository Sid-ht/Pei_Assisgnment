# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

def corrected_column_names(df):
    new_columns = [c.strip().replace(' ', '_').replace('-', '_').lower() for c in df.columns]
    for old, new in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old, new)
    return df

def read_orders(path: str) -> DataFrame:
    df = spark.read.option("multiline", True).json(path)
    new_df = corrected_column_names(df)
    return new_df

def read_products_csv(path: str) -> DataFrame:
    df = spark.read.option("header", True).csv(path)
    new_df = corrected_column_names(df)
    return new_df

def read_customers_xlsx_via_pandas(path: str):
    pandas_df = pd.read_excel(path)
    if 'phone' in pandas_df.columns:
        pandas_df['phone'] = pandas_df['phone'].astype(str)
    df = spark.createDataFrame(pandas_df)
    new_df = corrected_column_names(df)
    return new_df
