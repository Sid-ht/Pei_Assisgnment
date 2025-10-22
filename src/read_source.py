# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

def corrected_column_names(df):
    """
    Cleaning and standardizing column names
    """
    try:
        new_columns = [c.strip().replace(' ', '_').replace('-', '_').lower() for c in df.columns]
        for old, new in zip(df.columns, new_columns):
            df = df.withColumnRenamed(old, new)
        return df
    except Exception as e:
        print(f"Error while correcting column names: {e}")
        raise

def read_orders(path: str) -> DataFrame:
    """
    Reading JSON order data and standardizing column names.
    """
    try:
        df = spark.read.option("multiline", True).json(path)
        new_df = corrected_column_names(df)
        return new_df
    except FileNotFoundError:
        print(f"JSON file not found at path: {path}")
        raise
    except Exception as e:
        print(f"Failed to read JSON orders from {path}: {e}")
        raise

def read_products_csv(path: str) -> DataFrame:
    """
    Reading product data from CSV and standardizing column names.
    """
    try:
        df = spark.read.option("header", True).csv(path)
        new_df = corrected_column_names(df)
        return new_df
    except FileNotFoundError:
        print(f"CSV file not found at path: {path}")
        raise
    except Exception as e:
        print(f"Failed to read products CSV from {path}: {e}")
        raise


def read_customers_xlsx_via_pandas(path: str):
    """
    Reading customer data from an Excel file using pandas,
    converts to Spark DataFrame, and standardizes column names.
    """
    try:
        pandas_df = pd.read_excel(path)
        if 'phone' in pandas_df.columns:
            pandas_df['phone'] = pandas_df['phone'].astype(str)

        df = spark.createDataFrame(pandas_df)
        new_df = corrected_column_names(df)
        return new_df
    except FileNotFoundError:
        print(f"Excel file not found at path: {path}")
        raise
    except Exception as e:
        print(f"Unexpected error while reading Excel from {path}: {e}")
        raise
