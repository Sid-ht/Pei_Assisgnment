# Databricks notebook source
# MAGIC %run ../../serving/gold_agg

# COMMAND ----------

import pytest
@pytest.fixture(scope="module")
def sample_dataframes(spark):
    """Provide reusable test DataFrames for aggregation tests."""
    silver_df = spark.createDataFrame([
        (2024, 1, 10, 1000, 101),
        (2024, 2, 20, 2000, 102),
        (2023, 3, 30, 3000, 103)
    ], ["year", "product_id", "customer_id", "profit", "order_id"])

    products_df = spark.createDataFrame([
        (1, "Electronics", "Laptop"),
        (2, "Furniture", "Chair"),
        (3, "Electronics", "Mouse")
    ], ["product_id", "category", "sub_category"])

    customers_df = spark.createDataFrame([
        (10, "Sid"),
        (20, "Ram"),
        (30, "Sheen")
    ], ["customer_id", "customer_name"])

    return {
        "silver_df": silver_df,
        "products_df": products_df,
        "customers_df": customers_df
    }
