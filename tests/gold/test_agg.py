# Databricks notebook source
# MAGIC %run ../../serving/gold_agg

# COMMAND ----------

import sys
sys.dont_write_bytecode = True

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
        (30, "Ramesh")
    ], ["customer_id", "customer_name"])

    return {
        "silver_df": silver_df,
        "products_df": products_df,
        "customers_df": customers_df
    }

# COMMAND ----------

def test_agg_by_year(spark, sample_dataframes):
    """Test aggregation of profit by year"""
    silver_df = sample_dataframes["silver_df"]
    result = agg_by_year(silver_df)

    expected_columns = {"year", "year_wise_profit"}
    assert set(result.columns) == expected_columns, "Output schema mismatch"
    assert result.count() == 2, "Unexpected number of grouped records"


def test_agg_by_category(spark, sample_dataframes):
    """Test aggregation of profit by category"""
    result = agg_by_category(
        sample_dataframes["silver_df"], sample_dataframes["products_df"]
    )

    assert "category_wise_profit" in result.columns
    assert result.count() == 2, "Should have 2 categories aggregated"


def test_agg_by_sub_category(spark, sample_dataframes):
    """Test aggregation of profit by sub-category"""
    result = agg_by_sub_category(
        sample_dataframes["silver_df"], sample_dataframes["products_df"]
    )

    assert "sub_category_wise_profit" in result.columns
    assert result.count() == 3, "Should have 3 unique sub-categories"


def test_agg_by_customer(spark, sample_dataframes):
    """Test aggregation of profit by customer."""
    result = agg_by_customer(
        sample_dataframes["silver_df"], sample_dataframes["customers_df"]
    )

    expected_cols = {"customer_id", "customer_name", "customer_wise_profit"}
    assert set(result.columns) == expected_cols
    assert result.count() == 3, "Should have 3 customers aggregated"

    profits = {row["customer_name"]: row["customer_wise_profit"] for row in result.collect()}
    assert profits["Sid"] == 1000
    assert profits["Ram"] == 2000
    assert profits["Ramesh"] == 3000

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

!pytest -v /Workspace/Users/siddharthsinha.28@gmail.com/Pei_Assisgnment/tests/gold/test_agg.py --cache-dir=/tmp/pytest_cache


# COMMAND ----------

# MAGIC %pip install --upgrade pytest

# COMMAND ----------

!pytest -v --cache-dir=/tmp/pytest_cache /Workspace/Users/siddharthsinha.28@gmail.com/Pei_Assisgnment/tests/gold/test_agg.py

