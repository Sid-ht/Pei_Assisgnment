# Databricks notebook source
# MAGIC %run ../serving/gold_agg

# COMMAND ----------

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Global SparkSession fixture for all test layers."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest-ecommerce-datastes")
        .getOrCreate()
    )
    yield spark
    spark.stop()
