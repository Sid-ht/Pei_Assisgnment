# Databricks notebook source
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
