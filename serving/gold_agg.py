# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def agg_by_year(silver_df: DataFrame)-> DataFrame:
    try:
        agg_df = silver_df.groupBy(F.col('year')).agg(F.sum('profit').alias('year_wise_profit'))
        print("profit aggregation per year basis done")
        return agg_df
    except Exception as e:
        print(f"Error in agg_by_year: {e}")
        raise

def agg_by_category(silver_df: DataFrame, silver_df2: DataFrame )-> DataFrame:
    try:
        agg_df = silver_df.join(silver_df2, on ='product_id').groupBy(F.col('category')).agg(F.sum('profit').alias('category_wise_profit'))
        print("profit aggregation per category basis done")
        return agg_df
    except Exception as e:
        print(f"Error in agg_by_category: {e}")
        raise

def agg_by_sub_category(silver_df: DataFrame, silver_df2: DataFrame )-> DataFrame:
    try:
        agg_df = silver_df.join(silver_df2, on ='product_id').groupBy(F.col('sub_category')).agg(F.sum('profit').alias('sub_category_wise_profit'))
        print("profit aggregation per sub_category basis done")
        return agg_df
    except Exception as e:
        print(f"Error in agg_by_sub_category: {e}")
        raise

def agg_by_customer(silver_df: DataFrame, silver_df2: DataFrame )-> DataFrame:
    try:
        agg_df = silver_df.join(silver_df2, on ='customer_id').groupBy(F.col('customer_id'), F.col('customer_name')).agg(F.sum('profit').alias('customer_wise_profit'))
        print("profit aggregation per category basis done")
        return agg_df
    except Exception as e:
        print(f"Error in agg_by_customer: {e}")
        raise
