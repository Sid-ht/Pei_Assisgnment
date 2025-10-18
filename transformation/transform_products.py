# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def enrich_products(bronze_products_df: DataFrame)-> DataFrame:
    if "category" not in bronze_products_df.columns and "sub_category" not in bronze_products_df.columns:
        print('Exception columns category and sub-category not found')
        return bronze_products_df
    
    df_category = bronze_products_df.filter((F.col('category').isNotNull()) & (F.col('category') != ''))\
                                            .withColumn("category", F.trim(F.col("category")))

    df_sub_category = df_category.filter((F.col('sub_category').isNotNull()) & (F.col('sub_category') != ''))\
                                         .withColumn("sub_category", F.trim(F.col("sub_category")))
    return df_sub_category
    
def silver_products_write(df: DataFrame):   
    enrich_products(df).select('product_id','category','sub_category').write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("sales_processing.silver.products")

    if spark.catalog.tableExists('sales_processing.silver.products'):
            if not spark.table('sales_processing.silver.products').isEmpty():
                print(f'Silver Products table written successfully as delta table! ')
