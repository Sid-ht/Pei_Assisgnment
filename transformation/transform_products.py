# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def enrich_products(bronze_products_df: DataFrame)-> DataFrame:
    """
    Enriches the products DataFrame by trimming category and sub_category columns
    and filtering out rows where these columns are null or empty.
    """
    try:
        required_columns = ["category", "sub_category"]
        missing_columns = [c for c in required_columns if c not in bronze_products_df.columns]
        if missing_columns:
            print('Exception columns category and sub-category not found')
            return bronze_products_df
    
        # Filter non-null and non-empty 'category', trim whitespace
        df_category = bronze_products_df.filter((F.col('category').isNotNull()) & (F.col('category') != ''))\
                                            .withColumn("category", F.trim(F.col("category")))

        # Filter non-null and non-empty 'sub_category', trim whitespace
        df_sub_category = df_category.filter((F.col('sub_category').isNotNull()) & (F.col('sub_category') != ''))\
                                         .withColumn("sub_category", F.trim(F.col("sub_category")))
        return df_sub_category
    
    except Exception as e:
        print(f"Error while enriching products DataFrame: {e}")
        raise
    
def silver_products_write(df: DataFrame):
    """
    Writes the enriched products DataFrame to the silver Delta table.
    Ensures only required columns are written and checks for successful write.
    """
    table_name = "sales_processing.silver.products"
    try:
        enrich_products(df).select('product_id','category','sub_category').write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

        # Check if table exists and is not empty
        if spark.catalog.tableExists(table_name):
            if spark.table(table_name).count() > 0:
                print(f"Silver Products table '{table_name}' written successfully as Delta table!")
            else:
                print(f"Silver Products table '{table_name}' is empty after write.")
        else:
            print(f"Failed to create Silver Products table '{table_name}'.")

    except AnalysisException as ae:
        print(f"Spark AnalysisException: {ae}")
        raise
    except Exception as e:
        print(f"Unexpected error while writing Silver Products table: {e}")
        raise
