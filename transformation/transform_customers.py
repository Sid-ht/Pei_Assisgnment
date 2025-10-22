# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def clean_customer_name(df)-> DataFrame:
    """
    Cleans and standardizes the customer_name column by removing special characters, extra spaces
    """
    try:
        # Validate column existence
        if "customer_name" not in df.columns:
            print("Column customer_name not found in DataFrame. Returning original DataFrame.")
            return df
        else:
            cleaned_name_df = df.withColumn('customer_name',
                            F.regexp_replace(F.col('customer_name'), r'[^a-zA-Z\s]', ''))\
                            .withColumn('customer_name', F.regexp_replace(F.col('customer_name'), r'\s+', ' '))\
                            .withColumn('customer_name', F.trim(F.col('customer_name')))\
                            .withColumn('customer_name', F.regexp_replace(F.col('customer_name'), r'(?<=\b[a-z])\s(?=[a-z])', ''))\
                            .withColumn('customer_name',F.initcap(F.col('customer_name')))
            return cleaned_name_df
    except Exception as e:
        print(f"Error while transforming customer_name': {e}")
        raise

def enrich_customers(bronze_customers_df: DataFrame)-> DataFrame:
    """
    Enriches the Bronze Customers DataFrame
    """
    try:
        required_columns = ["customer_name", "country"]
        missing_cols = [c for c in required_columns if c not in bronze_customers_df.columns]
        if missing_cols:
            print('Exception columns customer_name and country not found')
            return bronze_customers_df
    
        df_customer_name = clean_customer_name(bronze_customers_df)\
                                    .filter((F.col('customer_name').isNotNull()) & (F.col('customer_name') != ''))\
                                    .withColumn('customer_name', F.trim(F.col("customer_name")))
        
        df_country = df_customer_name.filter((F.col('country').isNotNull()) & (F.col('country') != ''))\
                                         .withColumn("country", F.trim(F.col("country")))
        return df_country
    
    except Exception as e:
        print(f"Error while enriching customers DataFrame: {e}")
        raise

def silver_customers_write(df: DataFrame):
    """
    Writing the enriched customers DataFrame to the Silver Delta table.
    """
    table_name = "sales_processing.silver.customers"
    try:
        enrich_customers(df).select('customer_id','customer_name','country').write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)
    
        #Post write checks
        if spark.catalog.tableExists(table_name):
            if not spark.table(table_name).isEmpty():
                print(f'Silver Customers table written successfully as delta table! ')
            else:
                print(f"Silver Customers table '{table_name}' exists but is empty.")
        else:
            print(f"Failed to create Silver Customers table '{table_name}'.")
    
    except Exception as e:
        print(f"Error while writing Silver Customers table: {e}")
        raise
    
