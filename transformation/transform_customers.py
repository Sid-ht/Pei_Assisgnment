# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def clean_customer_name(df)-> DataFrame:
    cleaned_name_df = df.withColumn('customer_name',
                            F.regexp_replace(F.col('customer_name'), r'[^a-zA-Z\s]', ''))\
                            .withColumn('customer_name', F.regexp_replace(F.col('customer_name'), r'\s+', ' '))\
                            .withColumn('customer_name', F.trim(F.col('customer_name')))\
                            .withColumn('customer_name', F.regexp_replace(F.col('customer_name'), r'(?<=\b[a-z])\s(?=[a-z])', ''))\
                            .withColumn('customer_name',F.initcap(F.col('customer_name')))
    return cleaned_name_df

def enrich_customers(bronze_customers_df: DataFrame)-> DataFrame:
    if "customer_name" not in bronze_customers_df.columns and "country" not in bronze_customers_df.columns:
        print('Exception columns customer_name and country not found')
        return bronze_customers_df
    
    df_customer_name = clean_customer_name(bronze_customers_df)\
                                    .filter((F.col('customer_name').isNotNull()) & (F.col('customer_name') != ''))\
                                    .withColumn('customer_name', F.trim(F.col("customer_name")))
        
    df_country = df_customer_name.filter((F.col('country').isNotNull()) & (F.col('country') != ''))\
                                         .withColumn("country", F.trim(F.col("country")))
    return df_country

def silver_customers_write(df: DataFrame):
    enrich_customers(df).select('customer_id','customer_name','country').write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_processing.silver.customers")

    if spark.catalog.tableExists('sales_processing.silver.customers'):
            if not spark.table('sales_processing.silver.customers').isEmpty():
                print(f'Silver Customers table written successfully as delta table! ')
