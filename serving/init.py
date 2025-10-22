# Databricks notebook source
# MAGIC %run ./gold_agg

# COMMAND ----------

from pyspark.sql import DataFrame

class MainPipeline:
    """
    function to read all bronze and write to silver after transforming.
    """    
    # Enrich Bronze table and write to Silver table
    def agg(self,orders_df,products_df,customers_df):
        display(agg_by_year(orders_df))
        display(agg_by_category(orders_df,products_df))
        display(agg_by_sub_category(orders_df,products_df))
        display(agg_by_customer(orders_df,customers_df))

# main
if __name__ == "__main__":

    # Read Orders
    if spark.catalog.tableExists('sales_processing.silver.orders'):
        if not spark.table('sales_processing.silver.orders').isEmpty():
            orders_df = spark.table('sales_processing.silver.orders')

    # Read Products
    if spark.catalog.tableExists('sales_processing.silver.products'):
        if not spark.table('sales_processing.silver.products').isEmpty():
            products_df = spark.table('sales_processing.silver.products')
        

    # Read Customers
    if spark.catalog.tableExists('sales_processing.silver.customers'):
        if not spark.table('sales_processing.silver.customers').isEmpty():
            customers_df = spark.table('sales_processing.silver.customers')
    
    pipeline = MainPipeline()

    pipeline.agg(orders_df,products_df,customers_df)
