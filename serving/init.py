# Databricks notebook source
# MAGIC %run ./gold_agg

# COMMAND ----------

from pyspark.sql import DataFrame

class MainPipeline:
    """
    function to read all bronze and write to silver after transforming.
    """    
    # Enrich Bronze table and write to Silver table
    def silver_products(self,products_df):
        silver_products_write(products_df)

    def silver_orders(self,orders_df):
        silver_order_write(orders_df)

    def silver_customers(self,customers_df):
        silver_customers_write(customers_df)


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

    pipeline.agg()
