# Databricks notebook source
# MAGIC %run ./transform_products

# COMMAND ----------

# MAGIC %run ./transform_customers

# COMMAND ----------

# MAGIC %run ./transform_orders

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
    if spark.catalog.tableExists('sales_processing.bronze.orders'):
        if not spark.table('sales_processing.bronze.orders').isEmpty():
            orders_df = spark.table('sales_processing.bronze.orders')

    # Read Products
    if spark.catalog.tableExists('sales_processing.bronze.products'):
        if not spark.table('sales_processing.bronze.products').isEmpty():
            products_df = spark.table('sales_processing.bronze.products')
        

    # Read Customers
    if spark.catalog.tableExists('sales_processing.bronze.customers'):
        if not spark.table('sales_processing.bronze.customers').isEmpty():
            customers_df = spark.table('sales_processing.bronze.customers')
    
    pipeline = MainPipeline()

    pipeline.silver_products(products_df)
    pipeline.silver_orders(orders_df)
    pipeline.silver_customers(customers_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales_processing.silver.customers;
# MAGIC drop table sales_processing.silver.products;
# MAGIC drop table sales_processing.silver.orders;
