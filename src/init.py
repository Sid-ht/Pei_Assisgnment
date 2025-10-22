# Databricks notebook source
# MAGIC %run ./read_source

# COMMAND ----------

# MAGIC %run ./write_bronze

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import yaml
def load_config():
    """Load YAML config from given path"""
    config_path = "/Volumes/sales_processing/bronze/source_datasets/config.yaml"
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"Config file not found at: {config_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
        return None

# COMMAND ----------

from pyspark.sql import DataFrame

class MainPipeline:
    def __init__(self, config):
        self.config = config
    """
    Main function to read all sources using paths from config.
    """
    def read(self) -> (DataFrame, DataFrame, DataFrame):
        orders_path = self.config['paths']['orders']
        products_path = self.config['paths']['products']
        customers_path = self.config['paths']['customers']

        # Read Orders
        orders_df = read_orders(orders_path)
        print("Orders Preview:")
        display(orders_df)

        # Read Products
        products_df = read_products_csv(products_path)
        print("Products Preview:")
        display(products_df)

        # Read Customers
        customers_df = read_customers_xlsx_via_pandas(customers_path)
        print("Customers Preview:")
        display(customers_df)

        print("All sources have been read successfully.")
        return orders_df, products_df, customers_df

    
    def write(self, read_orders_df, read_products_df, read_customers_df):
        try:
            # Read Order
            orders_df = write_orders(read_orders_df)
            if spark.catalog.tableExists('sales_processing.bronze.orders'):
                if not spark.table('sales_processing.bronze.orders').isEmpty():
                    print('Orders Bronze Table written successfully!')
                else:
                    print('Orders Bronze Table not written successfully!')

            # Read Products
            products_df = write_products(read_products_df)
            if spark.catalog.tableExists('sales_processing.bronze.products'):
                if not spark.table('sales_processing.bronze.products').isEmpty():
                    print('Products Bronze Table written successfully!')
                else:
                    print('Orders Bronze Table not written successfully!')
        

            # Read Customers
            customers_df = write_customers(read_customers_df)
            if spark.catalog.tableExists('sales_processing.bronze.customers'):
                if not spark.table('sales_processing.bronze.customers').isEmpty():
                    print('Customers Bronze Table written successfully!')
                else:
                    print('Orders Bronze Table not written successfully!')

            print("All bronze tables created.")
        
        except Exception as e:
            print(f"Error writing sources: {e}")
            raise e

# Ensure main runs if executed directly
if __name__ == "__main__":
    config = load_config()

    pipeline = MainPipeline(config)
    orders_df,products_df,customers_df = pipeline.read()

    pipeline.write(orders_df,products_df,customers_df)


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales_processing.bronze.customers;
# MAGIC drop table sales_processing.bronze.products;
# MAGIC drop table sales_processing.bronze.orders;
# MAGIC
