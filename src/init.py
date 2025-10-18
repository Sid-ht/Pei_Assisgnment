# Databricks notebook source
# MAGIC %run ./read_source.py

# COMMAND ----------

# MAGIC %run ./write_bronze.py

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

def write_configs():
    """Load YAML config from given path"""
    config_path = "/Volumes/sales_processing/bronze/ingested_data/ingested_paths.yaml"
    
    try:
        with open(config_path, 'r') as file:
            write_config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"Config file not found at: {config_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
        return None

# COMMAND ----------

# Import config and read_sources
#Orders_Path = '/Volumes/sales_processing/bronze/source_datasets/Orders.json'
#Products_Path = '/Volumes/sales_processing/bronze/source_datasets/Products.csv'
#Customers_Path = '/Volumes/sales_processing/bronze/source_datasets/Customer.xlsx'
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
        orders_path = self.config['paths']['orders']
        products_path = self.config['paths']['products']
        customers_path = self.config['paths']['customers']

        # Read Orders
        write_orders(read_orders_df, orders_path)
        if spark.catalog.tableExists('sales_processing.bronze.orders'):
            if not spark.table('sales_processing.bronze.orders').isEmpty():
                print('Orders Bronze Table written successfully!')

        # Read Products
        products_df = write_products(read_orders_df, products_path)
        if spark.catalog.tableExists('sales_processing.bronze.products'):
            if not spark.table('sales_processing.bronze.products').isEmpty():
                print('Products Bronze Table written successfully!')
        

        # Read Customers
        customers_df = write_customers(read_orders_df, customers_path)
        if spark.catalog.tableExists('sales_processing.bronze.customers'):
            if not spark.table('sales_processing.bronze.customers').isEmpty():
                print('Customers Bronze Table written successfully!')

        print("All bronze tables created.")


# Ensure main runs if executed directly
if __name__ == "__main__":
    config = load_config()

    pipeline = MainPipeline(config)
    orders_df,products_df,customers_df = pipeline.read()

    write_config = write_configs()
    pipeline.write(orders_df,products_df,customers_df)

