-- Databricks notebook source
SELECT year, ROUND(SUM(profit), 2) AS total_profit
FROM sales_processing.silver.orders
GROUP BY year
ORDER BY year;

-- COMMAND ----------

SELECT o.year as year, p.category as product_category, ROUND(SUM(profit), 2) AS total_profit
FROM sales_processing.silver.orders o inner join sales_processing.silver.products p
on o.product_id = p.product_id
GROUP BY year, p.category
ORDER BY year, p.category;

-- COMMAND ----------

SELECT c.customer_name as customer_name, ROUND(SUM(profit), 2) AS total_profit
FROM sales_processing.silver.orders o inner join sales_processing.silver.customers c  
on o.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_profit DESC;

-- COMMAND ----------

SELECT c.customer_name as customer_name, year, ROUND(SUM(profit), 2) AS total_profit
FROM sales_processing.silver.orders o inner join sales_processing.silver.customers c
on o.customer_id = c.customer_id
GROUP BY customer_name, year
ORDER BY customer_name, year;
