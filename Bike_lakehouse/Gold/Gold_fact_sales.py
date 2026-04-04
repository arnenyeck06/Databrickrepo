# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold.fact_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_key FROM workspace.gold.dim_products LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT sls_prd_key FROM workspace.bronze.crm_sales_details LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.order_number,
# MAGIC     p.product_key
# MAGIC FROM workspace.silver.crm_sales_details s
# MAGIC LEFT JOIN workspace.gold.dim_products p
# MAGIC   ON p.product_key LIKE CONCAT('%', s.product_key)
# MAGIC LIMIT 10;

# COMMAND ----------

query = """
SELECT
    s.order_number,
    s.customer_id,
    s.order_date,
    s.ship_date,
    s.due_date,
    s.price,
    p.product_key,
    c.cust_surrogate_key AS customer_key
FROM workspace.silver.crm_sales_details s
LEFT JOIN workspace.gold.dim_products p
    ON p.product_key LIKE CONCAT('%', s.product_key)
LEFT JOIN workspace.gold.dim_customers c
    ON s.customer_id = c.customer_id
"""

df = spark.sql(query)
df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.fact_sales")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing into GOLD layer.