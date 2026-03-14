# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table if exists workspace.gold.dim_products

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY prd.product_start_date, prd.product_key) AS prdt_key,
    prd.product_id,
    trim(upper(prd.product_key)) AS product_number,
    prd.product_key,
    prd.product_name,
    pc.category,
    pc.subcategory,
    pc.maintenance,
    prd.product_size,
    prd.product_start_date
FROM workspace.silver.crm_products prd
LEFT JOIN workspace.silver.erp_category pc
    ON prd.category_id = pc.id
--WHERE prd.end_date IS NULL
"""

df = spark.sql(query)
df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.dim_products")
display(df)