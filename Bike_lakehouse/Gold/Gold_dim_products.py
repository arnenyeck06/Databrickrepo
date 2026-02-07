# Databricks notebook source
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

query = """
SELECT * from silver.crm_products 
"""
df = spark.sql(query) ## put into a Dataframe.
df.display()


# COMMAND ----------

(
    df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable("gold.dim_products")
)

# COMMAND ----------

