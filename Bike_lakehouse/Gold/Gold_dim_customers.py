# Databricks notebook source
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Transformation and Modeling.

# COMMAND ----------

query = """
SELECT
  row_number() over (order by ci.customer_id) as customer_key,
  ci.customer_id,
  ci.firstname,
  ci.lastname
from silver.crm_customers ci
"""
df = spark.sql(query) ## put into a Dataframe.
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write it Gold Table

# COMMAND ----------

(
    df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable("gold.dim_customers")
)

# COMMAND ----------

