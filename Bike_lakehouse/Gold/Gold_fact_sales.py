# Databricks notebook source
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

query= """
select * from silver.crm_sales_details
"""
df = spark.sql(query) ## put into a Dataframe.
df.display()


# COMMAND ----------

(
    df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable("gold.fact_sales")
)

# COMMAND ----------

