# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

from pyspark.sql.functions import col, to_date, try_to_date


# COMMAND ----------

df = spark.table("workspace.bronze.erp_category")
df.display()

# COMMAND ----------

## renaming columns
Rename_map = {"CAT":"Category",
             "SUBCAT":"Subcategory"
             }

for old_name, new_name in Rename_map.items():
    df = df.withColumnRenamed(old_name,new_name)
df.display()

# COMMAND ----------

## write data into silver table.
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.erp_category")
)

# COMMAND ----------

