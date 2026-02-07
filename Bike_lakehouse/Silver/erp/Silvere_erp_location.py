# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

from pyspark.sql.functions import col, to_date, try_to_date


# COMMAND ----------

df = spark.table("workspace.bronze.erp_location")
df.display()

# COMMAND ----------

## renaming columns
Rename_map = {"CNTRY":"Category" }

for old_name, new_name in Rename_map.items():
    df = df.withColumnRenamed(old_name,new_name)
df.display()

# COMMAND ----------

df.select("Category").distinct().display()


# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import functions as F

df = (
    df.withColumn(
        "Category",
        F.when(F.upper(F.col("Category")).isin(["US", "USA", "UNITED STATES"]), "United States")
         .when(F.upper(F.col("Category")).isin(["DE", "GERMANY"]), "Germany")
         .when(F.upper(F.col("Category")).isin(["UK", "UNITED KINGDOM"]), "United Kingdom")
         .when(F.upper(F.col("Category")) == "FRANCE", "France")
         .when(F.upper(F.col("Category")) == "CANADA", "Canada")
         .when(F.upper(F.col("Category")) == "AUSTRALIA", "Australia")
         .otherwise(F.col("Category"))  # Keep original value if no match
    )
)

df.display()

# COMMAND ----------

## write data into silver table.
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.erp_location")
)

# COMMAND ----------



# COMMAND ----------

