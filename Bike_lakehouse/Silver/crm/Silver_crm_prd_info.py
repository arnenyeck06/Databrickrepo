# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col


from pyspark.sql import functions as F

# COMMAND ----------

df = spark.table("workspace.bronze.crm_prd_info")
df.display()

# COMMAND ----------

df.filter(F.col("prd_cost").isNull()).count()

# COMMAND ----------

df.select("prd_line").distinct().display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## trim
# MAGIC

# COMMAND ----------

## trim
for fields in df.schema.fields:
    if isinstance(fields.dataType,StringType):
        df = df.withColumn(fields.name,trim(col(fields.name)))

df.display()

# COMMAND ----------


df = (
    df
   # create category_id from product_key to joining in gold
    .withColumn(
        "category_id",
        F.regexp_replace(F.substring(F.col("prd_key"), 1, 5), "-", "_")
    )
    # normalization
    .withColumn(
        "prd_line",
        F.when(F.upper(col("prd_line")) == "M", "Mountain")
         .when(F.upper(col("prd_line")) == "R", "Road")
         .when(F.upper(col("prd_line")) == "S", "Other Sales")
         .when(F.upper(col("prd_line")) == "T", "Touring")
         .otherwise("n/a")
    )
)

df.display()

# COMMAND ----------

# replacing null values in prd_cost with 0
df = df.fillna({"prd_cost": 0})
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## renaming

# COMMAND ----------

Rename_map = {"prd_id":"product_id",
             "prd_key":"product_key",
             "prd_nm":"product_name",
             "prd_cost":"product_cost",
             "prd_line":"product_size",
             "prd_start_dt":"product_start_date",
             "prd_end_dt":"product_end_date"
             }

for old_name, new_name in Rename_map.items():
    df = df.withColumnRenamed(old_name, new_name)
df.display()


# COMMAND ----------

cleaned_col = F.trim(F.upper(F.col("product_size")))

df = df.withColumn(
    "product_size",
    F.when(cleaned_col == "R", "Regular")
     .when(cleaned_col == "S", "Small")
     .when(cleaned_col == "M", "Medium")
     .when(cleaned_col == "T", "Tall")
     .otherwise(cleaned_col)
)

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write into silver table

# COMMAND ----------

## write data into silver table.
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.crm_products")
)