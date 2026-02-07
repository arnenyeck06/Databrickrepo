# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col


from pyspark.sql import functions as F

# COMMAND ----------

df = spark.table("workspace.bronze.crm_prd_info")
df.display()

# COMMAND ----------

## trim
for fields in df.schema.fields:
    if isinstance(fields.dataType,StringType):
        df = df.withColumn(fields.name,trim(col(fields.name)))

df.display()

# COMMAND ----------


# Calculate median
median_val = df.select(F.percentile_approx("prd_cost", 0.5)).first()[0]


# COMMAND ----------


df = df.fillna({"prd_cost": median_val}) \
       .withColumn(
           "prd_line",
           F.when(F.trim(F.upper(F.col("prd_line"))) == "R", "Regular")
            .when(F.trim(F.upper(F.col("prd_line"))) == "S", "Small")
            .when(F.trim(F.upper(F.col("prd_line"))) == "M", "Medium")
            .when(F.trim(F.upper(F.col("prd_line"))) == "T", "Tall")
            .otherwise(F.col("prd_line"))
       )


# COMMAND ----------

df.display()

# COMMAND ----------


## renaming columns
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

## write data into silver table.
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.crm_products")
)