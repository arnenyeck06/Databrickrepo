# Databricks notebook source
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

## Start preparing the first table from the bronze.
#--STEPS--
## REda from the Bronze
## DO some Data transformation
## Write it into silver Table.

# COMMAND ----------

df = spark.table("workspace.bronze.crm_cust_info")
df.display()

## we will trim the strings values titles.
# normalize ,marital status, gender, 

# COMMAND ----------

## trim
for fields in df.schema.fields:
    if isinstance(fields.dataType,StringType):
        df = df.withColumn(fields.name,trim(col(fields.name)))

df.display()

# COMMAND ----------

df = (
    df.withColumn("cst_marital_status",
    F.when(F.upper(F.col("cst_marital_status")) == "S", "Single")
    .when(F.upper(F.col("cst_marital_status")) == "M", "Married")
    
)
.withColumn("cst_gndr",
    F.when(F.upper(F.col("cst_gndr")) == "F", "Female")
    .when(F.upper(F.col("cst_gndr")) == "M", "Male")
    
))



# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## renaming columns

# COMMAND ----------

## renaming columns
Rename_map = {"cst_id":"customer_id",
             "cst_key":"customer_key",
             "cst_firstname":"firstname",
             "cst_lastname":"lastname",
             "cst_marital_status":"marital_status",
             "cst_gndr":"gender",
             "cst_create_date":"create_date"
             }



# COMMAND ----------

for old_name, new_name in Rename_map.items():
    df = df.withColumnRenamed(old_name,new_name)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data into Silver table
# MAGIC

# COMMAND ----------

(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.crm_customers")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.crm_customers

# COMMAND ----------

# import dependencies
# name maping
# read from the bronze
# do some data transformation(trimming, normalizing columns and renaming. write into silver.