# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading from Bronze

# COMMAND ----------

df = spark.table("workspace.bronze.crm_cust_info")
df.display()

## we will trim the strings values titles.
# normalize ,marital status, gender, 

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## trimming
# MAGIC

# COMMAND ----------


for fields in df.schema.fields:
    if isinstance(fields.dataType,StringType):
        df = df.withColumn(fields.name,trim(col(fields.name)))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## renaming
# MAGIC

# COMMAND ----------

## renaming columns

rename_map = {
    "cst_id"            : "customer_id",
    "cst_key"           : "customer_key",
    "cst_firstname"     : "firstname",
    "cst_lastname"      : "lastname",
    "cst_marital_status": "marital_status",
    "cst_gndr"          : "gender",
    "cst_create_date"   : "create_date",
}

for old_name, new_name in rename_map.items():
    df = df.withColumnRenamed(old_name, new_name)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalization

# COMMAND ----------

# DBTITLE 1,Normalization (fix unresolved column error)
marital_col = F.upper(F.col("marital_status"))
gender_col  = F.upper(F.col("gender"))

df = (
    df.withColumn(
        "marital_status",
        F.when(marital_col == "S", "Single")
         .when(marital_col == "M", "Married")
         .otherwise(F.col("marital_status"))
    )
    .withColumn(
        "gender",
        F.when(gender_col == "F", "Female")
         .when(gender_col == "M", "Male")
         .otherwise(F.col("gender"))
    )
)
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