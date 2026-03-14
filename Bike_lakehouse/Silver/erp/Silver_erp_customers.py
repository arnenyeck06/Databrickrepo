# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col
from pyspark.sql import functions as F



# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver.erp_customers")

# COMMAND ----------

df = spark.table("workspace.bronze.erp_customer")
df.display()

# COMMAND ----------

# DBTITLE 1,Cell 3
## trimming
for fields in df.schema.fields:
    if isinstance(fields.dataType,StringType):
        df = df.withColumn(fields.name, trim(col(fields.name)))
df.display()

# COMMAND ----------

## change GEN to gender, Bdate to birthdate
df = df.withColumnRenamed('GEN','gender').withColumnRenamed('BDATE','birthdate')
df.display()
##

# COMMAND ----------

df.dtypes

# COMMAND ----------

# clean birthdate >> remove future dates
df = df.withColumn("birthdate",
    F.when(F.col("birthdate") > F.current_date(), F.lit(None))
     .otherwise(F.col("birthdate"))
)

# COMMAND ----------

# clean CID >> remove 'NAS' prefix for joining in gold
df = df.withColumn("CID", F.regexp_replace(F.col("CID"), "NAS", ""))

# COMMAND ----------


df = (
    df.withColumn(
        "gender",
        F.when(F.upper(F.col("gender")) == "F", "Female")
         .when(F.upper(F.col("gender")) == "M", "Male")
         .otherwise(F.col("gender"))

    )
)

df.display()


# COMMAND ----------

## write data into silver table.
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.erp_customers")
)
df.display()


# COMMAND ----------

