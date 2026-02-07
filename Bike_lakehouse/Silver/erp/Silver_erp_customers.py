# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

from pyspark.sql.functions import col, to_date, try_to_date


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

from pyspark.sql import functions as F

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

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

