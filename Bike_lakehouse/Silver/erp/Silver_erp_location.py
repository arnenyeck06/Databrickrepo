# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col



# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver.erp_location")

# COMMAND ----------

df = spark.table("workspace.bronze.erp_location")
df.display()

# COMMAND ----------

# clean CID >> remove dash for joining in gold
df = df.withColumn("CID", F.regexp_replace(F.col("CID"), "-", ""))

# COMMAND ----------

## renaming columns
Rename_map = {"CNTRY":"country" }

for old_name, new_name in Rename_map.items():
    df = df.withColumnRenamed(old_name,new_name)
df.display()

# COMMAND ----------

df.select("country").distinct().display()


# COMMAND ----------

# handle nulls in country
df = df.withColumn("country",
    F.when(F.col("country").isNull() | (F.trim(F.col("country")) == ""), F.lit("n/a"))
     .otherwise(F.col("country"))
)

# COMMAND ----------

from pyspark.sql import functions as F

df = (
    df.withColumn(
        "country",
        F.when(F.upper(F.col("country")).isin(["US", "USA", "UNITED STATES"]), "United States")
         .when(F.upper(F.col("country")).isin(["DE", "GERMANY"]), "Germany")
         .when(F.upper(F.col("country")).isin(["UK", "UNITED KINGDOM"]), "United Kingdom")
         .when(F.upper(F.col("country")) == "FRANCE", "France")
         .when(F.upper(F.col("country")) == "CANADA", "Canada")
         .when(F.upper(F.col("country")) == "AUSTRALIA", "Australia")
         .otherwise(F.col("country")) 
    )
)

df.display()

# COMMAND ----------

# DBTITLE 1,Cell 7
## write data into silver table.
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.erp_location")
)
df.display()

# COMMAND ----------

