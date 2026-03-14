# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col



# COMMAND ----------

df = spark.table("workspace.bronze.crm_sales_details")
df.display()

# COMMAND ----------

## renaming columns
Rename_map = {"sls_ord_num":"order_number",
             "sls_prd_key":"product_key",
             "sls_cust_id":"customer_id",
             "sls_order_dt":"order_date",
             "sls_ship_dt":"ship_date",
             "sls_due_dt":"due_date",
             "sls_sales":"number_of_sales",
             "sls_quantity":"quantity",
             "sls_price":"price"
             }

for old_name, new_name in Rename_map.items():
    df = df.withColumnRenamed(old_name, new_name)


# COMMAND ----------

df.dtypes

# COMMAND ----------

def safe_to_date(c, fmt):
    return F.when(F.col(c).rlike("^\\d{8}$"), F.to_date(F.col(c), fmt)).otherwise(F.lit(None))

df = (
    df.withColumn("order_date", safe_to_date("order_date", "yyyyMMdd"))
      .withColumn("ship_date",  safe_to_date("ship_date",  "yyyyMMdd"))
      .withColumn("due_date",   safe_to_date("due_date",   "yyyyMMdd"))
)

# COMMAND ----------

# DBTITLE 1,Cell 8
## write data into silver table.
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.crm_sales_details")
)
