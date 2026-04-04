# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col


# COMMAND ----------

df = spark.table("workspace.silver.crm_sales_details")
df.display()

# COMMAND ----------

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

df.display()

# COMMAND ----------

df = (
    df.withColumn("order_date", F.to_date(F.col("order_date").cast("string"), "yyyyMMdd"))
      .withColumn("ship_date",  F.to_date(F.col("ship_date").cast("string"),  "yyyyMMdd"))
      .withColumn("due_date",   F.to_date(F.col("due_date").cast("string"),   "yyyyMMdd"))
)

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Cell 8
## write data into silver table.
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.crm_sales_details")
)

df.display()

# COMMAND ----------

#spark.sql("DROP TABLE IF EXISTS workspace.silver.crm_sales_details")