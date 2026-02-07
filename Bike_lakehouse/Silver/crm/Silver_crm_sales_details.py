# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

from pyspark.sql.functions import col, to_date, try_to_date


# COMMAND ----------

df = spark.table("workspace.bronze.crm_sales_details")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS matching_rows
# MAGIC FROM silver.crm_sales_details s
# MAGIC JOIN silver.crm_products p
# MAGIC     ON UPPER(TRIM(s.product_key)) = UPPER(TRIM(p.product_key));
# MAGIC

# COMMAND ----------

## change column names to full names
##format order date to date format
##format ship date to date format
##format due date to date format
##format sales to float
##format quantity to integer
##format price to float
##format customer id to integer
##format product id to integer
## renaming columns


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
df.display()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df = df.withColumn("order_date", try_to_date(col("order_date"), "yyyyMMdd")) \
       .withColumn("ship_date", try_to_date(col("ship_date"), "yyyyMMdd")) \
       .withColumn("due_date", try_to_date(col("due_date"), "yyyyMMdd"))
       
df.display()

# COMMAND ----------

# DBTITLE 1,Cell 8
## write data into silver table.
(
    df.write
    .mode("overwrite")
    #.option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("silver.crm_sales_details")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH monthly_sales AS (
# MAGIC     SELECT 
# MAGIC         s.customer_id,
# MAGIC         DATE_TRUNC('month', s.order_date) AS month,
# MAGIC         SUM(s.quantity * s.price) AS total_sales,
# MAGIC         p.product_id,
# MAGIC         p.product_name,
# MAGIC         p.product_cost,
# MAGIC         p.product_size
# MAGIC     FROM silver.crm_sales_details s
# MAGIC     JOIN silver.crm_products p
# MAGIC         ON UPPER(TRIM(s.product_key)) = UPPER(TRIM(p.product_key))
# MAGIC     GROUP BY 
# MAGIC         s.customer_id,
# MAGIC         DATE_TRUNC('month', s.order_date),
# MAGIC         p.product_id,
# MAGIC         p.product_name,
# MAGIC         p.product_cost,
# MAGIC         p.product_size
# MAGIC ),
# MAGIC ranked_sales AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         month,
# MAGIC         total_sales,
# MAGIC         product_id,
# MAGIC         product_name,
# MAGIC         product_cost,
# MAGIC         product_size,
# MAGIC         RANK() OVER (PARTITION BY month ORDER BY total_sales DESC) AS sales_rank
# MAGIC     FROM monthly_sales
# MAGIC )
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     month,
# MAGIC     total_sales,
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_cost,
# MAGIC     product_size
# MAGIC FROM ranked_sales
# MAGIC WHERE sales_rank = 1
# MAGIC ORDER BY month, customer_id;
# MAGIC

# COMMAND ----------

