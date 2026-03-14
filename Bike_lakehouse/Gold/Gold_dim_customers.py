# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

spark.sql("SELECT CID, birthdate FROM silver.erp_customers LIMIT 10").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write it Gold Table

# COMMAND ----------

query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS cust_surrogate_key,
    ci.customer_id,
    ci.customer_key,
    ci.firstname,
    ci.lastname,
    la.country,
    ci.marital_status,
    CASE
        WHEN ci.gender <> 'n/a' THEN ci.gender
        ELSE COALESCE(ca.gender, 'n/a')
    END AS gender,
    ca.birthdate   AS birthdate,
    ci.create_date AS create_date
FROM workspace.silver.crm_customers ci
LEFT JOIN workspace.silver.erp_customers ca
    ON ci.customer_key = ca.CID
LEFT JOIN workspace.silver.erp_location la
    ON ci.customer_key = la.CID
"""

df = spark.sql(query)
df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.dim_customers")
display(df)

# COMMAND ----------

