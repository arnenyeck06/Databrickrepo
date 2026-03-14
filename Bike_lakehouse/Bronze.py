# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC #Read from csv

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table workspace.bronze.crm_cust_info

# COMMAND ----------

# MAGIC %md
# MAGIC #Customers Info 

# COMMAND ----------

## customer info

df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("dbfs:/Volumes/workspace/bronze/source_systems/source_crm/cust_info.csv")
)
(

    df.write
      .mode("overwrite")
      .saveAsTable("workspace.bronze.crm_cust_info")
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion configuration.

# COMMAND ----------


INGESTION_CONFIG = [
   
    {
        "source": "crm",
        "path": "/Volumes/workspace/bronze/source_systems/source_crm/prd_info.csv",
        "table": "crm_prd_info"
    },
    {
        "source": "crm",
        "path": "/Volumes/workspace/bronze/source_systems/source_crm/sales_details.csv",
        "table": "crm_sales_details"
    },
    {
        "source": "erp",
        "path": "/Volumes/workspace/bronze/source_systems/source_erp/CUST_AZ12.csv",
        "table": "erp_customer"
    },
    {
        "source": "erp",
        "path": "/Volumes/workspace/bronze/source_systems/source_erp/LOC_A101.csv",
        "table": "erp_location"
    },
    {
        "source": "erp",
        "path": "/Volumes/workspace/bronze/source_systems/source_erp/PX_CAT_G1V2.csv",
        "table": "erp_category"
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingesting all files into bronze tables

# COMMAND ----------


for item in INGESTION_CONFIG:
    print(f"Ingesting {item['source']} → workspace.bronze.{item['table']}")

    df = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(item["path"])
    )

    (
        df.write
          .mode("overwrite")
          .format("delta")
          .saveAsTable(f"workspace.bronze.{item['table']}")
    )