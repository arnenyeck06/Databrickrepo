# Databricks notebook source
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

display(df)
(

    df.write
      .mode("overwrite")
      .saveAsTable("workspace.bronze.crm_cust_info")
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Product info

# COMMAND ----------


## prd_info
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/bronze/source_systems/source_crm/prd_info.csv")
)

display(df)
(
    df.write
      .mode("overwrite")
      .saveAsTable("workspace.bronze.crm_prd_info")
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Sales details info

# COMMAND ----------


## display sales details, saving in to the crm_sales_details table.
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/bronze/source_systems/source_crm/sales_details.csv")
)

display(df)
(
    df.write
      .mode("overwrite")
      .saveAsTable("workspace.bronze.crm_sales_details")
)


# COMMAND ----------

# MAGIC %md
# MAGIC #ERP cust
# MAGIC

# COMMAND ----------


## display ERP sales, saving in to the erp customer table.
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/bronze/source_systems/source_erp/CUST_AZ12.csv")
)


(
    df.write
      .mode("overwrite")
      .saveAsTable("workspace.bronze.erp_customer")
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # ERP Location

# COMMAND ----------


## display LOC-location, saving in to the erp_location table.
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/bronze/source_systems/source_erp/LOC_A101.csv")
)


(
    df.write
      .mode("overwrite")
      .saveAsTable("workspace.bronze.erp_location")
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # PX Category

# COMMAND ----------


## display LOC-location, saving in to the erp_location table.
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/bronze/source_systems/source_erp/PX_CAT_G1V2.csv")
)


(
    df.write
      .mode("overwrite")
      .saveAsTable("workspace.bronze.erp_category")
)
display(df)

# COMMAND ----------

 

# COMMAND ----------

# Save as bronze.ipnyb
#from bronze_config import INGESTION_CONFIG


# COMMAND ----------

#%sql
#USE CATALOG workspace;
#USE SCHEMA bronze;

# COMMAND ----------

# ## READ FROM CSV AND WRITE TO SILVER
# for item in INGESTION_CONGIF:
#     print(f"Ingesting {item['source']} -> silver.{item['table']}")
#     df = (
#         spark.read
#              .option("header", "true")
#              .option("inferSchema", "true")
#              .csv(item["path"])
#     )
#     (
#         df.write
#           .mode("overwrite")
#           .format("delta")
#           .saveAsTable(f"silver.{item['table']}")