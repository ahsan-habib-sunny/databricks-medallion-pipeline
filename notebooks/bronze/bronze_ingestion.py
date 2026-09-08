# Databricks notebook source
# You DO NOT need this in Databricks as spark session already starts when you create cluster
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ahs-corporation").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Defining the file path
# File paths from Volume
CRM_PATH = "/Volumes/ahs-corporation/bronze/raw_sources/CRM_Sources/"
ERP_PATH = "/Volumes/ahs-corporation/bronze/raw_sources/ERP_Sources/"

# COMMAND ----------

# DBTITLE 1,Ingest CRM Customer Info
# Ingest CRM Customer Info
from pyspark.sql.functions import current_timestamp, lit

# Read CSV
df_crm_cust = spark.read.option("header", "true").option("inferSchema", "true").csv(CRM_PATH + "cust_info.csv")

# Add metadata columns
df_crm_cust = df_crm_cust.withColumn("ingested_at", current_timestamp()) \
                          .withColumn("source_file", lit("cust_info.csv"))

# Write to bronze Delta table
df_crm_cust.write.format("delta").mode("overwrite").saveAsTable("`ahs-corporation`.bronze.crm_cust_info")

print("crm_cust_info loaded successfully!")

# COMMAND ----------

# DBTITLE 1,Ingest CRM Product Info
# Ingest CRM Product Info
from pyspark.sql.functions import current_timestamp, lit

df_crm_prd = spark.read.option("header", "true").option("inferSchema", "true") \
                        .csv(CRM_PATH + 'prd_info.csv')

# Add metadata columns
df_crm_prd = df_crm_prd.withColumn("ingested_at", current_timestamp()) \
                        .withColumn("source_file", lit("prd_info.csv"))

# write to bronze delta table
df_crm_prd.write.format("delta").mode("overwrite").saveAsTable("`ahs-corporation`.bronze.crm_prd_info")

print("crm_prd_info loaded successfully!")


# COMMAND ----------

# DBTITLE 1,Ingest CRM Sales Details
# Ingest CRM Sales Details

df_crm_sales = spark.read.option("header", "true").option("inferSchema", "true") \
                          .csv(CRM_PATH + 'sales_details.csv')

# Add metadata columns

df_crm_sales = df_crm_sales.withColumn("ingested_at", current_timestamp()) \
                            .withColumn("source_file", lit("sales_details.csv"))

# write to bronze delta table

df_crm_sales.write.format("delta").mode("overwrite").saveAsTable("`ahs-corporation`.bronze.crm_sales_details")

print("crm_sales_details loaded successfully")

# COMMAND ----------

# DBTITLE 1,Ingest ERP Customer
# Ingest ERP Customer info

df_erp_cust = spark.read.option("header","true").option("inferSchema","true") \
                        .csv(ERP_PATH + "CUST_AZ12.csv")

# Add metadata columns

df_erp_cust = df_erp_cust.withColumn("ingested_at", current_timestamp()) \
                         .withColumn("source_files", lit("CUST_AZ12.csv"))

# write to bronze delta table

df_erp_cust.write.format("delta").mode("overwrite").saveAsTable("`ahs-corporation`.bronze.erp_cust_az12")

print("erp_cust_az12 loaded successfully")

# COMMAND ----------

# DBTITLE 1,Ingest ERP Location
# Ingest ERP Location
df_erp_loc = spark.read.option("header", "true").option("inferSchema", "true").csv(ERP_PATH + "LOC_A101.csv")

# Add metadata columns
df_erp_loc = df_erp_loc.withColumn("ingested_at", current_timestamp()) \
                        .withColumn("source_file", lit("LOC_A101.csv"))

# write to bronze delta table
df_erp_loc.write.format("delta").mode("overwrite").saveAsTable("`ahs-corporation`.bronze.erp_loc_a101")

print("erp_loc_a101 loaded successfully!")

# COMMAND ----------

# DBTITLE 1,Ingest ERP Product Category
# Ingest ERP Product Category
df_erp_px_cat = spark.read.option("header", "true").option("inferSchema", "true").csv(ERP_PATH + "PX_CAT_G1V2.csv")

# Add metadata columns
df_erp_px_cat = df_erp_px_cat.withColumn("ingested_at", current_timestamp()) \
                        .withColumn("source_file", lit("PX_CAT_G1V2.csv"))

# write to bronze delta table
df_erp_px_cat.write.format("delta").mode("overwrite").saveAsTable("`ahs-corporation`.bronze.erp_px_cat")

print("erp_px_cat loaded successfully!")
