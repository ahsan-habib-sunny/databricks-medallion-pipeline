# Databricks notebook source
from pyspark.sql.functions import (
    col, when, row_number, lit, coalesce
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Read Silver tables
# let's read all the data tables of silver layers

df_crm_cust   = spark.read.table("`ahs-corporation`.silver.crm_cust_info")
df_erp_cust   = spark.read.table("`ahs-corporation`.silver.erp_cust_az12")
df_erp_loc    = spark.read.table("`ahs-corporation`.silver.erp_loc_a101")
df_crm_prd    = spark.read.table("`ahs-corporation`.silver.crm_prd_info")
df_erp_cat    = spark.read.table("`ahs-corporation`.silver.erp_px_cat")
df_crm_sales  = spark.read.table("`ahs-corporation`.silver.crm_sales_details")

# COMMAND ----------

# DBTITLE 1,Building the dimension customer table
# Join CRM customers with ERP customer and location data
df_dim_customers = df_crm_cust.alias("dcc") \
                              .join(df_erp_cust.alias("dec"), col("dcc.cst_key") == col("dec.cid"), "left") \
                              .join(df_erp_loc.alias("del"), col("dec.cid") == col("del.cid"), "left")

# Apply gender logic — prefer CRM gender, fall back to ERP
df_dim_customers = df_dim_customers.withColumn("gender",
                                      when(col("cst_gndr") != "n/a", col("cst_gndr"))
                                      .otherwise(coalesce(col("gen"), lit("n/a"))))

# Add surrogate key
window_spec = Window.orderBy("cst_id")
df_dim_customers = df_dim_customers.withColumn("customer_key", row_number().over(window_spec))

# Select final columns
df_dim_customers = df_dim_customers.select(
                                        col("customer_key"),
                                        col("cst_id").alias("customer_id"),
                                        col("cst_key").alias("customer_number"),
                                        col("cst_firstname").alias("first_name"),
                                        col("cst_lastname").alias("last_name"),
                                        col("cntry").alias("country"),
                                        col("cst_marital_status").alias("marital_status"),
                                        col("gender"),
                                        col("bdate").alias("birthdate"),
                                        col("cst_create_date").alias("create_date"))

# Write to gold
df_dim_customers.write.format("delta").mode("overwrite").saveAsTable("`ahs-corporation`.gold.dim_customers")

print("gold.dim_customers loaded successfully!")



# COMMAND ----------

# DBTITLE 1,Building the dimension product table
# Filter active products only (prd_end_dt is NULL)
df_active_prd = df_crm_prd.filter(col("prd_end_dt").isNull())

# Deduplicate — keep one row per product key, prioritise active and most recent
window_prod_spec = Window.partitionBy("prd_key") \
                         .orderBy(when(col("prd_end_dt").isNull(),0).otherwise(1),
                                  col("prd_start_dt").desc(),
                                  col("prd_id"))
                         
df_active_prd = df_active_prd.withColumn('rn', row_number().over(window_prod_spec)).filter(col("rn") == 1).drop(col("rn"))

# Join with product category

df_dim_products = df_active_prd.alias("dap") \
                               .join(df_erp_cat.alias("dec"), col("dap.cat_id") == col("dec.id"), "left")

# Add surrogate key
window_prod_key = Window.orderBy("prd_key","prd_start_dt")
df_dim_products = df_dim_products.withColumn("product_key", row_number().over(window_prod_key))

# Select final columns
df_dim_products = df_dim_products.select(
    col("product_key"),
    col("prd_id").alias("product_id"),
    col("prd_key").alias("product_number"),
    col("prd_nm").alias("product_name"),
    col("cat_id").alias("category_id"),
    col("cat").alias("category"),
    col("subcat").alias("subcategory"),
    col("maintenance").alias("maintenance"),
    col("prd_line").alias("product_line"),
    col("prd_cost").alias("product_cost"),
    col("prd_start_dt").alias("start_date")
)

# Write to gold
df_dim_products.write.format("delta") \
               .mode("overwrite") \
               .saveAsTable("`ahs-corporation`.gold.dim_products")


# COMMAND ----------

# DBTITLE 1,Building the fact sales table
# Read gold dimensions we just created
df_dim_customers = spark.read.table("`ahs-corporation`.gold.dim_customers")
df_dim_products  = spark.read.table("`ahs-corporation`.gold.dim_products")

# Join sales with dimensions
df_fact_sales = df_crm_sales.alias("dcs") \
    .join(df_dim_products.alias("ddp"),  col("dcs.sls_prd_key") == col("ddp.product_number"),  "left") \
    .join(df_dim_customers.alias("ddc"), col("dcs.sls_cust_id") == col("ddc.customer_id"), "left")

# Select final columns
df_fact_sales = df_fact_sales.select(
    col("sls_ord_num").alias("order_number"),
    col("product_key"),
    col("customer_key"),
    col("sls_order_dt").alias("order_date"),
    col("sls_ship_dt").alias("shipping_date"),
    col("sls_due_dt").alias("due_date"),
    col("sls_sales").alias("sales_amount"),
    col("sls_quantity").alias("quantity"),
    col("sls_price").alias("price")
)

# Write to gold
df_fact_sales.write.format("delta") \
             .mode("overwrite") \
             .saveAsTable("`ahs-corporation`.gold.fact_sales")

print("gold.fact_sales loaded successfully!")
