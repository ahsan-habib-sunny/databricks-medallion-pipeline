# Databricks notebook source
from pyspark.sql.functions import (
    col, trim, when, row_number, nullif, upper, lower, to_date, abs, length, current_date, substring, lit, replace, regexp_replace
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC CRM Customer Info

# COMMAND ----------

# DBTITLE 1,Read crm_cust_info from bronze
df = spark.read.table("`ahs-corporation`.bronze.crm_cust_info")
df.limit(10).display()

# COMMAND ----------

# let's check the column types
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Clean cst_id column
# step 1: cst_id is already numeric(double) let's make it integer and remove nulls and empty values if any
df = df.withColumn("cst_id", col("cst_id").cast("int"))
df = df.filter(col("cst_id").isNotNull())
print(df.schema["cst_id"].dataType)

# COMMAND ----------

# DBTITLE 1,Drop duplicates  customer details
# let's check if there's any duplicate customer data. If there's any, we will keep the latest record and drop others
total_count = df.count()
distinct_count = df.select("cst_id").distinct().count()

has_duplicates = total_count > distinct_count
print(f"Has duplicates: {has_duplicates}")
print(f"Total rows: {total_count} | Distinct cst_ids: {distinct_count}")

# let's keep those data where the records are latest for each customer

window_spec = Window.partitionBy(col("cst_id")).orderBy(col("cst_create_date").desc())
df = df.withColumn('flag_last_create_date', row_number().over(window_spec)) \
        .filter(col("flag_last_create_date") == 1) \
        .filter(col("cst_id").isNotNull()) \
        .drop(col("flag_last_create_date"))

total_count = df.count()
distinct_count = df.select("cst_id").distinct().count()

has_duplicates = total_count > distinct_count
print(f"Has duplicates: {has_duplicates}")
print(f"Total rows: {total_count} | Distinct cst_ids: {distinct_count}")

# COMMAND ----------

# DBTITLE 1,Fixing the columns that contain string type
# let's trim all the unnecessary white spaces of customer firstname, lastname, marital status and gender columns

df = df.withColumn("cst_firstname", trim(col("cst_firstname"))) \
       .withColumn("cst_lastname", trim(col("cst_lastname"))) \
       .withColumn("cst_marital_status", trim(col("cst_marital_status"))) \
       .withColumn("cst_gndr", trim(col("cst_gndr"))) 

# let's standardize marital status and gender

df = df.withColumn("cst_marital_status",when(upper(col("cst_marital_status")).isin("M", "MARRIED"), "Married") \
                                       .when(upper(col("cst_marital_status")).isin("S", "SINGLE"), "Single") \
                                       .when(upper(col("cst_marital_status")) == "D", "Divorced") \
                                       .otherwise("n/a")) \
       .withColumn("cst_gndr", when(upper(col("cst_gndr")).isin("M", "MALE"), "Male") \
                               .when(upper(col("cst_gndr")).isin("F", "FEMALE"), "Female") \
                               .otherwise("n/a"))

display(df.limit(5))     

# COMMAND ----------

# DBTITLE 1,Fix create date column
# cst_create_date column is string, let's change it to datetype
# Step 6 — Fix date format
df = df.withColumn(
    "cst_create_date",
    when(
        col("cst_create_date").rlike("^\d{4}-\d{2}-\d{2}$"),
        to_date(col("cst_create_date"), "yyyy-MM-dd")
    ).when(
        col("cst_create_date").rlike("^\d{2}/\d{2}/\d{4}$"),
        to_date(col("cst_create_date"), "dd/MM/yyyy")
    ).otherwise(None)
)

# COMMAND ----------

# DBTITLE 1,Select final columns and write to Silver
df_silver = df.select(
    "cst_id",
    "cst_key",
    "cst_firstname",
    "cst_lastname",
    "cst_marital_status",
    "cst_gndr",
    "cst_create_date"

)

df_silver.write.format("delta").mode("overwrite").saveAsTable("`ahs-corporation`.silver.crm_cust_info")

print("silver.crm_cust_info loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC CRM Product Info

# COMMAND ----------

# DBTITLE 1,Reading from bronze
# reading the data from bronze
df_prd = spark.read.table("`ahs-corporation`.bronze.crm_prd_info")
display(df_prd.limit(5))

# COMMAND ----------

# DBTITLE 1,Deduplicate
# Remove duplicates — keep one row per unique combination
window_spec = Window.partitionBy("prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt").orderBy("prd_id")

df_prd = df_prd.withColumn("rank", row_number().over(window_spec)) \
               .filter(col("rank") == 1) \
               .drop(col("rank"))


# COMMAND ----------

# DBTITLE 1,Extract cat_id and clean prd_key
from pyspark.sql.functions import substring, length, regexp_replace

# Extract cat_id from first 5 chars of prd_key, replace - with _
df_prd = df_prd.withColumn(
    "cat_id",
    regexp_replace(substring(trim(col("prd_key")), 1, 5), "-", "_")
)

# Extract prd_key from 7th character onwards
df_prd = df_prd.withColumn(
    "prd_key",
    trim(col("prd_key")))

# COMMAND ----------

# DBTITLE 1,Clean prd_cost and prd_line
# Clean prd_cost — handle nulls and negatives
df_prd = df_prd.withColumn('prd_cost',
                           when(col("prd_cost").isNull(),0) \
                          .when(col("prd_cost").cast("int") < 0, abs(col("prd_cost").cast("int"))) \
                          .otherwise(col("prd_cost").cast("int")))

# Standardise prd_line
df_prd = df_prd.withColumn(
    "prd_line",
    when(trim(col("prd_line")) == "T", "Touring")
    .when(trim(col("prd_line")) == "S", "Sports")
    .when(trim(col("prd_line")) == "R", "Road")
    .when(trim(col("prd_line")) == "M", "Mountain")
    .otherwise("n/a")
)

# COMMAND ----------

# DBTITLE 1,Fix date columns
from pyspark.sql.functions import lead

# Convert date columns
df_prd = df_prd.withColumn("prd_start_dt", to_date(col("prd_start_dt"))) \
               .withColumn("prd_end_dt", to_date(col("prd_end_dt")))

# Fix prd_end_dt using LEAD where end date is before start date
window_lead = Window.partitionBy("prd_key", "prd_nm").orderBy("prd_start_dt")

df_prd = df_prd.withColumn(
    "prd_end_dt",
    when(col("prd_end_dt").isNull(), None)
    .when(col("prd_end_dt") >= col("prd_start_dt"), col("prd_end_dt"))
    .otherwise(lead("prd_start_dt").over(window_lead))
)

# COMMAND ----------

# DBTITLE 1,Trim name and select final columns
# Trim product name
df_prd = df_prd.withColumn("prd_nm", trim(col("prd_nm")))

# Select final columns
df_prd_silver = df_prd.select(
    "prd_id",
    "prd_key",
    "cat_id",
    "prd_nm",
    "prd_cost",
    "prd_line",
    "prd_start_dt",
    "prd_end_dt"
)

# Write to silver
df_prd_silver.write.format("delta") \
             .mode("overwrite") \
             .saveAsTable("`ahs-corporation`.silver.crm_prd_info")

print("silver.crm_prd_info loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC CRM Sales Details
# MAGIC

# COMMAND ----------

# DBTITLE 1,Reading the data from bronze
# reading the sales data
df_sales = spark.read.table("`ahs-corporation`.bronze.crm_sales_details")
display(df_sales.limit(5))



# COMMAND ----------

# DBTITLE 1,Cast numeric columns and fix dates
# cast all numeric columns (sls_cust_id sls_sales sls_quantity sls_price)
df_sales = df_sales.withColumn("sls_cust_id", col("sls_cust_id").cast("int")) \
                   .withColumn("sls_sales", col("sls_sales").cast("int")) \
                   .withColumn("sls_quantity", col("sls_quantity").cast("int")) \
                   .withColumn("sls_price", col("sls_price").cast("int")) 

# Parse YYYYMMDD dates — only convert if exactly 8 characters
df_sales = df_sales.withColumn("sls_order_dt",
                        when(length(col("sls_order_dt").cast("string")) != 8, None)
                        .otherwise(to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")))

df_sales = df_sales.withColumn("sls_ship_dt",
                        when(length(col("sls_ship_dt")).cast("string") != 8, None)
                        .otherwise(to_date(col("sls_ship_dt").cast("string"), 'yyyyMMdd')))

df_sales = df_sales.withColumn("sls_due_dt",
                        when(length(col("sls_due_dt")).cast("string") != 8, None)
                        .otherwise(to_date(col("sls_due_dt").cast("string"), 'yyyyMMdd')))

# COMMAND ----------

# DBTITLE 1,Fix sales, quantity, price consistency
# Fix sls_sales
df_sales = df_sales.withColumn(
    "sls_sales",
    # 1. If sales is valid, keep it
    when(
        col("sls_sales").isNotNull() & 
        (col("sls_sales") > 0) & 
        (col("sls_sales") == col("sls_quantity") * col("sls_price")),
        col("sls_sales")
    )
    # 2. If quantity & price exist and > 0, recalculate sales
    .when(
        col("sls_quantity").isNotNull() & (col("sls_quantity") > 0) &
        col("sls_price").isNotNull() & (col("sls_price") > 0),
        abs(col("sls_quantity") * col("sls_price"))
    )
    # 3. Fallback default (e.g., set to NULL or 0)
    .otherwise(None)  # 
)

# Fix sls_quantity
df_sales = df_sales.withColumn(
    "sls_quantity",
    # 1. If sls_quantity is valid, keep it
    when(
        col("sls_quantity").isNotNull() & 
        (col("sls_quantity") > 0) & 
        (col("sls_quantity") == col("sls_sales") / col("sls_price")),
        col("sls_quantity")
    )
    # 2. If sales & price exist and > 0, recalculate sales
    .when(
        col("sls_sales").isNotNull() & (col("sls_sales") > 0) &
        col("sls_price").isNotNull() & (col("sls_price") > 0),
        abs(col("sls_sales") / col("sls_price"))
    )
    # 3. Fallback default (e.g., set to NULL or 0)
    .otherwise(None)  
)

# Fix sls_price
df_sales = df_sales.withColumn(
    "sls_price",
    # 1. If sls_price is valid, keep it
    when(
        col("sls_price").isNotNull() & 
        (col("sls_price") > 0) & 
        (col("sls_price") == col("sls_sales") / col("sls_quantity")),
        col("sls_price")
    )
    # 2. If sales & quantity exist and > 0, recalculate sales
    .when(
        col("sls_sales").isNotNull() & (col("sls_sales") > 0) &
        col("sls_quantity").isNotNull() & (col("sls_quantity") > 0),
        abs(col("sls_sales") / col("sls_quantity"))
    )
    # 3. Fallback default (e.g., set to NULL or 0)
    .otherwise(None)  
)

# COMMAND ----------

# DBTITLE 1,Write to silver layer
# Write to silver
df_sales_silver = df_sales.select(
    "sls_ord_num",
    "sls_prd_key",
    "sls_cust_id",
    "sls_order_dt",
    "sls_ship_dt",
    "sls_due_dt",
    "sls_sales",
    "sls_quantity",
    "sls_price"
)

df_sales_silver.write.format("delta") \
               .mode("overwrite") \
               .saveAsTable("`ahs-corporation`.silver.crm_sales_details")

print("silver.crm_sales_details loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ERP Customer AZ12

# COMMAND ----------

# DBTITLE 1,Read and clean
# read the erp_cust_az12 
df_erp_cust = spark.read.table("`ahs-corporation`.bronze.erp_cust_az12")
display(df_erp_cust.limit(10))

# COMMAND ----------

# DBTITLE 1,Cleaning and fixing the columns
# Strip NAS prefix only if present
df_erp_cust = df_erp_cust.withColumn("cid",
                            when(col("CID").startswith("NAS"),substring(trim(col("CID")),4,length(col("CID")))) \
                            .otherwise(trim(col("CID"))))

# Fix bdate — null out future dates and dates before 1900
df_erp_cust = df_erp_cust.withColumn("bdate",
                                when(col("BDATE") > current_date(), None) \
                                .when(col("BDATE") < to_date(lit("1900-01-01")), None) \
                                .otherwise(to_date(col("BDATE"))))

# Standardise gender
df_erp_cust = df_erp_cust.withColumn("gen",
                                when(trim(col("gen")).isin("F", "FEMALE"), "Female")
                                .when(trim(col("gen")).isin("M", "MALE"), "Male")
                                .otherwise("n/a"))


# COMMAND ----------

# DBTITLE 1,Write to Silver layer
# write to silver layer
df_erp_cust.select("cid", "bdate", "gen") \
           .write.format("delta") \
           .mode("overwrite") \
           .saveAsTable("`ahs-corporation`.silver.erp_cust_az12")

print("silver.erp_cust_az12 loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ERP Location

# COMMAND ----------

# DBTITLE 1,Read the data and fixing the table
# read the data
df_loc = spark.read.table("`ahs-corporation`.bronze.erp_loc_a101")

# Remove dashes from cid
df_loc = df_loc.withColumn("cid", regexp_replace(col("cid"), "-", ""))

# Standardise country names
df_loc = df_loc.withColumn(
    "cntry",
    when(trim(col("cntry")) == "DE", "Germany")
    .when(trim(col("cntry")).isin("US", "USA"), "United States")
    .when(trim(col("cntry")).isin("UK", "U.K."), "United Kingdom")
    .when(trim(col("cntry")) == "AUS", "Australia")
    .when(trim(col("cntry")) == "" , "n/a")
    .when(col("cntry").isNull(), "n/a")
    .otherwise(trim(col("cntry")))
)

# COMMAND ----------

# DBTITLE 1,Write to silver
# write to silver layer
df_loc.select("cid", "cntry") \
      .write.format("delta") \
      .mode("overwrite") \
      .saveAsTable("`ahs-corporation`.silver.erp_loc_a101")

print("silver.erp_loc_a101 loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ERP Product Category

# COMMAND ----------

# DBTITLE 1,Read, trim and write
df_cat = spark.read.table("`ahs-corporation`.bronze.erp_px_cat")

# Just trim all columns — no anomalies in this table
df_cat = df_cat.withColumn("id", trim(col("id"))) \
               .withColumn("cat", trim(col("cat"))) \
               .withColumn("subcat", trim(col("subcat"))) \
               .withColumn("maintenance", trim(col("maintenance")))

df_cat.select("id", "cat", "subcat", "maintenance") \
      .write.format("delta") \
      .mode("overwrite") \
      .saveAsTable("`ahs-corporation`.silver.erp_px_cat")

print("silver.erp_px_cat_g1v2 loaded successfully!")
