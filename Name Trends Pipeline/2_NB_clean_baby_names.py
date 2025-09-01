# Databricks notebook source
# Widget parameters
dbutils.widgets.text("catalog", "marketing", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("bronze_table", "baby_names_bronze", "Bronze Table Name")
dbutils.widgets.text("silver_table", "baby_names_silver", "Silver Table Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bronze_table_path = f"{catalog}.{schema}.{dbutils.widgets.get('bronze_table')}"
silver_table_path = f"{catalog}.{schema}.{dbutils.widgets.get('silver_table')}"

print(f"Reading from Bronze table: {bronze_table_path}")
print(f"Writing to Silver table: {silver_table_path}")

# Read from Bronze
bronze_df = spark.read.table(bronze_table_path)
print("Source Bronze table schema:")
bronze_df.printSchema()

# Data Quality Checks & Cleaning
# 1. Check for missing values
print("Checking for missing values...")
from pyspark.sql.functions import col, sum as spark_sum

# Count nulls in each column
null_counts = bronze_df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in bronze_df.columns])
print("Null counts per column:")
display(null_counts)

# 2. Data Cleaning & Transformation
silver_df = (bronze_df
    .dropDuplicates() # Duplicates
    .filter(col("Count").isNotNull()) # NULL count
    .filter(col("Count") > 0)  # Non-positive count
    .filter(col("Year") >= 1900)  # Year filter
)

# 3. Data Validation
initial_count = bronze_df.count()
final_count = silver_df.count()
data_loss_percentage = ((initial_count - final_count) / initial_count) * 100
data_loss_percentage_treshold = 10

print(f"Data quality report:")
print(f"  Initial records: {initial_count}")
print(f"  Final records: {final_count}")
print(f"  Records removed: {initial_count - final_count}")
print(f"  Data loss: {data_loss_percentage:.2f}%")

if data_loss_percentage > data_loss_percentage_treshold:
    print(f"WARNING: More than {data_loss_percentage_treshold}% of data was removed!")

# In real scenario, task should exit with error code here, ex.:
# dbutils.notebook.exit("FAIL: Excessive data loss during cleaning")

# 4. Write to Silver table
(silver_df.write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable(silver_table_path)
)

print(f"Data cleaning complete! Silver table created at: {silver_table_path}")
print(f"Final record count: {silver_df.count()}")
print("\nSample of Silver data:")
display(silver_df.limit(10))

#dbutils.notebook.exit("SUCCESS: Data cleaning completed successfully")