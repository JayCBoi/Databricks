# Databricks notebook source
# Widget parameters
dbutils.widgets.text("catalog", "marketing", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("silver_table", "baby_names_silver", "Silver Table Name")
dbutils.widgets.text("gold_table_prefix", "baby_names", "Gold Table Prefix")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
silver_table_path = f"{catalog}.{schema}.{dbutils.widgets.get('silver_table')}"
gold_prefix = dbutils.widgets.get("gold_table_prefix")

print(f"Reading from Silver table: {silver_table_path}")

# Read from Silver
silver_df = spark.read.table(silver_table_path)
print(f"Records available for analysis: {silver_df.count()}")

# Analysis. I will try to make 3 findings:
# 1. Top 10 most popular names each year (by sex)
# 2. Yearly summary statistics
# 3. Most popular names

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# ANALYSIS 1: Top 10 most popular names each year by sex
window_spec = Window.partitionBy("Year", "Sex").orderBy(F.desc("Count"))

top_names_df = (silver_df
    .withColumn("rank", F.rank().over(window_spec))
    .filter(F.col("rank") <= 10)
    .select("Year", "Sex", "First_Name", "Count", "County", "rank")
    .orderBy("Year", "Sex", "rank")
)

# ANALYSIS 2: Yearly summary statistics
yearly_stats_df = (silver_df
    .groupBy("Year", "Sex")
    .agg(
        F.sum("Count").alias("total_births"),
        F.count_distinct("First_Name").alias("unique_names"),
        F.avg("Count").alias("avg_name_count")
    )
    .orderBy("Year", "Sex")
)

# ANALYSIS 3: Most popular names of all time
all_time_popular_df = (silver_df
    .groupBy("First_Name", "Sex")
    .agg(F.sum("Count").alias("total_count_all_time"))
    .orderBy(F.desc("total_count_all_time"))
    .limit(20)  # Top 20 names of all time
)

# Now just to write these Gold tables into the Catalog
print("Writing Gold tables to Unity Catalog...")

top_names_path = f"{catalog}.{schema}.{gold_prefix}_top_names_by_year"
(top_names_df.write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable(top_names_path)
)

yearly_stats_path = f"{catalog}.{schema}.{gold_prefix}_yearly_stats"
(yearly_stats_df.write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable(yearly_stats_path)
)

all_time_path = f"{catalog}.{schema}.{gold_prefix}_all_time_popular"
(all_time_popular_df.write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable(all_time_path)
)

# Results
print("Analysis complete! Created Gold tables:")
print(f"  - {top_names_path} (Top names by year/sex)")
print(f"  - {yearly_stats_path} (Yearly statistics)")
print(f"  - {all_time_path} (All-time popular names)")

print("\nSample of top names for latest year:")
latest_year = silver_df.agg(F.max("Year")).first()[0]
display(top_names_df.filter(F.col("Year") == latest_year).limit(10))

print("\nYearly statistics sample:")
display(yearly_stats_df.limit(10))

print("\nAll-time popular names sample:")
display(all_time_popular_df.limit(10))

#dbutils.notebook.exit("SUCCESS: Analysis completed and Gold tables created")