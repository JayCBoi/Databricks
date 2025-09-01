# Databricks notebook source
# This notebook creates final reports and visualizations from Gold tables
dbutils.widgets.text("catalog", "marketing", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("gold_prefix", "baby_names", "Gold Table Prefix")
dbutils.widgets.text("report_table", "baby_names_insights", "Final Report Table")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
gold_prefix = dbutils.widgets.get("gold_prefix")
report_table_path = f"{catalog}.{schema}.{dbutils.widgets.get('report_table')}"

# Paths to Gold tables
top_names_path = f"{catalog}.{schema}.{gold_prefix}_top_names_by_year"
yearly_stats_path = f"{catalog}.{schema}.{gold_prefix}_yearly_stats"
all_time_path = f"{catalog}.{schema}.{gold_prefix}_all_time_popular"

print("Reading from Gold tables:")
print(f"  - {top_names_path}")
print(f"  - {yearly_stats_path}")
print(f"  - {all_time_path}")

top_names_df = spark.read.table(top_names_path)
yearly_stats_df = spark.read.table(yearly_stats_path)
all_time_df = spark.read.table(all_time_path)

# Analysis
from pyspark.sql import functions as F

# ANALYSIS 1: Trend Analysis - Most popular names over time
print("1. Analyzing naming trends over time...")
trend_analysis_df = (top_names_df
    .groupBy("First_Name", "Sex")
    .agg(
        F.count("Year").alias("years_in_top_10"),
        F.min("Year").alias("first_top_year"),
        F.max("Year").alias("last_top_year"),
        F.avg("rank").alias("average_rank")
    )
    .filter(F.col("years_in_top_10") >= 5)
    .orderBy(F.desc("years_in_top_10"), "average_rank")
)

# ANALYSIS 2: County Analysis (if data has county information)
print("2. Analyzing regional naming patterns...")
# Check if County data is available and meaningful
if "County" in top_names_df.columns:
    county_analysis_df = (top_names_df
        .groupBy("County", "First_Name", "Sex")
        .agg(F.count("Year").alias("years_popular"))
        .filter(F.col("years_popular") >= 3)
        .orderBy("County", F.desc("years_popular"))
    )
else:
    print("  - County data not available for analysis")
    county_analysis_df = None

# ANALYSIS 3: Create a comprehensive insights table
print("3. Creating comprehensive insights table...")
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create final insights table
insights_df = (trend_analysis_df
    .withColumn("analysis_type", F.lit("long_term_trend"))
    .withColumn("insight_summary", 
        F.concat(
            F.col("First_Name"), F.lit(" was in top 10 for "),
            F.col("years_in_top_10"), F.lit(" years ("),
            F.col("first_top_year"), F.lit("-"), F.col("last_top_year"),
            F.lit(") with average rank "), F.format_number("average_rank", 1)
        )
    )
    .select("analysis_type", "First_Name", "Sex", "years_in_top_10", 
            "first_top_year", "last_top_year", "average_rank", "insight_summary")
)

# Write final insights table to Unity Catalog
(insights_df.write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable(report_table_path)
)

# VISUALIZATION

# Converting to pandas, for smaller data
yearly_stats_pd = yearly_stats_df.filter(F.col("Year") >= 2010).toPandas()
top_names_pd = top_names_df.filter(F.col("Year") >= 2020).toPandas()

print("\n--- YEARLY BIRTH TRENDS ---")
display(yearly_stats_df.groupBy("Year").agg(F.sum("total_births").alias("total_births")).orderBy("Year"))

print("\n--- GENDER DISTRIBUTION TRENDS ---")
display(yearly_stats_df.groupBy("Year", "Sex").agg(F.sum("total_births").alias("total_births")).orderBy("Year", "Sex"))

print("\n--- TOP NAMES IN RECENT YEARS ---")
recent_top_names = top_names_df.filter(F.col("Year") >= 2020).select("Year", "Sex", "First_Name", "rank")
display(recent_top_names)

# Summary report
print("\n" + "="*60)
print("FINAL REPORT SUMMARY")
print("="*60)
total_years = yearly_stats_df.select("Year").distinct().count()
total_names = all_time_df.count()
avg_yearly_births = yearly_stats_df.agg(F.avg("total_births")).first()[0]

print(f"Analysis Period: {yearly_stats_df.agg(F.min('Year')).first()[0]} - {yearly_stats_df.agg(F.max('Year')).first()[0]}")
print(f"Total Years Analyzed: {total_years}")
print(f"Total Unique Names: {total_names:,}")
print(f"Average Yearly Births: {avg_yearly_births:,.0f}")
print(f"Longest-running popular name: {trend_analysis_df.first()['First_Name']} ({trend_analysis_df.first()['years_in_top_10']} years in top 10)")

#dbutils.notebook.exit("SUCCESS: Final reports and visualizations generated")