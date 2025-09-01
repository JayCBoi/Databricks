# Databricks notebook source
# Ingest procedure:
# 1.Download the file to the Volume
# 2.Read the CSV into a DataFrame
# 3.Clean column names - remove spaces and special characters
# 4.Write to Bronze table

# Widget Parameters
dbutils.widgets.text("catalog", "marketing", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("volume", "testingvolume", "Volume Name")
dbutils.widgets.text("download_url", "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv", "Source URL")
dbutils.widgets.text("file_name", "baby_names.csv", "File Name")
dbutils.widgets.text("table_name", "baby_names_bronze", "Bronze Table Name")

# Get the parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
download_url = dbutils.widgets.get("download_url")
file_name = dbutils.widgets.get("file_name")
table_name = dbutils.widgets.get("table_name")

path_volume = f"/Volumes/{catalog}/{schema}/{volume}"
path_table = f"{catalog}.{schema}.{table_name}"


# Checks
print(f"Downloading from: {download_url}")
print(f"Target Volume path: {path_volume}")
print(f"Target Bronze Table: {path_table}")

# 1. Get the file into the Volume
dbutils.fs.cp(download_url, f"{path_volume}/{file_name}")

# 2. Read the CSV into a DataFrame
df = spark.read.csv(f"{path_volume}/{file_name}", header=True, inferSchema=True, sep=",")

# 3. Clean column names - remove spaces and special characters

# Funtion for name cleanup
def clean_column_name(name):
    return name.replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "").replace("/", "_")

# Cleaning all rows
for col_name in df.columns:
    new_name = clean_column_name(col_name)
    if new_name != col_name:
        df = df.withColumnRenamed(col_name, new_name)

print("Cleaned columns:", df.columns)

# 4. Writing into Bronze catalog
df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(path_table)

# 5. Confirmation
record_count = df.count()
print(f"Ingestion complete! Loaded {record_count} records into {path_table}.")
display(df.limit(5))

dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

