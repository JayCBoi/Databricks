# Databricks notebook source
# MAGIC %md
# MAGIC # Marketing report using data from DataSimulation notebook.
# MAGIC Notebook is going to run as a daily job, calculating marketing KPI's.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now daily_stats are merged into daily_kpi_report
# MAGIC -- as a result, we get a table that contains daily stats for each channel
# MAGIC -- all needed to be done now is to schedule the task to run daily
# MAGIC WITH daily_stats AS (
# MAGIC   SELECT
# MAGIC     cl.channel,
# MAGIC     COUNT(cl.click_id) AS total_clicks,
# MAGIC     COUNT(p.purchase_id) AS total_purchases,
# MAGIC     ROUND( (COUNT(p.purchase_id) / NULLIF(COUNT(cl.click_id), 0) ) * 100, 2 ) AS conversion_rate_percent,
# MAGIC     SUM(COALESCE(p.revenue, 0)) AS total_revenue,
# MAGIC     :execution_date AS report_date
# MAGIC   FROM campaign_clicks cl
# MAGIC   LEFT JOIN purchases p ON cl.click_id = p.click_id
# MAGIC   WHERE to_date(cl.click_timestamp) = to_date(:execution_date, 'yyyy-MM-dd')
# MAGIC   GROUP BY cl.channel
# MAGIC )
# MAGIC
# MAGIC MERGE INTO daily_kpi_report AS target
# MAGIC USING daily_stats AS source
# MAGIC ON target.channel = source.channel AND target.report_date = source.report_date
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;