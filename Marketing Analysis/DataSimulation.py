# Databricks notebook source
# MAGIC %md
# MAGIC # Simulating data with two tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- campaign and clicks
# MAGIC CREATE OR REPLACE TABLE campaign_clicks (
# MAGIC   click_id STRING,
# MAGIC   campaign_id STRING,
# MAGIC   channel STRING, -- set to Email, Facebook or Google
# MAGIC   user_id STRING,
# MAGIC   click_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- 20 records to work with
# MAGIC INSERT INTO campaign_clicks VALUES
# MAGIC ('click_001', 'campaign_spring', 'Email', 'user_1', current_timestamp()),
# MAGIC ('click_002', 'campaign_spring', 'Facebook', 'user_2', current_timestamp()),
# MAGIC ('click_003', 'campaign_summer', 'Google', 'user_3', current_timestamp()),
# MAGIC ('click_004', 'campaign_spring', 'Email', 'user_1', current_timestamp()),
# MAGIC ('click_005', 'campaign_summer', 'Facebook', 'user_4', current_timestamp()),
# MAGIC ('click_006', 'campaign_summer', 'Google', 'user_5', current_timestamp()),
# MAGIC ('click_007', 'campaign_spring', 'Email', 'user_6', current_timestamp()),
# MAGIC ('click_008', 'campaign_summer', 'Facebook', 'user_7', current_timestamp()),
# MAGIC ('click_009', 'campaign_spring', 'Google', 'user_8', current_timestamp()),
# MAGIC ('click_010', 'campaign_summer', 'Email', 'user_9', current_timestamp()),
# MAGIC ('click_011', 'campaign_spring', 'Facebook', 'user_10', current_timestamp()),
# MAGIC ('click_012', 'campaign_summer', 'Google', 'user_11', current_timestamp()),
# MAGIC ('click_013', 'campaign_spring', 'Email', 'user_12', current_timestamp()),
# MAGIC ('click_014', 'campaign_summer', 'Facebook', 'user_13', current_timestamp()),
# MAGIC ('click_015', 'campaign_spring', 'Google', 'user_14', current_timestamp()),
# MAGIC ('click_016', 'campaign_summer', 'Email', 'user_15', current_timestamp()),
# MAGIC ('click_017', 'campaign_spring', 'Facebook', 'user_2', current_timestamp()),
# MAGIC ('click_018', 'campaign_summer', 'Google', 'user_4', current_timestamp()),
# MAGIC ('click_019', 'campaign_spring', 'Email', 'user_16', current_timestamp()),
# MAGIC ('click_020', 'campaign_summer', 'Facebook', 'user_17', current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- table for purchases
# MAGIC CREATE OR REPLACE TABLE purchases (
# MAGIC   purchase_id STRING,
# MAGIC   click_id STRING, -- bounds click with purchase
# MAGIC   revenue DOUBLE,
# MAGIC   purchase_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- 10 records, with timestamps simulating purchase some time after the click happened
# MAGIC INSERT INTO purchases VALUES
# MAGIC ('purchase_101', 'click_001', 29.99, current_timestamp() + interval 45 minutes), 
# MAGIC ('purchase_102', 'click_003', 99.99, current_timestamp() + interval 75 minutes),
# MAGIC ('purchase_103', 'click_005', 14.50, current_timestamp() + interval 90 minutes),
# MAGIC ('purchase_104', 'click_007', 45.00, current_timestamp() + interval 40 minutes),
# MAGIC ('purchase_105', 'click_009', 200.00, current_timestamp() + interval 105 minutes),
# MAGIC ('purchase_106', 'click_011', 32.50, current_timestamp() + interval 30 minutes),
# MAGIC ('purchase_107', 'click_013', 19.99, current_timestamp() + interval 45 minutes),
# MAGIC ('purchase_108', 'click_015', 87.25, current_timestamp() + interval 50 minutes),
# MAGIC ('purchase_109', 'click_017', 12.00, current_timestamp() + interval 45 minutes),
# MAGIC ('purchase_110', 'click_020', 65.80, current_timestamp() + interval 45 minutes);

# COMMAND ----------

# MAGIC %md
# MAGIC # Number of clicks per campaign:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   channel,
# MAGIC   COUNT(click_id) AS total_clicks
# MAGIC FROM campaign_clicks
# MAGIC GROUP BY channel
# MAGIC ORDER BY total_clicks DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # Number of _unique_ users per campaign:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   campaign_id,
# MAGIC   COUNT(DISTINCT user_id) AS unique_users
# MAGIC FROM campaign_clicks
# MAGIC GROUP BY campaign_id;

# COMMAND ----------

# MAGIC %md
# MAGIC # CLICK-THROUGH rate, which clicks ended up in user purchasing:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   cl.channel,
# MAGIC   COUNT(cl.click_id) AS total_clicks,
# MAGIC   COUNT(p.purchase_id) AS total_purchases,
# MAGIC   ROUND( (COUNT(p.purchase_id) / COUNT(cl.click_id)) * 100, 2 ) AS conversion_rate_percent,
# MAGIC   SUM(COALESCE(p.revenue, 0)) AS total_revenue
# MAGIC FROM campaign_clicks cl
# MAGIC LEFT JOIN purchases p ON cl.click_id = p.click_id -- L
# MAGIC GROUP BY cl.channel
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # OBSERVATIONS
# MAGIC We can see that even though Google channel has _the least_ number of clicks, it generated the most revenue. Also noticible is that the facebook has the best CTR, and E-Mail fails in both those categories.