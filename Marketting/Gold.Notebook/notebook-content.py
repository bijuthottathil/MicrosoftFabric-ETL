# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8fdbb7e2-f746-4e17-a891-f8a3ed8754de",
# META       "default_lakehouse_name": "FacebookLakehouse",
# META       "default_lakehouse_workspace_id": "0f013a70-fe67-4fa5-b30c-90b74f808a83",
# META       "known_lakehouses": [
# META         {
# META           "id": "8fdbb7e2-f746-4e17-a891-f8a3ed8754de"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Notebook 03: facebook_gold_aggregations.py (PySpark)
from pyspark.sql import functions as F

SILVER_FACT = "silver_facebook_ads_fact"

GOLD_DAILY_CAMPAIGN = "gold_fb_daily_campaign"
GOLD_DAILY_CAMPAIGN_REGION = "gold_fb_daily_campaign_region_age_gender"
GOLD_SUMMARY = "gold_fb_summary_latest7d"

s = spark.table(SILVER_FACT)

# A) Daily campaign KPIs
daily_campaign = (s.join(spark.table("silver_fb_ad_dim"), "ad_id", "left")
                    .join(spark.table("silver_fb_adset_dim"), "adset_id", "left")
                    .join(spark.table("silver_fb_campaign_dim"), "campaign_id", "left")
                    .groupBy("campaign_id","silver_fb_adset_dim.campaign_name","event_date")
                    .agg(
                        F.sum("impressions").alias("impressions"),
                        F.sum("clicks").alias("clicks"),
                        F.sum("spend").alias("spend"),
                        F.sum("conversions").alias("conversions"),
                        F.sum("conversion_value").alias("conversion_value")
                    )
                    .withColumn("ctr", F.when(F.col("impressions")>0, F.col("clicks")/F.col("impressions")*100).otherwise(F.lit(0.0)))
                    .withColumn("cpc", F.when(F.col("clicks")>0, F.col("spend")/F.col("clicks")).otherwise(F.lit(None)))
                    .withColumn("cpm", F.when(F.col("impressions")>0, F.col("spend")/F.col("impressions")*1000).otherwise(F.lit(None)))
                    .withColumn("purchase_roas", F.when(F.col("spend")>0, F.col("conversion_value")/F.col("spend")).otherwise(F.lit(None)))
                 )
daily_campaign.write.mode("overwrite").format("delta").saveAsTable(GOLD_DAILY_CAMPAIGN)

# B) Campaign x Region x Age x Gender

daily_camp_seg = (s.join(spark.table("silver_fb_ad_dim"), "ad_id", "left")
                    .join(spark.table("silver_fb_adset_dim"), "adset_id", "left")
                    .join(spark.table("silver_fb_campaign_dim"), "campaign_id", "left")
                    .groupBy("campaign_id","silver_fb_adset_dim.campaign_name","event_date","region","age","gender")
                    .agg(
                        F.sum("impressions").alias("impressions"),
                        F.sum("clicks").alias("clicks"),
                        F.sum("spend").alias("spend"),
                        F.sum("conversions").alias("conversions"),
                        F.sum("conversion_value").alias("conversion_value")
                    )
                    .withColumn("ctr", F.when(F.col("impressions")>0, F.col("clicks")/F.col("impressions")*100).otherwise(F.lit(0.0)))
                    .withColumn("cpc", F.when(F.col("clicks")>0, F.col("spend")/F.col("clicks")).otherwise(F.lit(None)))
                    .withColumn("cpm", F.when(F.col("impressions")>0, F.col("spend")/F.col("impressions")*1000).otherwise(F.lit(None)))
                    .withColumn("purchase_roas", F.when(F.col("spend")>0, F.col("conversion_value")/F.col("spend")).otherwise(F.lit(None)))
                 )
daily_camp_seg.write.mode("overwrite").format("delta").saveAsTable(GOLD_DAILY_CAMPAIGN_REGION)

# C) Rolling 7-day summary (all campaigns)
latest7 = (s.filter(F.col("event_date") >= F.date_sub(F.current_date(), 7))
             .groupBy()
             .agg(
                 F.sum("impressions").alias("impressions"),
                 F.sum("clicks").alias("clicks"),
                 F.sum("spend").alias("spend"),
                 F.sum("conversions").alias("conversions"),
                 F.sum("conversion_value").alias("conversion_value")
             )
             .withColumn("ctr", F.when(F.col("impressions")>0, F.col("clicks")/F.col("impressions")*100).otherwise(F.lit(0.0)))
             .withColumn("cpc", F.when(F.col("clicks")>0, F.col("spend")/F.col("clicks")).otherwise(F.lit(None)))
             .withColumn("cpm", F.when(F.col("impressions")>0, F.col("spend")/F.col("impressions")*1000).otherwise(F.lit(None)))
             .withColumn("purchase_roas", F.when(F.col("spend")>0, F.col("conversion_value")/F.col("spend")).otherwise(F.lit(None)))
          )
latest7.write.mode("overwrite").format("delta").saveAsTable(GOLD_SUMMARY)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Quick peek
display(spark.table(GOLD_DAILY_CAMPAIGN).orderBy(F.col("event_date")).limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from gold_fb_summary_latest7d

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from gold_fb_daily_campaign_region_age_gender

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
