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

# Notebook 02: facebook_silver_transform.py (PySpark)
from pyspark.sql import functions as F

BRONZE_TBL = "bronze_facebook_ads"

SILVER_FACT = "silver_facebook_ads_fact"
SILVER_CAMPAIGN_DIM = "silver_fb_campaign_dim"
SILVER_ADSET_DIM = "silver_fb_adset_dim"
SILVER_AD_DIM = "silver_fb_ad_dim"

bronze = spark.table(BRONZE_TBL)

# --- Dimensions ---
campaign_dim = (bronze
  .select("campaign_id","campaign_name","account","account_name")
  .dropDuplicates()
)
campaign_dim.write.mode("overwrite").format("delta").saveAsTable(SILVER_CAMPAIGN_DIM)

adset_dim = (bronze
  .select("adset_id","adset_name","campaign_id","campaign_name")
  .dropDuplicates()
)
adset_dim.write.mode("overwrite").format("delta").saveAsTable(SILVER_ADSET_DIM)

ad_dim = (bronze
  .select("ad_id","ad_name","adset_id","adset_name","ad_creation_time_ts","ad_delivery_status","objective")
  .dropDuplicates()
)
ad_dim.write.mode("overwrite").format("delta").saveAsTable(SILVER_AD_DIM)

# --- Fact (aggregate to ad_id x event_date x age x gender x region) ---
fact = (bronze
  .groupBy("ad_id","event_date","age","gender","region")
  .agg(
      F.sum("impressions").alias("impressions"),
      F.sum("clicks").alias("clicks"),
      F.sum("spend").alias("spend"),
      F.sum("conversions").alias("conversions"),
      F.sum("conversion_value").alias("conversion_value"),
      F.sum("video_plays").alias("video_plays"),
      F.sum("page_engagement").alias("page_engagement"),
      F.sum("reach").alias("reach")
  )
  .withColumn("ctr", F.when(F.col("impressions")>0, F.col("clicks")/F.col("impressions")*100).otherwise(F.lit(0.0)))
  .withColumn("cpc", F.when(F.col("clicks")>0, F.col("spend")/F.col("clicks")).otherwise(F.lit(None)))
  .withColumn("cpm", F.when(F.col("impressions")>0, F.col("spend")/F.col("impressions")*1000).otherwise(F.lit(None)))
  .withColumn("purchase_roas", F.when(F.col("spend")>0, F.col("conversion_value")/F.col("spend")).otherwise(F.lit(None)))
)

(fact.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .partitionBy("event_date")
   .saveAsTable(SILVER_FACT))

display(spark.table(SILVER_FACT).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from silver_facebook_ads_fact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
