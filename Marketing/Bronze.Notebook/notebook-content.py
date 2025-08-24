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

# Bronze Ingestion
from pyspark.sql import functions as F, types as T

ABFSS_ROOT = "abfss://0f013a70-fe67-4fa5-b30c-90b74f808a83@onelake.dfs.fabric.microsoft.com/8fdbb7e2-f746-4e17-a891-f8a3ed8754de/Files"
RAW_PATH = f"{ABFSS_ROOT}/raw/facebook"                       # partitioned folders date=YYYY-MM-DD
BRONZE_PATH = f"{ABFSS_ROOT}/bronze/facebook_oil_events_delta"
BRONZE_TBL = "bronze_facebook_ads"

schema = T.StructType([
    T.StructField("currency", T.StringType()),
    T.StructField("account", T.StringType()),
    T.StructField("account_name", T.StringType()),
    T.StructField("campaign_id", T.StringType()),
    T.StructField("campaign_name", T.StringType()),
    T.StructField("adset_id", T.StringType()),
    T.StructField("adset_name", T.StringType()),
    T.StructField("ad_id", T.StringType()),
    T.StructField("ad_name", T.StringType()),
    T.StructField("ad_creation_time", T.StringType()),
    T.StructField("ad_delivery_status", T.StringType()),
    T.StructField("objective", T.StringType()),
    T.StructField("clicks", T.IntegerType()),
    T.StructField("impressions", T.IntegerType()),
    T.StructField("reach", T.IntegerType()),
    T.StructField("frequency", T.DoubleType()),
    T.StructField("ctr", T.DoubleType()),
    T.StructField("cpc", T.DoubleType()),
    T.StructField("cpm", T.DoubleType()),
    T.StructField("spend", T.DoubleType()),
    T.StructField("video_plays", T.IntegerType()),
    T.StructField("page_engagement", T.IntegerType()),
    T.StructField("conversions", T.IntegerType()),
    T.StructField("conversion_value", T.DoubleType()),
    T.StructField("purchase_roas", T.DoubleType()),
    T.StructField("actions", T.IntegerType()),
    T.StructField("age", T.StringType()),
    T.StructField("gender", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("date", T.StringType()),
])

raw = (spark.read
       .option("header","true")
       .schema(schema)
       .csv(RAW_PATH))

bronze = (raw
  .withColumn("event_date", F.to_date("date"))
  .withColumn("ad_creation_time_ts", F.to_timestamp("ad_creation_time"))
)

(bronze
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema","true")
 .partitionBy("event_date")
 .save(BRONZE_PATH))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TBL}
USING DELTA
LOCATION '{BRONZE_PATH}'
""")

display(spark.table(BRONZE_TBL).limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *  from bronze_facebook_ads LIMIT 20

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
