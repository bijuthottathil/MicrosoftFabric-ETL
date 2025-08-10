# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b7813ff0-e30a-4b20-b4e3-d651b2927789",
# META       "default_lakehouse_name": "oillakehouse",
# META       "default_lakehouse_workspace_id": "c6813d3f-1905-4ac4-963d-66a7deed957b",
# META       "known_lakehouses": [
# META         {
# META           "id": "b7813ff0-e30a-4b20-b4e3-d651b2927789"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# --------- CONFIG ---------
RAW_PATH = "abfss://Oil_EventStreamWorkspace@onelake.dfs.fabric.microsoft.com/oillakehouse.Lakehouse/Files/raw/oil_events"
BRONZE_PATH = "abfss://Oil_EventStreamWorkspace@onelake.dfs.fabric.microsoft.com/oillakehouse.Lakehouse/Files/bronze/oil_events_delta"
BRONZE_TABLE = "bronze_oil_events"      # name you'll use in SQL

# If your raw files are partitioned (e.g., date=YYYY-MM-DD/hour=HH), Spark will read them all.
# Adjust the schema below if your raw JSON differs.

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql import functions as F

# --------- SCHEMA (matches the simulator we built) ---------
measures_schema = StructType([
    StructField("pressure_psi", DoubleType(), True),
    StructField("temperature_f", DoubleType(), True),
    StructField("flow_bbl_per_hr", DoubleType(), True),
    StructField("watercut_pct", DoubleType(), True),
    StructField("vibration_mm_s", DoubleType(), True),
    StructField("choke_pct", DoubleType(), True),
])

raw_schema = StructType([
    StructField("well_id", StringType(), True),
    StructField("rig_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("measures", measures_schema, True),
    StructField("status", StringType(), True),
    StructField("event_ts", StringType(), True),  # will cast to timestamp
    StructField("ingest_src", StringType(), True),
])

# --------- READ RAW (NDJSON) ---------
raw_df = (spark.read
    .schema(raw_schema)
    .json(RAW_PATH)  # NDJSON: one JSON object per line
)

# --------- FLATTEN + TYPE FIXES ---------
bronze_df = (
    raw_df
    .withColumn("event_ts", F.to_timestamp("event_ts"))  # cast string -> timestamp
    .withColumn("event_date", F.to_date("event_ts"))
    .select(
        "well_id", "rig_id", "sensor_id", "lat", "lon",
        F.col("measures.pressure_psi").alias("pressure_psi"),
        F.col("measures.temperature_f").alias("temperature_f"),
        F.col("measures.flow_bbl_per_hr").alias("flow_bbl_per_hr"),
        F.col("measures.watercut_pct").alias("watercut_pct"),
        F.col("measures.vibration_mm_s").alias("vibration_mm_s"),
        F.col("measures.choke_pct").alias("choke_pct"),
        "status", "event_ts", "event_date", "ingest_src"
    )
)

# --------- WRITE DELTA (partition by event_date) ---------
# Use overwrite first time; afterward prefer "append" (and set mergeSchema for additive columns).
(bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(BRONZE_PATH)
)

# --------- REGISTER/CREATE EXTERNAL TABLE ---------
# Register the Delta folder at BRONZE_PATH as a table you can query with SQL.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
USING DELTA
LOCATION '{BRONZE_PATH}'
""")

print(f"Delta written to: {BRONZE_PATH}")
print(f"Table available as: {BRONZE_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from bronze_oil_events

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# notebook: bronze_to_silver_oil.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
BRONZE_TBL = "bronze_oil_events"
SILVER_TBL = "silver_oil_events"

# Read stream from the Bronze table
bronze = (
    spark.readStream.table(BRONZE_TBL)
      # Watermark to bound late data; adjust for your operations latency
      .withWatermark("event_ts", "30 minutes")
)

# Basic typing & unit normalization
silver = (
    bronze
    .withColumn("pressure_kpa", F.col("pressure_psi") * F.lit(6.894757))
    .withColumn("temperature_c", (F.col("temperature_f") - 32) * 5/9)
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .withColumn("flow_bbl_per_hr", F.col("flow_bbl_per_hr").cast("double"))
    .withColumn("event_dt", F.to_date("event_ts"))
    # Simple quality flags
    .withColumn("q_pressure_ok", F.col("pressure_psi").between(0, 10000))
    .withColumn("q_temp_ok", F.col("temperature_f").between(-40, 400))
    .withColumn("q_flow_ok", F.col("flow_bbl_per_hr") >= 0)
)



# Write to Silver Delta (mergeSchema allows additive evolution)
query = (
    silver
    .writeStream
    .option("checkpointLocation", "abfss://Oil_EventStreamWorkspace@onelake.dfs.fabric.microsoft.com/oillakehouse.Lakehouse/Files/bronze/checkpoint")
    .outputMode("append")
    .format("delta")
    .trigger(processingTime="1 minute")
    .toTable(SILVER_TBL)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from silver_oil_events limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

SILVER = "silver_oil_events"
GOLD_DAILY = "gold_well_kpi_daily"

silver_df = spark.table(SILVER)

daily = (
    silver_df
    .groupBy("well_id", F.to_date("event_ts").alias("event_date"))
    .agg(
        F.countDistinct("sensor_id").alias("sensors_reporting"),
        F.avg("pressure_kpa").alias("avg_pressure_kpa"),
        F.avg("temperature_c").alias("avg_temp_c"),
        F.avg("flow_bbl_per_hr").alias("avg_flow_bbl_hr"),
        F.max("flow_bbl_per_hr").alias("peak_flow_bbl_hr"),
        F.avg("watercut_pct").alias("avg_watercut_pct"),
        F.sum(F.when(~(F.col("q_pressure_ok") & F.col("q_temp_ok") & F.col("q_flow_ok")), 1).otherwise(0)).alias("bad_records")
    )
)

(daily
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema","true")
 .saveAsTable(GOLD_DAILY)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * from gold_well_kpi_daily

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
