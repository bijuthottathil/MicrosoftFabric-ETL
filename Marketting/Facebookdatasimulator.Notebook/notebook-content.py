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

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Notebook 00: facebook_simulator_generate_csvs.py (PySpark)
from pyspark.sql import functions as F, types as T
import random, datetime as dt

# ---------- CONFIG ----------
ABFSS_ROOT = "./builtin/facebookfiles"
RAW_DIR = f"{ABFSS_ROOT}/raw/facebook"     # will create date=YYYY-MM-DD subfolders
DAYS = 5                                   # how many days to generate (back from today)
ROWS_PER_DAY = 5000
SEED = 42
rng = random.Random(SEED)

# Static dictionaries
currencies = ["USD"]
objectives = ["Conversions", "Traffic", "Video Views", "Engagement"]
ages = ["18-24","25-34","35-44","45-54","55-64","65+"]
genders = ["male","female"]
regions = ["North America","Europe","APAC","LATAM","Middle East & Africa"]
statuses = ["ACTIVE","PAUSED"]

accounts = [("acct_{}".format(i), f"Account_{i%50}") for i in range(1000, 1050)]
campaigns = [("cmp_{}".format(60000+i), f"Campaign_{i%30}") for i in range(1, 100)]
adsets = [("adset_{}".format(30000+i), f"AdSet_{i%60}") for i in range(1, 200)]
ads = [("ad_{}".format(700000+i), f"Ad_{i%200}") for i in range(1, 2000)]

def one_row(date_str):
    account_id, account_name = rng.choice(accounts)
    campaign_id, campaign_name = rng.choice(campaigns)
    adset_id, adset_name = rng.choice(adsets)
    ad_id, ad_name = rng.choice(ads)
    obj = rng.choice(objectives)
    status = rng.choice(statuses)
    age = rng.choice(ages)
    gender = rng.choice(genders)
    region = rng.choice(regions)
    currency = rng.choice(currencies)

    impressions = rng.randint(100, 20000)
    clicks = rng.randint(0, max(1, impressions // 3))
    spend = round(rng.uniform(1.0, 1000.0), 2) if impressions > 0 else 0.0
    conversions = rng.randint(0, max(1, clicks // 5))
    conv_value = round(conversions * rng.uniform(0.5, 8.0), 2)
    video_plays = rng.randint(0, impressions)
    page_engagement = rng.randint(clicks, clicks + rng.randint(0, 500))

    ctr = round((clicks / impressions) * 100, 2) if impressions else 0.0
    cpc = round(spend / clicks, 2) if clicks else None
    cpm = round((spend / impressions) * 1000, 2) if impressions else None
    roas = round((conv_value / spend), 2) if spend > 0 else None
    reach = rng.randint(impressions//2, impressions*2)
    frequency = round(reach / max(1, impressions), 2)

    ad_creation_time = (dt.datetime.strptime(date_str, "%Y-%m-%d") - dt.timedelta(days=rng.randint(10,120))).strftime("%Y-%m-%d %H:%M:%S")

    return (currency, account_id, account_name, campaign_id, campaign_name,
            adset_id, adset_name, ad_id, ad_name, ad_creation_time, status, obj,
            clicks, impressions, reach, frequency, ctr, cpc, cpm, spend,
            video_plays, page_engagement, conversions, conv_value, roas, 0, age, gender, region, date_str)

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

today = dt.date.today()
for d in range(DAYS):
    day = today - dt.timedelta(days=d)
    ds = day.strftime("%Y-%m-%d")
    rows = [one_row(ds) for _ in range(ROWS_PER_DAY)]
    df = spark.createDataFrame(rows, schema=schema)
    out = f"{RAW_DIR}/date={ds}/facebook_{ds}.csv"
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("header","true")
       .csv(out))
    print(f"Wrote {ROWS_PER_DAY} rows → {out}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
