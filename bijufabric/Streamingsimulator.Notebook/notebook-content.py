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

from pyspark.sql import SparkSession
import json, random, datetime as dt

# ---------- CONFIG ----------
OUT_ROOT = "abfss://Oil_EventStreamWorkspace@onelake.dfs.fabric.microsoft.com/oillakehouse.Lakehouse/Files/raw/oil_events"
EVENTS = 10000     # total events to generate
BATCH_SIZE = 1000  # number of events per file
SEED = 42

PERMIAN_LAT_RANGE = (30.5, 33.2)
PERMIAN_LON_RANGE = (-104.9, -100.5)
WELL_PREFIX = "TX-WELL-"
RIG_PREFIX = "RIG-"
SENSORS = [f"PRESS-{i:03d}" for i in range(1, 11)] + \
          [f"TEMP-{i:03d}"  for i in range(1, 11)] + \
          [f"FLOW-{i:03d}"  for i in range(1, 11)] + \
          [f"VIB-{i:03d}"   for i in range(1, 11)]

rng = random.Random(SEED)

def rand_coord():
    lat = rng.uniform(*PERMIAN_LAT_RANGE)
    lon = rng.uniform(*PERMIAN_LON_RANGE)
    return round(lat, 6), round(lon, 6)

def correlated_values():
    choke_pct = rng.uniform(20, 80)
    base_flow = 15 + (choke_pct / 80.0) * 110
    flow = max(0.0, rng.gauss(base_flow, 5.0))
    pressure_psi = max(500.0, min(9000.0, rng.gauss(2500 + (choke_pct * 35), 150)))
    temperature_f = max(60.0, min(350.0, rng.gauss(140 + (choke_pct * 0.8), 8)))
    vibration_mm_s = max(0.2, rng.gauss(2.5 + (flow / 80.0), 0.8))
    watercut_pct = rng.uniform(5, 45)
    return {
        "pressure_psi": round(pressure_psi, 2),
        "temperature_f": round(temperature_f, 2),
        "flow_bbl_per_hr": round(flow, 2),
        "watercut_pct": round(watercut_pct, 2),
        "vibration_mm_s": round(vibration_mm_s, 2),
        "choke_pct": round(choke_pct, 2),
    }

def build_event(wells, rigs):
    well_id = rng.choice(wells)
    rig_id  = rng.choice(rigs)
    sensor_id = rng.choice(SENSORS)
    lat, lon = rand_coord()
    measures = correlated_values()
    now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    evt_ts = now_utc - dt.timedelta(seconds=rng.randint(0, 90))
    return {
        "well_id": well_id,
        "rig_id": rig_id,
        "sensor_id": sensor_id,
        "lat": lat,
        "lon": lon,
        "measures": measures,
        "status": "OK" if rng.random() > 0.02 else "WARN",
        "event_ts": evt_ts.isoformat().replace("+00:00", "Z"),
        "ingest_src": "simulator"
    }

# ---------- GENERATE ----------
wells = [f"{WELL_PREFIX}{i:05d}" for i in range(100, 150)]
rigs  = [f"{RIG_PREFIX}{i:03d}" for i in range(1, 21)]

records = []
file_count = 0
remaining = EVENTS

while remaining > 0:
    n = min(BATCH_SIZE, remaining)
    for _ in range(n):
        records.append(build_event(wells, rigs))
    now_utc = dt.datetime.utcnow()
    y, m, d, h = now_utc.year, now_utc.month, now_utc.day, now_utc.hour
    partition_path = f"{OUT_ROOT}/date={y:04d}-{m:02d}-{d:02d}/hour={h:02d}/part-{file_count:05d}.json"

    # Save batch to the abfss path
    df = spark.createDataFrame([json.loads(json.dumps(r)) for r in records])
    df.write.mode("append").json(partition_path)

    print(f"Wrote {n} events to {partition_path}")
    records.clear()
    file_count += 1
    remaining -= n

print("Data generation complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.json("abfss://Oil_EventStreamWorkspace@onelake.dfs.fabric.microsoft.com/oillakehouse.Lakehouse/Files/raw/oil_events/date=2025-08-10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
