"""Local smoke test for the Databricks notebook.

Runs every Python cell of the notebook on a local Spark + Delta Lake session, using
SYNTHETIC stand-in data for `samples.nyctaxi.trips` (same schema, deliberately dirty).
It validates that the code executes and that the internal reconciliations hold.
Its numbers are NOT the project results: those come from running the notebook on Databricks.

Usage:  python local_test/run_local.py [workdir]
"""
import os
import random
import shutil
import sys
from datetime import datetime, timedelta

import matplotlib

matplotlib.use("Agg")

from pyspark.sql import SparkSession  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK = os.path.join(REPO, "notebooks", "ABD_Group_Project_NYC_Taxi_Lakehouse.py")
WORK = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "/tmp/abd_local_test")
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)

# ABD_CONNECT=1 runs through Spark Connect, the client/server protocol used by Databricks serverless
# (no SparkContext, no RDDs), which is the closest local equivalent of the target environment.
USE_CONNECT = os.environ.get("ABD_CONNECT") == "1"
builder = SparkSession.builder.remote("local[4]") if USE_CONNECT else SparkSession.builder.master("local[4]")
spark = (
    builder
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", os.path.join(WORK, "warehouse"))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
if not USE_CONNECT:
    spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} | Spark Connect: {USE_CONNECT}")

# ---- synthetic stand-in for samples.nyctaxi.trips ---------------------------
random.seed(42)
ZIPS = [10001, 10002, 10003, 10011, 10019, 10022, 10028, 10036, 10119, 10282, 10301,
        10451, 11201, 11211, 11215, 11101, 11371, 11430, 11354, 11691, 7114, 11001]
WEIGHTS = [9, 6, 8, 9, 10, 9, 7, 9, 3, 2, 1, 1, 3, 3, 2, 3, 4, 4, 1, 0.3, 0.3, 0.2]
start = datetime(2016, 1, 1)
rows = []
for _ in range(6000):
    pickup = start + timedelta(seconds=random.randint(0, 60 * 86400 - 1))
    dist = round(random.expovariate(1 / 2.8) + 0.3, 2)
    minutes = max(2.0, dist * random.uniform(3, 7))
    dropoff = pickup + timedelta(seconds=int(minutes * 60))
    fare = round(2.5 + 2.5 * dist + random.uniform(0, 4), 1)
    pz, dz = random.choices(ZIPS, WEIGHTS, k=2)
    rows.append((pickup, dropoff, dist, fare, pz, dz))
t = datetime(2016, 1, 15, 8, 0, 0)
rows += [
    (t, t, 1.0, 5.0, 10001, 10002),                              # invalid_time_range
    (t, t - timedelta(minutes=5), 1.0, 5.0, 10001, 10003),       # invalid_time_range
    (t, t + timedelta(hours=5), 3.0, 30.0, 10001, 10011),        # duration_too_long
    (t, t + timedelta(minutes=9), 0.0, 6.0, 10001, 10019),       # invalid_distance
    (t, t + timedelta(minutes=9), 250.0, 6.0, 10001, 10022),     # invalid_distance
    (t, t + timedelta(minutes=9), 2.0, -8.0, 10001, 10028),      # invalid_fare
    (t, t + timedelta(minutes=9), 2.0, 0.0, 10001, 10036),       # invalid_fare
    (t, t + timedelta(minutes=1), 30.0, 60.0, 10001, 11430),     # implausible_speed
    (t, t + timedelta(minutes=9), 2.0, 9.0, None, 10001),        # missing_values
    (None, t + timedelta(minutes=9), 2.0, 9.0, 10001, 10001),    # missing_values (null pickup)
    (t, t + timedelta(minutes=9), 2.0, 9.0, 10451, 10301),       # valid; 10301 becomes a dangling vertex
]
rows += rows[:25]                                                # exact duplicates
random.shuffle(rows)

spark.sql("CREATE DATABASE IF NOT EXISTS src_samples")
spark.createDataFrame(
    rows,
    "tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, trip_distance DOUBLE, "
    "fare_amount DOUBLE, pickup_zip INT, dropoff_zip INT",
).write.format("delta").mode("overwrite").saveAsTable("src_samples.trips")

# ---- Databricks shims and local substitutions --------------------------------
figures = []


def display(df):
    df.show(25, truncate=False)


def _show():
    import matplotlib.pyplot as plt

    path = os.path.join(WORK, f"figure_{len(figures) + 1}.png")
    plt.savefig(path, dpi=80)
    plt.close("all")
    figures.append(path)
    print(f"[figure saved to {path}]")


import matplotlib.pyplot as plt  # noqa: E402

plt.show = _show

SUBSTITUTIONS = [
    ('SOURCE_TABLE = "samples.nyctaxi.trips"', 'SOURCE_TABLE = "spark_catalog.src_samples.trips"'),
    ('spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")', "pass"),
    ('VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"', f'VOLUME_ROOT = r"{WORK}/volume"'),
]

source = open(NOTEBOOK, encoding="utf-8").read()
for old, new in SUBSTITUTIONS:
    assert source.count(old) == 1, f"substitution target not found exactly once: {old}"
    source = source.replace(old, new)

cells = source.split("\n# COMMAND ----------\n")
scope = {"spark": spark, "display": display}
executed = 0
for number, cell in enumerate(cells, start=1):
    body = cell.replace("# Databricks notebook source", "").strip()
    if not body or body.startswith("# MAGIC"):
        continue
    title = next((ln[len("# DBTITLE 1,"):] for ln in body.splitlines() if ln.startswith("# DBTITLE")), "")
    print(f"\n{'=' * 100}\nCELL {number}: {title}\n{'=' * 100}")
    exec(compile(body, f"<cell {number}>", "exec"), scope)
    executed += 1

# ---- assertions on the notebook's own reconciliations -------------------------
assert scope["balanced"], "Bronze != duplicates + Silver + Quarantine"
assert scope["streaming_matches_batch"], "streaming results differ from batch results"
assert scope["n_duplicates"] == 25, scope["n_duplicates"]
assert scope["n_quarantine"] == 10, scope["n_quarantine"]
total_rank = scope["pagerank_df"].agg({"pagerank": "sum"}).first()[0]
assert abs(total_rank - 1.0) < 1e-9, total_rank
assert len(figures) == 4, figures
print(f"\nLOCAL TEST PASSED: {executed} code cells executed, {len(figures)} figures rendered.")
spark.stop()
