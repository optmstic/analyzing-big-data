"""Local test for the Databricks notebook.

Runs every Python cell of the notebook on a local Spark + Delta Lake session, reading the same
source files (the `data/` folder of this repository) that are uploaded to the Databricks Volume.
The only substitutions are the Unity Catalog Volumes, which become local folders.

Usage:  python local_test/run_local.py [workdir]
        ABD_CONNECT=1 python local_test/run_local.py [workdir]   # through Spark Connect
"""
import os
import shutil
import sys

import matplotlib

matplotlib.use("Agg")

from pyspark.sql import SparkSession  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK = os.path.join(REPO, "notebooks", "ABD_Group_Project_CP_Rail_Present_and_Future.py")
DATA = os.path.join(REPO, "data")
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
    ('spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{RAW_VOLUME}")', "pass"),
    ('spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{SCHEMA}.{LANDING_VOLUME}")', "pass"),
    ('spark.sql(f"CREATE VOLUME {CATALOG}.{SCHEMA}.{LANDING_VOLUME}")', "pass"),
    ('RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{RAW_VOLUME}"', f'RAW_PATH = r"{DATA}"'),
    ('LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{LANDING_VOLUME}"', f'LANDING_ROOT = r"{WORK}/landing"'),
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
assert scope["n_checks_failed"] == 0, "a data quality check did not pass"
assert scope["streaming_matches_batch"], "streaming results differ from batch results"
assert scope["n_service_days"] == 548614, scope["n_service_days"]   # cross-checked with an independent pandas script
assert scope["n_stop_events"] == 8954555, scope["n_stop_events"]
total_rank = scope["pagerank_df"].agg({"pagerank": "sum"}).first()[0]
assert abs(total_rank - 1.0) < 1e-9, total_rank
assert len(figures) == 6, figures
print(f"\nLOCAL TEST PASSED: {executed} code cells executed, {len(figures)} figures rendered.")
spark.stop()
