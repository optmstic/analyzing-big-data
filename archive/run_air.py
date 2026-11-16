"""Local test for the air connectivity notebook.

Runs every Python cell of the notebook on a local Spark + Delta Lake session, reading the same
source files (the `data_air/` folder of this repository) that are uploaded to the Databricks Volume.
The only substitutions are the Unity Catalog catalog name and the Volume, which becomes a local folder;
`dbutils.fs`, `display()` and `%sql` cells are replaced by small local stand-ins.

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
NOTEBOOK = os.path.join(REPO, "notebooks", "ABD_Group_Project_Portugal_Air_Connectivity.py")
DATA = os.path.join(REPO, "data_air")
WORK = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "/tmp/air_local_test")
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
    .config("spark.driver.memory", "3g")
    .getOrCreate()
)
if not USE_CONNECT:
    spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} | Spark Connect: {USE_CONNECT}")

# ---- Databricks shims and local substitutions --------------------------------
figures = []


def display(obj):
    """Stand-in for the Databricks display(): DataFrames are shown as text, other objects are printed."""
    if hasattr(obj, "show"):
        obj.show(25, truncate=False)
    else:
        for item in obj:
            print(item)


class _LocalFile:
    """Stand-in for the FileInfo objects returned by dbutils.fs.ls."""

    def __init__(self, folder, name):
        self.name = name
        self.path = os.path.join(folder, name)
        self.size = os.path.getsize(self.path)

    def __repr__(self):
        return f"{self.name}  ({self.size:,} bytes)"


class _LocalFs:
    """Stand-in for dbutils.fs on the local file system."""

    def rm(self, path, recurse=False):
        existed = os.path.exists(path)
        shutil.rmtree(path, ignore_errors=True) if os.path.isdir(path) else (existed and os.remove(path))
        return existed

    def mkdirs(self, path):
        os.makedirs(path, exist_ok=True)
        return True

    def ls(self, path):
        return [_LocalFile(path, name) for name in sorted(os.listdir(path))]


class _LocalDbutils:
    fs = _LocalFs()


def _show():
    import matplotlib.pyplot as plt

    path = os.path.join(WORK, f"figure_{len(figures) + 1}.png")
    plt.savefig(path, dpi=160)
    plt.close("all")
    figures.append(path)
    print(f"[figure saved to {path}]")


import matplotlib.pyplot as plt  # noqa: E402

plt.show = _show

# DataFrame.display() exists only on Databricks
from pyspark.sql import DataFrame as _ClassicDataFrame  # noqa: E402

_ClassicDataFrame.display = lambda self, *args: display(self)
try:
    from pyspark.sql.connect.dataframe import DataFrame as _ConnectDataFrame

    _ConnectDataFrame.display = lambda self, *args: display(self)
except ImportError:
    pass

LOCAL_CATALOG = "spark_catalog"
SUBSTITUTIONS = [
    ('CATALOG = "workspace"', f'CATALOG = "{LOCAL_CATALOG}"'),
    ('VOLUME_PATH = "/Volumes/workspace/abd/abd"', f'VOLUME_PATH = r"{WORK}/volume"'),
    ('SOURCE_PATH = f"{VOLUME_PATH}/air_routes"', f'SOURCE_PATH = r"{DATA}"'),
    ('spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.abd.abd")', "pass"),
]

source = open(NOTEBOOK, encoding="utf-8").read()
for old, new in SUBSTITUTIONS:
    assert source.count(old) == 1, f"substitution target not found exactly once: {old}"
    source = source.replace(old, new)

cells = source.split("\n# COMMAND ----------\n")
scope = {"spark": spark, "display": display, "dbutils": _LocalDbutils()}
executed = 0
sql_cells = 0
for number, cell in enumerate(cells, start=1):
    body = cell.replace("# Databricks notebook source", "").strip()
    title = next((ln[len("# DBTITLE 1,"):] for ln in body.splitlines() if ln.startswith("# DBTITLE")), "")
    body = "\n".join(ln for ln in body.splitlines() if not ln.startswith("# DBTITLE")).strip()
    if not body or body.startswith("# MAGIC %md"):
        continue
    print(f"\n{'=' * 100}\nCELL {number}: {title}\n{'=' * 100}")
    if body.startswith("# MAGIC %sql"):
        # %sql cell: run the statement with spark.sql and show the result
        statement = "\n".join(ln[len("# MAGIC"):].lstrip(" ") if ln.startswith("# MAGIC") else ln for ln in body.splitlines())
        statement = statement.replace("%sql", "", 1).replace("workspace.", f"{LOCAL_CATALOG}.")
        display(spark.sql(statement))
        sql_cells += 1
    else:
        assert "# MAGIC" not in body, f"unsupported magic in cell {number}"
        exec(compile(body, f"<cell {number}>", "exec"), scope)
    executed += 1

# ---- assertions on the notebook's own reconciliations -------------------------
assert scope["n_checks_failed"] == 0, "a data quality check did not pass"
assert scope["streaming_matches_batch"], "streaming results differ from batch results"
# Cross-checked with an independent pandas script on the same files
assert scope["n_edges"] == 12473, scope["n_edges"]
assert scope["n_vertices"] == 602, scope["n_vertices"]
assert scope["n_direct"] == 331, scope["n_direct"]
assert scope["n_one_stop"] == 22312, scope["n_one_stop"]
assert scope["n_two_stop"] == 1350767, scope["n_two_stop"]
assert scope["n_network_one_stop"] == 922420, scope["n_network_one_stop"]
assert scope["hub_reach"]["direct"] == 106 and scope["hub_reach"]["within_one_stop"] == 562
assert scope["hub_reach"]["within_two_stops"] == 598
total_rank = scope["pagerank_df"].agg({"pagerank": "sum"}).first()[0]
assert abs(total_rank - 1.0) < 1e-9, total_rank
assert len(figures) == 8, figures
print(f"\nLOCAL TEST PASSED: {executed} cells executed ({sql_cells} of them %sql), {len(figures)} figures rendered.")
spark.stop()
