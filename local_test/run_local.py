"""Local test for the project notebook.

Runs every cell of notebooks/ABD_Project_Portugal_Air_Connectivity.py on a local Spark + Delta Lake session, with the
same air_project_data.zip that is uploaded to Databricks. Substitutions: the catalog `workspace` becomes
`spark_catalog`, the Volume becomes a local folder, and CREATE VOLUME is skipped. `dbutils.fs.rm`, `display()` and
`%sql` cells get small local stand-ins.

Usage:  python local_test/run_local.py [workdir]
        ABD_CONNECT=1 python local_test/run_local.py [workdir]   # through Spark Connect, as Databricks serverless
"""
import os
import shutil
import sys

import matplotlib

matplotlib.use("Agg")

from pyspark.sql import SparkSession  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK = os.path.join(REPO, "notebooks", "ABD_Project_Portugal_Air_Connectivity.py")
WORK = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "/tmp/abd_local_test")
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(os.path.join(WORK, "volume"))
shutil.copy(os.path.join(REPO, "air_project_data.zip"), os.path.join(WORK, "volume"))

USE_CONNECT = os.environ.get("ABD_CONNECT") == "1"
builder = SparkSession.builder.remote("local[4]") if USE_CONNECT else SparkSession.builder.master("local[4]")
spark = (
    builder.config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
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

figures = []


def display(obj):
    obj.show(30, truncate=False) if hasattr(obj, "show") else print(obj)


class _LocalFs:
    def rm(self, path, recurse=False):
        shutil.rmtree(path, ignore_errors=True)
        return True


class _LocalDbutils:
    fs = _LocalFs()


def _show():
    path = os.path.join(WORK, f"figure_{len(figures) + 1}.png")
    plt.savefig(path, dpi=160)
    plt.close("all")
    figures.append(path)
    print(f"[figure saved to {path}]")


import matplotlib.pyplot as plt  # noqa: E402

plt.show = _show


def localise(text: str) -> str:
    text = text.replace('"/Volumes/workspace/abd/abd"', f'r"{WORK}/volume"')
    text = text.replace('spark.sql("CREATE VOLUME IF NOT EXISTS workspace.abd.abd")', "pass")
    return text.replace("workspace.", "spark_catalog.")


source = open(NOTEBOOK, encoding="utf-8").read()
assert source.count('"/Volumes/workspace/abd/abd"') == 1 and source.count("CREATE VOLUME IF NOT EXISTS") == 1

scope = {"spark": spark, "display": display, "dbutils": _LocalDbutils()}
executed = sql_cells = 0
for number, cell in enumerate(source.split("\n# COMMAND ----------\n"), start=1):
    lines = [ln for ln in cell.splitlines() if not ln.startswith("# Databricks notebook source")]
    title = next((ln.split(",", 1)[1] for ln in lines if ln.startswith("# DBTITLE")), "")
    body = "\n".join(ln for ln in lines if not ln.startswith("# DBTITLE")).strip()
    if not body or body.startswith("# MAGIC %md"):
        continue
    print(f"\n{'=' * 100}\nCELL {number}: {title}\n{'=' * 100}")
    if body.startswith("# MAGIC %sql"):
        statement = "\n".join(ln[len("# MAGIC"):].lstrip(" ") for ln in body.splitlines()).replace("%sql", "", 1)
        display(spark.sql(localise(statement)))
        sql_cells += 1
    else:
        assert "# MAGIC" not in body, f"unsupported magic in cell {number}"
        exec(compile(localise(body), f"<cell {number}>", "exec"), scope)
    executed += 1

# ---- assertions -----------------------------------------------------------------
assert scope["checks_ok"], "a quality check did not pass"
assert scope["stream_ok"], "streaming result differs from batch"
print(f"\nedges={scope['n_edges']} airports={scope['n_airports']} one_stop_pt={scope['n_one_stop_journeys']} "
      f"one_stop_all={scope['n_all_one_stop_journeys']} lisbon={scope['lisbon']}")
print(f"LOCAL TEST PASSED: {executed} cells executed ({sql_cells} of them %sql), {len(figures)} figures rendered.")
spark.stop()
