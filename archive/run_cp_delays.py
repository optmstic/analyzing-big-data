"""Local test for the CP delays project (notebooks/cp_delays).

Runs the Unity Catalog setup SQL and the notebooks on a local Spark + Delta Lake session, reading the GTFS
files of the `data/` folder. Substitutions: the catalog `workspace` becomes `spark_catalog`, the Volume path
becomes `data/`, and CREATE VOLUME is skipped. `display()`, `DataFrame.display()` and `%sql` get local stand-ins.

Usage:  python local_test/run_cp_delays.py [workdir]
        ABD_CONNECT=1 python local_test/run_cp_delays.py [workdir]   # through Spark Connect
"""
import os
import shutil
import sys

from pyspark.sql import SparkSession

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS = os.path.join(REPO, "notebooks", "cp_delays")
DATA = os.path.join(REPO, "data")
WORK = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "/tmp/cp_delays_test")
ONLY = sys.argv[2].split(",") if len(sys.argv) > 2 else None   # optional: comma-separated notebook prefixes
if ONLY is None:
    shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK, exist_ok=True)

USE_CONNECT = os.environ.get("ABD_CONNECT") == "1"
builder = SparkSession.builder.remote("local[4]") if USE_CONNECT else SparkSession.builder.master("local[4]")
spark = (
    builder.config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", os.path.join(WORK, "warehouse"))
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.memory", "3g")
    .getOrCreate()
)
if not USE_CONNECT:
    spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} | Spark Connect: {USE_CONNECT}")


def display(obj):
    obj.show(30, truncate=False) if hasattr(obj, "show") else print(obj)


from pyspark.sql import DataFrame as _ClassicDataFrame  # noqa: E402

_ClassicDataFrame.display = lambda self, *args: display(self)
try:
    from pyspark.sql.connect.dataframe import DataFrame as _ConnectDataFrame

    _ConnectDataFrame.display = lambda self, *args: display(self)
except ImportError:
    pass


def localise(text: str) -> str:
    return text.replace("/Volumes/workspace/default/cp_raw", DATA).replace("workspace.", "spark_catalog.")


def magic_body(body: str, prefix: str) -> str:
    return "\n".join(ln[len(prefix):].lstrip(" ") if ln.startswith(prefix) else ln for ln in body.splitlines())


scope = {"spark": spark, "display": display}
for file_name in sorted(os.listdir(NOTEBOOKS)):
    if ONLY and not any(file_name.startswith(p) for p in ONLY):
        continue
    is_sql = file_name.endswith(".sql")
    comment = "--" if is_sql else "#"
    source = open(os.path.join(NOTEBOOKS, file_name), encoding="utf-8").read()
    print(f"\n{'#' * 100}\n# NOTEBOOK {file_name}\n{'#' * 100}")
    for number, cell in enumerate(source.split(f"\n{comment} COMMAND ----------\n"), start=1):
        lines = [ln for ln in cell.splitlines() if not ln.startswith(f"{comment} Databricks notebook source")]
        title = next((ln.split(",", 1)[1] for ln in lines if ln.startswith(f"{comment} DBTITLE")), "")
        body = "\n".join(ln for ln in lines if not ln.startswith(f"{comment} DBTITLE")).strip()
        if not body or body.startswith(f"{comment} MAGIC %md"):
            continue
        print(f"\n{'=' * 100}\nCELL {number}: {title}\n{'=' * 100}")
        if is_sql or body.startswith("# MAGIC %sql"):
            statement = body if is_sql else magic_body(body, "# MAGIC").replace("%sql", "", 1)
            statement = localise(statement).strip().rstrip(";")
            if statement.upper().startswith("CREATE VOLUME"):
                print("(CREATE VOLUME skipped locally)")
                continue
            display(spark.sql(statement))
        else:
            assert "# MAGIC" not in body, f"unsupported magic in cell {number} of {file_name}"
            exec(compile(localise(body), f"<{file_name} cell {number}>", "exec"), scope)

# ---- assertions ----------------------------------------------------------------
from pyspark.sql import functions as F  # noqa: E402

movements = spark.table("spark_catalog.cp_bronze.movements_raw")
total = movements.count()
events = movements.select("event_id").distinct().count()
missing = movements.filter(F.col("actual_arrival").isNull()).count()
partitions = movements.select("service_month").distinct().count()
print(f"\nrows={total:,} distinct events={events:,} duplicates={total - events:,} missing={missing:,}")
assert 2_000_000 <= total <= 5_000_000, total
assert partitions == 6, partitions
assert 0.002 < (total - events) / events < 0.004
assert 0.001 < missing / total < 0.003
comment = spark.sql("DESCRIBE TABLE EXTENDED spark_catalog.cp_bronze.movements_raw").filter("col_name = 'Comment'").first()
assert comment is not None and "SIMULATED" in comment["data_type"], "table comment lost on overwrite"
col_comment = [r for r in spark.sql("DESCRIBE TABLE spark_catalog.cp_bronze.movements_raw").collect() if r["col_name"] == "event_id"][0]
assert col_comment["comment"], "column comments lost on overwrite"
print("LOCAL TEST PASSED")
spark.stop()
