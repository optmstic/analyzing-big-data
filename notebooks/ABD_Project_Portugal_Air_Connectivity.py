# Databricks notebook source
# MAGIC %md
# MAGIC # Portugal's air connectivity, 2016-2026
# MAGIC ## A small lakehouse on Apache Spark and Databricks Free Edition
# MAGIC
# MAGIC **Course:** Analyzing Big Data 2026-2027 &nbsp;|&nbsp; NOVA IMS &nbsp;|&nbsp; **Project**
# MAGIC
# MAGIC **Student:** *name* &nbsp;|&nbsp; **Number:** *number*
# MAGIC
# MAGIC ---
# MAGIC ### The project in one minute
# MAGIC **Problem.** Portugal depends on its air links for tourism, the islands and the diaspora. How well connected are
# MAGIC its airports, where are the passengers, and where is demand heading?
# MAGIC
# MAGIC **Data.** Official Eurostat statistics: the air routes declared by 35 European countries (2016 to 2024) and the
# MAGIC monthly passengers of 496 European airports (2016 to 2026).
# MAGIC
# MAGIC **Three questions.**
# MAGIC
# MAGIC | # | Question | Spark technique | Section |
# MAGIC |---|---|---|---|
# MAGIC | Q1 | **Reach**: where can you fly from Portugal, directly and with one stop? | Graph as joins | 5 |
# MAGIC | Q2 | **Demand**: where are the passengers, how full are the planes, how seasonal is traffic? | Spark SQL | 6 |
# MAGIC | Q3 | **Trend**: how is demand evolving, and can the figures be kept up to date as new months arrive? | Structured Streaming | 7 |
# MAGIC
# MAGIC **Contents:** 1. Architecture &nbsp; 2. Setup &nbsp; 3. Bronze &nbsp; 4. Silver &nbsp; 5. Reach &nbsp; 6. Demand &nbsp;
# MAGIC 7. Trend and streaming &nbsp; 8. Conclusions
# MAGIC
# MAGIC > **How to run:** upload the file `air_project_data.zip` to the Volume `/Volumes/workspace/abd/abd`, attach
# MAGIC > *Serverless* compute and choose **Run all**. The notebook can be run again at any time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Architecture
# MAGIC
# MAGIC | Technology | Use in this project |
# MAGIC |---|---|
# MAGIC | **Databricks Free Edition**, serverless | Runs the notebook |
# MAGIC | **Unity Catalog** | A Volume for the files, a schema for the tables |
# MAGIC | **PySpark DataFrames** | Reading and parsing the files, joins, pivot |
# MAGIC | **Spark SQL** | The analysis (`%sql` cells, window functions) |
# MAGIC | **Structured Streaming** | Monthly figures updated incrementally |
# MAGIC | **Delta Lake** | Format of the tables: reliable writes and history |
# MAGIC
# MAGIC The data is organised in the **medallion architecture** seen in class:
# MAGIC
# MAGIC ```
# MAGIC  SOURCE FILES                 BRONZE                    SILVER                    GOLD
# MAGIC  (Volume, 1 zip)              (Parquet, as received)    (Delta, clean)            (Delta, answers)
# MAGIC
# MAGIC  airports.csv          -->    airports           -->    SV_Airports
# MAGIC  routes_annual_*.tsv   -->    routes_annual      -->    SV_Routes          -->    GD_Network_Year      (Q1)
# MAGIC  (38 files)                                                                      GD_Reach             (Q1)
# MAGIC  airports_monthly_*    -->    airports_monthly   -->    SV_Airport_Month   -->    GD_Portugal_Month    (Q2, Q3)
# MAGIC  (3 files)
# MAGIC  new months (JSON)     -->    BZ_Traffic_Stream  -------------------------->    GD_Country_Month_Stream (Q3)
# MAGIC                    streaming
# MAGIC ```
# MAGIC
# MAGIC * **Bronze**: the files as received, in Parquet.
# MAGIC * **Silver**: clean tables with proper types and names. The **business rules** are here.
# MAGIC * **Gold**: small tables that answer the questions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup

# COMMAND ----------

# DBTITLE 1,Configuration: every name and parameter in one place
import os
import zipfile

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

import matplotlib.pyplot as plt

# Where things are
VOLUME_PATH = "/Volumes/workspace/abd/abd"          # the Volume used in the ABD classes
SOURCE_ZIP = f"{VOLUME_PATH}/air_project_data.zip"  # the one file to upload
SOURCE_PATH = f"{VOLUME_PATH}/air_routes"           # the zip is extracted here
DL_PATH = f"{VOLUME_PATH}/DL_AIR"                   # data lake folder of the project
BRONZE_PATH = f"{DL_PATH}/Bronze"
LANDING_PATH = f"{DL_PATH}/Landing"                 # new months arrive here (streaming)
CHECKPOINT_PATH = f"{DL_PATH}/Checkpoints"
SCHEMA = "workspace.DL_AIR"                         # schema of the Silver and Gold tables

# What we analyse
HOME_COUNTRY = "PT"
MAIN_AIRPORTS = ["LIS", "OPO", "FAO", "FNC", "PDL"]  # Lisbon, Porto, Faro, Funchal, Ponta Delgada
NETWORK_YEAR = 2024       # latest year of the route statistics
LATEST_FULL_YEAR = 2025   # latest complete year of the airport statistics
PRE_PANDEMIC_YEAR = 2019
FORECAST_END_YEAR = 2030

# COMMAND ----------

# DBTITLE 1,Start clean, create the schema and extract the data files
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.abd")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.abd.abd")

# Remove the tables and folders of previous runs, so the notebook can always be run again
spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
spark.sql(f"CREATE SCHEMA {SCHEMA} COMMENT 'ABD project: air connectivity of Portugal, 2016-2026'")
dbutils.fs.rm(DL_PATH, True)

# The zip holds 42 small files: extract them into the Volume
if os.path.exists(SOURCE_ZIP):
    os.makedirs(SOURCE_PATH, exist_ok=True)
    with zipfile.ZipFile(SOURCE_ZIP) as archive:
        archive.extractall(SOURCE_PATH)
elif not os.path.exists(f"{SOURCE_PATH}/airports.csv"):
    raise FileNotFoundError(f"Upload air_project_data.zip to {VOLUME_PATH} and run the notebook again.")

source_files = sorted(os.listdir(SOURCE_PATH))
print(f"{len(source_files)} source files in {SOURCE_PATH}")
print("  airports.csv                :", "airports.csv" in source_files)
print("  routes_annual_<country>.tsv :", len([f for f in source_files if f.startswith("routes_annual_")]), "files")
print("  airports_monthly_<type>.tsv :", len([f for f in source_files if f.startswith("airports_monthly_")]), "files")

# COMMAND ----------

# DBTITLE 1,Two helper functions
def save_table(df: DataFrame, name: str, comment: str) -> None:
    """Save a DataFrame as a Delta table of the project schema, with a description."""
    df.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.{name}")
    spark.sql(f"COMMENT ON TABLE {SCHEMA}.{name} IS '{comment}'")
    print(f"Saved {SCHEMA}.{name}: {spark.table(f'{SCHEMA}.{name}').count():,} rows")


def table(name: str) -> DataFrame:
    """Read a table of the project schema."""
    return spark.table(f"{SCHEMA}.{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze: the files as received
# MAGIC
# MAGIC | Files | Content | Type |
# MAGIC |---|---|---|
# MAGIC | `airports.csv` | Names and coordinates of the airports of the world | Structured (CSV) |
# MAGIC | `routes_annual_<country>.tsv`, 38 files | Eurostat: passengers, seats and flights of each air route, per year, 2016 to 2024 | Text |
# MAGIC | `airports_monthly_<type>.tsv`, 3 files | Eurostat: passengers of each airport per month, 2016 to 2026 | Text |
# MAGIC
# MAGIC The Eurostat files are not tables: several fields are packed in the first column, there is one column per
# MAGIC period, and a missing value is written `:`. They are read as **lines of text** and parsed in the Silver layer.
# MAGIC All the files of a group are read in one statement with a wildcard (`*`).

# COMMAND ----------

# DBTITLE 1,Read the source files and save them as Parquet
airports_raw = spark.read.option("header", True).option("quote", '"').option("escape", '"').csv(f"{SOURCE_PATH}/airports.csv")
airports_raw.write.mode("overwrite").parquet(f"{BRONZE_PATH}/airports")

for group in ("routes_annual", "airports_monthly"):
    (
        spark.read.text(f"{SOURCE_PATH}/{group}_*.tsv")
        .withColumn("source_file", F.col("_metadata.file_name"))   # keep the name of the file of each line
        .write.mode("overwrite").parquet(f"{BRONZE_PATH}/{group}")
    )

for name in ("airports", "routes_annual", "airports_monthly"):
    print(f"Bronze {name:<17}: {spark.read.parquet(f'{BRONZE_PATH}/{name}').count():>8,} rows")

# COMMAND ----------

# DBTITLE 1,What a Eurostat file looks like
display(
    spark.read.parquet(f"{BRONZE_PATH}/routes_annual")
    .filter(F.col("source_file") == "routes_annual_pt.tsv")
    .select(F.substring("value", 1, 100).alias("first_100_characters_of_each_line"))
    .limit(4)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the example.** The first line is the header: the years. In each following line, the first field
# MAGIC `A,PAS,PAS_CRD_DEP,PT_LPFR_AT_LOWW` means: annual, passengers, *passengers carried on departures*, from Faro
# MAGIC (`LPFR`) in Portugal to Vienna (`LOWW`) in Austria. Then comes one value per year, `:` when there is none.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: clean tables
# MAGIC One function turns any group of Eurostat files into rows: **one row per series and period**. It uses `split` and
# MAGIC `posexplode`, seen in class. Each file has its own header (the United Kingdom file stops in 2019), so every value
# MAGIC is matched with the header **of its own file**.

# COMMAND ----------

# DBTITLE 1,A parser for the Eurostat files
def parse_eurostat(bronze_name: str) -> DataFrame:
    """Turn the lines of text of a Bronze dataset into rows: [series, period, value]."""
    lines = spark.read.parquet(f"{BRONZE_PATH}/{bronze_name}")
    is_header = F.col("value").startswith("freq")

    # The header of each file: position -> period
    periods = lines.filter(is_header).select(
        "source_file", F.posexplode(F.split("value", "\t")).alias("position", "period")
    )
    # The values of each line: position -> value
    values = lines.filter(~is_header).select(
        "source_file",
        F.split("value", "\t").getItem(0).alias("series"),
        F.posexplode(F.split("value", "\t")).alias("position", "cell"),
    )
    return (
        values.join(F.broadcast(periods), ["source_file", "position"])
        .filter(F.col("position") > 0)
        # keep the number, drop the cells without a value (":")
        .withColumn("value", F.expr("try_cast(regexp_extract(cell, '^ *([0-9]+)', 1) AS DOUBLE)"))
        .filter(F.col("value").isNotNull())
        .select("series", F.trim("period").alias("period"), "value")
    )

# COMMAND ----------

# DBTITLE 1,SV_Airports: one row per airport, with a readable code
airports = (
    spark.read.parquet(f"{BRONZE_PATH}/airports")
    .filter(F.col("icao_code").isNotNull())
    .select(
        "icao_code",
        F.coalesce("iata_code", "icao_code").alias("airport"),   # LIS, OPO... the code passengers know
        F.col("name").alias("airport_name"),
        F.col("iso_country").alias("country"),
        "continent",
        F.col("latitude_deg").cast("double").alias("latitude"),
        F.col("longitude_deg").cast("double").alias("longitude"),
    )
)
save_table(airports, "SV_Airports", "SILVER - airports with name, country and coordinates")

# COMMAND ----------

# MAGIC %md
# MAGIC **Routes.** Eurostat gives, for each reporting airport and partner airport, the departures and the arrivals.
# MAGIC We add both directions to get the **passengers of the route**, and put the measures side by side with a **pivot**:
# MAGIC
# MAGIC | Eurostat code | Meaning | Column |
# MAGIC |---|---|---|
# MAGIC | `PAS_CRD` | Passengers carried | `passengers` |
# MAGIC | `PAS_BRD` | Passengers on board | `passengers_on_board` |
# MAGIC | `ST_PAS` | Seats available | `seats` |
# MAGIC | `CAF_PAS` | Flights | `flights` |

# COMMAND ----------

# DBTITLE 1,SV_Routes: one row per reporting airport, partner airport and year
measure = F.split("series", ",").getItem(2)     # for example PAS_CRD_DEP
route = F.split("series", ",").getItem(3)       # for example PT_LPFR_AT_LOWW
names = table("SV_Airports").select("icao_code", "airport", "airport_name", "country")

routes = (
    parse_eurostat("routes_annual")
    .select(
        F.split(route, "_").getItem(0).alias("reporting_country"),
        F.split(route, "_").getItem(1).alias("from_icao"),
        F.split(route, "_").getItem(3).alias("to_icao"),
        F.col("period").cast("int").alias("year"),
        F.when(measure.startswith("PAS_CRD"), "passengers")
        .when(measure.startswith("PAS_BRD"), "passengers_on_board")
        .when(measure.startswith("ST_PAS"), "seats")
        .otherwise("flights")
        .alias("measure"),
        "value",
    )
    .groupBy("reporting_country", "from_icao", "to_icao", "year")
    .pivot("measure", ["passengers", "passengers_on_board", "seats", "flights"])
    .sum("value")                                                   # departures + arrivals
    .join(F.broadcast(names.select(F.col("icao_code").alias("from_icao"), F.col("airport").alias("from_airport"))),
          "from_icao", "left")
    .join(F.broadcast(names.select(F.col("icao_code").alias("to_icao"), F.col("airport").alias("to_airport"),
                                   F.col("airport_name").alias("to_airport_name"), F.col("country").alias("to_country"))),
          "to_icao", "left")
)
save_table(routes, "SV_Routes", "SILVER - passengers, seats and flights of each route per reporting airport and year")

# COMMAND ----------

# DBTITLE 1,SV_Airport_Month: passengers of each airport per month, total, national and international
airport_code = F.split("series", ",").getItem(3)    # for example PT_LPPT

airport_month = (
    parse_eurostat("airports_monthly")
    .select(
        F.split(airport_code, "_").getItem(0).alias("country"),
        F.split(airport_code, "_").getItem(1).alias("icao_code"),
        F.split("series", ",").getItem(5).alias("coverage"),         # TOTAL, NAT or INTL
        F.substring("period", 1, 4).cast("int").alias("year"),
        F.substring("period", 6, 2).cast("int").alias("month"),
        "value",
    )
    .groupBy("country", "icao_code", "year", "month")
    .pivot("coverage", ["TOTAL", "NAT", "INTL"])
    .sum("value")
    .withColumnRenamed("TOTAL", "passengers")
    .withColumnRenamed("NAT", "national_passengers")
    .withColumnRenamed("INTL", "international_passengers")
    .join(F.broadcast(names.select("icao_code", "airport", "airport_name")), "icao_code", "left")
)
save_table(airport_month, "SV_Airport_Month", "SILVER - monthly passengers of each European airport")

# COMMAND ----------

# DBTITLE 1,Quality checks: each line must be zero
checks = [
    ("Airports with a repeated code", table("SV_Airports").groupBy("airport").count().filter("count > 1").count()),
    ("Routes repeated (same airports and year)",
     table("SV_Routes").groupBy("reporting_country", "from_icao", "to_icao", "year").count().filter("count > 1").count()),
    ("Routes with a negative number of passengers", table("SV_Routes").filter("passengers < 0").count()),
    ("Routes outside the years 2016 to 2024", table("SV_Routes").filter("year < 2016 OR year > 2024").count()),
    ("Monthly rows repeated (same airport and month)",
     table("SV_Airport_Month").groupBy("country", "icao_code", "year", "month").count().filter("count > 1").count()),
    ("Monthly rows with an invalid month", table("SV_Airport_Month").filter("month < 1 OR month > 12").count()),
]
for description, problems in checks:
    print(f"{description:<50} {problems:>5}   {'OK' if problems == 0 else 'CHECK'}")
checks_ok = all(problems == 0 for _, problems in checks)
print(f"\nAll quality checks passed: {'YES' if checks_ok else 'NO'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Q1 - Reach: where can you fly from Portugal?
# MAGIC
# MAGIC ### 5.1 The European network, year by year
# MAGIC A **route** links two airports. A route between two reporting countries is declared twice, once by each airport,
# MAGIC so the two airports are put in alphabetical order and each pair is counted **once**.
# MAGIC
# MAGIC > To compare years fairly, only the countries that report in **every** year are used (the United Kingdom stopped
# MAGIC > reporting after 2019).

# COMMAND ----------

# DBTITLE 1,GD_Network_Year: routes and passengers of the European network per year
reporting_every_year = [
    row["reporting_country"]
    for row in table("SV_Routes").groupBy("reporting_country").agg(F.countDistinct("year").alias("years"))
    .filter("years = 9").collect()
]

network_year = (
    table("SV_Routes")
    .filter(F.col("reporting_country").isin(reporting_every_year) & (F.col("passengers") > 0))
    .withColumn("airport_a", F.least("from_icao", "to_icao"))
    .withColumn("airport_b", F.greatest("from_icao", "to_icao"))
    .groupBy("year", "airport_a", "airport_b")
    .agg(F.max("passengers").alias("passengers"))        # a route declared twice is counted once
    .groupBy("year")
    .agg(F.count("*").alias("routes"), F.round(F.sum("passengers") / 1e6, 0).alias("passengers_million"))
)
save_table(network_year, "GD_Network_Year", "GOLD - routes and passengers of the European network per year")

# COMMAND ----------

# DBTITLE 1,The network before, during and after the pandemic
# MAGIC %sql
# MAGIC SELECT n.year,
# MAGIC        n.routes,
# MAGIC        ROUND(100 * (n.routes / b.routes - 1), 0)                         AS routes_vs_2019_pct,
# MAGIC        n.passengers_million,
# MAGIC        ROUND(100 * (n.passengers_million / b.passengers_million - 1), 0) AS passengers_vs_2019_pct
# MAGIC FROM workspace.DL_AIR.GD_Network_Year n
# MAGIC CROSS JOIN (SELECT * FROM workspace.DL_AIR.GD_Network_Year WHERE year = 2019) b
# MAGIC ORDER BY n.year

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Direct flights and one stop
# MAGIC The network is a **graph**: airports are the vertices and routes are the edges. A journey with one stop is a
# MAGIC **join of the edges with themselves**: the destination of the first flight is the origin of the second.
# MAGIC
# MAGIC ```
# MAGIC   Lisbon --> Istanbul           (first flight)
# MAGIC              Istanbul --> Tokyo (second flight)        =>   Lisbon reaches Tokyo with one stop
# MAGIC ```
# MAGIC
# MAGIC This join multiplies the data: about 6,000 routes become more than 900,000 one-stop journeys. This is why it
# MAGIC is done with Spark.

# COMMAND ----------

# DBTITLE 1,The edges of the graph: every route of 2024, in both directions
routes_latest = (
    table("SV_Routes")
    .filter((F.col("year") == NETWORK_YEAR) & (F.col("passengers") > 0))
    .filter(F.col("from_airport").isNotNull() & F.col("to_airport").isNotNull())    # known airports only
    .filter(F.col("from_airport") != F.col("to_airport"))
)
edges = (
    routes_latest.select(F.col("from_airport").alias("src"), F.col("to_airport").alias("dst"))
    .union(routes_latest.select(F.col("to_airport").alias("src"), F.col("from_airport").alias("dst")))
    .distinct()
)
n_airports = edges.select("src").distinct().count()
n_edges = edges.count()
print(f"Airports: {n_airports:,}   Edges (routes in both directions): {n_edges:,}")

# COMMAND ----------

# DBTITLE 1,GD_Reach: airports reached directly and with one stop, from each Portuguese airport
home_airports = table("SV_Airports").filter(F.col("country") == HOME_COUNTRY).select(F.col("airport").alias("src"))
first_flight = edges.join(F.broadcast(home_airports), "src").select(F.col("src").alias("origin"), F.col("dst").alias("stop"))
second_flight = edges.select(F.col("src").alias("stop"), F.col("dst").alias("destination"))

one_stop_journeys = first_flight.join(second_flight, "stop").filter(F.col("destination") != F.col("origin"))
n_one_stop_journeys = one_stop_journeys.count()
n_all_one_stop_journeys = (
    edges.select(F.col("src").alias("origin"), F.col("dst").alias("stop"))
    .join(second_flight, "stop").filter(F.col("destination") != F.col("origin")).count()
)
print(f"One-stop journeys from the Portuguese airports : {n_one_stop_journeys:,}")
print(f"One-stop journeys in the whole network         : {n_all_one_stop_journeys:,}")

direct = first_flight.groupBy("origin").agg(F.countDistinct("stop").alias("direct"))
within_one_stop = (
    first_flight.select("origin", F.col("stop").alias("destination"))
    .union(one_stop_journeys.select("origin", "destination"))
    .groupBy("origin").agg(F.countDistinct("destination").alias("within_one_stop"))
)
reach = (
    direct.join(within_one_stop, "origin")
    .withColumn("pct_of_network_within_one_stop", F.round(100 * F.col("within_one_stop") / (n_airports - 1), 0))
)
save_table(reach, "GD_Reach", "GOLD - airports reached directly and within one stop from each Portuguese airport")

# COMMAND ----------

# DBTITLE 1,Reach of the Portuguese airports
# MAGIC %sql
# MAGIC SELECT r.origin AS airport, a.airport_name, r.direct, r.within_one_stop, r.pct_of_network_within_one_stop
# MAGIC FROM workspace.DL_AIR.GD_Reach r
# MAGIC JOIN workspace.DL_AIR.SV_Airports a ON r.origin = a.airport
# MAGIC ORDER BY r.within_one_stop DESC
# MAGIC LIMIT 8

# COMMAND ----------

# DBTITLE 1,Chart: direct destinations and one-stop reach
reach_pd = table("GD_Reach").filter(F.col("origin").isin(MAIN_AIRPORTS)).orderBy(F.desc("within_one_stop")).toPandas()

fig, ax = plt.subplots(figsize=(9, 4))
ax.bar(reach_pd["origin"], reach_pd["within_one_stop"], color="lightsteelblue", label="Within one stop")
ax.bar(reach_pd["origin"], reach_pd["direct"], color="steelblue", label="Direct")
ax.axhline(n_airports - 1, color="grey", linestyle=":", label="All airports of the network")
ax.set_title(f"Airports reached from the main Portuguese airports ({NETWORK_YEAR})")
ax.set_ylabel("Airports")
ax.set_ylim(0, n_airports * 1.25)
ax.legend(loc="upper right", ncol=3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Which hubs does Lisbon depend on? Airports used for the most one-stop destinations
# For each destination that Lisbon does not reach directly, count the airports where the stop can be made
lisbon_direct = first_flight.filter(F.col("origin") == "LIS").select(F.col("stop").alias("destination"))
lisbon_needs_a_stop = one_stop_journeys.filter(F.col("origin") == "LIS").join(lisbon_direct, "destination", "left_anti")
n_lisbon_one_stop = lisbon_needs_a_stop.select("destination").distinct().count()

hubs = (
    lisbon_needs_a_stop.groupBy("stop")
    .agg(F.countDistinct("destination").alias("destinations_opened"))
    .withColumn("pct_of_one_stop_destinations", F.round(100 * F.col("destinations_opened") / n_lisbon_one_stop, 0))
    .join(table("SV_Airports").select(F.col("airport").alias("stop"), "airport_name", "country"), "stop")
    .orderBy(F.desc("destinations_opened"))
)
display(hubs.select("stop", "airport_name", "country", "destinations_opened", "pct_of_one_stop_destinations").limit(6))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Q2 - Demand: where are the passengers?
# MAGIC
# MAGIC > **Counting the passengers of a country.** A passenger on an international flight is counted once, at the
# MAGIC > Portuguese airport. A passenger on a domestic flight is counted twice, at departure and at arrival. So the
# MAGIC > passengers of a country are **international + national / 2**.

# COMMAND ----------

# DBTITLE 1,GD_Portugal_Month: passengers of Portugal per month
portugal_month = (
    table("SV_Airport_Month")
    .filter(F.col("country") == HOME_COUNTRY)
    .groupBy("year", "month")
    .agg(F.sum(F.coalesce("international_passengers", F.lit(0)) + F.coalesce("national_passengers", F.lit(0)) / 2)
         .alias("passengers"))
)
save_table(portugal_month, "GD_Portugal_Month", "GOLD - air passengers of Portugal per month")

# COMMAND ----------

# DBTITLE 1,The Portuguese airports in 2025
# MAGIC %sql
# MAGIC WITH yearly AS (
# MAGIC     SELECT airport, airport_name, year, SUM(passengers) AS passengers, COUNT(*) AS months
# MAGIC     FROM workspace.DL_AIR.SV_Airport_Month
# MAGIC     WHERE country = 'PT'
# MAGIC     GROUP BY airport, airport_name, year
# MAGIC )
# MAGIC SELECT now.airport,
# MAGIC        now.airport_name,
# MAGIC        ROUND(now.passengers / 1e6, 1)                                   AS passengers_2025_million,
# MAGIC        ROUND(100 * now.passengers / SUM(now.passengers) OVER (), 0)     AS pct_of_portugal,
# MAGIC        ROUND(100 * (now.passengers / before.passengers - 1), 0)         AS growth_since_2019_pct
# MAGIC FROM yearly now
# MAGIC JOIN yearly before ON now.airport = before.airport AND before.year = 2019
# MAGIC WHERE now.year = 2025 AND now.months = 12
# MAGIC ORDER BY now.passengers DESC
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Chart: passengers per month, January 2016 to April 2026
monthly_pd = (
    table("SV_Airport_Month")
    .filter((F.col("country") == HOME_COUNTRY) & F.col("airport").isin(MAIN_AIRPORTS))
    .select("airport", "year", "month", "passengers").orderBy("year", "month").toPandas()
)

fig, ax = plt.subplots(figsize=(11, 4))
for airport in MAIN_AIRPORTS:
    series = monthly_pd[monthly_pd["airport"] == airport]
    ax.plot(series["year"] + (series["month"] - 1) / 12, series["passengers"] / 1e6, label=airport)
ax.set_title("Passengers per month")
ax.set_ylabel("Million passengers")
ax.grid(alpha=0.3)
ax.legend(ncol=5)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Seasonality: the busiest month compared with the quietest (2025)
# MAGIC %sql
# MAGIC SELECT airport,
# MAGIC        airport_name,
# MAGIC        MAX_BY(month, passengers)                    AS busiest_month,
# MAGIC        MIN_BY(month, passengers)                    AS quietest_month,
# MAGIC        ROUND(MAX(passengers) / MIN(passengers), 1)  AS busiest_vs_quietest
# MAGIC FROM workspace.DL_AIR.SV_Airport_Month
# MAGIC WHERE country = 'PT' AND year = 2025 AND airport IN ('LIS', 'OPO', 'FAO', 'FNC', 'PDL')
# MAGIC GROUP BY airport, airport_name
# MAGIC ORDER BY busiest_vs_quietest DESC

# COMMAND ----------

# DBTITLE 1,The 10 busiest routes of Portugal in 2024, and how full the planes are
# MAGIC %sql
# MAGIC SELECT from_airport,
# MAGIC        to_airport,
# MAGIC        to_airport_name,
# MAGIC        ROUND(passengers / 1e6, 2)                    AS passengers_million,
# MAGIC        ROUND(100 * passengers_on_board / seats, 0)   AS seats_occupied_pct
# MAGIC FROM workspace.DL_AIR.SV_Routes
# MAGIC WHERE reporting_country = 'PT' AND year = 2024
# MAGIC ORDER BY passengers DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Large routes with the most empty seats (at least 250,000 passengers)
# MAGIC %sql
# MAGIC SELECT from_airport,
# MAGIC        to_airport,
# MAGIC        to_airport_name,
# MAGIC        ROUND(passengers / 1e3, 0)                      AS passengers_thousand,
# MAGIC        ROUND(100 * passengers_on_board / seats, 0)     AS seats_occupied_pct,
# MAGIC        ROUND((seats - passengers_on_board) / 1e3, 0)   AS empty_seats_thousand
# MAGIC FROM workspace.DL_AIR.SV_Routes
# MAGIC WHERE reporting_country = 'PT' AND year = 2024 AND passengers >= 250000
# MAGIC ORDER BY seats_occupied_pct
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Q3 - Trend: how is demand evolving?
# MAGIC
# MAGIC ### 7.1 New months arrive: Structured Streaming
# MAGIC Airport statistics are published every month. Instead of recomputing everything, a **stream** adds each new
# MAGIC delivery to the totals. We simulate two deliveries of JSON files: first the months of 2025, then those of 2026.
# MAGIC
# MAGIC | Part | Here |
# MAGIC |---|---|
# MAGIC | **Source** | JSON files arriving in a folder of the Volume |
# MAGIC | **Transformation** | Total passengers per country and month |
# MAGIC | **Sink** | A Delta table |
# MAGIC | **Checkpoint** | Remembers which files were already read, so each file is processed exactly once |
# MAGIC
# MAGIC The trigger `availableNow` processes everything that is waiting and stops, which is how streaming runs on
# MAGIC serverless compute.

# COMMAND ----------

# DBTITLE 1,The streaming pipeline
record_schema = StructType([
    StructField("country", StringType()), StructField("airport", StringType()),
    StructField("period", StringType()), StructField("passengers", DoubleType()),
])

new_records = table("SV_Airport_Month").filter(F.col("year") >= LATEST_FULL_YEAR).select(
    "country", "airport", F.format_string("%d-%02d", "year", "month").alias("period"), "passengers"
)


def deliver(year: int) -> None:
    """Simulate a delivery: write the records of one year to the landing folder as JSON files."""
    new_records.filter(F.col("period").startswith(str(year))).repartition(4).write.mode("append").json(LANDING_PATH)


def run_stream() -> None:
    """Query 1: JSON files -> Bronze table.  Query 2: Bronze table -> totals per country and month."""
    (
        spark.readStream.schema(record_schema).json(LANDING_PATH)
        .writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze")
        .trigger(availableNow=True).toTable(f"{SCHEMA}.BZ_Traffic_Stream")
    ).awaitTermination()
    (
        spark.readStream.table(f"{SCHEMA}.BZ_Traffic_Stream")
        .groupBy("country", "period").agg(F.sum("passengers").alias("passengers"))
        .writeStream.format("delta").outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/gold")
        .trigger(availableNow=True).toTable(f"{SCHEMA}.GD_Country_Month_Stream")
    ).awaitTermination()
    months = table("GD_Country_Month_Stream").filter(F.col("country") == HOME_COUNTRY)
    print(f"Records received so far: {table('BZ_Traffic_Stream').count():,}   "
          f"Months of Portugal in the result: {months.count()}   "
          f"Latest month: {months.agg(F.max('period')).first()[0]}")

# COMMAND ----------

# DBTITLE 1,Delivery 1 (the months of 2025), then delivery 2 (the months of 2026)
deliver(LATEST_FULL_YEAR)
run_stream()

deliver(LATEST_FULL_YEAR + 1)
run_stream()      # the checkpoint makes this run read only the new files

# COMMAND ----------

# DBTITLE 1,Each micro-batch is one commit in the Delta history of the table
display(
    spark.sql(f"DESCRIBE HISTORY {SCHEMA}.BZ_Traffic_Stream")
    .select("version", "operation", F.expr("try_element_at(operationMetrics, 'numOutputRows')").alias("rows_written"))
    .orderBy("version")
)

# COMMAND ----------

# DBTITLE 1,Control: the streaming result is the same as a normal (batch) calculation
batch_totals = new_records.groupBy("country", "period").agg(F.sum("passengers").alias("passengers"))
differences = batch_totals.exceptAll(table("GD_Country_Month_Stream")).count()
stream_ok = differences == 0 and batch_totals.count() == table("GD_Country_Month_Stream").count()
print(f"Rows that differ between streaming and batch: {differences}")
print(f"Streaming result identical to batch: {'YES' if stream_ok else 'NO'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Passengers per year, and two scenarios for 2030
# MAGIC * **Scenario A**: passengers keep growing at the rate of 2025.
# MAGIC * **Scenario B**: passengers grow by the same number per year as before the pandemic (2016 to 2019).
# MAGIC
# MAGIC These are two simple reference lines, not a forecast: they ignore airport capacity, prices and the economy.

# COMMAND ----------

# DBTITLE 1,Passengers of Portugal per year
yearly = spark.sql(
    f"""
    SELECT year,
           ROUND(SUM(passengers) / 1e6, 1)                                                      AS passengers_million,
           ROUND(100 * (SUM(passengers) / LAG(SUM(passengers)) OVER (ORDER BY year) - 1), 1)    AS growth_pct
    FROM {SCHEMA}.GD_Portugal_Month
    WHERE year <= {LATEST_FULL_YEAR}
    GROUP BY year
    ORDER BY year
    """
)
display(yearly)

# COMMAND ----------

# DBTITLE 1,The first months of 2026 against the same months of 2025
latest_month = table("GD_Portugal_Month").filter(F.col("year") == LATEST_FULL_YEAR + 1).agg(F.max("month")).first()[0]
year_to_date = (
    table("GD_Portugal_Month").filter(F.col("month") <= latest_month)
    .groupBy().pivot("year", [LATEST_FULL_YEAR, LATEST_FULL_YEAR + 1]).sum("passengers").first()
)
growth_2026 = 100 * (year_to_date[str(LATEST_FULL_YEAR + 1)] / year_to_date[str(LATEST_FULL_YEAR)] - 1)
print(f"January to month {latest_month} of {LATEST_FULL_YEAR + 1}: {year_to_date[str(LATEST_FULL_YEAR + 1)] / 1e6:.1f} million passengers, "
      f"{growth_2026:+.1f}% against the same months of {LATEST_FULL_YEAR}")

# COMMAND ----------

# DBTITLE 1,Chart: history and two scenarios to 2030
history = {row["year"]: row["passengers_million"] for row in yearly.collect()}
latest = history[LATEST_FULL_YEAR]
growth_latest = history[LATEST_FULL_YEAR] / history[LATEST_FULL_YEAR - 1] - 1
yearly_gain_before = (history[PRE_PANDEMIC_YEAR] - history[2016]) / (PRE_PANDEMIC_YEAR - 2016)

future_years = list(range(LATEST_FULL_YEAR, FORECAST_END_YEAR + 1))
scenario_a = [latest * (1 + growth_latest) ** (y - LATEST_FULL_YEAR) for y in future_years]
scenario_b = [latest + yearly_gain_before * (y - LATEST_FULL_YEAR) for y in future_years]

fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(list(history), list(history.values()), marker="o", color="steelblue", label="Observed")
ax.plot(future_years, scenario_a, marker="o", linestyle="--", color="darkorange", label="Scenario A: growth of 2025")
ax.plot(future_years, scenario_b, marker="o", linestyle="--", color="seagreen", label="Scenario B: pre-pandemic pace")
ax.set_title("Air passengers of Portugal per year")
ax.set_ylabel("Million passengers")
ax.grid(alpha=0.3)
ax.legend()
plt.tight_layout()
plt.show()

print(f"{FORECAST_END_YEAR}: scenario A {scenario_a[-1]:.0f} million, scenario B {scenario_b[-1]:.0f} million")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Conclusions

# COMMAND ----------

# DBTITLE 1,The numbers behind the conclusions
network = {row["year"]: row for row in table("GD_Network_Year").collect()}
lisbon = table("GD_Reach").filter("origin = 'LIS'").first()
porto = table("GD_Reach").filter("origin = 'OPO'").first()
top_hub = hubs.first()

print("Q1 - REACH")
print(f"  European network: {100 * (network[2020]['routes'] / network[2019]['routes'] - 1):+.0f}% routes in 2020, "
      f"{100 * (network[NETWORK_YEAR]['routes'] / network[2019]['routes'] - 1):+.0f}% in {NETWORK_YEAR}, against 2019.")
print(f"  Lisbon: {lisbon['direct']} airports direct, {lisbon['within_one_stop']} within one stop "
      f"({lisbon['pct_of_network_within_one_stop']:.0f}% of the network).")
print(f"  Porto : {porto['direct']} airports direct, {porto['within_one_stop']} within one stop "
      f"({porto['pct_of_network_within_one_stop']:.0f}%).")
print(f"  Main hub for Lisbon: {top_hub['airport_name']}, a possible stop for "
      f"{top_hub['pct_of_one_stop_destinations']:.0f}% of the one-stop destinations.")
print(f"  One-stop journeys computed: {n_all_one_stop_journeys:,} in the whole network.")
print("\nQ2 - DEMAND")
print(f"  Portugal, {LATEST_FULL_YEAR}: {latest:.1f} million passengers, "
      f"{100 * (latest / history[PRE_PANDEMIC_YEAR] - 1):+.0f}% against {PRE_PANDEMIC_YEAR}.")
print("\nQ3 - TREND")
print(f"  Growth: {100 * growth_latest:.1f}% in {LATEST_FULL_YEAR}, {growth_2026:.1f}% so far in {LATEST_FULL_YEAR + 1}.")
print(f"  {FORECAST_END_YEAR}: between {min(scenario_a[-1], scenario_b[-1]):.0f} and "
      f"{max(scenario_a[-1], scenario_b[-1]):.0f} million passengers.")
print(f"\nCONTROLS: quality checks passed: {'YES' if checks_ok else 'NO'}; "
      f"streaming identical to batch: {'YES' if stream_ok else 'NO'}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Answers
# MAGIC | Question | Answer |
# MAGIC |---|---|
# MAGIC | **Q1 Reach** | Lisbon has direct flights to 106 airports, but reaches 562 with one stop: 94% of the European network. Porto, with 77 direct destinations, reaches 91%. One hub matters most: Istanbul is a possible stop for 40% of the destinations Lisbon cannot reach directly. |
# MAGIC | **Q2 Demand** | Portugal had 67 million air passengers in 2025, 22% more than in 2019. Lisbon handles half and Porto a quarter. Planes are full (80% to 89% of the seats on the busiest routes), except on the domestic routes Lisbon - Porto and Lisbon - Faro (about 72%). Faro is very seasonal: 4.4 times more passengers in July than in January. |
# MAGIC | **Q3 Trend** | The European network lost half of its routes in 2020 and recovered them all by 2023. In Portugal growth has settled at about 5% a year, and 3.7% in the first four months of 2026. Two simple scenarios give 86 to 91 million passengers in 2030. The streaming pipeline added the months of 2026 without reprocessing 2025, with the same result as a batch calculation. |
# MAGIC
# MAGIC ### Three recommendations
# MAGIC 1. **Promote Porto, Faro and the islands by their one-stop reach.** With one stop they reach between 66% and 91%
# MAGIC    of the network, far more than their direct flights suggest.
# MAGIC 2. **Review the domestic air routes.** Lisbon - Porto and Lisbon - Faro fly with more than a quarter of their
# MAGIC    seats empty: about 400,000 empty seats per direction in a year.
# MAGIC 3. **Fill the winter in Faro.** Its airport handles four times more passengers in summer than in winter, while
# MAGIC    Lisbon, Porto and Funchal are stable all year.
# MAGIC
# MAGIC ### What Spark made possible
# MAGIC * **A join that multiplies the data**: about 6,000 routes became more than 900,000 one-stop journeys.
# MAGIC * **42 files read in two statements**, each line interpreted with the header of its own file.
# MAGIC * **One engine** for files, SQL, a graph and a stream, on tables stored in Delta format.
# MAGIC
# MAGIC ### Limitations
# MAGIC * Route statistics end in 2024; only the airport statistics reach 2026.
# MAGIC * The network is the European one: a route is included when at least one of its airports is in a reporting country.
# MAGIC * "One stop" means that both routes existed in the same year. Timetables and prices are not in the data.
# MAGIC * The scenarios are simple reference lines, not a forecast.
# MAGIC
# MAGIC ### Sources
# MAGIC * Eurostat, datasets `avia_par` (air routes) and `avia_paoa` (airports), downloaded on 16 November 2026.
# MAGIC * Airport names and coordinates: OurAirports (ourairports.com), public domain.
