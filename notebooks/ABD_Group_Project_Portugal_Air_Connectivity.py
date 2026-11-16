# Databricks notebook source
# MAGIC %md
# MAGIC # Portugal's air connectivity
# MAGIC ## The global route network and the passengers who use it: a lakehouse on Apache Spark (Databricks Free Edition)
# MAGIC
# MAGIC **Course:** Analyzing Big Data 2026-2027 &nbsp;|&nbsp; NOVA IMS &nbsp;|&nbsp; **Group Project**
# MAGIC
# MAGIC | Group | Student | Number |
# MAGIC |---|---|---|
# MAGIC | *Group XX* | *Student name 1* | *number* |
# MAGIC | | *Student name 2* | *number* |
# MAGIC | | *Student name 3* | *number* |
# MAGIC | | *Student name 4* | *number* |
# MAGIC | | *Student name 5* | *number* |
# MAGIC | | *Student name 6* | *number* |
# MAGIC
# MAGIC ---
# MAGIC ### Table of contents
# MAGIC 1. Problem definition and motivation
# MAGIC 2. Big Data technologies and solution architecture
# MAGIC 3. Environment setup
# MAGIC 4. Data sources
# MAGIC 5. **Bronze** layer: source data and data rules
# MAGIC 6. **Silver** layer: business rules, modelling and data quality
# MAGIC 7. **Gold** layer: the world route network (Spark SQL)
# MAGIC 8. **Graph analytics**: hubs, reachability and PageRank
# MAGIC 9. Portugal's connectivity: every route with up to two stops
# MAGIC 10. Demand: the passengers of the Portuguese airports
# MAGIC 11. **Structured Streaming**: monthly traffic indicators
# MAGIC 12. The future: passenger scenarios to 2030
# MAGIC 13. Delta Lake: MERGE, audit history and time travel
# MAGIC 14. Conclusions and recommendations
# MAGIC
# MAGIC > **How to run:** upload the four data files of the project to the folder `air_routes` of the Volume
# MAGIC > `/Volumes/workspace/abd/abd` (see section 4), attach the notebook to *Serverless* compute and choose **Run all**.
# MAGIC > The notebook is idempotent: it rebuilds its data lake folders and tables on every run and never modifies the
# MAGIC > uploaded files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive summary
# MAGIC *The numbers of this summary are computed by the notebook and printed in section 14.*
# MAGIC
# MAGIC **The business problem.** Portugal lives from its air links: tourism, the diaspora, the islands and business
# MAGIC travel all depend on them. Airport operators, tourism boards and airline network planners need to know **how well
# MAGIC connected each Portuguese airport is**, **which foreign hubs that connectivity depends on**, and **where the
# MAGIC passengers actually are**.
# MAGIC
# MAGIC **What we did.** We combined two public sources in one lakehouse: a snapshot of the world's airline routes
# MAGIC (the structure of the network) and the official Eurostat statistics of passengers, seats and flights on every
# MAGIC route of the Portuguese airports (the use of the network). With Spark we enumerated **every** itinerary of up to
# MAGIC two stops from every Portuguese airport, which is not feasible on a single machine without sampling.
# MAGIC
# MAGIC **What we found.**
# MAGIC * **Reach.** Lisbon reaches about a hundred airports directly, more than a thousand with one stop, and more than
# MAGIC   three quarters of the airports of the network with two stops. Porto and Faro have about sixty direct
# MAGIC   destinations each, but several hundred with one stop.
# MAGIC * **Hubs.** Lisbon's one-stop reach does not depend on one hub: it is spread over 86 transfer airports, and 14
# MAGIC   of them are needed to cover half of the destinations. The gateways are specialised by region of the world.
# MAGIC * **Demand.** The Portuguese airports handled 64 million passengers in 2024. Lisbon handles about half, and the
# MAGIC   ten busiest routes carry a fifth of the traffic. Planes are full: about 86% of the seats are occupied, but only
# MAGIC   about 73% on the domestic routes Lisbon - Porto and Lisbon - Faro.
# MAGIC * **Seasonality.** Faro has four times more passengers in August than in January, and Ponta Delgada 2.6 times.
# MAGIC   Lisbon, Porto and Funchal are stable all year.
# MAGIC * **Trend.** Traffic is 19% above 2019 but well below the path it was following before the pandemic, and growth
# MAGIC   slowed to about 4% in 2024.
# MAGIC
# MAGIC **What we recommend** (section 14): keep a portfolio of long-haul gateways, promote the regional airports by
# MAGIC their one-stop reach, target winter demand in Faro and the Azores, review the domestic air routes with low
# MAGIC occupancy, and plan capacity for a range of scenarios instead of one forecast.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Problem definition and motivation
# MAGIC
# MAGIC ### Context
# MAGIC An airport is only as valuable as the places it connects to. For a country on the edge of Europe, with two island
# MAGIC regions and an economy in which tourism weighs heavily, air connectivity is strategic. It has two sides:
# MAGIC
# MAGIC * **Structure**: which airports are linked by a route, so which journeys are possible, with how many stops and
# MAGIC   through which hubs. This is a **graph** problem.
# MAGIC * **Use**: how many passengers, seats and flights each route really has, month by month. This is a
# MAGIC   **time-series** problem.
# MAGIC
# MAGIC Why it is a Big Data problem:
# MAGIC
# MAGIC * **Volume through combination.** The route file is small, but the question "how can I get from A to B?" multiplies
# MAGIC   it: 36,759 direct connections generate about 2.3 million one-stop itineraries and about 146 million two-stop
# MAGIC   itineraries. An earlier version of this analysis, written in pandas for another course, had to restrict the
# MAGIC   network to large airports and test a random sample of 60 airports. Spark computes the itineraries with
# MAGIC   distributed joins, with no sampling.
# MAGIC * **Variety.** Three relational CSV files, one wide statistical text file with embedded flags (Eurostat), and JSON
# MAGIC   event files (streaming).
# MAGIC * **Velocity.** Traffic statistics arrive every month and the indicators must be updated incrementally.
# MAGIC * **Veracity.** The route snapshot is undated and contains airlines that no longer exist; airline codes are
# MAGIC   duplicated; Eurostat marks missing values with symbols. Each of these is handled explicitly.
# MAGIC
# MAGIC ### Business questions
# MAGIC
# MAGIC | # | Business question | Who needs the answer | Processing type | Section |
# MAGIC |---|---|---|---|---|
# MAGIC | Q1 | How is the world route network structured, and which airports are its hubs? | Airline network planners | Spark SQL, graph analytics | 7, 8 |
# MAGIC | Q2 | How much of the world can be reached from each Portuguese airport with 0, 1 and 2 stops, and through which hubs? | Airport operators, tourism boards | Distributed joins, window functions | 9 |
# MAGIC | Q3 | Where are the passengers? Which airports and routes carry the traffic, how full are the planes, how seasonal is demand? | Airport and airline commercial teams | Spark SQL | 10 |
# MAGIC | Q4 | Can the monthly indicators be kept up to date as new statistics arrive, with the same results as batch? | Data teams | Structured Streaming | 11 |
# MAGIC | Q5 | Where is demand heading until 2030? | Capacity planners | Spark SQL (regression) | 12 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Big Data technologies and solution architecture
# MAGIC
# MAGIC ### 2.1 The project as a pipeline of the ABD classes
# MAGIC
# MAGIC | Class | Topic | Where it is applied in this project |
# MAGIC |---|---|---|
# MAGIC | ABD02, ABD03 | Databricks Free Edition, Unity Catalog, Volumes, `dbutils` | Sections 3 and 4: source files and data lake in the Volume `/Volumes/workspace/abd/abd` |
# MAGIC | ABD04, ABD05 | Spark basics, DataFrames, reading files, tables | Section 5: structured (CSV) and unstructured (text) files read into DataFrames |
# MAGIC | ABD06 | Spark SQL programming: aggregations, pivot, explode, dates, windowing, UDFs, PageRank | Sections 6 to 10 and 12 |
# MAGIC | ABD07 | The Data Lakehouse: medallion architecture, Delta Lake, MERGE, time travel | Sections 5, 6, 7 and 13 |
# MAGIC | ABD08 | Spark Structured Streaming: source, transformation, sink, checkpoint | Section 11 |
# MAGIC | ABD09 | Graph analytics: vertices and edges, degrees, motifs, subgraphs, BFS, PageRank | Sections 8 and 9 |
# MAGIC
# MAGIC ### 2.2 Technologies
# MAGIC
# MAGIC | Technology | How it is used in this project |
# MAGIC |---|---|
# MAGIC | **Databricks Free Edition** (serverless compute) | Execution environment for the whole notebook |
# MAGIC | **Apache Spark DataFrame API** (PySpark) | Ingestion, parsing, joins that enumerate itineraries, pivot, window functions, a UDF |
# MAGIC | **Spark SQL** | Gold layer and analysis: `%sql` cells, CTEs, window functions, conditional aggregation, regression aggregates |
# MAGIC | **Graph analytics on Spark** | Degrees, motif, subgraph, breadth-first search, **PageRank** and path enumeration, written with DataFrames |
# MAGIC | **Spark Structured Streaming** | Incremental ingestion of JSON traffic records: checkpoints, stream-static join, stateful deduplication, aggregation |
# MAGIC | **Delta Lake** | Storage format of the Silver and Gold tables: ACID writes, `MERGE`, history and time travel |
# MAGIC | **Unity Catalog** | Data organisation: a Volume for files, a schema for tables, table comments |
# MAGIC
# MAGIC ### 2.3 Data organisation: medallion architecture
# MAGIC The lakehouse follows the organisation of class ABD07: the layers live in folders of the Volume, the Bronze layer
# MAGIC is stored as **Parquet** in one folder per load date, and the Silver and Gold layers are **Delta** tables of a
# MAGIC dedicated schema, with the prefixes `SV_` and `GD_`.
# MAGIC
# MAGIC ```
# MAGIC  Volume /Volumes/workspace/abd/abd                              Schema workspace.DL_AIR
# MAGIC
# MAGIC  SOURCES (air_routes/)        BRONZE (DL_AIR/Bronze/<date>/)    SILVER (Delta tables SV_)       GOLD (Delta tables GD_)
# MAGIC  ---------------------        ------------------------------    ------------------------       -----------------------
# MAGIC  airports.csv           -->   Parquet: airports            -->  SV_Airports               -->  GD_Airport_Connectivity
# MAGIC  routes.csv            batch           routes                   SV_Airlines                    GD_Graph_Vertices / _Edges
# MAGIC  airlines.csv                          airlines                 SV_Routes                      GD_Airport_PageRank
# MAGIC                                                                 SV_Connections                 GD_Flights_From_Lisbon
# MAGIC                                                                                                GD_Global_Transfer_Hubs
# MAGIC                                                                                                GD_Portugal_Best_Itineraries
# MAGIC  avia_par_pt.tsv.gz     -->   Parquet: avia_par_pt         -->  SV_Air_Traffic            -->  GD_Airport_Year
# MAGIC  (Eurostat, text)      batch  (lines of text)                                                  GD_Route_Year
# MAGIC                                                                                                GD_Airport_Month
# MAGIC                                                                                                GD_Passenger_Scenarios
# MAGIC
# MAGIC  Monthly traffic        -->   BZ_Traffic_Stream            -->  SV_Traffic_Stream         -->  GD_Airport_Month_Stream
# MAGIC  records, JSON files  streaming      (append)                        (append)                       (complete)
# MAGIC  in DL_AIR/Landing/                checkpoint                       checkpoint                      checkpoint
# MAGIC ```
# MAGIC
# MAGIC * **Bronze**: the source data after the **data rules** (relevant columns, data types, nulls in key columns,
# MAGIC   duplicates). Each load has its own dated folder, so it can be reprocessed.
# MAGIC * **Silver**: the result of the **business rules**: clean entities (airports, airlines) and facts (routes,
# MAGIC   connections, traffic observations) at full detail.
# MAGIC * **Gold**: small aggregated tables, each one built to answer one business question.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Environment setup
# MAGIC All names, paths and parameters used by the pipeline are defined once in the next cell.

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import datetime
import os

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

import matplotlib.pyplot as plt

# ---- Unity Catalog: Volume for files and schema for tables --------------------
CATALOG = "workspace"
VOLUME_PATH = "/Volumes/workspace/abd/abd"   # the Volume used in the ABD classes
SOURCE_PATH = f"{VOLUME_PATH}/air_routes"    # uploaded source files (never modified by the notebook)
SCHEMA = "DL_AIR"                            # schema of the lakehouse tables (Silver and Gold)

# ---- Data lake folders: one per layer, as in class ABD07 ----------------------
DL_Path = f"{VOLUME_PATH}/DL_AIR"
DL_Bronze_Path = f"{DL_Path}/Bronze/"
DL_Landing_Path = f"{DL_Path}/Landing/traffic_records"   # landing zone for the streaming JSON records
DL_Checkpoint_Path = f"{DL_Path}/Checkpoints"            # one sub-folder per streaming query

# ---- Source files -----------------------------------------------------------
CSV_FILES = ["airports", "routes", "airlines"]
EUROSTAT_FILE = "avia_par_pt"       # avia_par_pt.tsv.gz

# ---- Scope of the analysis ----------------------------------------------------
HOME_COUNTRY = "PT"
HOME_HUB = "LIS"                    # origin of the breadth-first search
FLAG_CARRIER = "TP"                 # IATA code of TAP, used for the subgraph
MAIN_AIRPORTS = ["LIS", "OPO", "FAO", "FNC", "PDL"]   # airports shown in the charts

# ---- Business rules -----------------------------------------------------------
FORMER_ICAO_CODES = {"LPFU": "LPMA"}   # Madeira airport (Funchal): code used by Eurostat until 2005 -> current code
SHORT_HAUL_MAX_KM = 1500            # up to 1,500 km: short haul
MEDIUM_HAUL_MAX_KM = 4000           # up to 4,000 km: medium haul; above: long haul

# ---- Graph analytics --------------------------------------------------------
PAGERANK_DAMPING = 0.85
PAGERANK_MAX_ITER = 60
PAGERANK_TOLERANCE = 1e-5
BFS_MAX_HOPS = 20

# ---- Demand analysis (Eurostat) -----------------------------------------------
LATEST_YEAR = 2024                  # last complete year in the Eurostat file
PRE_PANDEMIC_YEAR = 2019
TREND_FIRST_YEAR = 2015             # pre-pandemic trend is fitted on 2015-2019
FORECAST_END_YEAR = 2030
LONG_RUN_FIRST_YEAR = 2004          # long-run growth rate: 2004 to 2019

# ---- Streaming simulation ---------------------------------------------------
STREAM_DROP_1_YEAR = 2023           # first drop: the 12 months of 2023
STREAM_DROP_2_YEAR = 2024           # second drop: the 12 months of 2024
FILES_PER_DROP = 6                  # JSON files written to the landing zone per drop
MAX_FILES_PER_TRIGGER = 2           # files consumed by each streaming micro-batch


def table_name(name: str) -> str:
    """Return the fully qualified Unity Catalog name of a lakehouse table."""
    return f"{CATALOG}.{SCHEMA}.{name}"


print(f"Source files     : {SOURCE_PATH}")
print(f"Data lake folder : {DL_Path}")
print(f"Lakehouse schema : {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Remove the tables and files of previous runs, create the schema and the folders
# The Volume of the classes is created only if it does not exist yet
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.abd")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.abd.abd")

# Removing old tables and files (from previous runs), so that the notebook can be run again at any time.
# The uploaded source files in SOURCE_PATH are kept.
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
dbutils.fs.rm(DL_Path, True)

# Creating our schema to store the lakehouse tables, and the folder for the source files
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
    "COMMENT 'ABD group project: air connectivity of Portugal (Silver and Gold tables)'"
)
dbutils.fs.mkdirs(SOURCE_PATH)

# COMMAND ----------

# DBTITLE 1,Verify the schema
spark.sql(f"DESCRIBE SCHEMA EXTENDED {CATALOG}.{SCHEMA}").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Helper functions
def save_table(df: DataFrame, name: str, comment: str) -> str:
    """Save `df` as a managed Delta table of the lakehouse schema and document it with a comment."""
    full_name = table_name(name)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    safe_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{safe_comment}'")
    print(f"Saved {full_name} ({spark.table(full_name).count():,} rows)")
    return full_name


def row_count(name: str) -> int:
    """Number of rows of a lakehouse table."""
    return spark.table(table_name(name)).count()


EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in kilometres between two points given in decimal degrees (column expressions)."""
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = F.sin(d_lat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(d_lon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(F.least(F.lit(1.0), a)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data sources
# MAGIC
# MAGIC | Source | Files | Content | Origin |
# MAGIC |---|---|---|---|
# MAGIC | **World airline routes** | `airports.csv`, `routes.csv`, `airlines.csv` | Airports of the world with location and type; direct routes between airports, one row per airline and route; airlines | Public static snapshot (Kaggle), the same data used in the group's earlier pandas project *Airport Network Route Analysis* |
# MAGIC | **Eurostat air transport** | `avia_par_pt.tsv.gz` | Passengers, seats and commercial flights between each main Portuguese airport and each partner airport, per month, quarter and year since 1993 | Eurostat dataset `avia_par_pt`, downloaded on 16 November 2026 |
# MAGIC
# MAGIC The files are uploaded unchanged with the Databricks interface to the folder `air_routes` of the Volume
# MAGIC `/Volumes/workspace/abd/abd` (Catalog > `workspace` > `abd` > `abd` > `air_routes` > *Upload to this volume*).
# MAGIC Databricks Free Edition restricts outbound internet access, so the files are uploaded instead of being downloaded
# MAGIC by the notebook.
# MAGIC
# MAGIC **What each source can and cannot say.**
# MAGIC * The route snapshot describes the **structure** of the network: which airports are linked. It is undated and has
# MAGIC   no frequencies, schedules, prices or passengers. Section 10.5 compares it with the routes that really operated
# MAGIC   in 2024.
# MAGIC * Eurostat describes the **use** of the routes of the Portuguese airports: real passengers, seats and flights. It
# MAGIC   has no airline dimension, and it lists the *main* partner airports of each airport, not every route.

# COMMAND ----------

# DBTITLE 1,Check the source files in the Volume
expected_files = [f"{name}.csv" for name in CSV_FILES] + [f"{EUROSTAT_FILE}.tsv.gz"]
missing_files = [f for f in expected_files if not os.path.exists(f"{SOURCE_PATH}/{f}")]

if missing_files:
    raise FileNotFoundError(
        f"Missing source files in {SOURCE_PATH}: {missing_files}. "
        "Upload the four files of the project data folder to this folder of the Volume and run the notebook again."
    )

display(dbutils.fs.ls(SOURCE_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze layer: source data and data rules
# MAGIC
# MAGIC ### 5.1 Read the source data
# MAGIC Before defining any rule we look at the files as they arrive: structure, data types and content.
# MAGIC * The three CSV files are **structured** data. Text fields are quoted and can contain commas and quotes, so the
# MAGIC   reader is told which character quotes and which one escapes.
# MAGIC * The Eurostat file is read as **text**, one row per line. Its layout is not relational (one column per period,
# MAGIC   values mixed with status flags), so it is treated as unstructured data until the Silver layer.

# COMMAND ----------

# DBTITLE 1,Structured data: the routes
def read_source_csv(name: str) -> DataFrame:
    """Read one source CSV file. Every column is read as text; quoted fields may contain commas and quotes."""
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{SOURCE_PATH}/{name}.csv")
    )


raw_routes = read_source_csv("routes")

raw_routes.printSchema()
print(f"Rows: {raw_routes.count():,}")
raw_routes.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Structured data: the airports
raw_airports = read_source_csv("airports")

print(f"Rows: {raw_airports.count():,}")
raw_airports.select(
    "ident", "type", "name", "latitude_deg", "longitude_deg", "continent", "iso_country", "icao_code", "iata_code"
).limit(5).display()

# COMMAND ----------

# DBTITLE 1,Checking for nulls in the airports
raw_airports.select(
    [F.count(F.when(F.isnull(c), True)).alias(c) for c in raw_airports.columns]
).display()

# COMMAND ----------

# DBTITLE 1,Structured data: the airlines
raw_airlines = read_source_csv("airlines")

raw_airlines.limit(5).display()
raw_airlines.groupBy("active").count().display()

# COMMAND ----------

# DBTITLE 1,Unstructured data: the Eurostat file as lines of text
raw_eurostat = spark.read.text(f"{SOURCE_PATH}/{EUROSTAT_FILE}.tsv.gz")

print(f"Lines: {raw_eurostat.count():,}")
raw_eurostat.select(F.substring("value", 1, 120).alias("first_120_characters_of_the_line")).limit(4).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Observations.**
# MAGIC * Every column is read as text, so numbers (coordinates, distance, stops) have to be converted.
# MAGIC * `airports` is a registry of every landing place in the world, including heliports and closed airfields. Only
# MAGIC   about one row in ten has an IATA code, the code that the routes use. Many descriptive columns (links, keywords,
# MAGIC   local codes) are mostly empty and are not taken to the Bronze layer.
# MAGIC * The continent code of North America is the text `NA`. It must stay a value and not be read as "not available".
# MAGIC * `airlines` writes missing values as the text `\N`, and the `active` flag is not consistent (`Y`, `N` and `n`).
# MAGIC * The Eurostat file packs four dimensions in the first column, has one column per period (year, quarter and
# MAGIC   month mixed) and marks missing values with `:`.
# MAGIC
# MAGIC ### 5.2 Apply the data rules and save the Bronze files
# MAGIC **This is where the "data" rules apply.** The same rules are applied to every CSV file, driven by one
# MAGIC specification per file:
# MAGIC
# MAGIC 1. **Relevant columns**: keep only the columns that carry information.
# MAGIC 2. **Missing-value markers**: the text `\N` becomes a real null.
# MAGIC 3. **Data types**: convert numbers (`try_cast`, so that an invalid value becomes null instead of stopping the pipeline).
# MAGIC 4. **Nulls**: drop the records with a null in a key column.
# MAGIC 5. **Duplicates**: drop repeated records.
# MAGIC
# MAGIC The result is saved in **Parquet** format, efficient and adequate for read-only data, in a folder named after the
# MAGIC load date so that any load can be reprocessed with minimal effort.

# COMMAND ----------

# DBTITLE 1,Bronze specification: columns, data types and keys of each CSV file
BRONZE_SPECS = {
    "airports": {
        "columns": {
            "ident": "string", "type": "string", "name": "string",
            "latitude_deg": "double", "longitude_deg": "double",
            "continent": "string", "iso_country": "string", "municipality": "string",
            "scheduled_service": "string", "icao_code": "string", "iata_code": "string",
        },
        "keys": ["ident"],
    },
    "routes": {
        "columns": {
            "airline_iata": "string", "airline_name": "string", "airline_country": "string",
            "src_iata": "string", "src_country": "string", "dst_iata": "string", "dst_country": "string",
            "codeshare": "string", "stops": "int", "equipment": "string", "distance_km": "int",
        },
        "keys": ["airline_iata", "src_iata", "dst_iata"],
    },
    "airlines": {
        "columns": {
            "airline_id": "int", "airline_name": "string", "iata_code": "string",
            "icao_code": "string", "country": "string", "active": "string",
        },
        "keys": ["airline_id"],
    },
}

# One folder per load date
date_tag = datetime.datetime.now().strftime("%Y-%b-%d")
DL_Bronze_Path_day = DL_Bronze_Path + date_tag + "/"
print(DL_Bronze_Path_day)

# COMMAND ----------

# DBTITLE 1,Apply the data rules and write the Bronze files (Parquet)
def typed_column(name: str, data_type: str):
    """Bronze value of a source text column: the marker \\N becomes null, numbers are converted."""
    value = F.when(F.col(name) == "\\N", None).otherwise(F.col(name))
    if data_type == "string":
        return value.alias(name)
    return F.expr(f"try_cast(`{name}` AS {data_type})").alias(name)


bronze_log = []

for name in CSV_FILES:
    spec = BRONZE_SPECS[name]
    raw = read_source_csv(name)

    raw_typed = raw.select([typed_column(c, t) for c, t in spec["columns"].items()])   # rules 1, 2 and 3
    raw_noNulls = raw_typed.dropna(subset=spec["keys"])                                # rule 4
    raw_noDup = raw_noNulls.dropDuplicates()                                           # rule 5
    raw_Final = raw_noDup.withColumn("_source_file", F.lit(f"{name}.csv")).withColumn(
        "_ingested_at", F.current_timestamp()
    )

    raw_Final.write.format("parquet").mode("overwrite").save(DL_Bronze_Path_day + name)
    bronze_log.append((f"{name}.csv", raw.count(), raw_noNulls.count(), raw_noDup.count()))

# The Eurostat file: lines of text, without empty lines and without repeated lines
raw = spark.read.text(f"{SOURCE_PATH}/{EUROSTAT_FILE}.tsv.gz")
raw_noNulls = raw.filter(F.length(F.trim("value")) > 0)
raw_noDup = raw_noNulls.dropDuplicates()
raw_Final = raw_noDup.withColumn("_source_file", F.lit(f"{EUROSTAT_FILE}.tsv.gz")).withColumn(
    "_ingested_at", F.current_timestamp()
)
raw_Final.write.format("parquet").mode("overwrite").save(DL_Bronze_Path_day + EUROSTAT_FILE)
bronze_log.append((f"{EUROSTAT_FILE}.tsv.gz", raw.count(), raw_noNulls.count(), raw_noDup.count()))

# Log with the statistics of the load, for control purposes
bronze_log_df = spark.createDataFrame(
    bronze_log, "source_file STRING, rows_read LONG, rows_after_null_rule LONG, rows_in_bronze LONG"
).withColumn("rows_removed", F.col("rows_read") - F.col("rows_in_bronze"))

bronze_log_df.display()

# COMMAND ----------

# DBTITLE 1,Check the Bronze files in the Volume
display(dbutils.fs.ls(DL_Bronze_Path_day))


def read_bronze(name: str) -> DataFrame:
    """Read one file of the Bronze layer (the load of the current run)."""
    return spark.read.parquet(DL_Bronze_Path_day + name)


read_bronze("routes").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver layer: business rules, modelling and data quality
# MAGIC **This is where the "business" rules apply.** The Bronze files are read and conformed into the entities and facts
# MAGIC of the problem. Each one is saved as a Delta table with the prefix `SV_`.
# MAGIC
# MAGIC ### 6.1 Reference data: airports and airlines

# COMMAND ----------

# DBTITLE 1,SV_Airports
sv_airports_df = read_bronze("airports").select(
    F.col("ident").alias("airport_ident"),
    "iata_code",
    "icao_code",
    F.col("name").alias("airport_name"),
    F.col("type").alias("airport_type"),
    F.col("latitude_deg").alias("latitude"),
    F.col("longitude_deg").alias("longitude"),
    "continent",
    F.col("iso_country").alias("country"),
    "municipality",
    (F.col("scheduled_service") == "yes").alias("has_scheduled_service"),
)

save_table(sv_airports_df, "SV_Airports", "SILVER reference data - airports of the world (one row per landing place)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Airlines.** The same IATA code appears in several rows of the airline file, because codes are reused when an
# MAGIC airline closes. The routes identify the airline only by this code, so the business rule is: **one row per IATA
# MAGIC code**, preferring the airline that is active. It is implemented with the window function `row_number`.

# COMMAND ----------

# DBTITLE 1,SV_Airlines: one row per IATA code
airline_priority = Window.partitionBy("iata_code").orderBy(F.col("is_active").desc(), F.col("airline_id"))

sv_airlines_df = (
    read_bronze("airlines")
    .withColumn("is_active", F.upper(F.trim("active")) == "Y")          # Y, N and n become a boolean
    .filter(F.col("iata_code").isNotNull() & (F.length("iata_code") == 2))   # '-' and empty codes are not airlines
    .withColumn("priority", F.row_number().over(airline_priority))
    .withColumn("airlines_sharing_the_code", F.count("*").over(Window.partitionBy("iata_code")))
    .filter(F.col("priority") == 1)
    .select(
        F.col("iata_code").alias("airline_iata"),
        F.col("airline_name").alias("registry_airline_name"),
        F.col("country").alias("registry_country"),
        "is_active",
        "airlines_sharing_the_code",
    )
)

save_table(sv_airlines_df, "SV_Airlines", "SILVER reference data - one airline per IATA code, preferring the active one")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Routes and connections
# MAGIC * A **route** is one airline flying between two airports (one row per airline, origin and destination).
# MAGIC * A **connection** is a pair of airports linked by at least one airline. It is the edge of the network.
# MAGIC
# MAGIC Business rules:
# MAGIC * a route from an airport to itself, or with no distance, is not a route and is removed;
# MAGIC * each route is enriched with the location of both airports (join with `SV_Airports`), with the status of the
# MAGIC   airline (join with `SV_Airlines`), and with the **haul category**, computed by a **UDF**;
# MAGIC * the great-circle distance is recomputed from the coordinates to validate the distance of the file.

# COMMAND ----------

# DBTITLE 1,A UDF for the haul category
def haul_category(distance_km):
    """Commercial category of a route by its distance: short, medium or long haul."""
    if distance_km is None:
        return None
    if distance_km <= SHORT_HAUL_MAX_KM:
        return "1 - Short haul"
    if distance_km <= MEDIUM_HAUL_MAX_KM:
        return "2 - Medium haul"
    return "3 - Long haul"


haul_category_udf = F.udf(haul_category, StringType())

# COMMAND ----------

# DBTITLE 1,SV_Routes
airports_with_iata = (
    spark.table(table_name("SV_Airports"))
    .filter(F.col("iata_code").isNotNull())
    .select("iata_code", "airport_name", "airport_type", "latitude", "longitude", "continent", "country", "municipality")
)
src_airports = airports_with_iata.select(
    F.col("iata_code").alias("src_iata"),
    F.col("airport_name").alias("src_airport"),
    F.col("latitude").alias("src_lat"),
    F.col("longitude").alias("src_lon"),
    F.col("continent").alias("src_continent"),
)
dst_airports = airports_with_iata.select(
    F.col("iata_code").alias("dst_iata"),
    F.col("airport_name").alias("dst_airport"),
    F.col("latitude").alias("dst_lat"),
    F.col("longitude").alias("dst_lon"),
    F.col("continent").alias("dst_continent"),
)

sv_routes_df = (
    read_bronze("routes")
    .filter((F.col("src_iata") != F.col("dst_iata")) & (F.col("distance_km") > 0))
    .join(src_airports, "src_iata", "left")
    .join(dst_airports, "dst_iata", "left")
    .join(F.broadcast(spark.table(table_name("SV_Airlines"))), "airline_iata", "left")
    .withColumn("great_circle_km", haversine_km(F.col("src_lat"), F.col("src_lon"), F.col("dst_lat"), F.col("dst_lon")))
    .withColumn("haul", haul_category_udf("distance_km"))
    .withColumn("is_domestic", F.col("src_country") == F.col("dst_country"))
    .withColumn("is_intercontinental", F.col("src_continent") != F.col("dst_continent"))
    .withColumn("is_codeshare", F.coalesce(F.col("codeshare") == "Y", F.lit(False)))
    .select(
        "airline_iata", "airline_name", "airline_country",
        F.coalesce("is_active", F.lit(False)).alias("airline_is_active"),
        "src_iata", "src_airport", "src_country", "src_continent", "src_lat", "src_lon",
        "dst_iata", "dst_airport", "dst_country", "dst_continent", "dst_lat", "dst_lon",
        "distance_km", "great_circle_km", "haul", "is_domestic", "is_intercontinental",
        "is_codeshare", "stops", "equipment",
    )
)

save_table(
    sv_routes_df,
    "SV_Routes",
    "SILVER - one row per airline and route (origin, destination), enriched with airport and airline attributes",
)

# COMMAND ----------

# DBTITLE 1,SV_Connections: one row per pair of airports linked by at least one airline
sv_connections_df = (
    spark.table(table_name("SV_Routes"))
    .groupBy("src_iata", "dst_iata")
    .agg(
        F.min("distance_km").alias("distance_km"),
        F.countDistinct("airline_iata").alias("airlines"),
        F.first("src_country").alias("src_country"),
        F.first("dst_country").alias("dst_country"),
        F.first("haul").alias("haul_of_first_route"),
    )
    .withColumn("haul", haul_category_udf("distance_km"))
    .drop("haul_of_first_route")
)

save_table(
    sv_connections_df,
    "SV_Connections",
    "SILVER - directed connection between two airports: shortest distance and number of airlines",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Eurostat: from a wide text file to a tidy table
# MAGIC The lines of text of the Bronze file are parsed with Spark only (`split` and `posexplode`, as in class ABD06):
# MAGIC 1. the header line gives the list of periods;
# MAGIC 2. `posexplode` turns the values of each line into rows and joins them to the periods **by position** (unpivot);
# MAGIC 3. the first column is split into frequency, unit, measure and route, and the route into reporting airport,
# MAGIC    partner country and partner airport;
# MAGIC 4. each cell is separated into a numeric value and a status flag; cells without a value (`:`) are dropped;
# MAGIC 5. the airports are identified by their ICAO code and joined with `SV_Airports` to get names and locations.
# MAGIC
# MAGIC | Eurostat code | Meaning | Name in Silver |
# MAGIC |---|---|---|
# MAGIC | `PAS_CRD` | Passengers carried (each passenger counted once per flight number) | `passengers_carried` |
# MAGIC | `PAS_BRD` | Passengers on board (counted on every flight stage) | `passengers_on_board` |
# MAGIC | `ST_PAS` | Passenger seats available | `seats` |
# MAGIC | `CAF_PAS` | Commercial passenger flights | `flights` |
# MAGIC
# MAGIC A suffix `_DEP` or `_ARR` gives the direction (departures or arrivals of the reporting airport); no suffix is the total.

# COMMAND ----------

# DBTITLE 1,A code that changed: the reporting airports and the years in which each one appears
eurostat_keys = (
    read_bronze(EUROSTAT_FILE)
    .select("value")
    .filter(F.col("value").startswith("A,PAS,PAS_CRD,"))
    .select(
        F.split(F.split(F.split("value", "\t").getItem(0), ",").getItem(3), "_").getItem(1).alias("reporting_icao"),
        F.posexplode(F.split("value", "\t")).alias("position", "cell"),
    )
    .filter((F.col("position") > 0) & F.col("cell").rlike("^[0-9]"))
)
header_periods = (
    read_bronze(EUROSTAT_FILE)
    .select("value")
    .filter(F.col("value").startswith("freq"))
    .select(F.posexplode(F.split("value", "\t")).alias("position", "period"))
    .select("position", F.trim("period").alias("period"))
    .filter(F.col("period").rlike("^[0-9]{4}$"))
)

display(
    eurostat_keys.join(F.broadcast(header_periods), "position")
    .groupBy("reporting_icao")
    .agg(F.min("period").alias("first_year"), F.max("period").alias("last_year"))
    .join(
        spark.table(table_name("SV_Airports")).select(
            F.col("icao_code").alias("reporting_icao"), "iata_code", "airport_name"
        ),
        "reporting_icao",
        "left",
    )
    .orderBy("reporting_icao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **A business rule for a code that changed.** One reporting code, `LPFU`, is not in the airport table. It appears
# MAGIC only until 2005, and the code of Madeira airport, `LPMA`, appears only from 2006. `LPFU` is the former ICAO code
# MAGIC of the same airport (Funchal), so the two are one series. The rule: **`LPFU` is replaced by `LPMA`**, for the
# MAGIC reporting airport and for the partner airport. In 2006, the year of the change, some routes have one part of the
# MAGIC year under each code; after the replacement the two parts are **added**, so that each route has one value per
# MAGIC period. The data quality report checks that no duplicates remain.

# COMMAND ----------

# DBTITLE 1,SV_Air_Traffic
lines = read_bronze(EUROSTAT_FILE).select("value")
is_header = F.col("value").startswith("freq")

periods = lines.filter(is_header).select(
    F.posexplode(F.split("value", "\t")).alias("position", "period")
).select("position", F.trim("period").alias("period"))

cells = lines.filter(~is_header).select(
    F.split("value", "\t").getItem(0).alias("series_key"),
    F.posexplode(F.split("value", "\t")).alias("position", "cell"),
)

icao_airports = (
    spark.table(table_name("SV_Airports"))
    .filter(F.col("icao_code").isNotNull())
    .select("icao_code", "iata_code", "airport_name", "country", "continent", "latitude", "longitude")
)

measure_code = F.split("series_key", ",").getItem(2)
route_code = F.split("series_key", ",").getItem(3)


def current_icao(code):
    """Business rule: replace a former ICAO code by the current code of the same airport."""
    mapped = code
    for former, current in FORMER_ICAO_CODES.items():
        mapped = F.when(code == former, current).otherwise(mapped)
    return mapped


sv_air_traffic_df = (
    cells.filter(F.col("position") > 0)
    .join(F.broadcast(periods), "position")
    .withColumn("value", F.expr("try_cast(nullif(regexp_extract(cell, '^([0-9.]+)', 1), '') AS DOUBLE)"))
    .filter(F.col("value").isNotNull())
    .select(
        F.split("series_key", ",").getItem(0).alias("frequency"),
        F.when(measure_code.startswith("PAS_CRD"), "passengers_carried")
        .when(measure_code.startswith("PAS_BRD"), "passengers_on_board")
        .when(measure_code.startswith("ST_PAS"), "seats")
        .when(measure_code.startswith("CAF_PAS"), "flights")
        .alias("measure"),
        F.when(measure_code.endswith("_DEP"), "DEPARTURES")
        .when(measure_code.endswith("_ARR"), "ARRIVALS")
        .otherwise("TOTAL")
        .alias("direction"),
        current_icao(F.split(route_code, "_").getItem(1)).alias("reporting_icao"),
        F.split(route_code, "_").getItem(2).alias("partner_country_eurostat"),
        current_icao(F.split(route_code, "_").getItem(3)).alias("partner_icao"),
        "period",
        F.substring("period", 1, 4).cast("int").alias("year"),
        F.when(F.col("period").rlike("^[0-9]{4}-[0-9]{2}$"), F.substring("period", 6, 2).cast("int")).alias("month"),
        F.when(F.col("period").contains("-Q"), F.substring("period", 7, 1).cast("int")).alias("quarter"),
        "value",
        F.expr("nullif(trim(regexp_replace(cell, '[0-9.:@]', '')), '')").alias("flag"),
    )
    # After the code change, the two parts of the year of the change belong to the same route: they are added
    .groupBy(
        "frequency", "measure", "direction", "reporting_icao", "partner_country_eurostat", "partner_icao",
        "period", "year", "month", "quarter",
    )
    .agg(F.sum("value").alias("value"), F.max("flag").alias("flag"))
    .withColumn("route_code", F.concat_ws("_", F.lit(HOME_COUNTRY), "reporting_icao", "partner_country_eurostat",
                                          "partner_icao"))
    .join(
        F.broadcast(icao_airports.select(
            F.col("icao_code").alias("reporting_icao"),
            F.col("iata_code").alias("reporting_iata"),
            F.col("airport_name").alias("reporting_airport"),
        )),
        "reporting_icao",
        "left",
    )
    .join(
        F.broadcast(icao_airports.select(
            F.col("icao_code").alias("partner_icao"),
            F.col("iata_code").alias("partner_iata"),
            F.col("airport_name").alias("partner_airport"),
            F.col("country").alias("partner_country"),
            F.col("continent").alias("partner_continent"),
        )),
        "partner_icao",
        "left",
    )
)

save_table(
    sv_air_traffic_df,
    "SV_Air_Traffic",
    "SILVER - Eurostat avia_par_pt: one row per route of a Portuguese airport, measure, direction and period",
)

# COMMAND ----------

# DBTITLE 1,How the Eurostat file expands: observations per frequency and measure (pivot)
display(
    spark.table(table_name("SV_Air_Traffic"))
    .filter(F.col("direction") == "TOTAL")
    .groupBy("measure")
    .pivot("frequency", ["A", "Q", "M"])
    .count()
    .withColumnRenamed("A", "annual")
    .withColumnRenamed("Q", "quarterly")
    .withColumnRenamed("M", "monthly")
    .orderBy("measure")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Data quality report
# MAGIC Each check counts the records that **violate** a rule. A check passes when the count is zero. Checks marked
# MAGIC *(info)* describe a known limitation of the sources: they are reported, not expected to be zero.

# COMMAND ----------

# DBTITLE 1,Data quality checks
airports_t = spark.table(table_name("SV_Airports"))
airlines_t = spark.table(table_name("SV_Airlines"))
routes_t = spark.table(table_name("SV_Routes"))
connections_t = spark.table(table_name("SV_Connections"))
traffic_t = spark.table(table_name("SV_Air_Traffic"))

annual_latest = traffic_t.filter(
    (F.col("frequency") == "A") & (F.col("year") == LATEST_YEAR) & (F.col("direction") == "TOTAL")
)
directional = (
    traffic_t.filter((F.col("frequency") == "A") & (F.col("year") == LATEST_YEAR)
                     & (F.col("measure") == "passengers_carried"))
    .groupBy("route_code")
    .pivot("direction", ["TOTAL", "DEPARTURES", "ARRIVALS"])
    .agg(F.sum("value"))
)

quality_checks = [
    ("Airports with a duplicated identifier",
     airports_t.groupBy("airport_ident").count().filter("count > 1").count()),
    ("Airports with a duplicated IATA code",
     airports_t.filter("iata_code IS NOT NULL").groupBy("iata_code").count().filter("count > 1").count()),
    ("Airports with a duplicated ICAO code",
     airports_t.filter("icao_code IS NOT NULL").groupBy("icao_code").count().filter("count > 1").count()),
    ("Airports without coordinates or with impossible coordinates",
     airports_t.filter(
         F.col("latitude").isNull() | F.col("longitude").isNull()
         | ~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180)
     ).count()),
    ("Airlines with a duplicated IATA code after the business rule",
     airlines_t.groupBy("airline_iata").count().filter("count > 1").count()),
    ("Routes duplicated (same airline, origin and destination)",
     routes_t.groupBy("airline_iata", "src_iata", "dst_iata").count().filter("count > 1").count()),
    ("Routes whose origin is not in the airport table",
     routes_t.filter(F.col("src_airport").isNull()).count()),
    ("Routes whose destination is not in the airport table",
     routes_t.filter(F.col("dst_airport").isNull()).count()),
    ("Routes from an airport to itself or without distance",
     routes_t.filter((F.col("src_iata") == F.col("dst_iata")) | (F.col("distance_km") <= 0)).count()),
    ("Routes whose distance differs more than 1% and 5 km from the great circle",
     routes_t.filter(
         (F.abs(F.col("distance_km") - F.col("great_circle_km")) > 5)
         & (F.abs(F.col("distance_km") - F.col("great_circle_km")) > 0.01 * F.col("great_circle_km"))
     ).count()),
    ("Connections duplicated (same origin and destination)",
     connections_t.groupBy("src_iata", "dst_iata").count().filter("count > 1").count()),
    ("Eurostat observations duplicated (same route, measure, direction and period)",
     traffic_t.groupBy("frequency", "measure", "direction", "route_code", "period")
     .count().filter("count > 1").count()),
    ("Eurostat observations with a negative value",
     traffic_t.filter(F.col("value") < 0).count()),
    ("Eurostat observations without a recognised measure",
     traffic_t.filter(F.col("measure").isNull()).count()),
    ("Eurostat reporting airports not found in the airport table",
     traffic_t.filter(F.col("reporting_iata").isNull()).select("reporting_icao").distinct().count()),
    (f"Eurostat routes of {LATEST_YEAR} where departures + arrivals differ from the total",
     directional.filter(F.abs(F.col("TOTAL") - F.col("DEPARTURES") - F.col("ARRIVALS")) > 1).count()),
    (f"Eurostat routes of {LATEST_YEAR} with more passengers on board than seats",
     annual_latest.groupBy("route_code").pivot("measure", ["passengers_on_board", "seats"]).agg(F.sum("value"))
     .filter(F.col("passengers_on_board") > F.col("seats")).count()),
    ("Source rows removed by the Bronze data rules",
     bronze_log_df.agg(F.sum("rows_removed")).first()[0]),
]

info_checks = [
    ("(info) Routes of an airline code that is not active in the registry",
     routes_t.filter(~F.col("airline_is_active")).count()),
    ("(info) IATA codes shared by more than one airline in the registry",
     airlines_t.filter(F.col("airlines_sharing_the_code") > 1).count()),
    ("(info) Bronze routes removed by the business rules (self-routes, no distance)",
     read_bronze("routes").count() - routes_t.count()),
    ("(info) Eurostat partner codes not found in the airport table",
     traffic_t.filter(F.col("partner_iata").isNull()).select("partner_icao").distinct().count()),
]

print(f"{'Data quality check':<78}{'Violations':>11}  Result")
print("-" * 98)
for description, violations in quality_checks:
    print(f"{description:<78}{violations:>11,}  {'PASS' if violations == 0 else 'REVIEW'}")
n_checks_failed = sum(1 for _, violations in quality_checks if violations != 0)
print("-" * 98)
print(f"{len(quality_checks) - n_checks_failed} of {len(quality_checks)} checks passed\n")
for description, value in info_checks:
    print(f"{description:<78}{value:>11,}")

# COMMAND ----------

# DBTITLE 1,Weight of the Eurostat partner codes that are not airports
# Eurostat uses some codes that are not airports (for example "unknown airport" of a country).
# They are kept in the totals and excluded only when an airport attribute is needed.
unmatched_share = (
    annual_latest.filter(F.col("measure") == "passengers_carried")
    .agg(
        F.sum("value").alias("passengers"),
        F.sum(F.when(F.col("partner_iata").isNull(), F.col("value")).otherwise(0)).alias("unmatched_passengers"),
    )
    .first()
)
pct_unmatched_passengers = 100 * unmatched_share["unmatched_passengers"] / unmatched_share["passengers"]
print(f"Passengers of {LATEST_YEAR} on routes whose partner code is not in the airport table: "
      f"{pct_unmatched_passengers:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold layer: the world route network
# MAGIC Gold tables provide summarised data for a specific use. Each one is saved as a Delta table with the prefix `GD_`
# MAGIC and explored with standard SQL (`%sql` cells), the natural option of business users.
# MAGIC
# MAGIC **Q1: how is the world route network structured, and which airports are its hubs?**

# COMMAND ----------

# DBTITLE 1,GD_Airport_Connectivity: one row per airport with its connectivity indicators
gd_airport_connectivity_df = spark.sql(
    f"""
    WITH outbound AS (
        SELECT src_iata                               AS iata_code,
               COUNT(DISTINCT dst_iata)               AS destinations,
               COUNT(DISTINCT dst_country)            AS countries_served,
               COUNT(DISTINCT airline_iata)           AS airlines,
               COUNT(*)                               AS routes,
               COUNT(DISTINCT CASE WHEN is_intercontinental THEN dst_iata END) AS intercontinental_destinations,
               ROUND(AVG(distance_km), 0)             AS avg_route_km,
               MAX(distance_km)                       AS longest_route_km
        FROM {table_name('SV_Routes')}
        GROUP BY src_iata
    )
    SELECT a.iata_code,
           a.airport_name,
           a.municipality,
           a.country,
           a.continent,
           a.airport_type,
           a.latitude,
           a.longitude,
           o.destinations,
           o.countries_served,
           o.airlines,
           o.routes,
           o.intercontinental_destinations,
           o.avg_route_km,
           o.longest_route_km,
           RANK() OVER (ORDER BY o.destinations DESC)                         AS world_rank,
           RANK() OVER (PARTITION BY a.country ORDER BY o.destinations DESC)  AS country_rank
    FROM outbound o
    JOIN {table_name('SV_Airports')} a ON o.iata_code = a.iata_code
    """
)

save_table(
    gd_airport_connectivity_df,
    "GD_Airport_Connectivity",
    "GOLD - connectivity indicators of each airport with outbound routes: destinations, countries, airlines, ranks",
)

# COMMAND ----------

# DBTITLE 1,Size of the network
# MAGIC %sql
# MAGIC SELECT (SELECT COUNT(*) FROM workspace.DL_AIR.SV_Routes)                       AS airline_routes,
# MAGIC        (SELECT COUNT(*) FROM workspace.DL_AIR.SV_Connections)                  AS connections_between_airports,
# MAGIC        (SELECT COUNT(DISTINCT airline_iata) FROM workspace.DL_AIR.SV_Routes)   AS airlines,
# MAGIC        (SELECT COUNT(*) FROM workspace.DL_AIR.GD_Airport_Connectivity)         AS airports_with_departures,
# MAGIC        (SELECT COUNT(DISTINCT country) FROM workspace.DL_AIR.GD_Airport_Connectivity) AS countries

# COMMAND ----------

# DBTITLE 1,The 15 best connected airports of the world
# MAGIC %sql
# MAGIC SELECT world_rank, iata_code, airport_name, country, destinations, countries_served, airlines,
# MAGIC        intercontinental_destinations, longest_route_km
# MAGIC FROM workspace.DL_AIR.GD_Airport_Connectivity
# MAGIC ORDER BY world_rank, iata_code
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,The Portuguese airports in the world network
# MAGIC %sql
# MAGIC SELECT country_rank, world_rank, iata_code, airport_name, municipality, destinations, countries_served, airlines,
# MAGIC        intercontinental_destinations, avg_route_km, longest_route_km
# MAGIC FROM workspace.DL_AIR.GD_Airport_Connectivity
# MAGIC WHERE country = 'PT'
# MAGIC ORDER BY country_rank, iata_code

# COMMAND ----------

# DBTITLE 1,Routes by haul category
# MAGIC %sql
# MAGIC SELECT haul,
# MAGIC        COUNT(*)                                             AS connections,
# MAGIC        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)   AS pct_of_connections,
# MAGIC        ROUND(AVG(distance_km), 0)                           AS avg_distance_km,
# MAGIC        ROUND(AVG(airlines), 2)                              AS avg_airlines_per_connection,
# MAGIC        ROUND(100.0 * AVG(CASE WHEN airlines = 1 THEN 1 ELSE 0 END), 1) AS pct_served_by_one_airline
# MAGIC FROM workspace.DL_AIR.SV_Connections
# MAGIC GROUP BY haul
# MAGIC ORDER BY haul

# COMMAND ----------

# DBTITLE 1,The 10 airlines with the largest networks
# MAGIC %sql
# MAGIC SELECT airline_iata,
# MAGIC        MAX(airline_name)                AS airline_name,
# MAGIC        MAX(airline_country)             AS airline_country,
# MAGIC        COUNT(*)                         AS routes,
# MAGIC        COUNT(DISTINCT src_iata)         AS airports_served,
# MAGIC        COUNT(DISTINCT src_country)      AS countries_served,
# MAGIC        ROUND(AVG(distance_km), 0)       AS avg_route_km
# MAGIC FROM workspace.DL_AIR.SV_Routes
# MAGIC GROUP BY airline_iata
# MAGIC ORDER BY routes DESC, airline_iata
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Chart: the airports of the world network, sized by number of destinations
connectivity_pd = (
    spark.table(table_name("GD_Airport_Connectivity"))
    .select("iata_code", "country", "latitude", "longitude", "destinations")
    .orderBy("destinations")
    .toPandas()
)
home_pd = connectivity_pd[connectivity_pd["country"] == HOME_COUNTRY]

fig, ax = plt.subplots(figsize=(13, 6.2))
ax.scatter(connectivity_pd["longitude"], connectivity_pd["latitude"],
           s=connectivity_pd["destinations"].astype(float) * 0.9 + 1,
           c=connectivity_pd["destinations"].astype(float), cmap="viridis", alpha=0.6, linewidths=0)
ax.scatter(home_pd["longitude"], home_pd["latitude"], s=home_pd["destinations"].astype(float) * 0.9 + 12,
           facecolors="none", edgecolors="red", linewidths=1.2, label="Portuguese airports")
ax.set_title("Airports with scheduled routes, sized by number of direct destinations")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.set_xlim(-180, 180)
ax.set_ylim(-60, 80)
ax.grid(alpha=0.3)
ax.legend(loc="lower left")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Graph analytics: hubs, reachability and PageRank
# MAGIC
# MAGIC The route network is a graph:
# MAGIC * **Vertices** are the airports that appear in at least one route.
# MAGIC * **Edges** are the directed connections between two airports, **weighted** by the number of airlines that fly them.
# MAGIC
# MAGIC Vertices and edges are stored as two DataFrames with the `id` / `src` / `dst` convention of GraphFrames, used in
# MAGIC class ABD09. In class GraphFrames ran on Colab, because it needs a JVM package that serverless compute cannot
# MAGIC install. Here the same operations (degrees, motifs, subgraphs, breadth-first search and PageRank) are written
# MAGIC directly with **DataFrame joins and aggregations**, as in the PageRank example of class ABD06.

# COMMAND ----------

# DBTITLE 1,Build the graph: vertices and edges
graph_edges_df = spark.table(table_name("SV_Connections")).select(
    F.col("src_iata").alias("src"),
    F.col("dst_iata").alias("dst"),
    F.col("airlines").alias("weight"),
    "distance_km",
    "haul",
)

save_table(
    graph_edges_df,
    "GD_Graph_Edges",
    "GOLD graph - directed connection between two airports, weight = number of airlines",
)
edges = spark.table(table_name("GD_Graph_Edges"))

graph_vertices_df = (
    edges.select(F.col("src").alias("id"))
    .union(edges.select(F.col("dst").alias("id")))
    .distinct()
    .join(
        airports_with_iata.select(
            F.col("iata_code").alias("id"), "airport_name", "municipality", "country", "continent",
            "airport_type", "latitude", "longitude",
        ),
        "id",
    )
)

save_table(graph_vertices_df, "GD_Graph_Vertices", "GOLD graph - one vertex per airport that appears in a route")
vertices = spark.table(table_name("GD_Graph_Vertices"))

n_vertices = vertices.count()
n_edges = edges.count()
print(f"\nVertices (airports)           : {n_vertices:,}")
print(f"Directed edges (connections)  : {n_edges:,}")
print(f"Density                       : {n_edges / (n_vertices * (n_vertices - 1)):.3%} of all possible connections")
print(f"Average out-degree            : {n_edges / n_vertices:.1f} destinations per airport")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Degrees: a network of a few hubs and many small airports

# COMMAND ----------

# DBTITLE 1,In-degree, out-degree and their distribution
out_degrees = edges.groupBy(F.col("src").alias("id")).agg(F.count("*").alias("out_degree"))
in_degrees = edges.groupBy(F.col("dst").alias("id")).agg(F.count("*").alias("in_degree"))

airport_degrees_df = (
    vertices.join(out_degrees, "id", "left")
    .join(in_degrees, "id", "left")
    .fillna(0, subset=["out_degree", "in_degree"])
)

degree_distribution_df = (
    airport_degrees_df.withColumn(
        "destinations_band",
        F.when(F.col("out_degree") == 0, "0 - arrivals only")
        .when(F.col("out_degree") == 1, "1 - one destination")
        .when(F.col("out_degree") <= 5, "2 - 2 to 5")
        .when(F.col("out_degree") <= 20, "3 - 6 to 20")
        .when(F.col("out_degree") <= 100, "4 - 21 to 100")
        .otherwise("5 - more than 100"),
    )
    .groupBy("destinations_band")
    .agg(F.count("*").alias("airports"), F.sum("out_degree").alias("connections"))
    .withColumn("pct_of_airports", F.round(100 * F.col("airports") / F.sum("airports").over(Window.partitionBy()), 1))
    .withColumn("pct_of_connections",
                F.round(100 * F.col("connections") / F.sum("connections").over(Window.partitionBy()), 1))
    .orderBy("destinations_band")
)

display(degree_distribution_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Motif: connections that exist in one direction only
# MAGIC The motif `(a)-[e1]->(b); (b)-[e2]->(a)` of GraphFrames finds the pairs of airports connected in both directions.
# MAGIC It is a join of the edge table with itself. The edges **without** a return edge (a left anti join) are the
# MAGIC one-way connections: routes flown as a triangle, or gaps in the data.

# COMMAND ----------

# DBTITLE 1,Reciprocal and one-way connections
return_edges = edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"))

n_reciprocal = edges.join(return_edges, ["src", "dst"], "left_semi").count()
one_way_df = edges.join(return_edges, ["src", "dst"], "left_anti")
n_one_way = one_way_df.count()

print(f"Connections with a return connection : {n_reciprocal:,} ({n_reciprocal / n_edges:.1%})")
print(f"One-way connections                  : {n_one_way:,} ({n_one_way / n_edges:.1%})")

display(
    one_way_df.groupBy("haul").agg(F.count("*").alias("one_way_connections")).orderBy("haul")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Subgraph: the network of the flag carrier
# MAGIC A subgraph is obtained by filtering the edges and dropping the vertices left without any edge (`filterEdges` and
# MAGIC `dropIsolatedVertices` in GraphFrames). Keeping only the routes of TAP shows how a hub-and-spoke airline is
# MAGIC organised.

# COMMAND ----------

# DBTITLE 1,Subgraph of the routes of TAP
carrier_edges_df = (
    spark.table(table_name("SV_Routes"))
    .filter(F.col("airline_iata") == FLAG_CARRIER)
    .select(F.col("src_iata").alias("src"), F.col("dst_iata").alias("dst"), "distance_km", "haul", "dst_continent")
)
carrier_vertices_df = (
    carrier_edges_df.select(F.col("src").alias("id")).union(carrier_edges_df.select(F.col("dst").alias("id"))).distinct()
)

n_carrier_edges = carrier_edges_df.count()
n_carrier_vertices = carrier_vertices_df.count()
print(f"Full graph : {n_vertices:>5,} airports, {n_edges:>6,} connections")
print(f"TAP graph  : {n_carrier_vertices:>5,} airports, {n_carrier_edges:>6,} connections")

carrier_hubs_df = (
    carrier_edges_df.groupBy("src")
    .agg(
        F.count("*").alias("out_degree"),
        F.sum(F.when(F.col("haul") == "3 - Long haul", 1).otherwise(0)).alias("long_haul_destinations"),
    )
    .withColumn("pct_of_tap_connections", F.round(100 * F.col("out_degree") / F.lit(n_carrier_edges), 1))
    .join(vertices.select(F.col("id").alias("src"), "airport_name", "country"), "src")
    .select("src", "airport_name", "country", "out_degree", "pct_of_tap_connections", "long_haul_destinations")
    .orderBy(F.desc("out_degree"), "src")
)

display(carrier_hubs_df.limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Breadth-first search: how many flights from Lisbon to anywhere?
# MAGIC Breadth-first search (BFS) finds the shortest path between vertices, measured in number of edges. Here one edge is
# MAGIC one flight, so the result is the **minimum number of flights** needed to reach each airport from Lisbon.
# MAGIC
# MAGIC The search advances one level at a time: the airports reached in the previous level (the *frontier*) are joined
# MAGIC with the edges to find the airports one flight further. Each level is one distributed join. The set of visited
# MAGIC airports is small, so it is kept on the driver.

# COMMAND ----------

# DBTITLE 1,Breadth-first search implemented with DataFrames
def breadth_first_search(edges: DataFrame, source_id: str, max_hops: int) -> DataFrame:
    """BFS from `source_id`. Returns one row per reached vertex: [id, flights, previous_id]."""
    visited = {source_id: (0, None)}   # id -> (flights, previous vertex on a shortest path)
    frontier = [source_id]

    for hop in range(1, max_hops + 1):
        frontier_df = spark.createDataFrame([(v,) for v in frontier], "src STRING")
        # Distributed step: all the edges that leave the frontier
        reached = edges.join(frontier_df, "src").select("src", "dst").collect()

        new_vertices = {}
        for row in sorted(reached, key=lambda r: (r["dst"], r["src"])):   # sorted, so the result is deterministic
            if row["dst"] not in visited and row["dst"] not in new_vertices:
                new_vertices[row["dst"]] = (hop, row["src"])
        if not new_vertices:
            break
        visited.update(new_vertices)
        frontier = list(new_vertices)
        print(f"  flight {hop}: {len(new_vertices):>5,} new airports reached")

    return spark.createDataFrame(
        [(v, flights, previous) for v, (flights, previous) in visited.items()],
        "id STRING, flights INT, previous_id STRING",
    )


bfs_df = breadth_first_search(edges, HOME_HUB, BFS_MAX_HOPS)

flights_from_hub_df = vertices.select("id", "airport_name", "country", "continent").join(bfs_df, "id", "left")

save_table(
    flights_from_hub_df,
    "GD_Flights_From_Lisbon",
    f"GOLD graph - minimum number of flights from {HOME_HUB} to each airport (NULL = not reachable)",
)

# COMMAND ----------

# DBTITLE 1,Airports of the world by number of flights from Lisbon
# MAGIC %sql
# MAGIC SELECT COALESCE(CAST(flights AS STRING), 'not reachable')    AS flights_from_lisbon,
# MAGIC        COUNT(*)                                              AS airports,
# MAGIC        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)    AS pct_of_airports,
# MAGIC        ROUND(100.0 * SUM(COUNT(*)) OVER (ORDER BY COALESCE(flights, 99)) / SUM(COUNT(*)) OVER (), 1)
# MAGIC            AS cumulative_pct
# MAGIC FROM workspace.DL_AIR.GD_Flights_From_Lisbon
# MAGIC GROUP BY flights
# MAGIC ORDER BY COALESCE(flights, 99)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.5 PageRank: the most important airports
# MAGIC Counting destinations measures local activity. **PageRank** measures the importance of an airport inside the whole
# MAGIC network: an airport is important if it receives connections **from airports that are themselves important**.
# MAGIC
# MAGIC `rank(v) = (1 - d) / N  +  d * ( SUM over edges u->v of rank(u) * weight(u,v) / weight_out(u)  +  dangling / N )`
# MAGIC
# MAGIC where `d` is the damping factor, `N` the number of airports, `weight` the number of airlines on the connection and
# MAGIC `dangling` the total rank of airports without departures (redistributed uniformly so that the ranks sum to 1).
# MAGIC
# MAGIC Each iteration is a distributed join and aggregation over the edges. The rank vector has one row per airport, so
# MAGIC it is small: it is collected to the driver and turned into a new DataFrame, which keeps the query plan of every
# MAGIC iteration short instead of letting the lineage grow with the number of iterations.

# COMMAND ----------

# DBTITLE 1,Weighted PageRank implemented with DataFrames
def weighted_pagerank(vertices: DataFrame, edges: DataFrame, damping: float, max_iter: int, tolerance: float):
    """Iterative weighted PageRank. Returns (ranks DataFrame [id, pagerank], iterations run)."""
    vertex_ids = [row["id"] for row in vertices.select("id").distinct().collect()]
    n = len(vertex_ids)

    # Transition probability of each edge: share of the weight leaving `src` that goes to `dst`
    out_weights = edges.groupBy("src").agg(F.sum("weight").alias("out_weight"))
    transitions = edges.join(out_weights, "src").select(
        "src", "dst", (F.col("weight") / F.col("out_weight")).alias("probability")
    )
    sources = {row["src"] for row in out_weights.select("src").collect()}
    dangling_ids = [v for v in vertex_ids if v not in sources]

    ranks = {v: 1.0 / n for v in vertex_ids}
    iterations = 0
    for iteration in range(1, max_iter + 1):
        iterations = iteration
        ranks_df = spark.createDataFrame(list(ranks.items()), "id STRING, rank DOUBLE")

        # Distributed step: every airport sends its rank along its outgoing edges
        contributions = (
            transitions.join(ranks_df, transitions["src"] == ranks_df["id"])
            .groupBy("dst")
            .agg(F.sum(F.col("probability") * F.col("rank")).alias("contribution"))
            .collect()
        )
        received = {row["dst"]: row["contribution"] for row in contributions}
        dangling_mass = sum(ranks[v] for v in dangling_ids)

        new_ranks = {
            v: (1.0 - damping) / n + damping * (received.get(v, 0.0) + dangling_mass / n)
            for v in vertex_ids
        }
        change = sum(abs(new_ranks[v] - ranks[v]) for v in vertex_ids)
        ranks = new_ranks
        converged = change < tolerance
        if iteration == 1 or iteration % 5 == 0 or converged:
            print(f"  iteration {iteration:>3}: total change in ranks = {change:.8f}")
        if converged:
            break

    ranks_df = spark.createDataFrame(list(ranks.items()), "id STRING, pagerank DOUBLE")
    return ranks_df, iterations


pagerank_df, pagerank_iterations = weighted_pagerank(
    vertices, edges, PAGERANK_DAMPING, PAGERANK_MAX_ITER, PAGERANK_TOLERANCE
)
print(f"\nPageRank stopped after {pagerank_iterations} iterations")
print(f"Sum of all ranks = {pagerank_df.agg(F.sum('pagerank')).first()[0]:.6f} (expected 1.0)")

# COMMAND ----------

# DBTITLE 1,Save the ranking and show the top 15 airports
airport_pagerank_df = (
    airport_degrees_df.join(pagerank_df, "id")
    .withColumn("pagerank", F.round("pagerank", 6))
    .withColumn("pagerank_position", F.rank().over(Window.orderBy(F.desc("pagerank"))))
    .withColumn("degree_position", F.rank().over(Window.orderBy(F.desc("out_degree"))))
    .select(
        "id", "airport_name", "country", "continent", "latitude", "longitude",
        "pagerank", "pagerank_position", "out_degree", "in_degree", "degree_position",
    )
)

save_table(
    airport_pagerank_df,
    "GD_Airport_PageRank",
    "GOLD graph - PageRank and degrees of each airport, with its position in both rankings",
)

# COMMAND ----------

# DBTITLE 1,Top 15 airports by PageRank
# MAGIC %sql
# MAGIC SELECT pagerank_position, degree_position, id AS iata_code, airport_name, country, pagerank, out_degree
# MAGIC FROM workspace.DL_AIR.GD_Airport_PageRank
# MAGIC ORDER BY pagerank_position, id
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,The Portuguese airports in the PageRank ranking
# MAGIC %sql
# MAGIC SELECT pagerank_position, degree_position, id AS iata_code, airport_name, pagerank, out_degree, in_degree
# MAGIC FROM workspace.DL_AIR.GD_Airport_PageRank
# MAGIC WHERE country = 'PT'
# MAGIC ORDER BY pagerank_position, id

# COMMAND ----------

# DBTITLE 1,Chart: top 15 airports by PageRank
top_pagerank_pd = (
    spark.table(table_name("GD_Airport_PageRank"))
    .orderBy("pagerank_position", "id")
    .limit(15)
    .toPandas()
    .iloc[::-1]
)
labels = [f"{iata} ({country})" for iata, country in zip(top_pagerank_pd["id"], top_pagerank_pd["country"])]

fig, ax = plt.subplots(figsize=(11, 5.5))
ax.barh(labels, top_pagerank_pd["pagerank"].astype(float), color="steelblue")
ax.set_title("Top 15 airports of the world by PageRank")
ax.set_xlabel("PageRank")
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Portugal's connectivity: every route with up to two stops
# MAGIC
# MAGIC **Q2: how much of the world can be reached from each Portuguese airport with 0, 1 and 2 stops, and through which hubs?**
# MAGIC
# MAGIC An itinerary with stops is a **join of the edge table with itself**: a one-stop itinerary joins two edges on the
# MAGIC intermediate airport, a two-stop itinerary joins three. The number of itineraries grows very fast with each join,
# MAGIC which is why this is done with Spark:
# MAGIC
# MAGIC | Itineraries | How they are obtained | Scope computed here |
# MAGIC |---|---|---|
# MAGIC | Direct | The edges | From the Portuguese airports |
# MAGIC | One stop | Edges joined with edges | **Whole world** (section 9.1) and from the Portuguese airports |
# MAGIC | Two stops | Edges joined with edges joined with edges | From the Portuguese airports |
# MAGIC
# MAGIC > **Scope decision.** The two-stop itineraries of the whole world are about 146 million rows. They are computed
# MAGIC > only for the Portuguese origins (about 1.5 million itineraries), which answers the business question and stays
# MAGIC > well inside the daily compute quota of Databricks Free Edition. The same code runs on the whole world by
# MAGIC > removing one filter.
# MAGIC
# MAGIC An itinerary never visits the same airport twice. Its length is the sum of the distances of its flights, and its
# MAGIC **detour factor** is that length divided by the great-circle distance between origin and destination
# MAGIC (1.00 = no detour).
# MAGIC
# MAGIC ### 9.1 The transfer hubs of the world, on all pairs of airports
# MAGIC For every pair of airports **without** a direct connection, we find the best one-stop itinerary (the shortest one)
# MAGIC and count how often each airport is the transfer point. The earlier pandas project estimated this from a random
# MAGIC sample of 60 large airports; here every pair is used.

# COMMAND ----------

# DBTITLE 1,All one-stop itineraries of the world and the best one for each pair
first_leg = edges.select(
    F.col("src").alias("origin"), F.col("dst").alias("stop_1"), F.col("distance_km").alias("km_1")
)
second_leg = edges.select(
    F.col("src").alias("stop_1"), F.col("dst").alias("destination"), F.col("distance_km").alias("km_2")
)

world_one_stop = first_leg.join(second_leg, "stop_1").filter(F.col("origin") != F.col("destination"))
n_world_one_stop = world_one_stop.count()

direct_pairs = edges.select(F.col("src").alias("origin"), F.col("dst").alias("destination"))
best_per_pair = Window.partitionBy("origin", "destination").orderBy(F.col("total_km"), F.col("stop_1"))

world_best_one_stop = (
    world_one_stop.join(direct_pairs, ["origin", "destination"], "left_anti")   # only pairs with no direct flight
    .withColumn("total_km", F.col("km_1") + F.col("km_2"))
    .withColumn("option_rank", F.row_number().over(best_per_pair))
    .filter(F.col("option_rank") == 1)
)

gd_global_transfer_hubs_df = (
    world_best_one_stop.groupBy(F.col("stop_1").alias("id"))
    .agg(F.count("*").alias("pairs_best_connected_via_hub"))
    .withColumn("pct_of_pairs",
                F.round(100 * F.col("pairs_best_connected_via_hub")
                        / F.sum("pairs_best_connected_via_hub").over(Window.partitionBy()), 2))
    .withColumn("hub_rank", F.rank().over(Window.orderBy(F.desc("pairs_best_connected_via_hub"))))
    .join(vertices.select("id", "airport_name", "country", "continent"), "id")
)

save_table(
    gd_global_transfer_hubs_df,
    "GD_Global_Transfer_Hubs",
    "GOLD - for every pair of airports without a direct connection: how often each airport is the best one-stop transfer",
)

n_pairs_one_stop = spark.table(table_name("GD_Global_Transfer_Hubs")).agg(
    F.sum("pairs_best_connected_via_hub")).first()[0]
print(f"\nOne-stop itineraries in the world                      : {n_world_one_stop:,}")
print(f"Pairs of airports connected with one stop but not direct : {n_pairs_one_stop:,}")

# COMMAND ----------

# DBTITLE 1,The 15 transfer hubs of the world
# MAGIC %sql
# MAGIC SELECT hub_rank, id AS iata_code, airport_name, country, pairs_best_connected_via_hub, pct_of_pairs,
# MAGIC        ROUND(SUM(pct_of_pairs) OVER (ORDER BY hub_rank, id), 1) AS cumulative_pct
# MAGIC FROM workspace.DL_AIR.GD_Global_Transfer_Hubs
# MAGIC ORDER BY hub_rank, id
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Every itinerary from the Portuguese airports

# COMMAND ----------

# DBTITLE 1,Direct, one-stop and two-stop itineraries from the Portuguese airports
home_airports = vertices.filter(F.col("country") == HOME_COUNTRY).select(F.col("id").alias("origin"))
third_leg = edges.select(
    F.col("src").alias("stop_2"), F.col("dst").alias("destination"), F.col("distance_km").alias("km_3")
)

home_first_leg = first_leg.join(F.broadcast(home_airports), "origin")

direct_itineraries = home_first_leg.select(
    "origin",
    F.col("stop_1").alias("destination"),
    F.lit(0).alias("stops"),
    F.lit(None).cast("string").alias("stop_1"),
    F.lit(None).cast("string").alias("stop_2"),
    F.col("km_1").alias("total_km"),
)

one_stop_itineraries = (
    home_first_leg.join(second_leg, "stop_1")
    .filter(F.col("destination") != F.col("origin"))
    .select(
        "origin", "destination", F.lit(1).alias("stops"), "stop_1",
        F.lit(None).cast("string").alias("stop_2"),
        (F.col("km_1") + F.col("km_2")).alias("total_km"),
    )
)

two_stop_itineraries = (
    home_first_leg.join(second_leg.withColumnRenamed("destination", "stop_2"), "stop_1")
    .filter(F.col("stop_2") != F.col("origin"))
    .join(third_leg, "stop_2")
    .filter((F.col("destination") != F.col("origin")) & (F.col("destination") != F.col("stop_1")))
    .select(
        "origin", "destination", F.lit(2).alias("stops"), "stop_1", "stop_2",
        (F.col("km_1") + F.col("km_2") + F.col("km_3")).alias("total_km"),
    )
)

n_direct = direct_itineraries.count()
n_one_stop = one_stop_itineraries.count()
n_two_stop = two_stop_itineraries.count()

print(f"Direct itineraries   : {n_direct:>10,}")
print(f"One-stop itineraries : {n_one_stop:>10,}")
print(f"Two-stop itineraries : {n_two_stop:>10,}")
print(f"Total                : {n_direct + n_one_stop + n_two_stop:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 The best itinerary for every origin and destination
# MAGIC For each pair, the itinerary kept is the one with the **fewest stops** and, among those, the **shortest**.
# MAGIC It is selected with `row_number` over a window partitioned by origin and destination.

# COMMAND ----------

# DBTITLE 1,GD_Portugal_Best_Itineraries
all_itineraries = direct_itineraries.unionByName(one_stop_itineraries).unionByName(two_stop_itineraries)

best_itinerary = Window.partitionBy("origin", "destination").orderBy(
    F.col("stops"), F.col("total_km"), F.col("stop_1"), F.col("stop_2")
)
options_per_pair = Window.partitionBy("origin", "destination")

origin_location = vertices.select(
    F.col("id").alias("origin"),
    F.col("airport_name").alias("origin_airport"),
    F.col("latitude").alias("origin_lat"),
    F.col("longitude").alias("origin_lon"),
)
destination_location = vertices.select(
    F.col("id").alias("destination"),
    F.col("airport_name").alias("destination_airport"),
    F.col("country").alias("destination_country"),
    F.col("continent").alias("destination_continent"),
    F.col("latitude").alias("destination_lat"),
    F.col("longitude").alias("destination_lon"),
)

gd_best_itineraries_df = (
    all_itineraries.withColumn("itineraries_found", F.count("*").over(options_per_pair))
    .withColumn("option_rank", F.row_number().over(best_itinerary))
    .filter(F.col("option_rank") == 1)
    .join(origin_location, "origin")
    .join(destination_location, "destination")
    .withColumn(
        "great_circle_km",
        haversine_km(F.col("origin_lat"), F.col("origin_lon"), F.col("destination_lat"), F.col("destination_lon")),
    )
    .withColumn("detour_factor", F.round(F.try_divide(F.col("total_km"), F.col("great_circle_km")), 3))
    .withColumn("itinerary", F.concat_ws(" > ", "origin", "stop_1", "stop_2", "destination"))
    .select(
        "origin", "origin_airport", "destination", "destination_airport", "destination_country",
        "destination_continent", "stops", "stop_1", "stop_2", "itinerary", "total_km",
        F.round("great_circle_km", 0).alias("great_circle_km"), "detour_factor", "itineraries_found",
    )
)

save_table(
    gd_best_itineraries_df,
    "GD_Portugal_Best_Itineraries",
    "GOLD - best itinerary (fewest stops, then shortest) from each Portuguese airport to each reachable airport",
)

# COMMAND ----------

# DBTITLE 1,Reach of each Portuguese airport: destinations by number of stops (pivot)
reach_df = (
    spark.table(table_name("GD_Portugal_Best_Itineraries"))
    .groupBy("origin", "origin_airport")
    .pivot("stops", [0, 1, 2])
    .count()
    .fillna(0)
    .withColumnRenamed("0", "direct")
    .withColumnRenamed("1", "one_stop")
    .withColumnRenamed("2", "two_stops")
    .withColumn("within_one_stop", F.col("direct") + F.col("one_stop"))
    .withColumn("within_two_stops", F.col("direct") + F.col("one_stop") + F.col("two_stops"))
    .withColumn("pct_of_world_within_one_stop", F.round(100 * F.col("within_one_stop") / F.lit(n_vertices - 1), 1))
    .withColumn("pct_of_world_within_two_stops", F.round(100 * F.col("within_two_stops") / F.lit(n_vertices - 1), 1))
    .orderBy(F.desc("within_one_stop"), "origin")
)

display(reach_df)

# COMMAND ----------

# DBTITLE 1,Chart: reach of the main Portuguese airports
reach_pd = reach_df.filter(F.col("origin").isin(MAIN_AIRPORTS)).toPandas()

fig, ax = plt.subplots(figsize=(11, 4.5))
bottom = [0.0] * len(reach_pd)
for column, label in (("direct", "Direct"), ("one_stop", "One stop"), ("two_stops", "Two stops")):
    values = reach_pd[column].astype(float).tolist()
    ax.bar(reach_pd["origin"], values, bottom=bottom, label=label)
    bottom = [b + v for b, v in zip(bottom, values)]
ax.axhline(n_vertices - 1, color="grey", linestyle=":", label="All airports of the network")
ax.set_title("Airports reachable from the main Portuguese airports, by number of stops")
ax.set_ylabel("Destinations")
ax.set_ylim(0, (n_vertices - 1) * 1.25)
ax.grid(axis="y", alpha=0.3)
ax.legend(loc="upper right", ncol=4)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Which hubs does Lisbon depend on? Transfer airports of the best one-stop itineraries
# MAGIC %sql
# MAGIC SELECT b.stop_1                                                  AS hub,
# MAGIC        v.airport_name,
# MAGIC        v.country,
# MAGIC        COUNT(*)                                                  AS destinations_best_reached_via_hub,
# MAGIC        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)        AS pct_of_one_stop_destinations,
# MAGIC        ROUND(SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC, b.stop_1) * 100.0 / SUM(COUNT(*)) OVER (), 1)
# MAGIC            AS cumulative_pct,
# MAGIC        ROUND(AVG(b.detour_factor), 2)                            AS avg_detour_factor
# MAGIC FROM workspace.DL_AIR.GD_Portugal_Best_Itineraries b
# MAGIC JOIN workspace.DL_AIR.GD_Graph_Vertices v ON b.stop_1 = v.id
# MAGIC WHERE b.origin = 'LIS' AND b.stops = 1
# MAGIC GROUP BY b.stop_1, v.airport_name, v.country
# MAGIC ORDER BY destinations_best_reached_via_hub DESC, hub
# MAGIC LIMIT 12

# COMMAND ----------

# DBTITLE 1,How long is the detour? Best itineraries from Lisbon by number of stops
# MAGIC %sql
# MAGIC SELECT stops,
# MAGIC        COUNT(*)                                        AS destinations,
# MAGIC        ROUND(AVG(great_circle_km), 0)                  AS avg_great_circle_km,
# MAGIC        ROUND(AVG(total_km), 0)                         AS avg_itinerary_km,
# MAGIC        ROUND(percentile_approx(detour_factor, 0.5), 2) AS median_detour_factor,
# MAGIC        ROUND(percentile_approx(detour_factor, 0.9), 2) AS p90_detour_factor,
# MAGIC        ROUND(AVG(itineraries_found), 0)                AS avg_itineraries_found_per_destination
# MAGIC FROM workspace.DL_AIR.GD_Portugal_Best_Itineraries
# MAGIC WHERE origin = 'LIS'
# MAGIC GROUP BY stops
# MAGIC ORDER BY stops

# COMMAND ----------

# DBTITLE 1,Reach of Lisbon by continent of the destination (pivot)
display(
    spark.table(table_name("GD_Portugal_Best_Itineraries"))
    .filter(F.col("origin") == HOME_HUB)
    .groupBy("destination_continent")
    .pivot("stops", [0, 1, 2])
    .count()
    .fillna(0)
    .withColumnRenamed("0", "direct")
    .withColumnRenamed("1", "one_stop")
    .withColumnRenamed("2", "two_stops")
    .orderBy(F.desc("direct"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Demand: the passengers of the Portuguese airports
# MAGIC
# MAGIC **Q3: where are the passengers? Which airports and routes carry the traffic, how full are the planes, how seasonal is demand?**
# MAGIC
# MAGIC The route network says where one *can* fly. The Eurostat statistics say where people *do* fly.
# MAGIC
# MAGIC > **Reading the totals.** Eurostat reports each route from the point of view of the Portuguese airport. A
# MAGIC > domestic route (for example Lisbon - Funchal) is reported by both airports, so its passengers are counted at
# MAGIC > both ends. Totals per airport are therefore **passengers handled by the airport**, and the sum over all
# MAGIC > airports counts domestic passengers twice.
# MAGIC
# MAGIC ### 10.1 Gold tables

# COMMAND ----------

# DBTITLE 1,GD_Airport_Year, GD_Route_Year and GD_Airport_Month
def traffic_indicators(frequency: str, group_columns: list) -> DataFrame:
    """Passengers, seats, flights and load factor per group, for one frequency (A = annual, M = monthly)."""
    return (
        spark.table(table_name("SV_Air_Traffic"))
        .filter((F.col("frequency") == frequency) & (F.col("direction") == "TOTAL"))
        .groupBy(*group_columns)
        .agg(
            F.sum(F.when(F.col("measure") == "passengers_carried", F.col("value"))).alias("passengers"),
            F.sum(F.when(F.col("measure") == "passengers_on_board", F.col("value"))).alias("passengers_on_board"),
            F.sum(F.when(F.col("measure") == "seats", F.col("value"))).alias("seats"),
            F.sum(F.when(F.col("measure") == "flights", F.col("value"))).alias("flights"),
            F.countDistinct("route_code").alias("routes"),
        )
        # Load factor: share of the seats offered that were occupied
        .withColumn("load_factor_pct", F.round(100 * F.try_divide(F.col("passengers_on_board"), F.col("seats")), 1))
        .withColumn("passengers_per_flight", F.round(F.try_divide(F.col("passengers_on_board"), F.col("flights")), 0))
    )


# The annual table is created with the years up to the year before the latest.
# The latest year is added in section 13 with a MERGE, as a new year of statistics would be.
gd_airport_year_df = traffic_indicators("A", ["reporting_iata", "reporting_airport", "year"])
save_table(
    gd_airport_year_df.filter(F.col("year") < LATEST_YEAR),
    "GD_Airport_Year",
    "GOLD - passengers, seats, flights and load factor per Portuguese airport and year",
)

save_table(
    traffic_indicators(
        "A",
        ["reporting_iata", "reporting_airport", "partner_icao", "partner_iata", "partner_airport",
         "partner_country", "partner_country_eurostat", "partner_continent", "year"],
    ),
    "GD_Route_Year",
    "GOLD - passengers, seats, flights and load factor per route of a Portuguese airport and year",
)

save_table(
    traffic_indicators("M", ["reporting_iata", "reporting_airport", "year", "month"]),
    "GD_Airport_Month",
    "GOLD - passengers, seats, flights and load factor per Portuguese airport and month",
)

# COMMAND ----------

# DBTITLE 1,Add the latest year to GD_Airport_Year with MERGE
airport_year_version_before_merge = (
    spark.sql(f"DESCRIBE HISTORY {table_name('GD_Airport_Year')}").agg(F.max("version")).first()[0]
)

gd_airport_year_df.filter(F.col("year") == LATEST_YEAR).createOrReplaceTempView("latest_year_traffic")

spark.sql(
    f"""
    MERGE INTO {table_name('GD_Airport_Year')} AS target
    USING latest_year_traffic AS source
       ON target.reporting_iata = source.reporting_iata AND target.year = source.year
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Airports: size, growth and recovery

# COMMAND ----------

# DBTITLE 1,Passengers handled per airport in the latest year, compared with 2019
# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC     SELECT * FROM workspace.DL_AIR.GD_Airport_Year WHERE year = 2024
# MAGIC ),
# MAGIC before AS (
# MAGIC     SELECT * FROM workspace.DL_AIR.GD_Airport_Year WHERE year = 2019
# MAGIC ),
# MAGIC previous AS (
# MAGIC     SELECT * FROM workspace.DL_AIR.GD_Airport_Year WHERE year = 2023
# MAGIC )
# MAGIC SELECT l.reporting_iata                                             AS airport,
# MAGIC        l.reporting_airport                                          AS airport_name,
# MAGIC        ROUND(l.passengers / 1e6, 2)                                 AS passengers_2024_million,
# MAGIC        ROUND(100.0 * l.passengers / SUM(l.passengers) OVER (), 1)   AS pct_of_total,
# MAGIC        ROUND(100.0 * (l.passengers / p.passengers - 1), 1)          AS growth_vs_2023_pct,
# MAGIC        ROUND(100.0 * (l.passengers / b.passengers - 1), 1)          AS growth_vs_2019_pct,
# MAGIC        l.routes                                                     AS routes_reported,
# MAGIC        l.load_factor_pct,
# MAGIC        l.passengers_per_flight
# MAGIC FROM latest l
# MAGIC LEFT JOIN before b ON l.reporting_iata = b.reporting_iata
# MAGIC LEFT JOIN previous p ON l.reporting_iata = p.reporting_iata
# MAGIC ORDER BY l.passengers DESC

# COMMAND ----------

# DBTITLE 1,Passengers per airport and year since 2015 (pivot), in millions
airport_year_pivot_df = (
    spark.table(table_name("GD_Airport_Year"))
    .filter((F.col("year") >= TREND_FIRST_YEAR) & F.col("reporting_iata").isin(MAIN_AIRPORTS))
    .groupBy("year")
    .pivot("reporting_iata", MAIN_AIRPORTS)
    .agg(F.round(F.sum("passengers") / 1e6, 2))
    .orderBy("year")
)

display(airport_year_pivot_df)

# COMMAND ----------

# DBTITLE 1,Chart: monthly passengers of the main airports
airport_month_pd = (
    spark.table(table_name("GD_Airport_Month"))
    .filter(F.col("reporting_iata").isin(MAIN_AIRPORTS) & (F.col("year") >= 2010))
    .select("reporting_iata", "year", "month", "passengers")
    .orderBy("year", "month")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(12, 4.8))
for airport in MAIN_AIRPORTS:
    series = airport_month_pd[airport_month_pd["reporting_iata"] == airport]
    ax.plot(series["year"] + (series["month"] - 1) / 12.0, series["passengers"].astype(float) / 1e6,
            linewidth=1.2, label=airport)
ax.axvspan(2020, 2022, color="grey", alpha=0.15, label="Pandemic")
ax.set_title("Passengers handled per month")
ax.set_xlabel("Year")
ax.set_ylabel("Million passengers")
ax.grid(alpha=0.3)
ax.legend(loc="upper left", ncol=3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 Routes: where the traffic concentrates

# COMMAND ----------

# DBTITLE 1,The 15 busiest routes of the latest year
# MAGIC %sql
# MAGIC SELECT RANK() OVER (ORDER BY passengers DESC)                        AS route_rank,
# MAGIC        reporting_iata                                                AS from_airport,
# MAGIC        COALESCE(partner_iata, partner_icao)                          AS to_airport,
# MAGIC        partner_airport,
# MAGIC        COALESCE(partner_country, partner_country_eurostat)           AS country,
# MAGIC        ROUND(passengers / 1e6, 2)                                    AS passengers_million,
# MAGIC        flights,
# MAGIC        load_factor_pct,
# MAGIC        passengers_per_flight
# MAGIC FROM workspace.DL_AIR.GD_Route_Year
# MAGIC WHERE year = 2024 AND passengers IS NOT NULL
# MAGIC ORDER BY passengers DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Concentration: share of the passengers carried by the busiest routes
route_concentration_df = spark.sql(
    f"""
    WITH ranked AS (
        SELECT passengers,
               ROW_NUMBER() OVER (ORDER BY passengers DESC, reporting_iata, partner_icao) AS route_position,
               COUNT(*) OVER ()                                                            AS routes,
               SUM(passengers) OVER ()                                                     AS all_passengers
        FROM {table_name('GD_Route_Year')}
        WHERE year = {LATEST_YEAR} AND passengers IS NOT NULL
    )
    SELECT MAX(routes)                                                                        AS routes,
           ROUND(100.0 * SUM(CASE WHEN route_position <= 10 THEN passengers END) / MAX(all_passengers), 1)
               AS pct_passengers_top_10_routes,
           ROUND(100.0 * SUM(CASE WHEN route_position <= 0.1 * routes THEN passengers END) / MAX(all_passengers), 1)
               AS pct_passengers_top_10pct_of_routes,
           ROUND(100.0 * SUM(CASE WHEN route_position > 0.5 * routes THEN passengers END) / MAX(all_passengers), 1)
               AS pct_passengers_bottom_half_of_routes
    FROM ranked
    """
)

display(route_concentration_df)

# COMMAND ----------

# DBTITLE 1,Passengers by country of the partner airport, latest year
# MAGIC %sql
# MAGIC SELECT COALESCE(partner_country, partner_country_eurostat)           AS country,
# MAGIC        COUNT(*)                                                      AS routes,
# MAGIC        ROUND(SUM(passengers) / 1e6, 2)                               AS passengers_million,
# MAGIC        ROUND(100.0 * SUM(passengers) / SUM(SUM(passengers)) OVER (), 1) AS pct_of_passengers,
# MAGIC        ROUND(100.0 * SUM(passengers_on_board) / SUM(seats), 1)       AS load_factor_pct
# MAGIC FROM workspace.DL_AIR.GD_Route_Year
# MAGIC WHERE year = 2024 AND passengers IS NOT NULL
# MAGIC GROUP BY COALESCE(partner_country, partner_country_eurostat)
# MAGIC ORDER BY passengers_million DESC
# MAGIC LIMIT 12

# COMMAND ----------

# DBTITLE 1,Routes with the lowest load factor among those with at least 100,000 passengers
# MAGIC %sql
# MAGIC SELECT reporting_iata                                       AS from_airport,
# MAGIC        COALESCE(partner_iata, partner_icao)                 AS to_airport,
# MAGIC        partner_airport,
# MAGIC        ROUND(passengers / 1e3, 0)                           AS passengers_thousand,
# MAGIC        ROUND(seats / 1e3, 0)                                AS seats_thousand,
# MAGIC        load_factor_pct,
# MAGIC        ROUND((seats - passengers_on_board) / 1e3, 0)        AS empty_seats_thousand
# MAGIC FROM workspace.DL_AIR.GD_Route_Year
# MAGIC WHERE year = 2024 AND passengers >= 100000 AND load_factor_pct IS NOT NULL
# MAGIC ORDER BY load_factor_pct ASC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.4 Seasonality
# MAGIC The **seasonality ratio** of an airport is the number of passengers of its busiest month divided by the number of
# MAGIC its quietest month. A ratio close to 1 means stable demand all year; a high ratio means that the airport, its
# MAGIC staff and the local economy work at very different intensities in summer and in winter.

# COMMAND ----------

# DBTITLE 1,Seasonality of each airport in the latest year
seasonality_df = spark.sql(
    f"""
    WITH monthly AS (
        SELECT reporting_iata, reporting_airport, month, passengers,
               SUM(passengers) OVER (PARTITION BY reporting_iata)  AS year_passengers
        FROM {table_name('GD_Airport_Month')}
        WHERE year = {LATEST_YEAR}
    )
    SELECT reporting_iata                                                  AS airport,
           reporting_airport                                               AS airport_name,
           ROUND(MAX(year_passengers) / 1e6, 2)                            AS passengers_million,
           MAX_BY(month, passengers)                                       AS busiest_month,
           MIN_BY(month, passengers)                                       AS quietest_month,
           ROUND(MAX(passengers) / MIN(passengers), 2)                     AS seasonality_ratio,
           ROUND(100.0 * SUM(CASE WHEN month BETWEEN 6 AND 9 THEN passengers END) / MAX(year_passengers), 1)
               AS pct_of_year_in_june_to_september
    FROM monthly
    GROUP BY reporting_iata, reporting_airport
    ORDER BY seasonality_ratio DESC
    """
)

display(seasonality_df)

# COMMAND ----------

# DBTITLE 1,Chart: monthly profile of the main airports (share of the year in each month)
profile_pd = (
    spark.table(table_name("GD_Airport_Month"))
    .filter((F.col("year") == LATEST_YEAR) & F.col("reporting_iata").isin(MAIN_AIRPORTS))
    .withColumn("pct_of_year",
                100 * F.col("passengers") / F.sum("passengers").over(Window.partitionBy("reporting_iata")))
    .select("reporting_iata", "month", "pct_of_year")
    .orderBy("month")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(11, 4.5))
for airport in MAIN_AIRPORTS:
    series = profile_pd[profile_pd["reporting_iata"] == airport]
    ax.plot(series["month"], series["pct_of_year"].astype(float), marker="o", label=airport)
ax.axhline(100 / 12, color="grey", linestyle=":", label="No seasonality")
ax.set_title(f"Share of the passengers of {LATEST_YEAR} in each month")
ax.set_xlabel("Month")
ax.set_ylabel("% of the year")
ax.set_xticks(range(1, 13))
ax.grid(alpha=0.3)
ax.legend(ncol=3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.5 The route snapshot against the routes that really operated
# MAGIC The route snapshot is undated. It lists airlines that stopped operating years ago (for example US Airways, which
# MAGIC disappeared in 2015), so it describes the network of an earlier period. Eurostat tells us which routes of the
# MAGIC Portuguese airports had traffic in 2024.
# MAGIC
# MAGIC Only one direction of the comparison is valid: a route that Eurostat reports and the snapshot does not have is
# MAGIC **new since the snapshot**. The opposite is not valid, because Eurostat lists only the main partner airports of
# MAGIC each airport, so a missing route may simply be too small to be listed.

# COMMAND ----------

# DBTITLE 1,Routes with traffic in the latest year: already in the snapshot, or new since then?
snapshot_pairs = spark.table(table_name("SV_Connections")).select(
    F.col("src_iata").alias("reporting_iata"), F.col("dst_iata").alias("partner_iata"), F.lit(True).alias("in_snapshot")
)

route_check_df = (
    spark.table(table_name("GD_Route_Year"))
    .filter((F.col("year") == LATEST_YEAR) & F.col("passengers").isNotNull() & F.col("partner_iata").isNotNull())
    .join(snapshot_pairs, ["reporting_iata", "partner_iata"], "left")
    .withColumn("in_snapshot", F.coalesce("in_snapshot", F.lit(False)))
)

snapshot_comparison_df = (
    route_check_df.groupBy("reporting_iata")
    .agg(
        F.count("*").alias("routes_with_traffic"),
        F.sum(F.when(F.col("in_snapshot"), 1).otherwise(0)).alias("already_in_snapshot"),
        F.sum(F.when(~F.col("in_snapshot"), 1).otherwise(0)).alias("new_since_snapshot"),
        F.round(100 * F.sum(F.when(~F.col("in_snapshot"), F.col("passengers")).otherwise(0)) / F.sum("passengers"), 1)
        .alias("pct_passengers_on_new_routes"),
    )
    .orderBy(F.desc("routes_with_traffic"))
)

display(snapshot_comparison_df)

# COMMAND ----------

# DBTITLE 1,The busiest routes that are not in the snapshot
display(
    route_check_df.filter(~F.col("in_snapshot"))
    .select(
        F.col("reporting_iata").alias("from_airport"),
        F.col("partner_iata").alias("to_airport"),
        "partner_airport",
        "partner_country",
        F.round(F.col("passengers") / 1e3, 0).alias("passengers_thousand"),
        "load_factor_pct",
    )
    .orderBy(F.desc("passengers_thousand"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Structured Streaming: monthly traffic indicators
# MAGIC
# MAGIC **Q4: can the monthly indicators be kept up to date as new statistics arrive, with the same results as batch?**
# MAGIC
# MAGIC Traffic statistics are published month after month. A reporting system should add each delivery to its
# MAGIC indicators without recomputing everything. We reproduce this scenario with the monthly records of two years:
# MAGIC
# MAGIC 1. A delivery system is simulated by exporting the monthly records as **JSON files** to the landing zone of the
# MAGIC    Volume in **two drops**: the twelve months of 2023 first, the twelve months of 2024 later.
# MAGIC 2. A chain of three Structured Streaming queries moves the records through the medallion layers:
# MAGIC
# MAGIC | Query | Source | Processing | Sink | Output mode |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | JSON files in the Volume | add file name and ingestion time | `BZ_Traffic_Stream` | append |
# MAGIC | 2 | `BZ_Traffic_Stream` | validation, stateful deduplication, stream-static join with `SV_Airports` | `SV_Traffic_Stream` | append |
# MAGIC | 3 | `SV_Traffic_Stream` | aggregation per airport and month | `GD_Airport_Month_Stream` | complete |
# MAGIC
# MAGIC Each query has the three parts seen in class ABD08: a **source** (`readStream`), a **transformation** and a
# MAGIC **sink** (`writeStream`) with an output mode and a checkpoint location.
# MAGIC
# MAGIC > **Scope note.** The records are real Eurostat observations. Only their arrival as a stream is simulated.
# MAGIC
# MAGIC Key concepts applied:
# MAGIC * **Checkpointing.** Each query stores its progress in the Volume. Started again, it processes **only the new
# MAGIC   data**, exactly once: this is what makes the pipeline incremental and fault tolerant.
# MAGIC * **Micro-batches.** `maxFilesPerTrigger` limits how many files each micro-batch reads.
# MAGIC * **Trigger.** In class the queries ran continuously with the default trigger, on Colab. Databricks serverless
# MAGIC   compute supports the `availableNow` trigger instead: the query processes everything available, in several
# MAGIC   micro-batches, and stops. Scheduling it periodically gives near real-time processing without an always-on cluster.
# MAGIC * **Sink.** The streams are written to Delta tables instead of the `memory` or `console` sinks used in class, so
# MAGIC   that the results are stored in the lakehouse and can be queried with SQL.

# COMMAND ----------

# DBTITLE 1,Simulate the delivery system: monthly records of two years, in two drops
monthly_records_df = (
    spark.table(table_name("SV_Air_Traffic"))
    .filter(
        (F.col("frequency") == "M")
        & (F.col("direction") == "TOTAL")
        & F.col("year").isin(STREAM_DROP_1_YEAR, STREAM_DROP_2_YEAR)
    )
    .select(
        F.concat_ws("|", "route_code", "period", "measure").alias("record_id"),
        "route_code",
        "reporting_icao",
        "partner_icao",
        "period",
        "measure",
        "value",
    )
)

drop_1_df = monthly_records_df.filter(F.col("period").startswith(str(STREAM_DROP_1_YEAR)))
drop_2_df = monthly_records_df.filter(F.col("period").startswith(str(STREAM_DROP_2_YEAR)))


def land_records(df: DataFrame) -> None:
    """Write a set of traffic records to the landing zone as new JSON files."""
    df.repartition(FILES_PER_DROP).write.mode("append").json(DL_Landing_Path)


print(f"Monthly records to deliver : {monthly_records_df.count():,}")
print(f"  drop 1 ({STREAM_DROP_1_YEAR})          : {drop_1_df.count():,}")
print(f"  drop 2 ({STREAM_DROP_2_YEAR})          : {drop_2_df.count():,}")

# COMMAND ----------

# DBTITLE 1,Definition of the three streaming queries
# File sources need an explicit schema in streaming: there is no schema inference at run time.
traffic_record_schema = StructType(
    [
        StructField("record_id", StringType(), True),
        StructField("route_code", StringType(), True),
        StructField("reporting_icao", StringType(), True),
        StructField("partner_icao", StringType(), True),
        StructField("period", StringType(), True),
        StructField("measure", StringType(), True),
        StructField("value", DoubleType(), True),
    ]
)


def clean_traffic_records(records: DataFrame, airports_ref: DataFrame) -> DataFrame:
    """Silver logic for traffic records. Works for batch and for streaming DataFrames."""
    reporting = airports_ref.filter(F.col("icao_code").isNotNull()).select(
        F.col("icao_code").alias("reporting_icao"),
        F.col("iata_code").alias("reporting_iata"),
        F.col("airport_name").alias("reporting_airport"),
    )
    return (
        records.filter(
            F.col("record_id").isNotNull() & F.col("value").isNotNull() & (F.col("value") >= 0)
            & F.col("period").rlike("^[0-9]{4}-[0-9]{2}$")
        )
        .dropDuplicates(["record_id"])
        .join(F.broadcast(reporting), "reporting_icao", "left")
        .select(
            "record_id",
            "route_code",
            "reporting_iata",
            "reporting_airport",
            "partner_icao",
            F.substring("period", 1, 4).cast("int").alias("year"),
            F.substring("period", 6, 2).cast("int").alias("month"),
            "measure",
            "value",
        )
    )


def airport_month_indicators(records: DataFrame) -> DataFrame:
    """Gold logic: passengers, seats, flights and load factor per airport and month. Batch or streaming."""
    return (
        records.groupBy("reporting_iata", "reporting_airport", "year", "month")
        .agg(
            F.sum(F.when(F.col("measure") == "passengers_carried", F.col("value"))).alias("passengers"),
            F.sum(F.when(F.col("measure") == "passengers_on_board", F.col("value"))).alias("passengers_on_board"),
            F.sum(F.when(F.col("measure") == "seats", F.col("value"))).alias("seats"),
            F.sum(F.when(F.col("measure") == "flights", F.col("value"))).alias("flights"),
        )
        .withColumn("load_factor_pct", F.round(100 * F.try_divide(F.col("passengers_on_board"), F.col("seats")), 1))
    )


def start_bronze_stream():
    """Query 1: JSON files in the landing zone -> BZ_Traffic_Stream (append)."""
    records = (
        spark.readStream.format("json")
        .schema(traffic_record_schema)
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(DL_Landing_Path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return (
        records.writeStream.format("delta")
        .queryName("q_bronze_traffic")
        .outputMode("append")
        .option("checkpointLocation", f"{DL_Checkpoint_Path}/bronze_traffic_stream")
        .trigger(availableNow=True)
        .toTable(table_name("BZ_Traffic_Stream"))
    )


def start_silver_stream():
    """Query 2: BZ_Traffic_Stream -> SV_Traffic_Stream (append)."""
    bronze_stream = spark.readStream.table(table_name("BZ_Traffic_Stream"))
    silver_stream = clean_traffic_records(bronze_stream, spark.table(table_name("SV_Airports")))
    return (
        silver_stream.writeStream.format("delta")
        .queryName("q_silver_traffic")
        .outputMode("append")
        .option("checkpointLocation", f"{DL_Checkpoint_Path}/silver_traffic_stream")
        .trigger(availableNow=True)
        .toTable(table_name("SV_Traffic_Stream"))
    )


def start_gold_stream():
    """Query 3: SV_Traffic_Stream -> GD_Airport_Month_Stream (complete)."""
    silver_stream = spark.readStream.table(table_name("SV_Traffic_Stream"))
    return (
        airport_month_indicators(silver_stream)
        .writeStream.format("delta")
        .queryName("q_gold_airport_month")
        .outputMode("complete")
        .option("checkpointLocation", f"{DL_Checkpoint_Path}/gold_airport_month_stream")
        .trigger(availableNow=True)
        .toTable(table_name("GD_Airport_Month_Stream"))
    )


def run_streaming_pipeline(label: str) -> None:
    """Run the three queries in sequence until all available data is processed."""
    for start_query in (start_bronze_stream, start_silver_stream, start_gold_stream):
        query = start_query()
        query.awaitTermination()
        print(f"Query {query.name:<22}: finished, active = {query.isActive}")
    print(f"[{label}] streaming tables after the run:")
    for name in ("BZ_Traffic_Stream", "SV_Traffic_Stream", "GD_Airport_Month_Stream"):
        print(f"  {name:<26}: {row_count(name):>8,} rows")


def show_stream_indicator() -> None:
    """Passengers per year of the main airports, read from the streaming Gold table."""
    display(
        spark.table(table_name("GD_Airport_Month_Stream"))
        .filter(F.col("reporting_iata").isin(MAIN_AIRPORTS))
        .groupBy("reporting_iata")
        .pivot("year", [STREAM_DROP_1_YEAR, STREAM_DROP_2_YEAR])
        .agg(F.round(F.sum("passengers") / 1e6, 2))
        .withColumnRenamed(str(STREAM_DROP_1_YEAR), f"passengers_{STREAM_DROP_1_YEAR}_million")
        .withColumnRenamed(str(STREAM_DROP_2_YEAR), f"passengers_{STREAM_DROP_2_YEAR}_million")
        .orderBy(F.desc(f"passengers_{STREAM_DROP_1_YEAR}_million"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 First drop: the months of 2023

# COMMAND ----------

# DBTITLE 1,Drop 1 arrives and the streaming pipeline runs
land_records(drop_1_df)
run_streaming_pipeline("after drop 1")

bronze_stream_version_after_drop_1 = (
    spark.sql(f"DESCRIBE HISTORY {table_name('BZ_Traffic_Stream')}").agg(F.max("version")).first()[0]
)

# COMMAND ----------

# DBTITLE 1,Streaming indicator after drop 1
show_stream_indicator()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 Second drop: incremental processing
# MAGIC The months of 2024 arrive in the landing zone and the **same** queries are started again. Thanks to the
# MAGIC checkpoints they do not reprocess 2023: only the new files are read, and the indicator gains a second year.

# COMMAND ----------

# DBTITLE 1,Drop 2 arrives and the streaming pipeline runs again
land_records(drop_2_df)
run_streaming_pipeline("after drop 2")

# COMMAND ----------

# DBTITLE 1,Streaming indicator after drop 2
show_stream_indicator()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.3 Evidence of micro-batches
# MAGIC Every streaming micro-batch is one ACID commit in the Delta transaction log. The history of the streaming Bronze
# MAGIC table shows how the files were consumed: several `STREAMING UPDATE` commits for each drop.

# COMMAND ----------

# DBTITLE 1,Delta history of the streaming Bronze table: one commit per micro-batch
display(
    spark.sql(f"DESCRIBE HISTORY {table_name('BZ_Traffic_Stream')}")
    .select(
        "version",
        "timestamp",
        "operation",
        F.expr("try_element_at(operationMetrics, 'numOutputRows')").alias("rows_written"),
    )
    .orderBy("version")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.4 Reconciliation: streaming vs batch
# MAGIC The same Silver and Gold logic (`clean_traffic_records` and `airport_month_indicators`) is applied **in batch**
# MAGIC to the same records. If the streaming pipeline is correct, both paths must give exactly the same result.

# COMMAND ----------

# DBTITLE 1,Compare the streaming tables with a batch computation
# The batch results are saved as tables, so both sides of the comparison are read from Delta.
save_table(
    clean_traffic_records(monthly_records_df, spark.table(table_name("SV_Airports"))),
    "SV_Traffic_Batch",
    "SILVER - monthly traffic records computed in batch (control for the streaming pipeline)",
)
batch_silver = spark.table(table_name("SV_Traffic_Batch"))
save_table(
    airport_month_indicators(batch_silver),
    "GD_Airport_Month_Batch",
    "GOLD - airport and month indicators computed in batch (control for the streaming pipeline)",
)
batch_gold = spark.table(table_name("GD_Airport_Month_Batch"))
stream_silver = spark.table(table_name("SV_Traffic_Stream"))
stream_gold = spark.table(table_name("GD_Airport_Month_Stream"))

n_batch_records = batch_silver.count()
n_stream_records = stream_silver.count()
only_in_batch = batch_silver.join(stream_silver, "record_id", "left_anti").count()
only_in_stream = stream_silver.join(batch_silver, "record_id", "left_anti").count()

gold_keys = ["reporting_iata", "year", "month", "passengers", "passengers_on_board", "seats", "flights"]
gold_differences = (
    batch_gold.select(*gold_keys).exceptAll(stream_gold.select(*gold_keys)).count()
    + stream_gold.select(*gold_keys).exceptAll(batch_gold.select(*gold_keys)).count()
)

print(f"Silver records  batch = {n_batch_records:,}   streaming = {n_stream_records:,}")
print(f"Records only in the batch result     : {only_in_batch:,}")
print(f"Records only in the streaming result : {only_in_stream:,}")
print(f"Gold rows       batch = {batch_gold.count():,}   streaming = {stream_gold.count():,}")
print(f"Gold rows that differ                : {gold_differences:,}")

streaming_matches_batch = (
    n_batch_records == n_stream_records and only_in_batch == 0 and only_in_stream == 0 and gold_differences == 0
)
print(f"\nStreaming results identical to batch results : {'YES' if streaming_matches_batch else 'NO'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. The future: passenger scenarios to 2030
# MAGIC
# MAGIC **Q5: where is demand heading until 2030?**
# MAGIC
# MAGIC The series has three regimes: steady growth until 2019, the collapse and recovery of the pandemic, and the years
# MAGIC since. A single model fitted on the whole series would average three different worlds. We therefore ask a
# MAGIC simpler and more robust question first, and build scenarios afterwards.
# MAGIC
# MAGIC ### 12.1 Is traffic back on its pre-pandemic trend?
# MAGIC A straight line is fitted, with the Spark SQL aggregates `regr_slope` and `regr_intercept`, on the annual
# MAGIC passengers of 2015 to 2019. Extending that line gives the traffic that would have been expected without the
# MAGIC pandemic, which is compared with what happened.

# COMMAND ----------

# DBTITLE 1,Total passengers handled per year and the pre-pandemic trend
national_year_df = spark.sql(
    f"""
    WITH national AS (
        SELECT year, SUM(passengers) / 1e6 AS passengers_million
        FROM {table_name('GD_Airport_Year')}
        WHERE passengers IS NOT NULL
        GROUP BY year
    ),
    trend AS (
        SELECT regr_slope(passengers_million, year)     AS slope,
               regr_intercept(passengers_million, year) AS intercept
        FROM national
        WHERE year BETWEEN {TREND_FIRST_YEAR} AND {PRE_PANDEMIC_YEAR}
    )
    SELECT n.year,
           ROUND(n.passengers_million, 2)                                             AS passengers_million,
           ROUND(100.0 * (n.passengers_million / LAG(n.passengers_million) OVER (ORDER BY n.year) - 1), 1)
               AS growth_pct,
           ROUND(t.intercept + t.slope * n.year, 2)                                   AS pre_pandemic_trend_million,
           ROUND(100.0 * (n.passengers_million / (t.intercept + t.slope * n.year) - 1), 1)
               AS gap_to_trend_pct,
           ROUND(t.slope, 2)                                                          AS trend_million_per_year
    FROM national n CROSS JOIN trend t
    WHERE n.year >= {TREND_FIRST_YEAR}
    ORDER BY n.year
    """
)

display(national_year_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.2 Scenarios to 2030
# MAGIC A forecast with one number would suggest a certainty that the data does not support. Capacity decisions are
# MAGIC better served by a **range** between two transparent scenarios, both starting from the latest year:
# MAGIC
# MAGIC * **Scenario A, growth at the latest pace**: passengers keep growing at the rate observed in the latest year.
# MAGIC * **Scenario B, pre-pandemic volume growth**: passengers grow by the same number of passengers per year as on the
# MAGIC   2015 to 2019 trend line.
# MAGIC
# MAGIC Neither scenario includes capacity limits. Lisbon airport, which handles about half of the traffic, is known to
# MAGIC operate close to its capacity, so the scenarios describe demand, not what the infrastructure can deliver.

# COMMAND ----------

# DBTITLE 1,GD_Passenger_Scenarios
latest_row = national_year_df.filter(F.col("year") == LATEST_YEAR).first()
latest_passengers_million = latest_row["passengers_million"]
latest_growth = latest_row["growth_pct"] / 100.0
trend_million_per_year = latest_row["trend_million_per_year"]
gap_to_trend_pct = latest_row["gap_to_trend_pct"]

print(f"Passengers handled in {LATEST_YEAR}       : {latest_passengers_million:.1f} million")
print(f"Growth in {LATEST_YEAR}                   : {latest_growth:.1%}")
print(f"Pre-pandemic trend (2015-2019) : +{trend_million_per_year:.2f} million passengers per year")
print(f"Gap to the pre-pandemic trend  : {gap_to_trend_pct:+.1f}%")

gd_passenger_scenarios_df = spark.sql(
    f"""
    SELECT year,
           ROUND({latest_passengers_million} * POWER(1 + {latest_growth}, year - {LATEST_YEAR}), 1)
               AS scenario_a_latest_pace_million,
           ROUND({latest_passengers_million} + {trend_million_per_year} * (year - {LATEST_YEAR}), 1)
               AS scenario_b_pre_pandemic_volume_million
    FROM (SELECT explode(sequence({LATEST_YEAR + 1}, {FORECAST_END_YEAR})) AS year)
    """
)

save_table(
    gd_passenger_scenarios_df,
    "GD_Passenger_Scenarios",
    "GOLD - passengers handled by the Portuguese airports per year until 2030, in two scenarios",
)

display(spark.table(table_name("GD_Passenger_Scenarios")).orderBy("year"))

# COMMAND ----------

# DBTITLE 1,Chart: observed passengers, pre-pandemic trend and scenarios
history_pd = national_year_df.toPandas()
scenarios_pd = spark.table(table_name("GD_Passenger_Scenarios")).orderBy("year").toPandas()

fig, ax = plt.subplots(figsize=(11, 4.8))
ax.plot(history_pd["year"], history_pd["passengers_million"].astype(float), marker="o", color="steelblue",
        label="Observed")
ax.plot(history_pd["year"], history_pd["pre_pandemic_trend_million"].astype(float), color="grey", linestyle=":",
        label="Pre-pandemic trend (2015-2019)")
ax.plot([LATEST_YEAR] + scenarios_pd["year"].tolist(),
        [float(latest_passengers_million)] + scenarios_pd["scenario_a_latest_pace_million"].astype(float).tolist(),
        color="darkorange", linestyle="--", marker="o", label="Scenario A: latest pace")
ax.plot([LATEST_YEAR] + scenarios_pd["year"].tolist(),
        [float(latest_passengers_million)]
        + scenarios_pd["scenario_b_pre_pandemic_volume_million"].astype(float).tolist(),
        color="seagreen", linestyle="--", marker="o", label="Scenario B: pre-pandemic volume growth")
ax.set_title("Passengers handled by the Portuguese airports per year")
ax.set_xlabel("Year")
ax.set_ylabel("Million passengers")
ax.grid(alpha=0.3)
ax.legend(loc="upper left")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Delta Lake: MERGE, audit history and time travel
# MAGIC The Silver and Gold tables of the project are Delta tables, so each write is an ACID transaction recorded in a log.
# MAGIC This gives capabilities that plain files in a data lake do not have:
# MAGIC * **Updates and merges**: `GD_Airport_Year` was loaded in two steps, the second one with a `MERGE` (section 10.1).
# MAGIC   A MERGE makes the load idempotent: running it twice does not duplicate rows.
# MAGIC * **Audit**: what changed and when (`DESCRIBE HISTORY`).
# MAGIC * **Time travel**: query a table as it was at a previous version (`VERSION AS OF`), which is useful to reproduce
# MAGIC   a report or to recover from a wrong load.

# COMMAND ----------

# DBTITLE 1,History of GD_Airport_Year: the initial load and the MERGE
# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.DL_AIR.GD_Airport_Year

# COMMAND ----------

# DBTITLE 1,Time travel: GD_Airport_Year before and after the MERGE
print(f"GD_Airport_Year at version {airport_year_version_before_merge} (before the MERGE):")
spark.sql(
    f"""
    SELECT MAX(year) AS latest_year, COUNT(*) AS rows
    FROM {table_name('GD_Airport_Year')} VERSION AS OF {airport_year_version_before_merge}
    """
).show()

print("GD_Airport_Year now:")
spark.sql(f"SELECT MAX(year) AS latest_year, COUNT(*) AS rows FROM {table_name('GD_Airport_Year')}").show()

# COMMAND ----------

# DBTITLE 1,Time travel: the streaming Bronze table before and after the second drop
rows_after_drop_1 = spark.sql(
    f"SELECT COUNT(*) FROM {table_name('BZ_Traffic_Stream')} VERSION AS OF {bronze_stream_version_after_drop_1}"
).first()[0]
rows_now = row_count("BZ_Traffic_Stream")

print(f"Version {bronze_stream_version_after_drop_1} (after drop 1) : {rows_after_drop_1:>8,} records")
print(f"Current version           : {rows_now:>8,} records")
print(f"Records added by drop 2   : {rows_now - rows_after_drop_1:>8,}")

# COMMAND ----------

# DBTITLE 1,Tables of the lakehouse schema
display(
    spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
    .filter(~F.col("isTemporary"))
    .select("tableName")
    .withColumn(
        "layer",
        F.when(F.lower("tableName").startswith("bz_"), "1 - Bronze (streaming)")
        .when(F.lower("tableName").startswith("sv_"), "2 - Silver")
        .otherwise("3 - Gold"),
    )
    .orderBy("layer", "tableName")
)

# COMMAND ----------

# DBTITLE 1,Folders of the data lake in the Volume
display(dbutils.fs.ls(DL_Path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Conclusions and recommendations
# MAGIC The next cell computes the headline numbers directly from the Gold tables, so that the conclusions always reflect
# MAGIC the data that was actually processed.

# COMMAND ----------

# DBTITLE 1,Key findings computed from the Gold tables
best = spark.table(table_name("GD_Portugal_Best_Itineraries"))
hub_reach = reach_df.filter(F.col("origin") == HOME_HUB).first()
top_world_airport = (
    spark.table(table_name("GD_Airport_Connectivity")).orderBy("world_rank", "iata_code").first()
)
top_pagerank = spark.table(table_name("GD_Airport_PageRank")).orderBy("pagerank_position", "id").first()
hub_pagerank = spark.table(table_name("GD_Airport_PageRank")).filter(F.col("id") == HOME_HUB).first()
world_hubs = spark.table(table_name("GD_Global_Transfer_Hubs")).orderBy("hub_rank", "id").limit(10).collect()
pct_top10_world_hubs = sum(row["pct_of_pairs"] for row in world_hubs)

lisbon_hubs = (
    best.filter((F.col("origin") == HOME_HUB) & (F.col("stops") == 1))
    .groupBy("stop_1").count().orderBy(F.desc("count"), "stop_1").collect()
)
lisbon_one_stop_total = sum(row["count"] for row in lisbon_hubs)
lisbon_top5_hubs = [row["stop_1"] for row in lisbon_hubs[:5]]
pct_lisbon_top5_hubs = 100 * sum(row["count"] for row in lisbon_hubs[:5]) / lisbon_one_stop_total
hubs_for_half, covered = 0, 0
for row in lisbon_hubs:
    hubs_for_half += 1
    covered += row["count"]
    if covered >= 0.5 * lisbon_one_stop_total:
        break
lisbon_detour = (
    best.filter((F.col("origin") == HOME_HUB) & (F.col("stops") == 1))
    .agg(F.percentile_approx("detour_factor", 0.5)).first()[0]
)
bfs_summary = (
    spark.table(table_name("GD_Flights_From_Lisbon"))
    .agg(
        F.max("flights").alias("max_flights"),
        F.sum(F.when(F.col("flights").isNull(), 1).otherwise(0)).alias("unreachable"),
        F.sum(F.when(F.col("flights") <= 2, 1).otherwise(0)).alias("within_two_flights"),
    )
    .first()
)
carrier_hub = carrier_hubs_df.first()

airports_latest = (
    spark.table(table_name("GD_Airport_Year")).filter(F.col("year") == LATEST_YEAR).orderBy(F.desc("passengers")).collect()
)
total_latest = sum(row["passengers"] for row in airports_latest)
national_load = (
    spark.table(table_name("GD_Airport_Year")).filter(F.col("year") == LATEST_YEAR)
    .agg((100 * F.sum("passengers_on_board") / F.sum("seats")).alias("load_factor")).first()["load_factor"]
)
concentration = route_concentration_df.first()
top_route = (
    spark.table(table_name("GD_Route_Year"))
    .filter((F.col("year") == LATEST_YEAR) & F.col("passengers").isNotNull())
    .orderBy(F.desc("passengers")).first()
)
seasonality = {row["airport"]: row for row in seasonality_df.collect()}
pre_pandemic_row = national_year_df.filter(F.col("year") == PRE_PANDEMIC_YEAR).first()
scenario_end = spark.table(table_name("GD_Passenger_Scenarios")).filter(F.col("year") == FORECAST_END_YEAR).first()
new_routes = route_check_df.agg(
    F.count("*").alias("routes"), F.sum(F.when(~F.col("in_snapshot"), 1).otherwise(0)).alias("new_routes")
).first()

print("DATA")
print(f"  Route network: {row_count('SV_Routes'):,} airline routes, {n_edges:,} connections, {n_vertices:,} airports.")
print(f"  Eurostat: {row_count('SV_Air_Traffic'):,} observations of passengers, seats and flights.")
print(f"  Data quality: {len(quality_checks) - n_checks_failed} of {len(quality_checks)} checks passed.")
print("\nQ1 - THE WORLD NETWORK")
print(f"  Best connected airport: {top_world_airport['iata_code']} ({top_world_airport['destinations']} destinations)."
      f" Highest PageRank: {top_pagerank['id']}.")
print(f"  Lisbon: {hub_pagerank['out_degree']} destinations, position {hub_pagerank['degree_position']} by destinations"
      f" and {hub_pagerank['pagerank_position']} by PageRank among {n_vertices:,} airports.")
print(f"  {n_one_way / n_edges:.1%} of the connections exist in one direction only.")
print(f"  TAP: {carrier_hub['pct_of_tap_connections']}% of its connections leave from {carrier_hub['src']}.")
print(f"  Every reachable airport is at most {bfs_summary['max_flights']} flights from Lisbon;"
      f" {bfs_summary['unreachable']} airports cannot be reached.")
print("\nQ2 - THE CONNECTIVITY OF PORTUGAL")
print(f"  Itineraries enumerated from the Portuguese airports: {n_direct:,} direct, {n_one_stop:,} with one stop,"
      f" {n_two_stop:,} with two stops.")
print(f"  One-stop itineraries of the whole world: {n_world_one_stop:,}.")
print(f"  Lisbon reaches {hub_reach['direct']} airports directly, {hub_reach['within_one_stop']:,} within one stop"
      f" ({hub_reach['pct_of_world_within_one_stop']}% of the network) and {hub_reach['within_two_stops']:,} within two"
      f" stops ({hub_reach['pct_of_world_within_two_stops']}%).")
print(f"  Lisbon's one-stop destinations are best reached through {len(lisbon_hubs)} different hubs."
      f" The top five ({', '.join(lisbon_top5_hubs)}) serve {pct_lisbon_top5_hubs:.1f}% of them;"
      f" {hubs_for_half} hubs are needed to serve half.")
print(f"  Median detour of a one-stop itinerary from Lisbon: {lisbon_detour:.2f} times the direct distance.")
print(f"  In the world, 10 airports are the best transfer point for {pct_top10_world_hubs:.1f}% of the pairs"
      f" connected with one stop.")
print("\nQ3 - DEMAND")
print(f"  {total_latest / 1e6:.1f} million passengers handled in {LATEST_YEAR};"
      f" {airports_latest[0]['reporting_iata']} handled {100 * airports_latest[0]['passengers'] / total_latest:.1f}%.")
print(f"  Load factor: {national_load:.1f}% of the seats offered were occupied.")
print(f"  The 10 busiest routes carry {concentration['pct_passengers_top_10_routes']}% of the passengers;"
      f" the bottom half of the {concentration['routes']} routes carries"
      f" {concentration['pct_passengers_bottom_half_of_routes']}%.")
print(f"  Busiest route: {top_route['reporting_iata']} - {top_route['partner_iata']}"
      f" ({top_route['passengers'] / 1e6:.2f} million passengers).")
print(f"  Seasonality ratio (busiest month / quietest month): "
      + ", ".join(f"{a} {seasonality[a]['seasonality_ratio']}" for a in MAIN_AIRPORTS if a in seasonality) + ".")
print(f"  {new_routes['new_routes']} of the {new_routes['routes']} routes with traffic in {LATEST_YEAR}"
      f" are not in the route snapshot.")
print("\nQ4 - STREAMING")
print(f"  Streaming results identical to batch results: {'YES' if streaming_matches_batch else 'NO'}"
      f" ({n_stream_records:,} monthly records processed in two drops).")
print("\nQ5 - THE FUTURE")
print(f"  {LATEST_YEAR}: {latest_passengers_million:.1f} million passengers,"
      f" {100 * (latest_passengers_million / pre_pandemic_row['passengers_million'] - 1):+.1f}% compared with"
      f" {PRE_PANDEMIC_YEAR}, growth of {latest_growth:.1%} in the year.")
print(f"  Gap to the pre-pandemic trend: {gap_to_trend_pct:+.1f}%.")
scenario_values = sorted(
    [scenario_end["scenario_a_latest_pace_million"], scenario_end["scenario_b_pre_pandemic_volume_million"]]
)
print(f"  {FORECAST_END_YEAR}: between {scenario_values[0]:.1f} and {scenario_values[1]:.1f} million passengers"
      f" (scenario A {scenario_end['scenario_a_latest_pace_million']:.1f},"
      f" scenario B {scenario_end['scenario_b_pre_pandemic_volume_million']:.1f}).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Answers to the business questions
# MAGIC * **Q1, the world network.** The network is extremely unequal: two thirds of the airports have at most five
# MAGIC   destinations, while the 2% of airports with more than a hundred destinations hold a quarter of all connections
# MAGIC   (section 8.1). PageRank confirms the same group of hubs, with differences in the order (section 8.5). Lisbon is
# MAGIC   in the top 2% of the world by number of destinations, and the network of TAP is centred on it (section 8.3).
# MAGIC * **Q2, the connectivity of Portugal.** Direct flights reach 3% of the network from Lisbon, one stop reaches a
# MAGIC   third and two stops more than three quarters (section 9.3). One stop multiplies the reach of Lisbon by more than
# MAGIC   ten, and that of Porto and Faro by a similar factor. The detour imposed by a stop is small: the typical one-stop
# MAGIC   itinerary from Lisbon is only 3% longer than the direct distance. The one-stop reach is spread over many
# MAGIC   transfer airports, specialised by region: no single hub is critical, but each region of the world depends on a
# MAGIC   few gateways.
# MAGIC * **Q3, demand.** Lisbon handles half of the passengers and Porto a fifth (section 10.2). The busiest routes link
# MAGIC   Lisbon and Porto to Madrid, Paris, Barcelona, London and Amsterdam, and Lisbon to the islands (section 10.3).
# MAGIC   Planes fly full, with the notable exception of the short domestic routes between Lisbon, Porto and Faro. Faro
# MAGIC   and the Azores are strongly seasonal; Lisbon, Porto and Funchal are not (section 10.4).
# MAGIC * **Q4, streaming.** The streaming pipeline processed two years of monthly records in two instalments, read only
# MAGIC   the new files in the second run, and reached exactly the same result as the batch computation (section 11.4).
# MAGIC * **Q5, the future.** Traffic is above its 2019 level but well below the path it was following before the
# MAGIC   pandemic, and its growth slowed in 2024 (section 12.1). The two scenarios for 2030 differ by about ten million
# MAGIC   passengers: they are a range for planning, not a forecast (section 12.2).
# MAGIC
# MAGIC ### Recommendations
# MAGIC | # | Recommendation | For whom | Evidence |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Manage the long-haul gateways as a portfolio.** Lisbon's one-stop reach is spread over many hubs, each one opening a region of the world. Track which gateway serves which region, and make sure every region keeps at least two alternatives. | Airport operator, tourism board | Section 9.3, hubs of Lisbon and reach by continent |
# MAGIC | 2 | **Promote the regional airports by their one-stop reach.** Porto, Faro, Funchal and Ponta Delgada have between 17 and 62 direct destinations, but each reaches more than 500 airports with one stop. | Regional tourism boards | Section 9.3, reach table |
# MAGIC | 3 | **Target winter demand in Faro and the Azores.** Faro handles four times more passengers in August than in January, so airport and tourism capacity sits idle in winter. Route incentives and campaigns should be aimed at the low season. Funchal shows that an island destination can be stable all year. | Airport commercial teams | Section 10.4, seasonality ratio |
# MAGIC | 4 | **Review the short domestic air routes.** Lisbon - Porto and Lisbon - Faro have the lowest occupancy among the large routes (about 73% against 86% on average) and together offer about 400,000 empty seats in each direction per year. They are the natural candidates for capacity adjustments and for coordination with rail. | Airline network planners, transport policy | Section 10.3, load factor |
# MAGIC | 5 | **Plan with a range.** The two scenarios differ by about ten million passengers in 2030. Infrastructure decisions should be tested against both ends, and Lisbon's capacity limit must be considered separately. | Capacity planners | Section 12.2 |
# MAGIC
# MAGIC ### What we learned about Big Data processing
# MAGIC * **Joins create the volume.** Megabytes of routes became millions of itineraries. The pandas version of this
# MAGIC   analysis had to sample; Spark enumerated every one-stop itinerary of the world and every itinerary of up to two
# MAGIC   stops from Portugal with ordinary joins and window functions.
# MAGIC * **Veracity needs explicit decisions.** Reused airline codes, an undated snapshot, non-airport codes in Eurostat
# MAGIC   and domestic passengers counted at both ends were each identified, measured and documented.
# MAGIC * **One engine, one set of rules.** The same functions computed the batch and the streaming indicators, which is
# MAGIC   what guarantees that they agree.
# MAGIC * **Graph algorithms are joins.** PageRank, breadth-first search and motifs were written as joins and aggregations
# MAGIC   that Spark distributes like any other query.
# MAGIC * **Delta Lake** turns files into reliable tables: MERGE, history and time travel on the same Gold table.
# MAGIC
# MAGIC ### Limitations
# MAGIC * The route snapshot is **undated and old**: it contains airlines that no longer operate and misses recent routes
# MAGIC   (section 10.5). It describes the structure of the network, not today's schedule. For example, Moscow appears as
# MAGIC   Lisbon's first gateway to the east, a link that depends on routes between the European Union and Russia that
# MAGIC   have not operated since 2022. Reach and hub results should be read as structural, not as a travel planner.
# MAGIC * An itinerary is a sequence of connections. Schedules, connection times, prices, visas and airline alliances are
# MAGIC   not in the data, so some itineraries are not bookable in practice.
# MAGIC * Eurostat has no airline dimension and lists only the main partner airports of each airport. Domestic passengers
# MAGIC   are counted at both airports.
# MAGIC * The Eurostat file ends in December 2024.
# MAGIC * The scenarios extend past growth and ignore capacity limits, prices and the economy.
# MAGIC * Streaming used simulated file drops and the `availableNow` trigger. The Gold stream uses *complete* output mode;
# MAGIC   with an unbounded stream a **watermark** would be added so that old state is released.
# MAGIC
# MAGIC ### Future work
# MAGIC * Replace the route snapshot with a current schedule source, so that structure and traffic refer to the same year.
# MAGIC * Extend the two-stop enumeration to the whole world (about 146 million itineraries) on a larger cluster.
# MAGIC * **Machine learning with Spark ML**: a demand model per route with explanatory variables (distance, seasonality,
# MAGIC   competition, tourism) instead of scenarios.
# MAGIC * Add the Eurostat files of other countries to compare Portugal with Spain and with other peripheral markets.
# MAGIC
# MAGIC ### References
# MAGIC * Airports, routes and airlines: public static snapshot (Kaggle), used in the group's earlier project
# MAGIC   *Airport Network Route Analysis*.
# MAGIC * Eurostat, *Air passenger transport between the main airports of Portugal and their main partner airports
# MAGIC   (routes data)*, dataset `avia_par_pt`, downloaded on 16 November 2026.
# MAGIC * Damji, J., Wenig, B., Das, T., Lee, D. (2020). *Learning Spark: Lightning-fast Data Analysis*. O'Reilly.
# MAGIC * Chambers, B., Zaharia, M. (2018). *Spark: The Definitive Guide*. O'Reilly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean the environment (optional)
# MAGIC The cell below removes everything the project created. It is commented so that the tables stay available for
# MAGIC exploration after **Run all**; the next run rebuilds everything anyway.

# COMMAND ----------

# DBTITLE 1,Drop the lakehouse schema and the data lake folders
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
# dbutils.fs.rm(DL_Path, True)
