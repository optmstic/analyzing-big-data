# Databricks notebook source
# MAGIC %md
# MAGIC # Portugal's air connectivity, 2016-2026
# MAGIC ## The European route network and the passengers who use it: a lakehouse on Apache Spark (Databricks Free Edition)
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
# MAGIC 7. **Gold** layer: the European route network, 2016-2024 (Spark SQL)
# MAGIC 8. **Graph analytics**: hubs, reachability and PageRank
# MAGIC 9. Portugal's connectivity: every itinerary with up to two stops
# MAGIC 10. Demand: the passengers of the Portuguese airports, 2016-2026
# MAGIC 11. **Structured Streaming**: monthly traffic indicators
# MAGIC 12. The future: passenger scenarios to 2030
# MAGIC 13. Delta Lake: MERGE, audit history and time travel
# MAGIC 14. Conclusions and recommendations
# MAGIC
# MAGIC > **How to run:** upload the 49 data files of the project to the folder `air_routes` of the Volume
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
# MAGIC connected each Portuguese airport is**, **which hubs that connectivity relies on**, and **where the passengers
# MAGIC actually are and where demand is heading**.
# MAGIC
# MAGIC **What we did.** We built one lakehouse from official Eurostat statistics only: the passengers, seats and flights
# MAGIC of every main air route declared by 35 European countries for each year from 2016 to 2024, and the monthly
# MAGIC traffic of 496 European airports from January 2016 to 2026. From the route declarations we reconstructed the
# MAGIC real European route network of each year and, with Spark, enumerated **every** itinerary of up to two stops
# MAGIC from every Portuguese airport.
# MAGIC
# MAGIC **What we found.**
# MAGIC * **Reach.** In 2024 Lisbon had direct flights to 106 airports and reached 562 with one stop: 94% of the
# MAGIC   network. Porto, with 77 direct destinations, reached almost as many (544).
# MAGIC * **Hubs.** That reach is concentrated: Istanbul alone is the best transfer for a fifth of Lisbon's one-stop
# MAGIC   destinations, and five hubs for almost half.
# MAGIC * **Resilience.** The European network lost half of its routes in 2020; by 2024 it had 8% more routes and 14%
# MAGIC   more passengers than in 2019.
# MAGIC * **Demand.** Portugal had 67 million air passengers in 2025, 22% more than in 2019, the 7th largest growth among
# MAGIC   33 European countries. Planes are full (86% of the seats), except on the short domestic routes (about 72%).
# MAGIC * **Seasonality.** Faro handles more than four times as many passengers in its busiest month as in its quietest;
# MAGIC   Lisbon, Porto and Funchal are stable all year.
# MAGIC * **Trend.** After the recovery, growth settled at about 5% a year, and at 3.7% in the first four months of 2026.
# MAGIC   Traffic is still 19% below the path it was following before the pandemic.
# MAGIC
# MAGIC **What we recommend** (section 14): reduce the dependence on a single transfer hub, promote the regional
# MAGIC airports by their one-stop reach, target winter demand in Faro and the Azores, review the domestic air routes
# MAGIC with low occupancy, and plan capacity for a range of scenarios instead of one forecast.

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
# MAGIC * **Use**: how many passengers, seats and flights each route and airport really has, month by month. This is a
# MAGIC   **time-series** problem.
# MAGIC
# MAGIC Why it is a Big Data problem:
# MAGIC
# MAGIC * **Volume through combination.** The source files are compact, but the question "how can I get from A to B?"
# MAGIC   multiplies them: the 12,475 routes of one year generate about 920,000 one-stop itineraries, and the two-stop
# MAGIC   itineraries from the Portuguese airports alone are 1.35 million. An earlier analysis of this kind by the group,
# MAGIC   written in pandas for another course, had to work on a random sample of 60 airports. Spark computes the
# MAGIC   itineraries with distributed joins, with no sampling.
# MAGIC * **Variety.** One relational CSV file, 48 wide statistical text files with embedded flags, in three different
# MAGIC   layouts, and JSON event files (streaming).
# MAGIC * **Velocity.** Traffic statistics arrive every month and the indicators must be updated incrementally.
# MAGIC * **Veracity.** Every route between two reporting countries is declared twice, once by each airport, and the two
# MAGIC   declarations do not always agree. Countries stop reporting (the United Kingdom after 2019). Missing and
# MAGIC   confidential values are marked with symbols. Each of these is measured and handled explicitly.
# MAGIC
# MAGIC ### Business questions
# MAGIC
# MAGIC | # | Business question | Who needs the answer | Processing type | Section |
# MAGIC |---|---|---|---|---|
# MAGIC | Q1 | How is the European route network structured, how did it change from 2016 to 2024, and which airports are its hubs? | Airline network planners | Spark SQL, graph analytics | 7, 8 |
# MAGIC | Q2 | How much of the network can be reached from each Portuguese airport with 0, 1 and 2 stops, and through which hubs? | Airport operators, tourism boards | Distributed joins, window functions | 9 |
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
# MAGIC | **Apache Spark DataFrame API** (PySpark) | Multi-file ingestion, parsing, joins that enumerate itineraries, pivot, window functions, a UDF |
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
# MAGIC  airports.csv           -->   Parquet: airports            -->  SV_Airports
# MAGIC
# MAGIC  routes_annual_*.tsv    -->   Parquet: routes_annual       -->  SV_Route_Reports          -->  GD_Network_Year
# MAGIC  38 files, 35 countries batch  (lines of text + file name)       SV_Flows                       GD_Airport_Connectivity
# MAGIC                                                                                                GD_Graph_Vertices / _Edges
# MAGIC                                                                                                GD_Airport_PageRank
# MAGIC                                                                                                GD_Flights_From_Lisbon
# MAGIC                                                                                                GD_Transfer_Hubs
# MAGIC                                                                                                GD_Portugal_Best_Itineraries
# MAGIC                                                                                                GD_Portugal_Reach_Year
# MAGIC                                                                                                GD_Route_Year
# MAGIC
# MAGIC  routes_pt_monthly_*    -->   Parquet: routes_pt_monthly   -->  SV_Route_Month            -->  (route seasonality)
# MAGIC  4 files               batch
# MAGIC
# MAGIC  airports_monthly_*     -->   Parquet: airports_monthly    -->  SV_Airport_Month          -->  GD_Airport_Month
# MAGIC  6 files               batch                                                                   GD_Country_Year
# MAGIC                                                                                                GD_Portugal_Month
# MAGIC                                                                                                GD_Passenger_Scenarios
# MAGIC
# MAGIC  Monthly traffic        -->   BZ_Traffic_Stream            -->  SV_Traffic_Stream         -->  GD_Country_Month_Stream
# MAGIC  records, JSON files  streaming      (append)                        (append)                       (complete)
# MAGIC  in DL_AIR/Landing/                checkpoint                       checkpoint                      checkpoint
# MAGIC ```
# MAGIC
# MAGIC * **Bronze**: the source data after the **data rules** (relevant columns, data types, nulls in key columns,
# MAGIC   duplicates). Each load has its own dated folder, so it can be reprocessed.
# MAGIC * **Silver**: the result of the **business rules**: clean entities (airports) and facts (route reports, flows,
# MAGIC   monthly traffic) at full detail.
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

# ---- Source files: name of the Bronze dataset -> (file name prefix, number of files expected) -------
EUROSTAT_FILE_GROUPS = {
    "routes_annual": ("routes_annual_", 38),          # route reports of 35 countries, 2016-2024
    "routes_pt_monthly": ("routes_pt_monthly_", 4),   # monthly routes of the Portuguese airports, 2016-2024
    "airports_monthly": ("airports_monthly_", 6),     # monthly traffic of the European airports, 2016-2026
}

# ---- Scope of the analysis ----------------------------------------------------
HOME_COUNTRY = "PT"
HOME_HUB = "LIS"                    # origin of the breadth-first search
MAIN_AIRPORTS = ["LIS", "OPO", "FAO", "FNC", "PDL"]   # airports shown in the charts

FIRST_YEAR = 2016
PRE_PANDEMIC_YEAR = 2019
NETWORK_YEAR = 2024                 # latest year of the route statistics
LATEST_FULL_YEAR = 2025             # latest complete year of the airport statistics
FORECAST_END_YEAR = 2030

# ---- Business rules -----------------------------------------------------------
# Airports that closed during the period: the registry no longer lists their codes, but they reported traffic.
# (ICAO code, IATA code, name, latitude, longitude, continent, country, municipality), from the registry itself.
CLOSED_AIRPORTS = [
    ("EDDT", "TXL", "Berlin-Tegel Otto Lilienthal International Airport", 52.557675, 13.29185, "EU", "DE", "Berlin"),
    ("EGCN", "DSA", "Doncaster Sheffield Airport", 53.475287, -1.004267, "EU", "GB", "Doncaster"),
    ("EGPM", "SCS", "Scatsta Airport", 60.4328, -1.29611, "EU", "GB", "Shetland Islands"),
]
SHORT_HAUL_MAX_KM = 1500            # up to 1,500 km: short haul
MEDIUM_HAUL_MAX_KM = 4000           # up to 4,000 km: medium haul; above: long haul
MIRROR_TOLERANCE_PCT = 5.0          # two declarations of the same flow agree if they differ by less than this

# ---- Graph analytics --------------------------------------------------------
PAGERANK_DAMPING = 0.85
PAGERANK_MAX_ITER = 60
PAGERANK_TOLERANCE = 1e-5
BFS_MAX_HOPS = 20

# ---- Streaming simulation ---------------------------------------------------
STREAM_DROP_1_YEAR = 2025           # first drop: the months of 2025
STREAM_DROP_2_YEAR = 2026           # second drop: the months of 2026 published so far
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
    "COMMENT 'ABD group project: air connectivity of Portugal 2016-2026 (Silver and Gold tables)'"
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
# MAGIC Every traffic figure of the project is an official statistic of **Eurostat**, downloaded on 16 November 2026
# MAGIC with the Eurostat data API, filtered to the period from 2016 onwards, and uploaded unchanged.
# MAGIC
# MAGIC | Files | Eurostat dataset | Content | Period |
# MAGIC |---|---|---|---|
# MAGIC | `routes_annual_<country>.tsv` (38 files, 35 countries; Germany is split in 4 files) | `avia_par_<country>` | For each route between a main airport of the country and a partner airport: passengers carried, passengers on board, seats and flights, separately for **departures** and **arrivals** | Annual, 2016 to 2024 |
# MAGIC | `routes_pt_monthly_<measure>.tsv` (4 files) | `avia_par_pt` | The same measures (total of both directions) for the routes of the Portuguese airports | Monthly, January 2016 to December 2024 |
# MAGIC | `airports_monthly_<measure>_<coverage>.tsv` (6 files) | `avia_paoa` | Passengers carried and commercial flights of each main airport of Europe, in total and split in national and international transport | Monthly, January 2016 to 2026 |
# MAGIC | `airports.csv` | | Registry of the airports of the world with codes, names and coordinates (public snapshot, Kaggle). Used only to give a name and a location to the airport codes | |
# MAGIC
# MAGIC The files are uploaded with the Databricks interface to the folder `air_routes` of the Volume
# MAGIC `/Volumes/workspace/abd/abd` (Catalog > `workspace` > `abd` > `abd` > `air_routes` > *Upload to this volume*).
# MAGIC Databricks Free Edition restricts outbound internet access, so the files are uploaded instead of being downloaded
# MAGIC by the notebook.
# MAGIC
# MAGIC **Coverage, stated up front.**
# MAGIC * The **route** statistics end in **2024**: the figures of 2025 were not yet published. The **airport** statistics
# MAGIC   reach 2026 (April 2026 for the Portuguese airports).
# MAGIC * A route is in the data when at least one of its two airports is a main airport of a reporting country. Routes
# MAGIC   between two airports outside Europe are not covered, so the network is the **European network and its links to
# MAGIC   the rest of the world**.
# MAGIC * The United Kingdom reported until 2019 and Bosnia and Herzegovina only from 2021 to 2023. Czechia publishes no
# MAGIC   route dataset. Their airports still appear, as partners in the reports of the other countries.
# MAGIC * The statistics have no airline dimension.

# COMMAND ----------

# DBTITLE 1,Check the source files in the Volume
source_file_names = [f.name for f in dbutils.fs.ls(SOURCE_PATH)]

problems = []
if "airports.csv" not in source_file_names:
    problems.append("airports.csv is missing")
for dataset, (prefix, expected) in EUROSTAT_FILE_GROUPS.items():
    found = len([n for n in source_file_names if n.startswith(prefix) and n.endswith(".tsv")])
    print(f"{prefix + '*.tsv':<28}: {found:>3} files (expected {expected})")
    if found != expected:
        problems.append(f"{prefix}*.tsv: {found} files found, {expected} expected")

if problems:
    raise FileNotFoundError(
        f"Source files missing in {SOURCE_PATH}: {problems}. "
        "Upload the 49 files of the project data folder to this folder of the Volume and run the notebook again."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze layer: source data and data rules
# MAGIC
# MAGIC ### 5.1 Read the source data
# MAGIC Before defining any rule we look at the files as they arrive: structure, data types and content.
# MAGIC * `airports.csv` is **structured** data. Text fields are quoted and can contain commas and quotes, so the reader
# MAGIC   is told which character quotes and which one escapes.
# MAGIC * The Eurostat files are read as **text**, one row per line. Their layout is not relational (several dimensions
# MAGIC   packed in the first column, one column per period, values mixed with status flags), so they are treated as
# MAGIC   unstructured data until the Silver layer. All the files of a group are read **in one statement**, with a
# MAGIC   wildcard in the path, and the name of the file of each line is kept from the file metadata.

# COMMAND ----------

# DBTITLE 1,Structured data: the airports
def read_source_csv(name: str) -> DataFrame:
    """Read one source CSV file. Every column is read as text; quoted fields may contain commas and quotes."""
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{SOURCE_PATH}/{name}.csv")
    )


raw_airports = read_source_csv("airports")

raw_airports.printSchema()
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

# DBTITLE 1,Unstructured data: the route files of all countries, read as lines of text
def read_source_text(prefix: str) -> DataFrame:
    """Read all the Eurostat files of a group as lines of text, keeping the name of the file of each line."""
    return spark.read.text(f"{SOURCE_PATH}/{prefix}*.tsv").withColumn("_source_file", F.col("_metadata.file_name"))


raw_routes = read_source_text("routes_annual_")

print(f"Lines: {raw_routes.count():,} in {raw_routes.select('_source_file').distinct().count()} files")
raw_routes.select(
    "_source_file", F.substring("value", 1, 95).alias("first_95_characters_of_the_line")
).filter(F.col("_source_file") == "routes_annual_pt.tsv").limit(4).display()

# COMMAND ----------

# DBTITLE 1,The files do not all cover the same years: header line of three of them
raw_routes.filter(
    F.col("value").startswith("freq")
    & F.col("_source_file").isin("routes_annual_pt.tsv", "routes_annual_uk.tsv", "routes_annual_ba.tsv")
).select("_source_file", F.regexp_replace(F.col("value"), "\t", " | ").alias("header_line")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Observations.**
# MAGIC * `airports` is a registry of every landing place in the world, including heliports and closed airfields. Only
# MAGIC   about one row in ten has an ICAO code, the code that Eurostat uses. Many descriptive columns (links, keywords,
# MAGIC   local codes) are mostly empty and are not taken to the Bronze layer. The continent code of North America is
# MAGIC   the text `NA`: it must stay a value and not be read as "not available".
# MAGIC * In the Eurostat files the first column packs four dimensions (`frequency, unit, measure, route`), and the route
# MAGIC   code packs four more (`country_airport_partnercountry_partnerairport`). There is one column per period, and a
# MAGIC   missing value is written `:`.
# MAGIC * **Each file has its own header.** The United Kingdom file stops in 2019 and the Bosnian file covers 2021 to
# MAGIC   2023. The position of a value in a line therefore means a different year in different files, so each line
# MAGIC   must be interpreted with the header **of its own file**.
# MAGIC
# MAGIC ### 5.2 Apply the data rules and save the Bronze files
# MAGIC **This is where the "data" rules apply.**
# MAGIC
# MAGIC For the CSV file, driven by a specification: keep the **relevant columns**, convert the **data types**
# MAGIC (`try_cast`, so that an invalid value becomes null instead of stopping the pipeline), drop the records with a
# MAGIC **null** in a key column, and drop **duplicates**.
# MAGIC
# MAGIC For the text files: drop **empty lines** and **repeated lines**, and keep the **file name** of every line.
# MAGIC
# MAGIC The result is saved in **Parquet** format, efficient and adequate for read-only data, in a folder named after the
# MAGIC load date so that any load can be reprocessed with minimal effort.

# COMMAND ----------

# DBTITLE 1,Apply the data rules and write the Bronze files (Parquet)
AIRPORT_COLUMNS = {
    "ident": "string", "type": "string", "name": "string",
    "latitude_deg": "double", "longitude_deg": "double",
    "continent": "string", "iso_country": "string", "municipality": "string",
    "scheduled_service": "string", "icao_code": "string", "iata_code": "string",
}
AIRPORT_KEYS = ["ident"]

# One folder per load date
date_tag = datetime.datetime.now().strftime("%Y-%b-%d")
DL_Bronze_Path_day = DL_Bronze_Path + date_tag + "/"
print(DL_Bronze_Path_day)

bronze_log = []

# ---- airports.csv -----------------------------------------------------------------
raw = read_source_csv("airports")
raw_typed = raw.select(
    [F.col(c) if t == "string" else F.expr(f"try_cast(`{c}` AS {t})").alias(c) for c, t in AIRPORT_COLUMNS.items()]
)
raw_noNulls = raw_typed.dropna(subset=AIRPORT_KEYS)
raw_noDup = raw_noNulls.dropDuplicates()
raw_Final = raw_noDup.withColumn("_source_file", F.lit("airports.csv")).withColumn(
    "_ingested_at", F.current_timestamp()
)
raw_Final.write.format("parquet").mode("overwrite").save(DL_Bronze_Path_day + "airports")
bronze_log.append(("airports.csv", 1, raw.count(), raw_noNulls.count(), raw_noDup.count()))

# ---- Eurostat text files: one Bronze dataset per group of files -----------------------
for dataset, (prefix, expected) in EUROSTAT_FILE_GROUPS.items():
    raw = read_source_text(prefix)
    raw_noNulls = raw.filter(F.length(F.trim("value")) > 0)
    raw_noDup = raw_noNulls.dropDuplicates()
    raw_Final = raw_noDup.withColumn("_ingested_at", F.current_timestamp())

    raw_Final.write.format("parquet").mode("overwrite").save(DL_Bronze_Path_day + dataset)
    bronze_log.append((f"{prefix}*.tsv", expected, raw.count(), raw_noNulls.count(), raw_noDup.count()))

# Log with the statistics of the load, for control purposes
bronze_log_df = spark.createDataFrame(
    bronze_log, "source_files STRING, files INT, rows_read LONG, rows_after_null_rule LONG, rows_in_bronze LONG"
).withColumn("rows_removed", F.col("rows_read") - F.col("rows_in_bronze"))

bronze_log_df.display()

# COMMAND ----------

# DBTITLE 1,Check the Bronze files in the Volume
display(dbutils.fs.ls(DL_Bronze_Path_day))


def read_bronze(name: str) -> DataFrame:
    """Read one dataset of the Bronze layer (the load of the current run)."""
    return spark.read.parquet(DL_Bronze_Path_day + name)


read_bronze("routes_annual").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver layer: business rules, modelling and data quality
# MAGIC **This is where the "business" rules apply.** The Bronze files are read and conformed into the entities and facts
# MAGIC of the problem. Each one is saved as a Delta table with the prefix `SV_`.
# MAGIC
# MAGIC ### 6.1 Reference data: airports
# MAGIC Eurostat identifies airports by their four-letter ICAO code. Passengers know them by the three-letter IATA code.
# MAGIC Two business rules:
# MAGIC * every airport gets one `airport_code`: the IATA code when it exists, the ICAO code otherwise;
# MAGIC * the registry removes the codes of airports that have closed, but three airports that closed during the period
# MAGIC   (Berlin-Tegel in 2020, Doncaster Sheffield and Scatsta) reported traffic before closing. They are added back
# MAGIC   from a small reference list, so that their traffic is not lost.

# COMMAND ----------

# DBTITLE 1,SV_Airports
closed_airports_df = spark.createDataFrame(
    CLOSED_AIRPORTS,
    "icao_code STRING, iata_code STRING, airport_name STRING, latitude DOUBLE, longitude DOUBLE, "
    "continent STRING, country STRING, municipality STRING",
).withColumn("airport_type", F.lit("closed"))

sv_airports_df = (
    read_bronze("airports")
    .filter(F.col("icao_code").isNotNull())
    .select(
        "icao_code",
        "iata_code",
        F.col("name").alias("airport_name"),
        F.col("latitude_deg").alias("latitude"),
        F.col("longitude_deg").alias("longitude"),
        "continent",
        F.col("iso_country").alias("country"),
        "municipality",
        F.col("type").alias("airport_type"),
    )
    .unionByName(closed_airports_df)
    .withColumn("airport_code", F.coalesce("iata_code", "icao_code"))
)

save_table(sv_airports_df, "SV_Airports", "SILVER reference data - airports with an ICAO code, with names and coordinates")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 From wide text files to tidy observations
# MAGIC One function parses any group of Eurostat files with Spark only (`split` and `posexplode`, as in class ABD06):
# MAGIC 1. the header line **of each file** gives the list of its periods;
# MAGIC 2. `posexplode` turns the values of each line into rows, and they are joined to the periods by **file and
# MAGIC    position** (unpivot);
# MAGIC 3. each cell is separated into a numeric value and a status flag; cells without a value (`:`) are dropped.

# COMMAND ----------

# DBTITLE 1,A parser for the Eurostat files
def parse_eurostat(bronze_dataset: str) -> DataFrame:
    """Unpivot a Bronze dataset of Eurostat lines into one row per series and period: [file, series_key, period, value, flag]."""
    lines = read_bronze(bronze_dataset).select("_source_file", "value")
    is_header = F.col("value").startswith("freq")

    periods = lines.filter(is_header).select(
        "_source_file", F.posexplode(F.split("value", "\t")).alias("position", "period")
    ).select("_source_file", "position", F.trim("period").alias("period"))

    cells = lines.filter(~is_header).select(
        "_source_file",
        F.split("value", "\t").getItem(0).alias("series_key"),
        F.posexplode(F.split("value", "\t")).alias("position", "cell"),
    )

    return (
        cells.filter(F.col("position") > 0)
        .withColumn("cell", F.trim("cell"))
        .withColumn("value", F.expr("try_cast(nullif(regexp_extract(cell, '^([0-9]+([.][0-9]+)?)', 1), '') AS DOUBLE)"))
        .filter(F.col("value").isNotNull())
        .join(F.broadcast(periods), ["_source_file", "position"])
        .select(
            "_source_file",
            "series_key",
            "period",
            "value",
            F.expr("nullif(trim(regexp_replace(cell, '[0-9.:@]', '')), '')").alias("flag"),
        )
    )


def key_part(index: int):
    """The n-th dimension of the series key (the first column of the Eurostat files)."""
    return F.split("series_key", ",").getItem(index)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Route reports
# MAGIC One row per **reporting airport, partner airport, direction and year**, with the four measures side by side
# MAGIC (pivot):
# MAGIC
# MAGIC | Eurostat code | Meaning | Column in Silver |
# MAGIC |---|---|---|
# MAGIC | `PAS_CRD` | Passengers carried (each passenger counted once per flight number) | `passengers` |
# MAGIC | `PAS_BRD` | Passengers on board (counted on every flight stage) | `passengers_on_board` |
# MAGIC | `ST_PAS` | Passenger seats available | `seats` |
# MAGIC | `CAF_PAS` | Commercial passenger flights | `flights` |
# MAGIC
# MAGIC The suffix `_DEP` or `_ARR` of the measure is the direction, seen from the reporting airport.
# MAGIC
# MAGIC **Load factor rule.** The load factor is the share of the seats that were occupied (passengers on board divided
# MAGIC by seats). Some reports declare fewer seats than passengers on board, which is impossible: the seat count is
# MAGIC incomplete. Those seats are discarded, and the load factor is computed only from reports with valid seats.

# COMMAND ----------

# DBTITLE 1,SV_Route_Reports
measure_code = key_part(2)
route_code = key_part(3)

sv_route_reports_df = (
    parse_eurostat("routes_annual")
    .select(
        F.split(route_code, "_").getItem(0).alias("reporting_country"),
        F.split(route_code, "_").getItem(1).alias("reporting_icao"),
        F.split(route_code, "_").getItem(2).alias("partner_country"),
        F.split(route_code, "_").getItem(3).alias("partner_icao"),
        F.when(measure_code.endswith("_DEP"), "DEPARTURES").otherwise("ARRIVALS").alias("direction"),
        F.col("period").cast("int").alias("year"),
        F.when(measure_code.startswith("PAS_CRD"), "passengers")
        .when(measure_code.startswith("PAS_BRD"), "passengers_on_board")
        .when(measure_code.startswith("ST_PAS"), "seats")
        .when(measure_code.startswith("CAF_PAS"), "flights")
        .alias("measure"),
        "value",
    )
    .groupBy("reporting_country", "reporting_icao", "partner_country", "partner_icao", "direction", "year")
    .pivot("measure", ["passengers", "passengers_on_board", "seats", "flights"])
    .agg(F.sum("value"))
    # Business rule: a report with fewer seats than passengers on board has an incomplete seat count.
    # Its seats are discarded, and `seated_passengers` keeps the passengers of the reports with valid seats,
    # so that a load factor is always computed from passengers and seats of the same reports.
    .withColumn("seats_reported", F.col("seats"))
    .withColumn("seats", F.when((F.col("seats") > 0) & (F.col("passengers_on_board") <= F.col("seats")), F.col("seats")))
    .withColumn("seated_passengers", F.when(F.col("seats").isNotNull(), F.col("passengers_on_board")))
)

save_table(
    sv_route_reports_df,
    "SV_Route_Reports",
    "SILVER - Eurostat route reports: one row per reporting airport, partner airport, direction and year",
)

# COMMAND ----------

# DBTITLE 1,Which countries report in which years? (pivot)
reporting_years_df = (
    spark.table(table_name("SV_Route_Reports"))
    .groupBy("reporting_country")
    .agg(
        F.countDistinct("year").alias("years_reported"),
        F.min("year").alias("first_year"),
        F.max("year").alias("last_year"),
        F.countDistinct("reporting_icao").alias("reporting_airports"),
    )
)

# Countries that report every year: only their routes can be compared over time
all_years = spark.table(table_name("SV_Route_Reports")).select("year").distinct().count()
consistent_countries = sorted(
    row["reporting_country"]
    for row in reporting_years_df.filter(F.col("years_reported") == all_years).collect()
)
print(f"Years in the data                : {all_years}")
print(f"Countries reporting              : {reporting_years_df.count()}")
print(f"Countries reporting in every year: {len(consistent_countries)}")

display(reporting_years_df.filter(F.col("years_reported") < all_years).orderBy("reporting_country"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Flows: reconciling two declarations of the same route
# MAGIC A **flow** is the traffic from one airport to another in one year. It is the directed edge of the network.
# MAGIC
# MAGIC A flow between two reporting airports is declared **twice**: the origin airport declares it as a departure and
# MAGIC the destination airport declares it as an arrival. A flow with an airport outside the reporting countries is
# MAGIC declared once. Business rules:
# MAGIC
# MAGIC 1. Each declaration is turned into an (origin, destination) pair: a departure goes from the reporting airport to
# MAGIC    the partner, an arrival from the partner to the reporting airport.
# MAGIC 2. The two views are combined with a **full outer join**. When both exist, the declaration of the **origin
# MAGIC    airport** is kept, and the difference between the two is stored as a quality indicator.
# MAGIC 3. Airports are identified by their code alone: an airport reported under two country codes (the binational
# MAGIC    airport of Basel-Mulhouse) is one airport.
# MAGIC 4. Each flow is enriched with the names and locations of both airports (join with `SV_Airports`), the
# MAGIC    great-circle distance, and the **haul category**, computed by a **UDF**.
# MAGIC 5. A flow belongs to the **comparable panel** when it is declared by at least one country that reports in every
# MAGIC    year. Only those flows are used to compare years.

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

# DBTITLE 1,SV_Flows
reports = spark.table(table_name("SV_Route_Reports"))
MEASURES = ["passengers", "passengers_on_board", "seats", "seated_passengers", "flights"]

# View of the origin airport: its declared departures
declared_by_origin = (
    reports.filter(F.col("direction") == "DEPARTURES")
    .groupBy(F.col("reporting_icao").alias("origin_icao"), F.col("partner_icao").alias("destination_icao"), "year")
    .agg(
        *[F.sum(m).alias(f"origin_{m}") for m in MEASURES],
        F.max(F.col("reporting_country").isin(consistent_countries)).alias("origin_in_panel"),
    )
)
# View of the destination airport: its declared arrivals
declared_by_destination = (
    reports.filter(F.col("direction") == "ARRIVALS")
    .groupBy(F.col("partner_icao").alias("origin_icao"), F.col("reporting_icao").alias("destination_icao"), "year")
    .agg(
        *[F.sum(m).alias(f"destination_{m}") for m in MEASURES],
        F.max(F.col("reporting_country").isin(consistent_countries)).alias("destination_in_panel"),
    )
)

airports_ref = spark.table(table_name("SV_Airports"))
origin_airports = airports_ref.select(
    F.col("icao_code").alias("origin_icao"),
    F.col("airport_code").alias("origin"),
    F.col("airport_name").alias("origin_airport"),
    F.col("country").alias("origin_country"),
    F.col("continent").alias("origin_continent"),
    F.col("latitude").alias("origin_lat"),
    F.col("longitude").alias("origin_lon"),
)
destination_airports = airports_ref.select(
    F.col("icao_code").alias("destination_icao"),
    F.col("airport_code").alias("destination"),
    F.col("airport_name").alias("destination_airport"),
    F.col("country").alias("destination_country"),
    F.col("continent").alias("destination_continent"),
    F.col("latitude").alias("destination_lat"),
    F.col("longitude").alias("destination_lon"),
)

sv_flows_df = (
    declared_by_origin.join(declared_by_destination, ["origin_icao", "destination_icao", "year"], "full_outer")
    .withColumn(
        "declared_by",
        F.when(F.col("origin_passengers").isNotNull() & F.col("destination_passengers").isNotNull(), "both airports")
        .when(F.col("origin_passengers").isNotNull(), "origin airport only")
        .otherwise("destination airport only"),
    )
    # Rule 2: the declaration of the origin airport is preferred
    .select(
        "origin_icao", "destination_icao", "year", "declared_by",
        *[F.coalesce(f"origin_{m}", f"destination_{m}").alias(m) for m in MEASURES],
        F.round(
            100 * F.try_divide(F.abs(F.col("origin_passengers") - F.col("destination_passengers")),
                               F.col("origin_passengers")), 2
        ).alias("mirror_difference_pct"),
        (F.coalesce("origin_in_panel", F.lit(False)) | F.coalesce("destination_in_panel", F.lit(False)))
        .alias("in_comparable_panel"),
    )
    .join(F.broadcast(origin_airports), "origin_icao", "left")
    .join(F.broadcast(destination_airports), "destination_icao", "left")
    .withColumn(
        "distance_km",
        F.round(haversine_km(F.col("origin_lat"), F.col("origin_lon"),
                             F.col("destination_lat"), F.col("destination_lon")), 0).cast("int"),
    )
    .withColumn("haul", haul_category_udf("distance_km"))
    .withColumn("load_factor_pct", F.round(100 * F.try_divide(F.col("seated_passengers"), F.col("seats")), 1))
    .withColumn("is_domestic", F.col("origin_country") == F.col("destination_country"))
    .withColumn("is_intercontinental", F.col("origin_continent") != F.col("destination_continent"))
    # A flow is an edge of the network when both airports are known and different, and it carried passengers
    .withColumn(
        "is_network_edge",
        F.col("origin").isNotNull() & F.col("destination").isNotNull()
        & (F.col("origin_icao") != F.col("destination_icao")) & (F.coalesce("passengers", F.lit(0)) > 0),
    )
    .select(
        "year",
        F.coalesce("origin", "origin_icao").alias("origin"), "origin_icao", "origin_airport", "origin_country",
        "origin_continent", "origin_lat", "origin_lon",
        F.coalesce("destination", "destination_icao").alias("destination"), "destination_icao",
        "destination_airport", "destination_country", "destination_continent", "destination_lat", "destination_lon",
        "passengers", "passengers_on_board", "seats", "seated_passengers", "flights", "load_factor_pct",
        "distance_km", "haul", "is_domestic", "is_intercontinental",
        "declared_by", "mirror_difference_pct", "in_comparable_panel", "is_network_edge",
    )
)

save_table(
    sv_flows_df,
    "SV_Flows",
    "SILVER - directed traffic flow between two airports in one year, reconciled from the declarations of both airports",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Monthly routes of Portugal and monthly traffic of the airports
# MAGIC The same parser is applied to the two other groups of files. Only the meaning of the series key changes.

# COMMAND ----------

# DBTITLE 1,SV_Route_Month: monthly traffic of the routes of the Portuguese airports
airport_names = airports_ref.select("icao_code", "airport_code", "airport_name", "country", "continent")

sv_route_month_df = (
    parse_eurostat("routes_pt_monthly")
    .select(
        F.split(key_part(3), "_").getItem(1).alias("reporting_icao"),
        F.split(key_part(3), "_").getItem(3).alias("partner_icao"),
        F.substring("period", 1, 4).cast("int").alias("year"),
        F.substring("period", 6, 2).cast("int").alias("month"),
        F.when(key_part(2) == "PAS_CRD", "passengers")
        .when(key_part(2) == "PAS_BRD", "passengers_on_board")
        .when(key_part(2) == "ST_PAS", "seats")
        .when(key_part(2) == "CAF_PAS", "flights")
        .alias("measure"),
        "value",
    )
    .groupBy("reporting_icao", "partner_icao", "year", "month")
    .pivot("measure", ["passengers", "passengers_on_board", "seats", "flights"])
    .agg(F.sum("value"))
    .join(
        F.broadcast(airport_names.select(
            F.col("icao_code").alias("reporting_icao"),
            F.col("airport_code").alias("reporting_airport_code"),
            F.col("airport_name").alias("reporting_airport"),
        )),
        "reporting_icao", "left",
    )
    .join(
        F.broadcast(airport_names.select(
            F.col("icao_code").alias("partner_icao"),
            F.col("airport_code").alias("partner_airport_code"),
            F.col("airport_name").alias("partner_airport"),
            F.col("country").alias("partner_country"),
        )),
        "partner_icao", "left",
    )
)

save_table(
    sv_route_month_df,
    "SV_Route_Month",
    "SILVER - passengers, seats and flights of each route of a Portuguese airport per month (both directions)",
)

# COMMAND ----------

# DBTITLE 1,SV_Airport_Month: monthly traffic of the European airports, total, national and international
sv_airport_month_df = (
    parse_eurostat("airports_monthly")
    .select(
        F.split(key_part(3), "_").getItem(0).alias("country_eurostat"),
        F.split(key_part(3), "_").getItem(1).alias("icao_code"),
        key_part(5).alias("coverage"),                      # TOTAL, NAT (national) or INTL (international)
        F.substring("period", 1, 4).cast("int").alias("year"),
        F.substring("period", 6, 2).cast("int").alias("month"),
        F.when(key_part(2) == "PAS_CRD", "passengers").when(key_part(2) == "CAF_PAS", "flights").alias("measure"),
        "value",
    )
    .groupBy("country_eurostat", "icao_code", "coverage", "year", "month")
    .pivot("measure", ["passengers", "flights"])
    .agg(F.sum("value"))
    .join(F.broadcast(airport_names.select("icao_code", "airport_code", "airport_name")), "icao_code", "left")
    .withColumn("airport_code", F.coalesce("airport_code", "icao_code"))
    .withColumn("month_start", F.make_date("year", "month", F.lit(1)))
)

save_table(
    sv_airport_month_df,
    "SV_Airport_Month",
    "SILVER - passengers carried and flights of each main European airport per month, by coverage (TOTAL, NAT, INTL)",
)

# COMMAND ----------

# DBTITLE 1,Period covered by each Silver table
print(f"{'Table':<20}{'Rows':>10}   Period")
for name, expression in (
    ("SV_Route_Reports", "concat(min(year), ' to ', max(year))"),
    ("SV_Flows", "concat(min(year), ' to ', max(year))"),
    ("SV_Route_Month", "concat(min(year * 100 + month), ' to ', max(year * 100 + month))"),
    ("SV_Airport_Month", "concat(min(year * 100 + month), ' to ', max(year * 100 + month))"),
):
    period = spark.table(table_name(name)).agg(F.expr(expression)).first()[0]
    print(f"{name:<20}{row_count(name):>10,}   {period}")

latest_home_month = (
    spark.table(table_name("SV_Airport_Month"))
    .filter((F.col("country_eurostat") == HOME_COUNTRY) & (F.col("coverage") == "TOTAL"))
    .agg(F.max(F.struct("year", "month")).alias("latest"))
    .first()["latest"]
)
LATEST_YEAR_TO_DATE, LATEST_MONTH = latest_home_month["year"], latest_home_month["month"]
print(f"\nLatest month of the Portuguese airports: {LATEST_YEAR_TO_DATE}-{LATEST_MONTH:02d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 Data quality report
# MAGIC Each check counts the records that **violate** a rule. A check passes when the count is zero. Lines marked
# MAGIC *(info)* describe a known characteristic of the sources: they are reported, not expected to be zero.

# COMMAND ----------

# DBTITLE 1,Data quality checks
airports_t = spark.table(table_name("SV_Airports"))
reports_t = spark.table(table_name("SV_Route_Reports"))
flows_t = spark.table(table_name("SV_Flows"))
route_month_t = spark.table(table_name("SV_Route_Month"))
airport_month_t = spark.table(table_name("SV_Airport_Month"))
edges_all_years = flows_t.filter(F.col("is_network_edge"))

quality_checks = [
    ("Airports with a duplicated ICAO code",
     airports_t.groupBy("icao_code").count().filter("count > 1").count()),
    ("Airports with a duplicated airport code",
     airports_t.groupBy("airport_code").count().filter("count > 1").count()),
    ("Airports without coordinates or with impossible coordinates",
     airports_t.filter(
         F.col("latitude").isNull() | F.col("longitude").isNull()
         | ~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180)
     ).count()),
    ("Route reports duplicated (same airports, direction and year)",
     reports_t.groupBy("reporting_country", "reporting_icao", "partner_country", "partner_icao", "direction", "year")
     .count().filter("count > 1").count()),
    ("Route reports with a year outside 2016-2024",
     reports_t.filter(~F.col("year").between(FIRST_YEAR, NETWORK_YEAR)).count()),
    ("Route reports with a negative value",
     reports_t.filter((F.col("passengers") < 0) | (F.col("seats_reported") < 0) | (F.col("flights") < 0)).count()),
    ("Route reports whose reporting airport is not in the airport table",
     reports_t.join(airports_t, reports_t["reporting_icao"] == airports_t["icao_code"], "left_anti")
     .select("reporting_icao").distinct().count()),
    ("Flows duplicated (same origin, destination and year)",
     flows_t.groupBy("origin_icao", "destination_icao", "year").count().filter("count > 1").count()),
    ("Network edges from an airport to itself",
     edges_all_years.filter(F.col("origin") == F.col("destination")).count()),
    ("Network edges without distance",
     edges_all_years.filter(F.col("distance_km").isNull()).count()),
    ("Network edges with a load factor above 100%",
     edges_all_years.filter(F.col("load_factor_pct") > 100).count()),
    ("Passengers lost between the route reports and the flows (departures)",
     int(round(
         reports_t.filter("direction = 'DEPARTURES'").agg(F.sum("passengers")).first()[0]
         - flows_t.filter("declared_by <> 'destination airport only'").agg(F.sum("passengers")).first()[0]
     ))),
    ("Monthly route rows duplicated (same airports and month)",
     route_month_t.groupBy("reporting_icao", "partner_icao", "year", "month").count().filter("count > 1").count()),
    ("Monthly airport rows duplicated (same airport, coverage and month)",
     airport_month_t.groupBy("country_eurostat", "icao_code", "coverage", "year", "month")
     .count().filter("count > 1").count()),
    ("Monthly airport rows with an invalid month",
     airport_month_t.filter(~F.col("month").between(1, 12)).count()),
    ("Source rows removed by the Bronze data rules",
     bronze_log_df.agg(F.sum("rows_removed")).first()[0]),
]

print(f"{'Data quality check':<74}{'Violations':>11}  Result")
print("-" * 94)
for description, violations in quality_checks:
    print(f"{description:<74}{violations:>11,}  {'PASS' if violations == 0 else 'REVIEW'}")
n_checks_failed = sum(1 for _, violations in quality_checks if violations != 0)
print("-" * 94)
print(f"{len(quality_checks) - n_checks_failed} of {len(quality_checks)} checks passed")

# COMMAND ----------

# DBTITLE 1,(info) How well do the two declarations of the same flow agree?
# MAGIC %sql
# MAGIC SELECT declared_by,
# MAGIC        COUNT(*)                                                         AS flows,
# MAGIC        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)               AS pct_of_flows,
# MAGIC        ROUND(percentile_approx(mirror_difference_pct, 0.5), 2)          AS median_difference_pct,
# MAGIC        ROUND(percentile_approx(mirror_difference_pct, 0.9), 2)          AS p90_difference_pct,
# MAGIC        ROUND(100.0 * AVG(CASE WHEN mirror_difference_pct < 5 THEN 1.0
# MAGIC                               WHEN mirror_difference_pct IS NOT NULL THEN 0.0 END), 1) AS pct_agreeing_within_5pct
# MAGIC FROM workspace.DL_AIR.SV_Flows
# MAGIC WHERE passengers >= 1000
# MAGIC GROUP BY declared_by
# MAGIC ORDER BY flows DESC

# COMMAND ----------

# DBTITLE 1,(info) Reports whose seat count was discarded, per year
display(
    spark.table(table_name("SV_Route_Reports"))
    .groupBy("year")
    .agg(
        F.count("*").alias("reports"),
        F.sum(F.when(F.col("seats_reported").isNotNull() & F.col("seats").isNull(), 1).otherwise(0))
        .alias("seats_discarded"),
    )
    .withColumn("pct_discarded", F.round(100 * F.col("seats_discarded") / F.col("reports"), 2))
    .orderBy("year")
)

# COMMAND ----------

# DBTITLE 1,(info) Passengers on flows that are not network edges, per year
display(
    spark.table(table_name("SV_Flows"))
    .groupBy("year")
    .agg(
        F.round(F.sum("passengers") / 1e6, 1).alias("passengers_million"),
        F.round(100 * F.sum(F.when(~F.col("is_network_edge"), F.col("passengers")).otherwise(0)) / F.sum("passengers"), 2)
        .alias("pct_outside_the_network"),
    )
    .orderBy("year")
)

# COMMAND ----------

# DBTITLE 1,(info) Partner codes that are not airports, and their weight
unknown_summary = (
    spark.table(table_name("SV_Flows"))
    .filter(F.col("year") == NETWORK_YEAR)
    .agg(
        F.sum("passengers").alias("passengers"),
        F.sum(F.when(~F.col("is_network_edge"), F.col("passengers")).otherwise(0)).alias("outside_network"),
        F.countDistinct(F.when(F.col("origin_airport").isNull(), F.col("origin_icao"))).alias("unknown_origins"),
        F.countDistinct(F.when(F.col("destination_airport").isNull(), F.col("destination_icao")))
        .alias("unknown_destinations"),
    )
    .first()
)
pct_outside_network = 100 * unknown_summary["outside_network"] / unknown_summary["passengers"]
print(f"Codes of {NETWORK_YEAR} that are not in the airport table: "
      f"{max(unknown_summary['unknown_origins'], unknown_summary['unknown_destinations'])}"
      " (Eurostat uses some codes for 'unknown airport' of a country)")
print(f"Passengers of {NETWORK_YEAR} on flows that are not network edges: {pct_outside_network:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold layer: the European route network, 2016-2024
# MAGIC Gold tables provide summarised data for a specific use. Each one is saved as a Delta table with the prefix `GD_`
# MAGIC and explored with standard SQL (`%sql` cells), the natural option of business users.
# MAGIC
# MAGIC **Q1: how is the European route network structured, how did it change, and which airports are its hubs?**
# MAGIC
# MAGIC In this section a **route** is a directed network edge: an origin airport, a destination airport and the traffic
# MAGIC between them in one year.

# COMMAND ----------

# DBTITLE 1,GD_Network_Year: size of the network in each year (comparable panel)
gd_network_year_df = spark.sql(
    f"""
    WITH edges AS (
        SELECT * FROM {table_name('SV_Flows')} WHERE is_network_edge AND in_comparable_panel
    ),
    airports AS (
        SELECT year, origin AS airport FROM edges
        UNION
        SELECT year, destination AS airport FROM edges
    ),
    airport_counts AS (
        SELECT year, COUNT(*) AS airports FROM airports GROUP BY year
    )
    SELECT e.year,
           MAX(a.airports)                                              AS airports,
           COUNT(*)                                                     AS routes,
           ROUND(SUM(e.passengers) / 1e6, 1)                            AS passengers_million,
           ROUND(SUM(e.flights) / 1e6, 2)                               AS flights_million,
           ROUND(100.0 * SUM(e.seated_passengers) / SUM(e.seats), 1)    AS load_factor_pct,
           ROUND(SUM(e.passengers_on_board) / SUM(e.flights), 0)        AS passengers_per_flight,
           ROUND(100.0 * SUM(CASE WHEN e.is_intercontinental THEN e.passengers END) / SUM(e.passengers), 1)
               AS pct_passengers_intercontinental
    FROM edges e
    JOIN airport_counts a ON e.year = a.year
    GROUP BY e.year
    """
)

save_table(
    gd_network_year_df,
    "GD_Network_Year",
    "GOLD - airports, routes, passengers and load factor of the European network per year (comparable panel)",
)

# COMMAND ----------

# DBTITLE 1,The network year by year, with the change against 2019
# MAGIC %sql
# MAGIC SELECT n.year,
# MAGIC        n.airports,
# MAGIC        n.routes,
# MAGIC        ROUND(100.0 * (n.routes / b.routes - 1), 1)                           AS routes_vs_2019_pct,
# MAGIC        n.passengers_million,
# MAGIC        ROUND(100.0 * (n.passengers_million / b.passengers_million - 1), 1)   AS passengers_vs_2019_pct,
# MAGIC        n.flights_million,
# MAGIC        n.load_factor_pct,
# MAGIC        n.passengers_per_flight,
# MAGIC        n.pct_passengers_intercontinental
# MAGIC FROM workspace.DL_AIR.GD_Network_Year n
# MAGIC CROSS JOIN (SELECT * FROM workspace.DL_AIR.GD_Network_Year WHERE year = 2019) b
# MAGIC ORDER BY n.year

# COMMAND ----------

# DBTITLE 1,Chart: routes and passengers of the network per year
network_year_pd = spark.table(table_name("GD_Network_Year")).orderBy("year").toPandas()

fig, ax_routes = plt.subplots(figsize=(11, 4.5))
ax_routes.bar(network_year_pd["year"], network_year_pd["routes"].astype(float), color="lightsteelblue", label="Routes")
ax_routes.set_ylabel("Routes (directed)")
ax_routes.set_xlabel("Year")
ax_routes.set_xticks(network_year_pd["year"])
ax_passengers = ax_routes.twinx()
ax_passengers.plot(network_year_pd["year"], network_year_pd["passengers_million"].astype(float), color="darkblue",
                   marker="o", linewidth=2, label="Passengers")
ax_passengers.set_ylabel("Million passengers")
ax_passengers.set_ylim(0, None)
ax_routes.set_title("The European route network: routes and passengers per year (countries reporting every year)")
handles = ax_routes.get_legend_handles_labels()[0] + ax_passengers.get_legend_handles_labels()[0]
ax_routes.legend(handles, ["Routes", "Passengers"], loc="lower left")
ax_routes.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 The airports of the network in the latest year

# COMMAND ----------

# DBTITLE 1,GD_Airport_Connectivity: one row per airport with its connectivity indicators
gd_airport_connectivity_df = spark.sql(
    f"""
    WITH outbound AS (
        SELECT origin                                    AS airport_code,
               COUNT(*)                                  AS destinations,
               COUNT(DISTINCT destination_country)       AS countries_served,
               SUM(passengers)                           AS departing_passengers,
               ROUND(100.0 * SUM(seated_passengers) / SUM(seats), 1) AS load_factor_pct,
               COUNT(CASE WHEN is_intercontinental THEN 1 END)       AS intercontinental_destinations,
               ROUND(SUM(passengers * distance_km) / SUM(passengers), 0) AS avg_trip_km,
               MAX(distance_km)                          AS longest_route_km,
               MAX(CASE WHEN declared_by <> 'destination airport only' THEN 1 ELSE 0 END) = 1 AS is_reporting_airport
        FROM {table_name('SV_Flows')}
        WHERE is_network_edge AND year = {NETWORK_YEAR}
        GROUP BY origin
    )
    SELECT a.airport_code,
           a.airport_name,
           a.municipality,
           a.country,
           a.continent,
           a.latitude,
           a.longitude,
           o.is_reporting_airport,
           o.destinations,
           o.countries_served,
           o.intercontinental_destinations,
           o.departing_passengers,
           o.load_factor_pct,
           o.avg_trip_km,
           o.longest_route_km,
           CASE WHEN o.is_reporting_airport THEN
               RANK() OVER (PARTITION BY o.is_reporting_airport ORDER BY o.destinations DESC) END         AS rank_by_destinations,
           CASE WHEN o.is_reporting_airport THEN
               RANK() OVER (PARTITION BY o.is_reporting_airport ORDER BY o.departing_passengers DESC) END AS rank_by_passengers
    FROM outbound o
    JOIN {table_name('SV_Airports')} a ON o.airport_code = a.airport_code
    """
)

save_table(
    gd_airport_connectivity_df,
    "GD_Airport_Connectivity",
    f"GOLD - connectivity indicators of each airport in {NETWORK_YEAR}: destinations, countries, passengers, ranks",
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Reading the rankings.** An airport of a reporting country has all its main routes in the data. An airport
# MAGIC > outside the reporting countries (London, Dubai, New York) appears only through its routes **to** the reporting
# MAGIC > countries. The rankings below are therefore restricted to the reporting airports.

# COMMAND ----------

# DBTITLE 1,The 15 best connected reporting airports
# MAGIC %sql
# MAGIC SELECT rank_by_destinations, airport_code, airport_name, country, destinations, countries_served,
# MAGIC        intercontinental_destinations, ROUND(departing_passengers / 1e6, 1) AS departing_passengers_million,
# MAGIC        rank_by_passengers, load_factor_pct
# MAGIC FROM workspace.DL_AIR.GD_Airport_Connectivity
# MAGIC WHERE is_reporting_airport
# MAGIC ORDER BY rank_by_destinations, airport_code
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,The Portuguese airports in the network
# MAGIC %sql
# MAGIC SELECT airport_code, airport_name, is_reporting_airport, rank_by_destinations, rank_by_passengers,
# MAGIC        destinations, countries_served, intercontinental_destinations,
# MAGIC        ROUND(departing_passengers / 1e6, 2) AS departing_passengers_million,
# MAGIC        load_factor_pct, avg_trip_km, longest_route_km
# MAGIC FROM workspace.DL_AIR.GD_Airport_Connectivity
# MAGIC WHERE country = 'PT'
# MAGIC ORDER BY departing_passengers DESC

# COMMAND ----------

# DBTITLE 1,Routes and passengers by haul category
# MAGIC %sql
# MAGIC SELECT haul,
# MAGIC        COUNT(*)                                                          AS routes,
# MAGIC        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)                AS pct_of_routes,
# MAGIC        ROUND(SUM(passengers) / 1e6, 1)                                   AS passengers_million,
# MAGIC        ROUND(100.0 * SUM(passengers) / SUM(SUM(passengers)) OVER (), 1)  AS pct_of_passengers,
# MAGIC        ROUND(SUM(passengers) / COUNT(*) / 1e3, 0)                        AS passengers_per_route_thousand,
# MAGIC        ROUND(100.0 * SUM(seated_passengers) / SUM(seats), 1)             AS load_factor_pct,
# MAGIC        ROUND(SUM(passengers_on_board) / SUM(flights), 0)                 AS passengers_per_flight
# MAGIC FROM workspace.DL_AIR.SV_Flows
# MAGIC WHERE is_network_edge AND year = 2024
# MAGIC GROUP BY haul
# MAGIC ORDER BY haul

# COMMAND ----------

# DBTITLE 1,Chart: the airports of the network, sized by departing passengers
connectivity_pd = (
    spark.table(table_name("GD_Airport_Connectivity"))
    .select("airport_code", "country", "latitude", "longitude", "destinations", "departing_passengers")
    .orderBy("departing_passengers")
    .toPandas()
)
home_pd = connectivity_pd[connectivity_pd["country"] == HOME_COUNTRY]

fig, ax = plt.subplots(figsize=(13, 6.2))
ax.scatter(connectivity_pd["longitude"], connectivity_pd["latitude"],
           s=connectivity_pd["departing_passengers"].astype(float) / 1.2e5 + 2,
           c=connectivity_pd["destinations"].astype(float), cmap="viridis", alpha=0.6, linewidths=0)
ax.scatter(home_pd["longitude"], home_pd["latitude"], s=home_pd["departing_passengers"].astype(float) / 1.2e5 + 14,
           facecolors="none", edgecolors="red", linewidths=1.2, label="Portuguese airports")
ax.set_title(f"Airports of the network in {NETWORK_YEAR}: size = departing passengers, colour = destinations")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.set_xlim(-130, 150)
ax.set_ylim(-40, 75)
ax.grid(alpha=0.3)
ax.legend(loc="lower left")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Graph analytics: hubs, reachability and PageRank
# MAGIC
# MAGIC The route network of the latest year is a graph:
# MAGIC * **Vertices** are the airports.
# MAGIC * **Edges** are the directed routes between two airports, **weighted** by the passengers they carried.
# MAGIC
# MAGIC Vertices and edges are stored as two DataFrames with the `id` / `src` / `dst` convention of GraphFrames, used in
# MAGIC class ABD09. In class GraphFrames ran on Colab, because it needs a JVM package that serverless compute cannot
# MAGIC install. Here the same operations (degrees, motifs, subgraphs, breadth-first search and PageRank) are written
# MAGIC directly with **DataFrame joins and aggregations**, as in the PageRank example of class ABD06.

# COMMAND ----------

# DBTITLE 1,Build the graph: vertices and edges
graph_edges_df = (
    spark.table(table_name("SV_Flows"))
    .filter(F.col("is_network_edge") & (F.col("year") == NETWORK_YEAR))
    .select(
        F.col("origin").alias("src"),
        F.col("destination").alias("dst"),
        F.col("passengers").alias("weight"),
        "distance_km",
        "haul",
        "flights",
        "load_factor_pct",
    )
)

save_table(
    graph_edges_df,
    "GD_Graph_Edges",
    f"GOLD graph - directed route between two airports in {NETWORK_YEAR}, weight = passengers carried",
)
edges = spark.table(table_name("GD_Graph_Edges"))

graph_vertices_df = (
    edges.select(F.col("src").alias("id"))
    .union(edges.select(F.col("dst").alias("id")))
    .distinct()
    .join(
        spark.table(table_name("SV_Airports")).select(
            F.col("airport_code").alias("id"), "airport_name", "municipality", "country", "continent",
            "latitude", "longitude",
        ),
        "id",
    )
)

save_table(graph_vertices_df, "GD_Graph_Vertices", f"GOLD graph - one vertex per airport of the network of {NETWORK_YEAR}")
vertices = spark.table(table_name("GD_Graph_Vertices"))

n_vertices = vertices.count()
n_edges = edges.count()
print(f"\nVertices (airports)       : {n_vertices:,}")
print(f"Directed edges (routes)   : {n_edges:,}")
print(f"Density                   : {n_edges / (n_vertices * (n_vertices - 1)):.2%} of all possible routes")
print(f"Average out-degree        : {n_edges / n_vertices:.1f} destinations per airport")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Degrees: a network of a few hubs and many small airports

# COMMAND ----------

# DBTITLE 1,In-degree, out-degree and their distribution
out_degrees = edges.groupBy(F.col("src").alias("id")).agg(
    F.count("*").alias("out_degree"), F.sum("weight").alias("passengers_out")
)
in_degrees = edges.groupBy(F.col("dst").alias("id")).agg(
    F.count("*").alias("in_degree"), F.sum("weight").alias("passengers_in")
)

airport_degrees_df = (
    vertices.join(out_degrees, "id", "left")
    .join(in_degrees, "id", "left")
    .fillna(0, subset=["out_degree", "in_degree", "passengers_out", "passengers_in"])
)

degree_distribution_df = (
    airport_degrees_df.withColumn(
        "destinations_band",
        F.when(F.col("out_degree") <= 5, "1 - up to 5")
        .when(F.col("out_degree") <= 20, "2 - 6 to 20")
        .when(F.col("out_degree") <= 50, "3 - 21 to 50")
        .when(F.col("out_degree") <= 100, "4 - 51 to 100")
        .otherwise("5 - more than 100"),
    )
    .groupBy("destinations_band")
    .agg(F.count("*").alias("airports"), F.sum("out_degree").alias("routes"), F.sum("passengers_out").alias("passengers"))
    .withColumn("pct_of_airports", F.round(100 * F.col("airports") / F.sum("airports").over(Window.partitionBy()), 1))
    .withColumn("pct_of_routes", F.round(100 * F.col("routes") / F.sum("routes").over(Window.partitionBy()), 1))
    .withColumn("pct_of_passengers",
                F.round(100 * F.col("passengers") / F.sum("passengers").over(Window.partitionBy()), 1))
    .select("destinations_band", "airports", "pct_of_airports", "routes", "pct_of_routes", "pct_of_passengers")
    .orderBy("destinations_band")
)

display(degree_distribution_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Motif: routes that exist in one direction only
# MAGIC The motif `(a)-[e1]->(b); (b)-[e2]->(a)` of GraphFrames finds the pairs of airports connected in both directions.
# MAGIC It is a join of the edge table with itself. The edges **without** a return edge (a left anti join) are the
# MAGIC one-way routes. For the pairs connected in both directions, the motif also tells whether the traffic is balanced.

# COMMAND ----------

# DBTITLE 1,Reciprocal and one-way routes, and the balance of the two directions
return_edges = edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"), F.col("weight").alias("return_weight"))

reciprocal_df = edges.join(return_edges, ["src", "dst"])
n_reciprocal = reciprocal_df.count()
n_one_way = edges.join(return_edges, ["src", "dst"], "left_anti").count()
one_way_passengers = (
    edges.join(return_edges, ["src", "dst"], "left_anti").agg(F.coalesce(F.sum("weight"), F.lit(0.0))).first()[0]
)
total_passengers_network = edges.agg(F.sum("weight")).first()[0]

print(f"Routes with a return route : {n_reciprocal:,} ({n_reciprocal / n_edges:.1%})")
print(f"One-way routes             : {n_one_way:,} ({n_one_way / n_edges:.1%} of the routes,"
      f" {one_way_passengers / total_passengers_network:.2%} of the passengers)")

display(
    reciprocal_df.filter(F.col("weight") >= 10000)
    .withColumn("imbalance_pct", 100 * F.abs(F.col("weight") - F.col("return_weight")) / F.col("weight"))
    .agg(
        F.count("*").alias("routes_with_at_least_10000_passengers"),
        F.round(F.percentile_approx("imbalance_pct", 0.5), 2).alias("median_imbalance_pct"),
        F.round(F.percentile_approx("imbalance_pct", 0.9), 2).alias("p90_imbalance_pct"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Subgraph: the domestic network of Portugal
# MAGIC A subgraph is obtained by filtering the edges and dropping the vertices left without any edge (`filterEdges` and
# MAGIC `dropIsolatedVertices` in GraphFrames). Keeping only the routes between two Portuguese airports shows the
# MAGIC network that links the mainland, Madeira and the Azores.

# COMMAND ----------

# DBTITLE 1,Subgraph of the routes between Portuguese airports
home_ids = vertices.filter(F.col("country") == HOME_COUNTRY).select("id")

domestic_edges_df = edges.join(home_ids.withColumnRenamed("id", "src"), "src").join(
    home_ids.withColumnRenamed("id", "dst"), "dst"
)
n_domestic_edges = domestic_edges_df.count()
n_domestic_vertices = (
    domestic_edges_df.select("src").union(domestic_edges_df.select("dst")).distinct().count()
)
domestic_passengers = domestic_edges_df.agg(F.sum("weight")).first()[0]

print(f"Full graph      : {n_vertices:>4,} airports, {n_edges:>6,} routes")
print(f"Domestic graph  : {n_domestic_vertices:>4,} airports, {n_domestic_edges:>6,} routes,"
      f" {domestic_passengers / 1e6:.1f} million passengers")

display(
    domestic_edges_df.groupBy("src")
    .agg(F.count("*").alias("domestic_destinations"), F.round(F.sum("weight") / 1e6, 2).alias("passengers_million"))
    .withColumn("pct_of_domestic_passengers",
                F.round(100 * F.col("passengers_million") * 1e6 / F.lit(domestic_passengers), 1))
    .join(vertices.select(F.col("id").alias("src"), "airport_name"), "src")
    .select("src", "airport_name", "domestic_destinations", "passengers_million", "pct_of_domestic_passengers")
    .orderBy(F.desc("passengers_million"), "src")
)

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
        print(f"  flight {hop}: {len(new_vertices):>4,} new airports reached")

    return spark.createDataFrame(
        [(v, flights, previous) for v, (flights, previous) in visited.items()],
        "id STRING, flights INT, previous_id STRING",
    )


bfs_df = breadth_first_search(edges, HOME_HUB, BFS_MAX_HOPS)

flights_from_hub_df = vertices.select("id", "airport_name", "country", "continent").join(bfs_df, "id", "left")

save_table(
    flights_from_hub_df,
    "GD_Flights_From_Lisbon",
    f"GOLD graph - minimum number of flights from {HOME_HUB} to each airport in {NETWORK_YEAR} (NULL = not reachable)",
)

# COMMAND ----------

# DBTITLE 1,Airports of the network by number of flights from Lisbon
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
# MAGIC network: an airport is important if it receives passengers **from airports that are themselves important**.
# MAGIC
# MAGIC `rank(v) = (1 - d) / N  +  d * ( SUM over edges u->v of rank(u) * weight(u,v) / weight_out(u)  +  dangling / N )`
# MAGIC
# MAGIC where `d` is the damping factor, `N` the number of airports, `weight` the passengers of the route and `dangling`
# MAGIC the total rank of airports without departures (redistributed uniformly so that the ranks sum to 1). It can be
# MAGIC read as the share of time that a traveller who keeps boarding flights in proportion to their passengers would
# MAGIC spend at each airport.
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

    # Transition probability of each edge: share of the passengers leaving `src` that go to `dst`
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

# DBTITLE 1,Save the ranking
airport_pagerank_df = (
    airport_degrees_df.join(pagerank_df, "id")
    .withColumn("pagerank", F.round("pagerank", 6))
    .withColumn("pagerank_position", F.rank().over(Window.orderBy(F.desc("pagerank"))))
    .withColumn("degree_position", F.rank().over(Window.orderBy(F.desc("out_degree"))))
    .withColumn("passengers_position", F.rank().over(Window.orderBy(F.desc("passengers_out"))))
    .select(
        "id", "airport_name", "country", "continent", "latitude", "longitude",
        "pagerank", "pagerank_position", "out_degree", "in_degree", "degree_position",
        "passengers_out", "passengers_position",
    )
)

save_table(
    airport_pagerank_df,
    "GD_Airport_PageRank",
    "GOLD graph - PageRank, degrees and passengers of each airport, with its position in each ranking",
)

# COMMAND ----------

# DBTITLE 1,Top 15 airports by PageRank
# MAGIC %sql
# MAGIC SELECT pagerank_position, passengers_position, degree_position, id AS airport_code, airport_name, country,
# MAGIC        pagerank, out_degree, ROUND(passengers_out / 1e6, 1) AS passengers_out_million
# MAGIC FROM workspace.DL_AIR.GD_Airport_PageRank
# MAGIC ORDER BY pagerank_position, id
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,The Portuguese airports in the PageRank ranking
# MAGIC %sql
# MAGIC SELECT pagerank_position, passengers_position, degree_position, id AS airport_code, airport_name,
# MAGIC        pagerank, out_degree, ROUND(passengers_out / 1e6, 2) AS passengers_out_million
# MAGIC FROM workspace.DL_AIR.GD_Airport_PageRank
# MAGIC WHERE country = 'PT'
# MAGIC ORDER BY pagerank_position, id
# MAGIC LIMIT 6

# COMMAND ----------

# DBTITLE 1,Chart: top 15 airports by PageRank
top_pagerank_pd = (
    spark.table(table_name("GD_Airport_PageRank"))
    .orderBy("pagerank_position", "id")
    .limit(15)
    .toPandas()
    .iloc[::-1]
)
labels = [f"{code} ({country})" for code, country in zip(top_pagerank_pd["id"], top_pagerank_pd["country"])]

fig, ax = plt.subplots(figsize=(11, 5.5))
ax.barh(labels, top_pagerank_pd["pagerank"].astype(float), color="steelblue")
ax.set_title(f"Top 15 airports of the network by PageRank, {NETWORK_YEAR}")
ax.set_xlabel("PageRank")
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Portugal's connectivity: every itinerary with up to two stops
# MAGIC
# MAGIC **Q2: how much of the network can be reached from each Portuguese airport with 0, 1 and 2 stops, and through which hubs?**
# MAGIC
# MAGIC An itinerary with stops is a **join of the edge table with itself**: a one-stop itinerary joins two edges on the
# MAGIC intermediate airport, a two-stop itinerary joins three. The number of itineraries grows very fast with each join,
# MAGIC which is why this is done with Spark:
# MAGIC
# MAGIC | Itineraries | How they are obtained | Scope computed here |
# MAGIC |---|---|---|
# MAGIC | Direct | The edges | From the Portuguese airports |
# MAGIC | One stop | Edges joined with edges | **Whole network** (section 9.1) and from the Portuguese airports |
# MAGIC | Two stops | Edges joined with edges joined with edges | From the Portuguese airports |
# MAGIC
# MAGIC An itinerary never visits the same airport twice. Its length is the sum of the distances of its flights, and its
# MAGIC **detour factor** is that length divided by the great-circle distance between origin and destination
# MAGIC (1.00 = no detour).
# MAGIC
# MAGIC > **What an itinerary is here.** A sequence of routes that operated in the same year. Because the data covers
# MAGIC > the routes with at least one airport in a reporting country, the transfers of the itineraries are at European
# MAGIC > airports or at airports directly linked to Europe. Schedules and connection times are not in the data.
# MAGIC
# MAGIC ### 9.1 The transfer hubs of the network, on all pairs of airports
# MAGIC For every pair of airports **without** a direct route, we find the best one-stop itinerary (the shortest one) and
# MAGIC count how often each airport is the transfer point. Every pair of the network is used, with no sampling.

# COMMAND ----------

# DBTITLE 1,All one-stop itineraries of the network and the best one for each pair
first_leg = edges.select(
    F.col("src").alias("origin"), F.col("dst").alias("stop_1"), F.col("distance_km").alias("km_1")
)
second_leg = edges.select(
    F.col("src").alias("stop_1"), F.col("dst").alias("destination"), F.col("distance_km").alias("km_2")
)

network_one_stop = first_leg.join(second_leg, "stop_1").filter(F.col("origin") != F.col("destination"))
n_network_one_stop = network_one_stop.count()

direct_pairs = edges.select(F.col("src").alias("origin"), F.col("dst").alias("destination"))
best_per_pair = Window.partitionBy("origin", "destination").orderBy(F.col("total_km"), F.col("stop_1"))

network_best_one_stop = (
    network_one_stop.join(direct_pairs, ["origin", "destination"], "left_anti")   # only pairs with no direct route
    .withColumn("total_km", F.col("km_1") + F.col("km_2"))
    .withColumn("option_rank", F.row_number().over(best_per_pair))
    .filter(F.col("option_rank") == 1)
)

gd_transfer_hubs_df = (
    network_best_one_stop.groupBy(F.col("stop_1").alias("id"))
    .agg(F.count("*").alias("pairs_best_connected_via_hub"))
    .withColumn("pct_of_pairs",
                F.round(100 * F.col("pairs_best_connected_via_hub")
                        / F.sum("pairs_best_connected_via_hub").over(Window.partitionBy()), 2))
    .withColumn("hub_rank", F.rank().over(Window.orderBy(F.desc("pairs_best_connected_via_hub"))))
    .join(vertices.select("id", "airport_name", "country", "continent"), "id")
)

save_table(
    gd_transfer_hubs_df,
    "GD_Transfer_Hubs",
    "GOLD - for every pair of airports without a direct route: how often each airport is the best one-stop transfer",
)

n_pairs_one_stop = spark.table(table_name("GD_Transfer_Hubs")).agg(F.sum("pairs_best_connected_via_hub")).first()[0]
print(f"\nOne-stop itineraries in the network                      : {n_network_one_stop:,}")
print(f"Pairs of airports connected with one stop but not direct : {n_pairs_one_stop:,}")

# COMMAND ----------

# DBTITLE 1,The 15 transfer hubs of the network
# MAGIC %sql
# MAGIC SELECT hub_rank, id AS airport_code, airport_name, country, pairs_best_connected_via_hub, pct_of_pairs,
# MAGIC        ROUND(SUM(pct_of_pairs) OVER (ORDER BY hub_rank, id), 1) AS cumulative_pct
# MAGIC FROM workspace.DL_AIR.GD_Transfer_Hubs
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
    f"GOLD - best itinerary (fewest stops, then shortest) from each Portuguese airport to each reachable airport, {NETWORK_YEAR}",
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
    .withColumn("pct_of_network_within_one_stop", F.round(100 * F.col("within_one_stop") / F.lit(n_vertices - 1), 1))
    .withColumn("pct_of_network_within_two_stops", F.round(100 * F.col("within_two_stops") / F.lit(n_vertices - 1), 1))
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
ax.set_title(f"Airports reachable from the main Portuguese airports, by number of stops ({NETWORK_YEAR})")
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
# MAGIC ### 9.4 Reach year by year: what the pandemic did to connectivity
# MAGIC Because the network is dated, the same joins can be run for **every year**. For each Portuguese airport and
# MAGIC year we count the direct destinations and the airports reachable within one stop, using only the routes that
# MAGIC operated in that year.
# MAGIC
# MAGIC > **Comparable panel.** Only the routes declared by countries that report in every year are used, so that the
# MAGIC > years can be compared. In the earlier years a few partners were declared with codes that are not airports
# MAGIC > (section 10.5), so the counts of those years are slightly understated.

# COMMAND ----------

# DBTITLE 1,GD_Portugal_Reach_Year: direct destinations and one-stop reach per year
gd_reach_year_df = spark.sql(
    f"""
    WITH edges AS (
        SELECT year, origin, destination
        FROM {table_name('SV_Flows')}
        WHERE is_network_edge AND in_comparable_panel
    ),
    home_edges AS (
        SELECT * FROM edges WHERE origin IN (SELECT id FROM {table_name('GD_Graph_Vertices')} WHERE country = '{HOME_COUNTRY}')
    ),
    reachable AS (
        SELECT year, origin, destination, 0 AS stops FROM home_edges
        UNION ALL
        SELECT h.year, h.origin, e.destination, 1 AS stops
        FROM home_edges h
        JOIN edges e ON h.year = e.year AND h.destination = e.origin
        WHERE e.destination <> h.origin
    )
    SELECT year,
           origin,
           COUNT(DISTINCT CASE WHEN stops = 0 THEN destination END) AS direct_destinations,
           COUNT(DISTINCT destination)                              AS within_one_stop
    FROM reachable
    GROUP BY year, origin
    """
)

save_table(
    gd_reach_year_df,
    "GD_Portugal_Reach_Year",
    "GOLD - direct destinations and airports within one stop of each Portuguese airport, per year (comparable panel)",
)

# COMMAND ----------

# DBTITLE 1,Direct destinations of the main airports per year (pivot)
display(
    spark.table(table_name("GD_Portugal_Reach_Year"))
    .filter(F.col("origin").isin(MAIN_AIRPORTS))
    .groupBy("year")
    .pivot("origin", MAIN_AIRPORTS)
    .agg(F.first("direct_destinations"))
    .orderBy("year")
)

# COMMAND ----------

# DBTITLE 1,Airports within one stop of the main airports per year (pivot)
reach_year_pivot_df = (
    spark.table(table_name("GD_Portugal_Reach_Year"))
    .filter(F.col("origin").isin(MAIN_AIRPORTS))
    .groupBy("year")
    .pivot("origin", MAIN_AIRPORTS)
    .agg(F.first("within_one_stop"))
    .orderBy("year")
)

display(reach_year_pivot_df)

# COMMAND ----------

# DBTITLE 1,Chart: direct destinations and one-stop reach per year
reach_year_pd = (
    spark.table(table_name("GD_Portugal_Reach_Year")).filter(F.col("origin").isin(MAIN_AIRPORTS)).orderBy("year").toPandas()
)

fig, (ax_direct, ax_one_stop) = plt.subplots(1, 2, figsize=(13, 4.5))
for airport in MAIN_AIRPORTS:
    series = reach_year_pd[reach_year_pd["origin"] == airport]
    ax_direct.plot(series["year"], series["direct_destinations"].astype(float), marker="o", label=airport)
    ax_one_stop.plot(series["year"], series["within_one_stop"].astype(float), marker="o", label=airport)
ax_direct.set_title("Direct destinations")
ax_one_stop.set_title("Airports within one stop")
for ax in (ax_direct, ax_one_stop):
    ax.set_xlabel("Year")
    ax.set_ylim(0, None)
    ax.grid(alpha=0.3)
ax_direct.set_ylabel("Airports")
ax_direct.legend(ncol=3, loc="upper left")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Demand: the passengers of the Portuguese airports, 2016-2026
# MAGIC
# MAGIC **Q3: where are the passengers? Which airports and routes carry the traffic, how full are the planes, how seasonal is demand?**
# MAGIC
# MAGIC The route network says where one *can* fly. This section measures where people *do* fly, with two sources:
# MAGIC the monthly traffic of each airport, which reaches **2026**, and the route statistics, which reach **2024**.
# MAGIC
# MAGIC > **Counting the passengers of a country.** A passenger of an international flight is counted once in the
# MAGIC > country, at the Portuguese airport. A passenger of a domestic flight is counted twice, at the departure
# MAGIC > airport and at the arrival airport. The passengers of a country are therefore computed as
# MAGIC > **international + national / 2**. The totals of a single airport need no correction.
# MAGIC
# MAGIC ### 10.1 Gold tables

# COMMAND ----------

# DBTITLE 1,GD_Airport_Month: monthly traffic of each airport, with national and international passengers side by side
gd_airport_month_df = (
    spark.table(table_name("SV_Airport_Month"))
    .groupBy("country_eurostat", "icao_code", "airport_code", "airport_name", "year", "month", "month_start")
    .agg(
        F.sum(F.when(F.col("coverage") == "TOTAL", F.col("passengers"))).alias("passengers"),
        F.sum(F.when(F.col("coverage") == "NAT", F.col("passengers"))).alias("national_passengers"),
        F.sum(F.when(F.col("coverage") == "INTL", F.col("passengers"))).alias("international_passengers"),
        F.sum(F.when(F.col("coverage") == "TOTAL", F.col("flights"))).alias("flights"),
    )
)

save_table(
    gd_airport_month_df,
    "GD_Airport_Month",
    "GOLD - passengers (total, national, international) and flights of each European airport per month",
)

# COMMAND ----------

# DBTITLE 1,GD_Country_Year: passengers of each country per complete year
gd_country_year_df = spark.sql(
    f"""
    WITH monthly AS (
        SELECT country_eurostat                                                        AS country,
               year,
               month,
               SUM(COALESCE(international_passengers, 0) + COALESCE(national_passengers, 0) / 2) AS passengers,
               SUM(passengers)                                                         AS passengers_handled,
               SUM(flights)                                                            AS flights
        FROM {table_name('GD_Airport_Month')}
        GROUP BY country_eurostat, year, month
    )
    SELECT country,
           year,
           SUM(passengers)         AS passengers,
           SUM(passengers_handled) AS passengers_handled,
           SUM(flights)            AS flights
    FROM monthly
    GROUP BY country, year
    HAVING COUNT(*) = 12          -- complete years only
    """
)

save_table(
    gd_country_year_df,
    "GD_Country_Year",
    "GOLD - air passengers of each country per complete year (international + national / 2)",
)

# COMMAND ----------

# DBTITLE 1,GD_Route_Year: traffic of each route seen from its reporting airport
gd_route_year_df = (
    spark.table(table_name("SV_Route_Reports"))
    .groupBy("reporting_country", "reporting_icao", "partner_icao", "year")
    .agg(
        F.min("partner_country").alias("partner_country_eurostat"),
        F.sum("passengers").alias("passengers"),
        F.sum("flights").alias("flights"),
        F.sum("seats").alias("seats"),
        F.sum("seated_passengers").alias("seated_passengers"),
        F.sum("passengers_on_board").alias("passengers_on_board"),
    )
    .withColumn("load_factor_pct", F.round(100 * F.try_divide(F.col("seated_passengers"), F.col("seats")), 1))
    .withColumn("passengers_per_flight", F.round(F.try_divide(F.col("passengers_on_board"), F.col("flights")), 0))
    .join(
        F.broadcast(airports_ref.select(
            F.col("icao_code").alias("reporting_icao"),
            F.col("airport_code").alias("from_airport"),
            F.col("airport_name").alias("from_airport_name"),
        )),
        "reporting_icao", "left",
    )
    .join(
        F.broadcast(airports_ref.select(
            F.col("icao_code").alias("partner_icao"),
            F.col("airport_code").alias("to_airport"),
            F.col("airport_name").alias("to_airport_name"),
            F.col("country").alias("to_country"),
            F.col("continent").alias("to_continent"),
        )),
        "partner_icao", "left",
    )
    .withColumn("from_airport", F.coalesce("from_airport", "reporting_icao"))
    .withColumn("to_airport", F.coalesce("to_airport", "partner_icao"))
)

save_table(
    gd_route_year_df,
    "GD_Route_Year",
    "GOLD - passengers (both directions), flights, seats and load factor of each route per reporting airport and year",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Airports: size, growth and recovery

# COMMAND ----------

# DBTITLE 1,Passengers handled per Portuguese airport in the latest complete year
airport_year_df = spark.sql(
    f"""
    WITH yearly AS (
        SELECT airport_code, airport_name, year,
               SUM(passengers)               AS passengers,
               SUM(national_passengers)      AS national_passengers,
               SUM(international_passengers) AS international_passengers,
               SUM(flights)                  AS flights,
               COUNT(*)                      AS months
        FROM {table_name('GD_Airport_Month')}
        WHERE country_eurostat = '{HOME_COUNTRY}'
        GROUP BY airport_code, airport_name, year
    )
    SELECT l.airport_code,
           l.airport_name,
           ROUND(l.passengers / 1e6, 2)                                AS passengers_million,
           ROUND(100.0 * l.passengers / SUM(l.passengers) OVER (), 1)  AS pct_of_total,
           ROUND(100.0 * l.international_passengers / l.passengers, 1) AS pct_international,
           ROUND(100.0 * (l.passengers / p.passengers - 1), 1)         AS growth_vs_previous_year_pct,
           ROUND(100.0 * (l.passengers / b.passengers - 1), 1)         AS growth_vs_2019_pct,
           ROUND(l.passengers / l.flights, 0)                          AS passengers_per_flight
    FROM yearly l
    LEFT JOIN yearly p ON l.airport_code = p.airport_code AND p.year = {LATEST_FULL_YEAR - 1} AND p.months = 12
    LEFT JOIN yearly b ON l.airport_code = b.airport_code AND b.year = {PRE_PANDEMIC_YEAR} AND b.months = 12
    WHERE l.year = {LATEST_FULL_YEAR} AND l.months = 12 AND l.passengers >= 100000
    ORDER BY l.passengers DESC
    """
)

display(airport_year_df)

# COMMAND ----------

# DBTITLE 1,Passengers per airport and year (pivot), in millions
display(
    spark.table(table_name("GD_Airport_Month"))
    .filter((F.col("country_eurostat") == HOME_COUNTRY) & F.col("airport_code").isin(MAIN_AIRPORTS)
            & (F.col("year") <= LATEST_FULL_YEAR))
    .groupBy("year")
    .pivot("airport_code", MAIN_AIRPORTS)
    .agg(F.round(F.sum("passengers") / 1e6, 2))
    .orderBy("year")
)

# COMMAND ----------

# DBTITLE 1,The latest twelve months: rolling annual passengers with a window function
rolling_df = spark.sql(
    f"""
    WITH monthly AS (
        SELECT airport_code, month_start, year, month, passengers,
               SUM(passengers) OVER (PARTITION BY airport_code ORDER BY month_start
                                     ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)    AS passengers_12_months,
               COUNT(*) OVER (PARTITION BY airport_code ORDER BY month_start
                              ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)           AS months_in_window,
               LAG(passengers, 12) OVER (PARTITION BY airport_code ORDER BY month_start) AS passengers_same_month_last_year
        FROM {table_name('GD_Airport_Month')}
        WHERE country_eurostat = '{HOME_COUNTRY}'
    )
    SELECT airport_code,
           date_format(month_start, 'yyyy-MM')                                      AS month,
           ROUND(passengers / 1e6, 3)                                               AS passengers_million,
           ROUND(100.0 * (passengers / passengers_same_month_last_year - 1), 1)     AS growth_vs_same_month_last_year_pct,
           ROUND(passengers_12_months / 1e6, 2)                                     AS passengers_last_12_months_million
    FROM monthly
    WHERE months_in_window = 12
    """
)

display(
    rolling_df.filter(
        (F.col("airport_code") == HOME_HUB) & (F.col("month") >= f"{LATEST_FULL_YEAR}-07")
    ).orderBy("month")
)

# COMMAND ----------

# DBTITLE 1,Chart: monthly passengers of the main airports, January 2016 to the latest month
airport_month_pd = (
    spark.table(table_name("GD_Airport_Month"))
    .filter((F.col("country_eurostat") == HOME_COUNTRY) & F.col("airport_code").isin(MAIN_AIRPORTS))
    .select("airport_code", "year", "month", "passengers")
    .orderBy("year", "month")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(12, 4.8))
for airport in MAIN_AIRPORTS:
    series = airport_month_pd[airport_month_pd["airport_code"] == airport]
    ax.plot(series["year"] + (series["month"] - 1) / 12.0, series["passengers"].astype(float) / 1e6,
            linewidth=1.2, label=airport)
ax.axvspan(2020.17, 2022.25, color="grey", alpha=0.15, label="Pandemic")
ax.set_title(f"Passengers handled per month, January {FIRST_YEAR} to {LATEST_YEAR_TO_DATE}-{LATEST_MONTH:02d}")
ax.set_xlabel("Year")
ax.set_ylabel("Million passengers")
ax.grid(alpha=0.3)
ax.legend(loc="upper left", ncol=3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 Portugal in the European context
# MAGIC Is the growth of Portugal exceptional? The same indicator is available for every reporting country, so the
# MAGIC change since 2019 can be ranked.

# COMMAND ----------

# DBTITLE 1,Passengers per country: latest complete year against 2019
country_growth_df = spark.sql(
    f"""
    SELECT l.country,
           ROUND(b.passengers / 1e6, 1)                           AS passengers_2019_million,
           ROUND(l.passengers / 1e6, 1)                           AS passengers_latest_million,
           ROUND(100.0 * (l.passengers / b.passengers - 1), 1)    AS growth_since_2019_pct,
           RANK() OVER (ORDER BY l.passengers / b.passengers DESC) AS growth_rank,
           COUNT(*) OVER ()                                       AS countries_ranked
    FROM {table_name('GD_Country_Year')} l
    JOIN {table_name('GD_Country_Year')} b ON l.country = b.country AND b.year = {PRE_PANDEMIC_YEAR}
    WHERE l.year = {LATEST_FULL_YEAR} AND b.passengers >= 1000000
    """
)

display(country_growth_df.orderBy("growth_rank").limit(12))
display(country_growth_df.filter(F.col("country") == HOME_COUNTRY))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.4 Routes: where the traffic concentrates (2024)
# MAGIC The route statistics end in 2024. Each route is seen from the Portuguese airport that reports it, and its
# MAGIC passengers are the sum of both directions.

# COMMAND ----------

# DBTITLE 1,The 15 busiest routes of the Portuguese airports
# MAGIC %sql
# MAGIC SELECT RANK() OVER (ORDER BY passengers DESC)  AS route_rank,
# MAGIC        from_airport,
# MAGIC        to_airport,
# MAGIC        to_airport_name,
# MAGIC        to_country,
# MAGIC        ROUND(passengers / 1e6, 2)              AS passengers_million,
# MAGIC        flights,
# MAGIC        load_factor_pct,
# MAGIC        passengers_per_flight
# MAGIC FROM workspace.DL_AIR.GD_Route_Year
# MAGIC WHERE reporting_country = 'PT' AND year = 2024
# MAGIC ORDER BY passengers DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Concentration: share of the passengers carried by the busiest routes
route_concentration_df = spark.sql(
    f"""
    WITH ranked AS (
        SELECT passengers,
               ROW_NUMBER() OVER (ORDER BY passengers DESC, reporting_icao, partner_icao) AS route_position,
               COUNT(*) OVER ()                                                            AS routes,
               SUM(passengers) OVER ()                                                     AS all_passengers
        FROM {table_name('GD_Route_Year')}
        WHERE reporting_country = '{HOME_COUNTRY}' AND year = {NETWORK_YEAR} AND passengers > 0
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

# DBTITLE 1,Passengers by country of the partner airport
# MAGIC %sql
# MAGIC SELECT COALESCE(to_country, 'unknown')                                     AS country,
# MAGIC        COUNT(*)                                                            AS routes,
# MAGIC        ROUND(SUM(passengers) / 1e6, 2)                                     AS passengers_million,
# MAGIC        ROUND(100.0 * SUM(passengers) / SUM(SUM(passengers)) OVER (), 1)    AS pct_of_passengers,
# MAGIC        ROUND(100.0 * SUM(seated_passengers) / SUM(seats), 1)               AS load_factor_pct
# MAGIC FROM workspace.DL_AIR.GD_Route_Year
# MAGIC WHERE reporting_country = 'PT' AND year = 2024
# MAGIC GROUP BY COALESCE(to_country, 'unknown')
# MAGIC ORDER BY passengers_million DESC
# MAGIC LIMIT 12

# COMMAND ----------

# DBTITLE 1,Routes with the lowest load factor among those with at least 100,000 passengers
# MAGIC %sql
# MAGIC SELECT from_airport,
# MAGIC        to_airport,
# MAGIC        to_airport_name,
# MAGIC        ROUND(passengers / 1e3, 0)                    AS passengers_thousand,
# MAGIC        ROUND(seats / 1e3, 0)                         AS seats_thousand,
# MAGIC        load_factor_pct,
# MAGIC        ROUND((seats - seated_passengers) / 1e3, 0)   AS empty_seats_thousand
# MAGIC FROM workspace.DL_AIR.GD_Route_Year
# MAGIC WHERE reporting_country = 'PT' AND year = 2024 AND passengers >= 100000 AND load_factor_pct IS NOT NULL
# MAGIC ORDER BY load_factor_pct ASC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.5 Routes gained and lost between 2019 and 2024
# MAGIC With dated route statistics the route map of two years can be compared directly. A **full outer join** of the
# MAGIC routes of 2019 with those of 2024 classifies every route as kept, gained or lost.
# MAGIC
# MAGIC **A veracity trap.** In 2019 some partners were not declared with an airport code but with the code of an air
# MAGIC traffic region (for example `KZMA`, the Miami region, or `SBRE`, the Recife region). In 2024 the same routes are
# MAGIC declared with the airport code. A naive comparison would report those routes as lost and gained. The rule: a
# MAGIC route with an unidentified partner code is never counted as lost, and a route of 2024 is counted as gained only
# MAGIC if, in 2019, the same Portuguese airport had **no** unidentified partner in the **same country**. The
# MAGIC remaining cases are labelled *not comparable*.

# COMMAND ----------

# DBTITLE 1,Route map of the Portuguese airports: 2019 against 2024
route_change_df = spark.sql(
    f"""
    WITH before AS (
        SELECT from_airport, to_airport, to_airport_name, partner_country_eurostat, passengers,
               to_airport_name IS NOT NULL AS is_identified
        FROM {table_name('GD_Route_Year')}
        WHERE reporting_country = '{HOME_COUNTRY}' AND year = {PRE_PANDEMIC_YEAR} AND passengers > 0
    ),
    after AS (
        SELECT from_airport, to_airport, to_airport_name, partner_country_eurostat, passengers,
               to_airport_name IS NOT NULL AS is_identified
        FROM {table_name('GD_Route_Year')}
        WHERE reporting_country = '{HOME_COUNTRY}' AND year = {NETWORK_YEAR} AND passengers > 0
    ),
    unidentified_before AS (
        SELECT DISTINCT from_airport, partner_country_eurostat FROM before WHERE NOT is_identified
    )
    SELECT COALESCE(a.from_airport, b.from_airport)                         AS from_airport,
           COALESCE(a.to_airport, b.to_airport)                             AS to_airport,
           COALESCE(a.to_airport_name, b.to_airport_name)                   AS to_airport_name,
           COALESCE(a.partner_country_eurostat, b.partner_country_eurostat) AS partner_country,
           b.passengers                                                     AS passengers_before,
           a.passengers                                                     AS passengers_after,
           CASE WHEN a.passengers IS NOT NULL AND b.passengers IS NOT NULL THEN 'Kept'
                WHEN b.passengers IS NULL AND u.from_airport IS NULL AND a.is_identified THEN 'Gained'
                WHEN a.passengers IS NULL AND b.is_identified THEN 'Lost'
                ELSE 'Not comparable' END                                   AS status
    FROM before b
    FULL OUTER JOIN after a ON b.from_airport = a.from_airport AND b.to_airport = a.to_airport
    LEFT JOIN unidentified_before u
           ON a.from_airport = u.from_airport AND a.partner_country_eurostat = u.partner_country_eurostat
    """
)

route_change_summary_df = (
    route_change_df.groupBy("status")
    .agg(
        F.count("*").alias("routes"),
        F.round(F.sum("passengers_before") / 1e6, 2).alias(f"passengers_{PRE_PANDEMIC_YEAR}_million"),
        F.round(F.sum("passengers_after") / 1e6, 2).alias(f"passengers_{NETWORK_YEAR}_million"),
    )
    .orderBy("status")
)

display(route_change_summary_df)

# COMMAND ----------

# DBTITLE 1,The largest routes gained and the largest routes lost
display(
    route_change_df.filter(F.col("status") == "Gained")
    .select("status", "from_airport", "to_airport", "to_airport_name", "partner_country",
            F.round(F.col("passengers_after") / 1e3, 0).alias("passengers_2024_thousand"))
    .orderBy(F.desc("passengers_2024_thousand"))
    .limit(8)
)
display(
    route_change_df.filter(F.col("status") == "Lost")
    .select("status", "from_airport", "to_airport", "to_airport_name", "partner_country",
            F.round(F.col("passengers_before") / 1e3, 0).alias("passengers_2019_thousand"))
    .orderBy(F.desc("passengers_2019_thousand"))
    .limit(8)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.6 Seasonality
# MAGIC The **seasonality ratio** of an airport is the number of passengers of its busiest month divided by the number of
# MAGIC its quietest month. A ratio close to 1 means stable demand all year; a high ratio means that the airport, its
# MAGIC staff and the local economy work at very different intensities in summer and in winter.

# COMMAND ----------

# DBTITLE 1,Seasonality of each Portuguese airport in the latest complete year
seasonality_df = spark.sql(
    f"""
    WITH monthly AS (
        SELECT airport_code, airport_name, month, passengers,
               SUM(passengers) OVER (PARTITION BY airport_code) AS year_passengers,
               COUNT(*) OVER (PARTITION BY airport_code)        AS months
        FROM {table_name('GD_Airport_Month')}
        WHERE country_eurostat = '{HOME_COUNTRY}' AND year = {LATEST_FULL_YEAR}
    )
    SELECT airport_code                                                    AS airport,
           airport_name,
           ROUND(MAX(year_passengers) / 1e6, 2)                            AS passengers_million,
           MAX_BY(month, passengers)                                       AS busiest_month,
           MIN_BY(month, passengers)                                       AS quietest_month,
           ROUND(MAX(passengers) / MIN(passengers), 2)                     AS seasonality_ratio,
           ROUND(100.0 * SUM(CASE WHEN month BETWEEN 6 AND 9 THEN passengers END) / MAX(year_passengers), 1)
               AS pct_of_year_in_june_to_september
    FROM monthly
    WHERE months = 12 AND year_passengers >= 100000
    GROUP BY airport_code, airport_name
    ORDER BY seasonality_ratio DESC
    """
)

display(seasonality_df)

# COMMAND ----------

# DBTITLE 1,Chart: monthly profile of the main airports (share of the year in each month)
profile_pd = (
    spark.table(table_name("GD_Airport_Month"))
    .filter((F.col("country_eurostat") == HOME_COUNTRY) & (F.col("year") == LATEST_FULL_YEAR)
            & F.col("airport_code").isin(MAIN_AIRPORTS))
    .withColumn("pct_of_year",
                100 * F.col("passengers") / F.sum("passengers").over(Window.partitionBy("airport_code")))
    .select("airport_code", "month", "pct_of_year")
    .orderBy("month")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(11, 4.5))
for airport in MAIN_AIRPORTS:
    series = profile_pd[profile_pd["airport_code"] == airport]
    ax.plot(series["month"], series["pct_of_year"].astype(float), marker="o", label=airport)
ax.axhline(100 / 12, color="grey", linestyle=":", label="No seasonality")
ax.set_title(f"Share of the passengers of {LATEST_FULL_YEAR} in each month")
ax.set_xlabel("Month")
ax.set_ylabel("% of the year")
ax.set_xticks(range(1, 13))
ax.grid(alpha=0.3)
ax.legend(ncol=3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Seasonality of the busiest routes (monthly route statistics, 2024)
# MAGIC %sql
# MAGIC WITH monthly AS (
# MAGIC     SELECT reporting_airport_code, partner_airport_code, partner_airport, month, passengers,
# MAGIC            SUM(passengers) OVER (PARTITION BY reporting_icao, partner_icao) AS year_passengers,
# MAGIC            COUNT(*) OVER (PARTITION BY reporting_icao, partner_icao)        AS months
# MAGIC     FROM workspace.DL_AIR.SV_Route_Month
# MAGIC     WHERE year = 2024 AND passengers > 0
# MAGIC )
# MAGIC SELECT reporting_airport_code                                  AS from_airport,
# MAGIC        partner_airport_code                                    AS to_airport,
# MAGIC        partner_airport                                         AS to_airport_name,
# MAGIC        ROUND(MAX(year_passengers) / 1e6, 2)                    AS passengers_million,
# MAGIC        MAX_BY(month, passengers)                               AS busiest_month,
# MAGIC        MIN_BY(month, passengers)                               AS quietest_month,
# MAGIC        ROUND(MAX(passengers) / MIN(passengers), 2)             AS seasonality_ratio
# MAGIC FROM monthly
# MAGIC WHERE months = 12
# MAGIC GROUP BY reporting_airport_code, partner_airport_code, partner_airport
# MAGIC ORDER BY passengers_million DESC
# MAGIC LIMIT 12

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Structured Streaming: monthly traffic indicators
# MAGIC
# MAGIC **Q4: can the monthly indicators be kept up to date as new statistics arrive, with the same results as batch?**
# MAGIC
# MAGIC Airport traffic statistics are published month after month. A reporting system should add each delivery to its
# MAGIC indicators without recomputing everything. We reproduce this scenario with the most recent records of all the
# MAGIC European airports:
# MAGIC
# MAGIC 1. A delivery system is simulated by exporting the monthly airport records as **JSON files** to the landing zone
# MAGIC    of the Volume in **two drops**: the months of 2025 first, the months of 2026 later.
# MAGIC 2. A chain of three Structured Streaming queries moves the records through the medallion layers:
# MAGIC
# MAGIC | Query | Source | Processing | Sink | Output mode |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | JSON files in the Volume | add file name and ingestion time | `BZ_Traffic_Stream` | append |
# MAGIC | 2 | `BZ_Traffic_Stream` | validation, stateful deduplication, stream-static join with `SV_Airports` | `SV_Traffic_Stream` | append |
# MAGIC | 3 | `SV_Traffic_Stream` | aggregation per country and month | `GD_Country_Month_Stream` | complete |
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

# DBTITLE 1,Simulate the delivery system: monthly airport records of two years, in two drops
monthly_records_df = (
    spark.table(table_name("SV_Airport_Month"))
    .filter(F.col("year").isin(STREAM_DROP_1_YEAR, STREAM_DROP_2_YEAR))
    .select(
        F.concat_ws("|", "country_eurostat", "icao_code", "coverage",
                    F.col("year").cast("string"), F.col("month").cast("string")).alias("record_id"),
        F.col("country_eurostat").alias("country"),
        "icao_code",
        "coverage",
        F.date_format("month_start", "yyyy-MM").alias("period"),
        "passengers",
        "flights",
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
        StructField("country", StringType(), True),
        StructField("icao_code", StringType(), True),
        StructField("coverage", StringType(), True),
        StructField("period", StringType(), True),
        StructField("passengers", DoubleType(), True),
        StructField("flights", DoubleType(), True),
    ]
)


def clean_traffic_records(records: DataFrame, airports_ref: DataFrame) -> DataFrame:
    """Silver logic for traffic records. Works for batch and for streaming DataFrames."""
    return (
        records.filter(
            F.col("record_id").isNotNull()
            & F.col("coverage").isin("TOTAL", "NAT", "INTL")
            & F.col("period").rlike("^[0-9]{4}-[0-9]{2}$")
            & (F.coalesce("passengers", F.lit(0.0)) >= 0)
        )
        .dropDuplicates(["record_id"])
        .join(F.broadcast(airports_ref.select("icao_code", "airport_code", "airport_name")), "icao_code", "left")
        .select(
            "record_id",
            "country",
            "icao_code",
            F.coalesce("airport_code", "icao_code").alias("airport_code"),
            "airport_name",
            "coverage",
            F.substring("period", 1, 4).cast("int").alias("year"),
            F.substring("period", 6, 2).cast("int").alias("month"),
            "passengers",
            "flights",
        )
    )


def country_month_indicators(records: DataFrame) -> DataFrame:
    """Gold logic: passengers and flights per country and month. Batch or streaming."""
    return (
        records.groupBy("country", "year", "month")
        .agg(
            F.sum(F.when(F.col("coverage") == "INTL", F.col("passengers"))).alias("international_passengers"),
            F.sum(F.when(F.col("coverage") == "NAT", F.col("passengers"))).alias("national_passengers"),
            F.sum(F.when(F.col("coverage") == "TOTAL", F.col("passengers"))).alias("passengers_handled"),
            F.sum(F.when(F.col("coverage") == "TOTAL", F.col("flights"))).alias("flights"),
        )
        # International passengers are counted once in the country, national passengers at both airports
        .withColumn(
            "passengers",
            F.coalesce("international_passengers", F.lit(0.0)) + F.coalesce("national_passengers", F.lit(0.0)) / 2,
        )
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
    """Query 3: SV_Traffic_Stream -> GD_Country_Month_Stream (complete)."""
    silver_stream = spark.readStream.table(table_name("SV_Traffic_Stream"))
    return (
        country_month_indicators(silver_stream)
        .writeStream.format("delta")
        .queryName("q_gold_country_month")
        .outputMode("complete")
        .option("checkpointLocation", f"{DL_Checkpoint_Path}/gold_country_month_stream")
        .trigger(availableNow=True)
        .toTable(table_name("GD_Country_Month_Stream"))
    )


def run_streaming_pipeline(label: str) -> None:
    """Run the three queries in sequence until all available data is processed."""
    for start_query in (start_bronze_stream, start_silver_stream, start_gold_stream):
        query = start_query()
        query.awaitTermination()
        print(f"Query {query.name:<22}: finished, active = {query.isActive}")
    print(f"[{label}] streaming tables after the run:")
    for name in ("BZ_Traffic_Stream", "SV_Traffic_Stream", "GD_Country_Month_Stream"):
        print(f"  {name:<26}: {row_count(name):>8,} rows")


def show_stream_indicator() -> None:
    """Monthly passengers of Portugal, read from the streaming Gold table."""
    display(
        spark.table(table_name("GD_Country_Month_Stream"))
        .filter(F.col("country") == HOME_COUNTRY)
        .groupBy("month")
        .pivot("year", [STREAM_DROP_1_YEAR, STREAM_DROP_2_YEAR])
        .agg(F.round(F.sum("passengers") / 1e6, 3))
        .withColumnRenamed(str(STREAM_DROP_1_YEAR), f"passengers_{STREAM_DROP_1_YEAR}_million")
        .withColumnRenamed(str(STREAM_DROP_2_YEAR), f"passengers_{STREAM_DROP_2_YEAR}_million")
        .orderBy("month")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 First drop: the months of 2025

# COMMAND ----------

# DBTITLE 1,Drop 1 arrives and the streaming pipeline runs
land_records(drop_1_df)
run_streaming_pipeline("after drop 1")

bronze_stream_version_after_drop_1 = (
    spark.sql(f"DESCRIBE HISTORY {table_name('BZ_Traffic_Stream')}").agg(F.max("version")).first()[0]
)

# COMMAND ----------

# DBTITLE 1,Streaming indicator after drop 1: passengers of Portugal per month
show_stream_indicator()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 Second drop: incremental processing
# MAGIC The months of 2026 arrive in the landing zone and the **same** queries are started again. Thanks to the
# MAGIC checkpoints they do not reprocess 2025: only the new files are read, and the indicator gains a second column.

# COMMAND ----------

# DBTITLE 1,Drop 2 arrives and the streaming pipeline runs again
land_records(drop_2_df)
run_streaming_pipeline("after drop 2")

# COMMAND ----------

# DBTITLE 1,Streaming indicator after drop 2: passengers of Portugal per month
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
# MAGIC The same Silver and Gold logic (`clean_traffic_records` and `country_month_indicators`) is applied **in batch**
# MAGIC to the same records. If the streaming pipeline is correct, both paths must give exactly the same result.

# COMMAND ----------

# DBTITLE 1,Compare the streaming tables with a batch computation
# The batch results are saved as tables, so both sides of the comparison are read from Delta.
save_table(
    clean_traffic_records(monthly_records_df, spark.table(table_name("SV_Airports"))),
    "SV_Traffic_Batch",
    "SILVER - monthly airport records computed in batch (control for the streaming pipeline)",
)
batch_silver = spark.table(table_name("SV_Traffic_Batch"))
save_table(
    country_month_indicators(batch_silver),
    "GD_Country_Month_Batch",
    "GOLD - country and month indicators computed in batch (control for the streaming pipeline)",
)
batch_gold = spark.table(table_name("GD_Country_Month_Batch"))
stream_silver = spark.table(table_name("SV_Traffic_Stream"))
stream_gold = spark.table(table_name("GD_Country_Month_Stream"))

n_batch_records = batch_silver.count()
n_stream_records = stream_silver.count()
only_in_batch = batch_silver.join(stream_silver, "record_id", "left_anti").count()
only_in_stream = stream_silver.join(batch_silver, "record_id", "left_anti").count()

gold_keys = ["country", "year", "month", "passengers", "passengers_handled", "flights"]
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
# MAGIC ### 12.1 The monthly series of Portugal, loaded in two steps
# MAGIC `GD_Portugal_Month` holds the passengers of the country per month (international + national / 2). It is created
# MAGIC with the months up to December 2025. The months of 2026 are then added with a **MERGE**, exactly as a new
# MAGIC delivery of statistics would be: rows that already exist for a month are updated, the others are inserted.

# COMMAND ----------

# DBTITLE 1,GD_Portugal_Month: create with the months up to the latest complete year
portugal_month_df = spark.sql(
    f"""
    SELECT month_start,
           year,
           month,
           SUM(COALESCE(international_passengers, 0) + COALESCE(national_passengers, 0) / 2) AS passengers,
           SUM(international_passengers)                                                     AS international_passengers,
           SUM(national_passengers) / 2                                                      AS national_passengers,
           SUM(flights)                                                                      AS flights
    FROM {table_name('GD_Airport_Month')}
    WHERE country_eurostat = '{HOME_COUNTRY}'
    GROUP BY month_start, year, month
    """
)

save_table(
    portugal_month_df.filter(F.col("year") <= LATEST_FULL_YEAR),
    "GD_Portugal_Month",
    "GOLD - air passengers of Portugal per month (international + national / 2) and flights",
)

# COMMAND ----------

# DBTITLE 1,MERGE the months of the current year into GD_Portugal_Month
portugal_month_version_before_merge = (
    spark.sql(f"DESCRIBE HISTORY {table_name('GD_Portugal_Month')}").agg(F.max("version")).first()[0]
)

portugal_month_df.filter(F.col("year") > LATEST_FULL_YEAR).createOrReplaceTempView("new_months")

spark.sql(
    f"""
    MERGE INTO {table_name('GD_Portugal_Month')} AS target
    USING new_months AS source
       ON target.month_start = source.month_start
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.2 Is traffic back on its pre-pandemic trend?
# MAGIC A straight line is fitted, with the Spark SQL aggregates `regr_slope` and `regr_intercept`, on the annual
# MAGIC passengers of 2016 to 2019. Extending that line gives the traffic that would have been expected without the
# MAGIC pandemic, which is compared with what happened.

# COMMAND ----------

# DBTITLE 1,Passengers of Portugal per year and the pre-pandemic trend
national_year_df = spark.sql(
    f"""
    WITH national AS (
        SELECT year, SUM(passengers) / 1e6 AS passengers_million
        FROM {table_name('GD_Portugal_Month')}
        WHERE year <= {LATEST_FULL_YEAR}
        GROUP BY year
    ),
    trend AS (
        SELECT regr_slope(passengers_million, year)     AS slope,
               regr_intercept(passengers_million, year) AS intercept
        FROM national
        WHERE year BETWEEN {FIRST_YEAR} AND {PRE_PANDEMIC_YEAR}
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
    ORDER BY n.year
    """
)

display(national_year_df)

# COMMAND ----------

# DBTITLE 1,The current year so far, against the same months of the previous year
year_to_date_df = spark.sql(
    f"""
    SELECT {LATEST_MONTH}                                                                    AS months_compared,
           ROUND(SUM(CASE WHEN year = {LATEST_YEAR_TO_DATE} THEN passengers END) / 1e6, 2)   AS passengers_{LATEST_YEAR_TO_DATE}_million,
           ROUND(SUM(CASE WHEN year = {LATEST_YEAR_TO_DATE - 1} THEN passengers END) / 1e6, 2) AS passengers_{LATEST_YEAR_TO_DATE - 1}_million,
           ROUND(100.0 * (SUM(CASE WHEN year = {LATEST_YEAR_TO_DATE} THEN passengers END)
                          / SUM(CASE WHEN year = {LATEST_YEAR_TO_DATE - 1} THEN passengers END) - 1), 1) AS growth_pct
    FROM {table_name('GD_Portugal_Month')}
    WHERE year IN ({LATEST_YEAR_TO_DATE}, {LATEST_YEAR_TO_DATE - 1}) AND month <= {LATEST_MONTH}
    """
)

display(year_to_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.3 Scenarios to 2030
# MAGIC A forecast with one number would suggest a certainty that the data does not support. Capacity decisions are
# MAGIC better served by a **range** between two transparent scenarios, both starting from the latest complete year:
# MAGIC
# MAGIC * **Scenario A, growth at the latest pace**: passengers keep growing at the rate observed in the latest complete
# MAGIC   year.
# MAGIC * **Scenario B, pre-pandemic volume growth**: passengers grow by the same number of passengers per year as on the
# MAGIC   2016 to 2019 trend line.
# MAGIC
# MAGIC Neither scenario includes capacity limits. Lisbon airport, which handles about half of the traffic, is known to
# MAGIC operate close to its capacity, so the scenarios describe demand, not what the infrastructure can deliver.

# COMMAND ----------

# DBTITLE 1,GD_Passenger_Scenarios
latest_row = national_year_df.filter(F.col("year") == LATEST_FULL_YEAR).first()
latest_passengers_million = latest_row["passengers_million"]
latest_growth = latest_row["growth_pct"] / 100.0
trend_million_per_year = latest_row["trend_million_per_year"]
gap_to_trend_pct = latest_row["gap_to_trend_pct"]
year_to_date_growth = year_to_date_df.first()["growth_pct"]

print(f"Passengers in {LATEST_FULL_YEAR}                    : {latest_passengers_million:.1f} million")
print(f"Growth in {LATEST_FULL_YEAR}                        : {latest_growth:.1%}")
print(f"Growth in the first {LATEST_MONTH} months of {LATEST_YEAR_TO_DATE}  : {year_to_date_growth:.1f}%")
print(f"Pre-pandemic trend ({FIRST_YEAR}-{PRE_PANDEMIC_YEAR})      : +{trend_million_per_year:.2f} million passengers per year")
print(f"Gap to the pre-pandemic trend       : {gap_to_trend_pct:+.1f}%")

gd_passenger_scenarios_df = spark.sql(
    f"""
    SELECT year,
           ROUND({latest_passengers_million} * POWER(1 + {latest_growth}, year - {LATEST_FULL_YEAR}), 1)
               AS scenario_a_latest_pace_million,
           ROUND({latest_passengers_million} + {trend_million_per_year} * (year - {LATEST_FULL_YEAR}), 1)
               AS scenario_b_pre_pandemic_volume_million
    FROM (SELECT explode(sequence({LATEST_FULL_YEAR + 1}, {FORECAST_END_YEAR})) AS year)
    """
)

save_table(
    gd_passenger_scenarios_df,
    "GD_Passenger_Scenarios",
    "GOLD - air passengers of Portugal per year until 2030, in two scenarios",
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
        label=f"Pre-pandemic trend ({FIRST_YEAR}-{PRE_PANDEMIC_YEAR})")
ax.plot([LATEST_FULL_YEAR] + scenarios_pd["year"].tolist(),
        [float(latest_passengers_million)] + scenarios_pd["scenario_a_latest_pace_million"].astype(float).tolist(),
        color="darkorange", linestyle="--", marker="o", label="Scenario A: latest pace")
ax.plot([LATEST_FULL_YEAR] + scenarios_pd["year"].tolist(),
        [float(latest_passengers_million)]
        + scenarios_pd["scenario_b_pre_pandemic_volume_million"].astype(float).tolist(),
        color="seagreen", linestyle="--", marker="o", label="Scenario B: pre-pandemic volume growth")
ax.set_title("Air passengers of Portugal per year")
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
# MAGIC * **Updates and merges**: `GD_Portugal_Month` was loaded in two steps, the second one with a `MERGE`
# MAGIC   (section 12.1). A MERGE makes the load idempotent: running it twice does not duplicate rows.
# MAGIC * **Audit**: what changed and when (`DESCRIBE HISTORY`).
# MAGIC * **Time travel**: query a table as it was at a previous version (`VERSION AS OF`), which is useful to reproduce
# MAGIC   a report or to recover from a wrong load.

# COMMAND ----------

# DBTITLE 1,History of GD_Portugal_Month: the initial load and the MERGE
# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.DL_AIR.GD_Portugal_Month

# COMMAND ----------

# DBTITLE 1,Time travel: GD_Portugal_Month before and after the MERGE
print(f"GD_Portugal_Month at version {portugal_month_version_before_merge} (before the MERGE):")
spark.sql(
    f"""
    SELECT date_format(MAX(month_start), 'yyyy-MM') AS latest_month, COUNT(*) AS months
    FROM {table_name('GD_Portugal_Month')} VERSION AS OF {portugal_month_version_before_merge}
    """
).show()

print("GD_Portugal_Month now:")
spark.sql(
    f"SELECT date_format(MAX(month_start), 'yyyy-MM') AS latest_month, COUNT(*) AS months "
    f"FROM {table_name('GD_Portugal_Month')}"
).show()

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
network = {row["year"]: row for row in spark.table(table_name("GD_Network_Year")).collect()}
hub_reach = reach_df.filter(F.col("origin") == HOME_HUB).first()
second_reach = reach_df.filter(F.col("origin") == "OPO").first()
top_airport = (
    spark.table(table_name("GD_Airport_Connectivity"))
    .filter("is_reporting_airport").orderBy("rank_by_destinations", "airport_code").first()
)
top_pagerank = spark.table(table_name("GD_Airport_PageRank")).orderBy("pagerank_position", "id").first()
hub_pagerank = spark.table(table_name("GD_Airport_PageRank")).filter(F.col("id") == HOME_HUB).first()
network_hubs = spark.table(table_name("GD_Transfer_Hubs")).orderBy("hub_rank", "id").limit(5).collect()
pct_top5_network_hubs = sum(row["pct_of_pairs"] for row in network_hubs)
top_degree_band = degree_distribution_df.orderBy(F.desc("destinations_band")).first()
bottom_degree_band = degree_distribution_df.orderBy("destinations_band").first()

lisbon_hubs = (
    best.filter((F.col("origin") == HOME_HUB) & (F.col("stops") == 1))
    .groupBy("stop_1").count().orderBy(F.desc("count"), "stop_1").collect()
)
lisbon_one_stop_total = sum(row["count"] for row in lisbon_hubs)
lisbon_top5_hubs = [row["stop_1"] for row in lisbon_hubs[:5]]
pct_lisbon_top_hub = 100 * lisbon_hubs[0]["count"] / lisbon_one_stop_total
pct_lisbon_top5_hubs = 100 * sum(row["count"] for row in lisbon_hubs[:5]) / lisbon_one_stop_total
lisbon_detour = (
    best.filter((F.col("origin") == HOME_HUB) & (F.col("stops") == 1))
    .agg(F.percentile_approx("detour_factor", 0.5)).first()[0]
)
bfs_summary = (
    spark.table(table_name("GD_Flights_From_Lisbon"))
    .agg(
        F.max("flights").alias("max_flights"),
        F.sum(F.when(F.col("flights").isNull(), 1).otherwise(0)).alias("unreachable"),
    )
    .first()
)
reach_by_year = {
    row["year"]: row
    for row in spark.table(table_name("GD_Portugal_Reach_Year")).filter(F.col("origin") == HOME_HUB).collect()
}
mirror = spark.sql(
    f"""
    SELECT percentile_approx(mirror_difference_pct, 0.5) AS median_difference,
           100.0 * AVG(CASE WHEN mirror_difference_pct < {MIRROR_TOLERANCE_PCT} THEN 1.0 ELSE 0.0 END) AS pct_agreeing
    FROM {table_name('SV_Flows')}
    WHERE declared_by = 'both airports' AND passengers >= 1000
    """
).first()

airports_latest = airport_year_df.collect()
portugal_growth = country_growth_df.filter(F.col("country") == HOME_COUNTRY).first()
home_load = spark.sql(
    f"""
    SELECT 100.0 * SUM(seated_passengers) / SUM(seats) AS load_factor, SUM(passengers) AS passengers
    FROM {table_name('GD_Route_Year')}
    WHERE reporting_country = '{HOME_COUNTRY}' AND year = {NETWORK_YEAR}
    """
).first()
concentration = route_concentration_df.first()
top_route = (
    spark.table(table_name("GD_Route_Year"))
    .filter((F.col("reporting_country") == HOME_COUNTRY) & (F.col("year") == NETWORK_YEAR))
    .orderBy(F.desc("passengers")).first()
)
domestic_trunk = {
    row["to_airport"]: row
    for row in spark.table(table_name("GD_Route_Year"))
    .filter((F.col("reporting_country") == HOME_COUNTRY) & (F.col("year") == NETWORK_YEAR)
            & (F.col("from_airport") == HOME_HUB) & F.col("to_airport").isin("OPO", "FAO"))
    .collect()
}
seasonality = {row["airport"]: row for row in seasonality_df.collect()}
route_changes = {row["status"]: row["routes"] for row in route_change_summary_df.collect()}
pre_pandemic_row = national_year_df.filter(F.col("year") == PRE_PANDEMIC_YEAR).first()
scenario_end = spark.table(table_name("GD_Passenger_Scenarios")).filter(F.col("year") == FORECAST_END_YEAR).first()
scenario_values = sorted(
    [scenario_end["scenario_a_latest_pace_million"], scenario_end["scenario_b_pre_pandemic_volume_million"]]
)

print("DATA")
print(f"  Route reports: {row_count('SV_Route_Reports'):,} ({FIRST_YEAR}-{NETWORK_YEAR}, {reporting_years_df.count()} countries)."
      f" Flows: {row_count('SV_Flows'):,}. Monthly airport rows: {row_count('SV_Airport_Month'):,}"
      f" (to {LATEST_YEAR_TO_DATE}-{LATEST_MONTH:02d} for Portugal).")
print(f"  Data quality: {len(quality_checks) - n_checks_failed} of {len(quality_checks)} checks passed."
      f" Two declarations of the same flow differ by {mirror['median_difference']:.2f}% (median);"
      f" {mirror['pct_agreeing']:.1f}% agree within {MIRROR_TOLERANCE_PCT:.0f}%.")
print("\nQ1 - THE EUROPEAN NETWORK")
print(f"  {NETWORK_YEAR}: {n_vertices:,} airports, {n_edges:,} routes, {network[NETWORK_YEAR]['passengers_million']:,.0f} million passengers,"
      f" load factor {network[NETWORK_YEAR]['load_factor_pct']}%.")
print(f"  Routes against {PRE_PANDEMIC_YEAR}: {100 * (network[2020]['routes'] / network[PRE_PANDEMIC_YEAR]['routes'] - 1):+.1f}% in 2020,"
      f" {100 * (network[NETWORK_YEAR]['routes'] / network[PRE_PANDEMIC_YEAR]['routes'] - 1):+.1f}% in {NETWORK_YEAR}."
      f" Passengers: {100 * (network[2020]['passengers_million'] / network[PRE_PANDEMIC_YEAR]['passengers_million'] - 1):+.1f}% in 2020,"
      f" {100 * (network[NETWORK_YEAR]['passengers_million'] / network[PRE_PANDEMIC_YEAR]['passengers_million'] - 1):+.1f}% in {NETWORK_YEAR}.")
print(f"  {top_degree_band['pct_of_airports']}% of the airports (more than 100 destinations) carry"
      f" {top_degree_band['pct_of_passengers']}% of the passengers; {bottom_degree_band['pct_of_airports']}% have at most"
      f" 5 destinations.")
print(f"  Best connected airport: {top_airport['airport_code']} ({top_airport['destinations']} destinations)."
      f" Highest PageRank: {top_pagerank['id']}.")
print(f"  Lisbon: {hub_pagerank['out_degree']} destinations; position {hub_pagerank['pagerank_position']} by PageRank,"
      f" {hub_pagerank['passengers_position']} by passengers and {hub_pagerank['degree_position']} by destinations"
      f" among {n_vertices:,} airports.")
print(f"  Every airport of the network is at most {bfs_summary['max_flights']} flights from Lisbon;"
      f" {bfs_summary['unreachable']} cannot be reached.")
print("\nQ2 - THE CONNECTIVITY OF PORTUGAL")
print(f"  Itineraries enumerated from the Portuguese airports: {n_direct:,} direct, {n_one_stop:,} with one stop,"
      f" {n_two_stop:,} with two stops. One-stop itineraries of the whole network: {n_network_one_stop:,}.")
print(f"  Lisbon reaches {hub_reach['direct']} airports directly, {hub_reach['within_one_stop']} within one stop"
      f" ({hub_reach['pct_of_network_within_one_stop']}% of the network) and {hub_reach['within_two_stops']} within two"
      f" stops ({hub_reach['pct_of_network_within_two_stops']}%).")
print(f"  Porto reaches {second_reach['direct']} airports directly and {second_reach['within_one_stop']} within one stop"
      f" ({second_reach['pct_of_network_within_one_stop']}%).")
print(f"  Lisbon's one-stop destinations are best reached through {len(lisbon_hubs)} hubs. {lisbon_hubs[0]['stop_1']}"
      f" alone serves {pct_lisbon_top_hub:.1f}%; the top five ({', '.join(lisbon_top5_hubs)}) serve {pct_lisbon_top5_hubs:.1f}%.")
print(f"  In the whole network the top five transfer hubs ({', '.join(row['id'] for row in network_hubs)})"
      f" are the best transfer for {pct_top5_network_hubs:.1f}% of the pairs.")
print(f"  Median detour of a one-stop itinerary from Lisbon: {lisbon_detour:.2f} times the direct distance.")
print(f"  Lisbon, direct destinations (comparable panel): {reach_by_year[PRE_PANDEMIC_YEAR]['direct_destinations']} in"
      f" {PRE_PANDEMIC_YEAR}, {reach_by_year[2020]['direct_destinations']} in 2020,"
      f" {reach_by_year[NETWORK_YEAR]['direct_destinations']} in {NETWORK_YEAR}. Within one stop:"
      f" {reach_by_year[PRE_PANDEMIC_YEAR]['within_one_stop']}, {reach_by_year[2020]['within_one_stop']},"
      f" {reach_by_year[NETWORK_YEAR]['within_one_stop']}.")
print("\nQ3 - DEMAND")
print(f"  {LATEST_FULL_YEAR}: {latest_passengers_million:.1f} million passengers in Portugal,"
      f" {100 * (latest_passengers_million / pre_pandemic_row['passengers_million'] - 1):+.1f}% compared with {PRE_PANDEMIC_YEAR};"
      f" rank {portugal_growth['growth_rank']} of {portugal_growth['countries_ranked']} countries by growth since {PRE_PANDEMIC_YEAR}.")
print("  Passengers handled per airport: "
      + ", ".join(f"{row['airport_code']} {row['passengers_million']} M ({row['pct_of_total']}%)" for row in airports_latest[:5]) + ".")
print(f"  Load factor of the Portuguese routes in {NETWORK_YEAR}: {home_load['load_factor']:.1f}%."
      f" Lisbon - Porto {domestic_trunk['OPO']['load_factor_pct']}%, Lisbon - Faro {domestic_trunk['FAO']['load_factor_pct']}%.")
print(f"  The 10 busiest routes carry {concentration['pct_passengers_top_10_routes']}% of the passengers;"
      f" the bottom half of the {concentration['routes']} routes carries"
      f" {concentration['pct_passengers_bottom_half_of_routes']}%."
      f" Busiest route: {top_route['from_airport']} - {top_route['to_airport']}"
      f" ({top_route['passengers'] / 1e6:.2f} million).")
print(f"  Seasonality ratio in {LATEST_FULL_YEAR} (busiest month / quietest month): "
      + ", ".join(f"{a} {seasonality[a]['seasonality_ratio']}" for a in MAIN_AIRPORTS if a in seasonality) + ".")
print(f"  Routes {PRE_PANDEMIC_YEAR} -> {NETWORK_YEAR}: {route_changes.get('Kept', 0)} kept, {route_changes.get('Gained', 0)} gained,"
      f" {route_changes.get('Lost', 0)} lost, {route_changes.get('Not comparable', 0)} not comparable.")
print("\nQ4 - STREAMING")
print(f"  Streaming results identical to batch results: {'YES' if streaming_matches_batch else 'NO'}"
      f" ({n_stream_records:,} monthly records processed in two drops).")
print("\nQ5 - THE FUTURE")
print(f"  Growth: {latest_growth:.1%} in {LATEST_FULL_YEAR}, {year_to_date_growth:.1f}% in the first {LATEST_MONTH} months of"
      f" {LATEST_YEAR_TO_DATE}. Gap to the pre-pandemic trend in {LATEST_FULL_YEAR}: {gap_to_trend_pct:+.1f}%.")
print(f"  {FORECAST_END_YEAR}: between {scenario_values[0]:.1f} and {scenario_values[1]:.1f} million passengers"
      f" (scenario A {scenario_end['scenario_a_latest_pace_million']:.1f},"
      f" scenario B {scenario_end['scenario_b_pre_pandemic_volume_million']:.1f}).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Answers to the business questions
# MAGIC * **Q1, the European network.** The network is very unequal: the 4% of airports with more than a hundred
# MAGIC   destinations carry 45% of the passengers, while 45% of the airports have at most five destinations
# MAGIC   (section 8.1). Istanbul is the first airport by destinations, by passengers and by PageRank. Lisbon is the 11th
# MAGIC   airport of the network by PageRank and by passengers. The network proved resilient: it lost half of its routes
# MAGIC   and three quarters of its passengers in 2020, and in 2024 it had 8% more routes and 14% more passengers than in
# MAGIC   2019 (section 7).
# MAGIC * **Q2, the connectivity of Portugal.** Lisbon has direct flights to 106 airports and reaches 94% of the network
# MAGIC   with one stop; Porto, with 77 direct destinations, reaches 90% (section 9.3). A regional airport is therefore
# MAGIC   almost as well connected as the capital once a transfer is accepted. The detour imposed by a stop is small: the
# MAGIC   typical one-stop itinerary from Lisbon is 5% longer than the direct distance. This reach is concentrated:
# MAGIC   Istanbul alone is the best transfer for a fifth of Lisbon's one-stop destinations, and five hubs (Istanbul,
# MAGIC   Madrid, Frankfurt, Paris and Amsterdam) for almost half. Year by year, the direct destinations of Lisbon fell
# MAGIC   from 99 to 71 in 2020 and were back above the 2019 level by 2023 (section 9.4).
# MAGIC * **Q3, demand.** Portugal had 67 million air passengers in 2025, 22% more than in 2019, the 7th largest growth
# MAGIC   among 33 European countries (section 10.3). Lisbon handles half of them and Porto almost a quarter; Funchal,
# MAGIC   Ponta Delgada and Porto grew the most since 2019 (section 10.2). Planes fly full (86% of the seats in 2024), with the notable
# MAGIC   exception of the short domestic routes Lisbon - Porto and Lisbon - Faro (about 72%). Faro and the Azores are
# MAGIC   strongly seasonal; Lisbon, Porto and Funchal are not (section 10.6). Between 2019 and 2024 the Portuguese
# MAGIC   airports gained twice as many routes as they lost; the losses include the routes to Moscow (section 10.5).
# MAGIC * **Q4, streaming.** The streaming pipeline processed the monthly records of 2025 and 2026 in two instalments,
# MAGIC   read only the new files in the second run, and reached exactly the same result as the batch computation
# MAGIC   (section 11.4).
# MAGIC * **Q5, the future.** Traffic is 22% above 2019 but 19% below the path it was following before the pandemic.
# MAGIC   After the recovery, growth settled at about 5% a year in 2024 and 2025, and at 3.7% in the first four months of
# MAGIC   2026 (section 12.2). The two scenarios give between 86 and 90 million passengers in 2030: a range for planning,
# MAGIC   not a forecast (section 12.3).
# MAGIC
# MAGIC ### Recommendations
# MAGIC | # | Recommendation | For whom | Evidence |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Reduce the dependence on a single transfer hub.** One fifth of Lisbon's one-stop reach, and most of its reach into Asia, passes through Istanbul. Each region of the world should keep at least two alternative gateways, and the routes to the five main hubs should be monitored first. | Airport operator, tourism board | Section 9.3, hubs of Lisbon |
# MAGIC | 2 | **Promote the regional airports by their one-stop reach.** Porto reaches 90% of the network with one stop and Faro, Funchal and Ponta Delgada between 66% and 79%, with far fewer direct destinations than Lisbon. | Regional tourism boards | Section 9.3, reach table |
# MAGIC | 3 | **Target winter demand in Faro and the Azores.** Faro handles more than four times as many passengers in its busiest month as in its quietest, so airport and tourism capacity sits idle in winter. Funchal shows that an island destination can be stable all year. | Airport commercial teams | Section 10.6, seasonality ratio |
# MAGIC | 4 | **Review the short domestic air routes.** Lisbon - Porto and Lisbon - Faro have the lowest occupancy among the large routes (about 72% against 86% on average), with about 400,000 empty seats per direction in a year. They are the natural candidates for capacity adjustments and for coordination with rail. | Airline network planners, transport policy | Section 10.4, load factor |
# MAGIC | 5 | **Plan with a range, and plan for shocks.** The scenarios for 2030 differ by more than four million passengers, and the network lost half of its routes in a single year in 2020. Infrastructure decisions should be tested against both scenarios, and Lisbon's capacity limit considered separately. | Capacity planners | Sections 7, 9.4 and 12.3 |
# MAGIC
# MAGIC ### What we learned about Big Data processing
# MAGIC * **Joins create the volume.** A few megabytes of route statistics became 920,000 one-stop itineraries for the
# MAGIC   whole network and 1.35 million two-stop itineraries from Portugal, computed with ordinary joins and window
# MAGIC   functions, with no sampling.
# MAGIC * **Veracity needs explicit decisions.** The most valuable work of the project was finding and handling what the
# MAGIC   data does not say by itself: every route between two reporting countries is declared twice (the two
# MAGIC   declarations differ by 0.5% in the median, and a rule chooses one); countries stop reporting, so only a
# MAGIC   comparable panel can be followed over time; a seat count below the number of passengers is an error; partners
# MAGIC   declared with region codes in 2019 would have been reported as new routes in 2024.
# MAGIC * **Multi-file ingestion.** 48 files with three layouts and different periods were read with wildcards, and each
# MAGIC   line was interpreted with the header of its own file, using the file metadata.
# MAGIC * **One engine, one set of rules.** The same functions computed the batch and the streaming indicators, which is
# MAGIC   what guarantees that they agree.
# MAGIC * **Graph algorithms are joins.** PageRank, breadth-first search and motifs were written as joins and aggregations
# MAGIC   that Spark distributes like any other query.
# MAGIC * **Delta Lake** turns files into reliable tables: MERGE, history and time travel on the same Gold table.
# MAGIC
# MAGIC ### Limitations
# MAGIC * The **route statistics end in 2024**; only the airport statistics reach 2026.
# MAGIC * The network covers the routes with at least one airport in a reporting country: it is the European network and
# MAGIC   its links to the rest of the world, not the world network. An airport outside Europe appears only through its
# MAGIC   routes to Europe, and itineraries that transfer between two airports outside Europe are not found.
# MAGIC * Eurostat publishes the **main** routes of the **main** airports, so very small routes are missing.
# MAGIC * An itinerary is a sequence of routes that operated in the same year. Schedules, connection times, prices and
# MAGIC   airline alliances are not in the data, so some itineraries are not bookable in practice. There is no airline
# MAGIC   dimension.
# MAGIC * Distances are great-circle distances between airports.
# MAGIC * The scenarios extend past growth and ignore capacity limits, prices and the economy.
# MAGIC * Streaming used simulated file drops and the `availableNow` trigger. The Gold stream uses *complete* output mode;
# MAGIC   with an unbounded stream a **watermark** would be added so that old state is released.
# MAGIC
# MAGIC ### Future work
# MAGIC * Add the route statistics of 2025 when Eurostat publishes them, with the MERGE already in place.
# MAGIC * Use the monthly route files of all countries to follow the network month by month.
# MAGIC * **Machine learning with Spark ML**: a demand model per route with explanatory variables (distance, seasonality,
# MAGIC   competition, tourism) instead of scenarios.
# MAGIC * Add airline schedules to turn structural itineraries into bookable ones.
# MAGIC
# MAGIC ### References
# MAGIC * Eurostat, *Air passenger transport between the main airports of each reporting country and their main partner
# MAGIC   airports (routes data)*, datasets `avia_par_<country>`, downloaded on 16 November 2026.
# MAGIC * Eurostat, *Air passenger transport by type of schedule, transport coverage and main airports*, dataset
# MAGIC   `avia_paoa`, downloaded on 16 November 2026.
# MAGIC * Airport registry: public snapshot of the airports of the world (Kaggle), used for names and coordinates.
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
