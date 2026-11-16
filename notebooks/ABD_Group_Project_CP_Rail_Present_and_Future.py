# Databricks notebook source
# MAGIC %md
# MAGIC # Comboios de Portugal: the rail network today and tomorrow
# MAGIC ## A lakehouse with batch, streaming and graph analytics on Apache Spark (Databricks Free Edition)
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
# MAGIC 5. **Bronze** layer: raw ingestion
# MAGIC 6. **Silver** layer: cleaning, modelling and data quality
# MAGIC 7. **Gold** layer, the present: what CP runs today (Spark SQL)
# MAGIC 8. **Structured Streaming**: live station departure indicators
# MAGIC 9. **Graph analytics**: the network of stations
# MAGIC 10. The future of supply: the 2027 timetable
# MAGIC 11. The future of demand: passenger projection to 2030
# MAGIC 12. Delta Lake: audit history and time travel
# MAGIC 13. Conclusions
# MAGIC
# MAGIC > **How to run:** upload the nine data files of the project to the Volume `raw_files` (see section 4),
# MAGIC > attach the notebook to *Serverless* compute and choose **Run all**. The notebook is idempotent: it rebuilds all
# MAGIC > its tables on every run and never modifies the uploaded files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Problem definition and motivation
# MAGIC
# MAGIC ### Context
# MAGIC CP - Comboios de Portugal is the national passenger rail operator. Rail is at the centre of Portuguese mobility
# MAGIC policy: it is the backbone of the Lisbon and Porto metropolitan areas, it links the main cities of the country, and
# MAGIC it is the transport mode expected to absorb demand as the country decarbonises. Planning a railway means answering two
# MAGIC questions at the same time: **what service is offered today**, and **where supply and demand are heading**.
# MAGIC
# MAGIC The data needed to answer them is a typical Big Data combination:
# MAGIC
# MAGIC * **Volume**: a timetable is compact on disk but describes every stop of every train on every day. Expanding the
# MAGIC   calendar of CP for the two years in the feed produces hundreds of thousands of train runs and millions of stop events.
# MAGIC * **Velocity**: a station needs its departure indicators refreshed as events arrive, not once a day.
# MAGIC * **Variety**: relational timetable files (GTFS), a wide statistical file with embedded flags (Eurostat), and JSON
# MAGIC   event files (streaming).
# MAGIC * **Veracity**: the timetable feed is a rolling snapshot, so it is incomplete in the past and only partially
# MAGIC   published for the future. Ignoring this produces wrong conclusions, as section 7.1 demonstrates.
# MAGIC
# MAGIC ### Problem
# MAGIC Build one trustworthy data platform that describes the CP network **in the present** and gives an evidence-based
# MAGIC view of **its future**, answering five business questions:
# MAGIC
# MAGIC | # | Business question | Time | Processing type | Section |
# MAGIC |---|---|---|---|---|
# MAGIC | Q1 | How much service does CP run today, on which services, and when during the week and the day? | Present | Batch, Spark SQL | 7 |
# MAGIC | Q2 | Can station departure indicators be kept up to date as events arrive, with the same results as batch? | Present | Structured Streaming | 8 |
# MAGIC | Q3 | Seen as a network, which stations are the hubs and which sections carry the most trains? | Present | Graph analytics | 9 |
# MAGIC | Q4 | What does the timetable already published for 2027 change, and how complete is it? | Future | Batch, Spark SQL | 10 |
# MAGIC | Q5 | Where is passenger demand heading until 2030? | Future | Spark SQL (regression) | 11 |
# MAGIC
# MAGIC ### Why it matters
# MAGIC * For **passengers**: frequency and journey time are the two attributes that decide whether the train is a real alternative.
# MAGIC * For the **operator**: knowing the hubs and the busiest sections shows where a disruption hurts the most and
# MAGIC   where extra capacity is worth the most.
# MAGIC * For **planning**: comparing the trend of demand with the supply that is scheduled shows whether the network is
# MAGIC   keeping up with the passengers it is attracting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Big Data technologies and solution architecture
# MAGIC
# MAGIC | Technology | How it is used in this project |
# MAGIC |---|---|
# MAGIC | **Databricks Free Edition** (serverless compute) | Execution environment for the whole notebook |
# MAGIC | **Apache Spark DataFrame API** (PySpark) | Ingestion, parsing, calendar expansion, joins, window functions |
# MAGIC | **Spark SQL** | Gold layer and analysis: CTEs, window functions, conditional aggregation, linear regression aggregates |
# MAGIC | **Spark Structured Streaming** | Incremental ingestion of JSON departure events: checkpoints, stream-static join, stateful deduplication, windowed aggregation |
# MAGIC | **Graph analytics on Spark** | Station network: degrees, junctions, busiest sections and an iterative **PageRank** written with DataFrames |
# MAGIC | **Delta Lake** | ACID storage format of every table, transaction log for audit and time travel |
# MAGIC | **Unity Catalog** | Data organisation: `catalog.schema.table` namespace, table comments, and **Volumes** for raw files and for the streaming landing zone |
# MAGIC
# MAGIC ### Data organisation: medallion architecture
# MAGIC
# MAGIC ```
# MAGIC                                   Unity Catalog schema: <catalog>.abd_cp_rail
# MAGIC
# MAGIC  SOURCES                       BRONZE (raw)             SILVER (clean, modelled)         GOLD (business)
# MAGIC  -------                       ------------             ------------------------         ---------------
# MAGIC  Volume raw_files
# MAGIC   CP timetable (GTFS)    -->   bronze_gtfs_*       -->  dim_station, dim_service    -->  gold_daily_supply
# MAGIC   7 CSV files           batch  (7 tables)               silver_trips                     gold_weekly_supply
# MAGIC                                                         silver_stop_times                gold_station_activity
# MAGIC                                                         silver_trip_segments             gold_hourly_departures
# MAGIC                                                         silver_service_days              gold_journey_times
# MAGIC                                                                                          gold_graph_vertices / _edges
# MAGIC                                                                                          gold_station_pagerank
# MAGIC   Eurostat rail          -->   bronze_eurostat_*   -->  silver_rail_passengers      -->  gold_passenger_forecast
# MAGIC   passengers, 2 TSV     batch  (2 tables)               dim_country                      gold_passenger_growth_by_country
# MAGIC
# MAGIC  Volume landing
# MAGIC   departure events       -->   bronze_departures_  -->  silver_departures_stream    -->  gold_station_departures_stream
# MAGIC   JSON files          streaming      stream (append)          (append)                        (complete)
# MAGIC                                  checkpoint                  checkpoint                       checkpoint
# MAGIC ```
# MAGIC
# MAGIC * **Bronze** keeps every file exactly as received (all columns as text) plus the source file name and the ingestion time.
# MAGIC * **Silver** holds typed, validated and modelled data: dimensions (stations, services) and facts (trips, stop times,
# MAGIC   segments, operating days, passenger statistics).
# MAGIC * **Gold** holds small aggregated tables that directly answer the business questions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Environment setup
# MAGIC All names, dates and parameters used by the pipeline are defined once in the next cell.

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import os

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import matplotlib.pyplot as plt

# ---- Target namespace (Unity Catalog) ---------------------------------------
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "abd_cp_rail"
RAW_VOLUME = "raw_files"      # uploaded source files (never modified by the notebook)
LANDING_VOLUME = "landing"    # streaming landing zone and checkpoints (recreated on every run)

# ---- Source files -----------------------------------------------------------
GTFS_FILES = ["agency", "routes", "stops", "trips", "stop_times", "calendar", "calendar_dates"]
EUROSTAT_FILES = ["rail_pa_total", "rail_pa_quartal"]

# ---- Reference dates --------------------------------------------------------
SNAPSHOT_DATE = "2026-11-16"        # day on which the CP feed and the Eurostat files were downloaded
PRESENT_WEEK_START = "2026-11-16"   # Monday of the reference week for "the present"
FUTURE_WEEK_START = "2027-01-18"    # Monday of the reference week for "the future" (2027 timetable)
STREAM_DAY = "2026-11-18"           # Wednesday replayed as a stream of departure events

# ---- Data quality rules -----------------------------------------------------
PORTUGAL_LAT_RANGE = (36.9, 42.2)   # mainland Portugal bounding box
PORTUGAL_LON_RANGE = (-9.6, -6.1)
MAX_PLAUSIBLE_SPEED_KMH = 250       # above the maximum speed of any CP train

# ---- Timetable publication status (section 10) ------------------------------
PARTIAL_PUBLICATION_THRESHOLD = 0.8  # future/present trains below this ratio = partially published

# ---- Streaming simulation ---------------------------------------------------
FILES_PER_DROP = 6            # JSON files written to the landing zone per drop
MAX_FILES_PER_TRIGGER = 2     # files consumed by each streaming micro-batch
DEPARTURE_BOARD_STATION = "Lisboa Oriente"

# ---- Graph analytics --------------------------------------------------------
PAGERANK_DAMPING = 0.85
PAGERANK_MAX_ITER = 100
PAGERANK_TOLERANCE = 1e-5

# ---- Demand projection (section 11) -----------------------------------------
COUNTRY = "PT"
TREND_START_YEAR = 2023       # first full year after the pandemic restrictions
BACKTEST_LAST_TRAIN_YEAR = 2024
LONG_RUN_FIRST_YEAR = 2004    # long-run growth rate used by the lower-bound scenario
LONG_RUN_LAST_YEAR = 2019
FORECAST_END_YEAR = 2030
PANDEMIC_YEARS = (2020, 2021, 2022)


def table_name(name: str) -> str:
    """Return the fully qualified Unity Catalog name of a project table."""
    return f"{CATALOG}.{SCHEMA}.{name}"


print(f"Project schema : {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create the schema and the volumes, and reset the tables
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
    "COMMENT 'ABD group project: Comboios de Portugal, present and future (bronze, silver and gold tables)'"
)

# Idempotency: drop every table of a previous run. The raw files are kept.
for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").filter(~F.col("isTemporary")).collect():
    spark.sql(f"DROP TABLE IF EXISTS {table_name(row['tableName'])}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{RAW_VOLUME}")
spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{SCHEMA}.{LANDING_VOLUME}")
spark.sql(f"CREATE VOLUME {CATALOG}.{SCHEMA}.{LANDING_VOLUME}")
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{RAW_VOLUME}"
LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{LANDING_VOLUME}"

LANDING_PATH = f"{LANDING_ROOT}/departure_events"   # landing zone for streaming JSON events
CHECKPOINT_PATH = f"{LANDING_ROOT}/_checkpoints"     # one sub-folder per streaming query

print(f"Raw files      : {RAW_PATH}")
print(f"Landing zone   : {LANDING_PATH}")
print(f"Checkpoints    : {CHECKPOINT_PATH}")

# COMMAND ----------

# DBTITLE 1,Helper functions
def save_table(df: DataFrame, name: str, comment: str) -> str:
    """Overwrite a managed Delta table with `df` and document it with a comment."""
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
    """Number of rows of a project table."""
    return spark.table(table_name(name)).count()


def week_label(week_start: str) -> str:
    """Human readable label of a reference week."""
    return f"Week of {week_start}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data sources
# MAGIC
# MAGIC | Source | Files | Content | Origin |
# MAGIC |---|---|---|---|
# MAGIC | **CP timetable (GTFS)** | `agency.txt`, `routes.txt`, `stops.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, `calendar_dates.txt` | The official public timetable of CP in the *General Transit Feed Specification* format: stations, trains, the time of every stop and the days on which each train runs | `https://publico.cp.pt/gtfs/gtfs.zip` |
# MAGIC | **Eurostat rail passengers** | `rail_pa_total.tsv` (annual, all countries), `rail_pa_quartal.tsv` (quarterly, all countries) | Passengers transported by rail (thousand passengers and million passenger-kilometres) | Eurostat datasets `rail_pa_total` and `rail_pa_quartal` |
# MAGIC
# MAGIC Both sources were downloaded on **16 November 2026** (the CP feed had been generated the day before) and uploaded unchanged to the Volume `raw_files`
# MAGIC (Catalog > `abd_cp_rail` > `raw_files` > *Upload to this volume*). Databricks Free Edition restricts outbound
# MAGIC internet access, so the files are uploaded instead of being downloaded by the notebook.
# MAGIC
# MAGIC **Scope note.** The Eurostat figures cover all rail passenger operators in Portugal, not only CP. They are used
# MAGIC as the measure of rail demand in the country.

# COMMAND ----------

# DBTITLE 1,Check that all source files are in the Volume
expected_files = [f"{name}.txt" for name in GTFS_FILES] + [f"{name}.tsv" for name in EUROSTAT_FILES]
missing_files = [f for f in expected_files if not os.path.exists(f"{RAW_PATH}/{f}")]

if missing_files:
    raise FileNotFoundError(
        f"Missing source files in {RAW_PATH}: {missing_files}. "
        "Upload the nine files of the project data folder to this Volume and run the notebook again."
    )

print(f"{'File':<24}{'Size (KB)':>12}")
for file_name in expected_files:
    print(f"{file_name:<24}{os.path.getsize(f'{RAW_PATH}/{file_name}') / 1024:>12,.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze layer: raw ingestion
# MAGIC Every file becomes one Bronze table. Nothing is interpreted at this stage: all columns are kept as text, and two
# MAGIC technical columns record **which file** each row came from and **when** it was ingested.
# MAGIC
# MAGIC * The GTFS files are comma-separated with a header.
# MAGIC * The Eurostat files are ingested as **raw lines of text**. Their layout is not relational (one column per year or
# MAGIC   quarter, values mixed with status flags), so parsing is left to the Silver layer.

# COMMAND ----------

# DBTITLE 1,Ingest the GTFS files and the Eurostat files into Bronze
def with_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Add the source file and the ingestion timestamp to a DataFrame read from files."""
    return df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn(
        "_ingested_at", F.current_timestamp()
    )


for name in GTFS_FILES:
    raw_df = spark.read.option("header", True).option("inferSchema", False).csv(f"{RAW_PATH}/{name}.txt")
    save_table(
        with_ingestion_metadata(raw_df),
        f"bronze_gtfs_{name}",
        f"BRONZE - CP GTFS file {name}.txt as received, all columns as text",
    )

for name in EUROSTAT_FILES:
    raw_df = spark.read.text(f"{RAW_PATH}/{name}.tsv")
    save_table(
        with_ingestion_metadata(raw_df),
        f"bronze_eurostat_{name}",
        f"BRONZE - Eurostat file {name}.tsv as received, one row per line of text",
    )

# COMMAND ----------

# DBTITLE 1,First look at the raw timetable
display(spark.table(table_name("bronze_gtfs_stop_times")).limit(5))
display(spark.table(table_name("bronze_gtfs_calendar")).limit(5))

# COMMAND ----------

# DBTITLE 1,First look at the raw Eurostat file
display(
    spark.table(table_name("bronze_eurostat_rail_pa_total"))
    .select(F.substring("value", 1, 110).alias("first_110_characters_of_the_line"))
    .limit(4)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Observations.**
# MAGIC * `stop_times` has one row per stop of each train, with times as text. Times can exceed `24:00:00` for trains that
# MAGIC   run past midnight, so they cannot be cast to a timestamp directly.
# MAGIC * `calendar` does not list dates: it gives a validity period and seven weekday flags per service. The days on
# MAGIC   which a train really runs must be **generated** from it and corrected with the exceptions in `calendar_dates`.
# MAGIC * The Eurostat file packs three dimensions in the first column, has one column per period, and marks missing
# MAGIC   or confidential values with `:` and letter flags.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver layer: cleaning, modelling and data quality
# MAGIC
# MAGIC ### 6.1 Dimensions: stations and services
# MAGIC The feed identifies the type of train only by a short code or a line name. A small reference table translates
# MAGIC it into the commercial service of CP and into three service groups used throughout the analysis.

# COMMAND ----------

# DBTITLE 1,dim_station
dim_station_df = spark.table(table_name("bronze_gtfs_stops")).select(
    F.col("stop_id").alias("station_id"),
    F.regexp_replace(F.trim("stop_name"), r"\s+", " ").alias("station_name"),
    F.col("stop_lat").cast("double").alias("latitude"),
    F.col("stop_lon").cast("double").alias("longitude"),
)

save_table(dim_station_df, "dim_station", "SILVER dimension - CP stations with coordinates")

# COMMAND ----------

# DBTITLE 1,dim_service: commercial services of CP
dim_service_df = spark.createDataFrame(
    [
        ("AP", "Alfa Pendular", "Long distance", 1),
        ("IC", "Intercidades", "Long distance", 2),
        ("IR", "InterRegional", "Regional", 3),
        ("R", "Regional", "Regional", 4),
        ("Linha de Sintra", "Urbanos de Lisboa", "Urban", 5),
        ("Linha da Azambuja", "Urbanos de Lisboa", "Urban", 5),
        ("Linha de Cascais", "Urbanos de Lisboa", "Urban", 5),
        ("Linha do Sado", "Urbanos de Lisboa", "Urban", 5),
        ("Linha de Aveiro", "Urbanos do Porto", "Urban", 6),
        ("Linha de Braga", "Urbanos do Porto", "Urban", 6),
        ("Linha de Guimaraes", "Urbanos do Porto", "Urban", 6),
        ("Linha do Marco", "Urbanos do Porto", "Urban", 6),
        ("Linha de Leixoes", "Urbanos do Porto", "Urban", 6),
        ("U", "Urbanos de Coimbra", "Urban", 7),
    ],
    "route_short_name STRING, service_name STRING, service_group STRING, service_order INT",
)

save_table(
    dim_service_df,
    "dim_service",
    "SILVER dimension - maps the GTFS route short name to the CP commercial service and service group",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Stop times and segments
# MAGIC * Times are converted to **seconds after midnight of the service day**, which keeps trains that run past midnight
# MAGIC   correct (`24:30:00` becomes 88,200 seconds).
# MAGIC * The **last stop** of each trip is flagged with a window function. A train only arrives there, so that row is a
# MAGIC   *call* at the station but not a *departure*.
# MAGIC * A **segment** is the run between two consecutive stops of a train. It is obtained with the window function
# MAGIC   `LEAD` over the stops of each trip, and its length is the great-circle (haversine) distance between the two
# MAGIC   stations.
# MAGIC
# MAGIC > **Assumption.** The feed has no track geometry, so distances are straight lines between consecutive stations.
# MAGIC > They are a lower bound of the real track length, and so are the speeds derived from them.

# COMMAND ----------

# DBTITLE 1,silver_stop_times
def time_to_seconds(column_name: str):
    """Convert a GTFS 'HH:MM:SS' text (hours may exceed 23) into seconds after midnight."""
    parts = F.split(F.col(column_name), ":")
    return (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )


silver_stop_times_df = (
    spark.table(table_name("bronze_gtfs_stop_times"))
    .select(
        "trip_id",
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        F.col("stop_id").alias("station_id"),
        time_to_seconds("arrival_time").alias("arrival_sec"),
        time_to_seconds("departure_time").alias("departure_sec"),
    )
    # the last stop of a trip is an arrival only: it must not be counted as a departure
    .withColumn(
        "is_last_stop",
        F.col("stop_sequence") == F.max("stop_sequence").over(Window.partitionBy("trip_id")),
    )
)

save_table(
    silver_stop_times_df,
    "silver_stop_times",
    "SILVER - one row per stop of each trip, times in seconds after midnight of the service day, last stop flagged",
)

# COMMAND ----------

# DBTITLE 1,silver_trip_segments: runs between consecutive stops
EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in kilometres between two points given in decimal degrees."""
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = F.sin(d_lat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(d_lon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


stations = spark.table(table_name("dim_station"))
trip_order = Window.partitionBy("trip_id").orderBy("stop_sequence")

silver_trip_segments_df = (
    spark.table(table_name("silver_stop_times"))
    .withColumn("to_station_id", F.lead("station_id").over(trip_order))
    .withColumn("to_arrival_sec", F.lead("arrival_sec").over(trip_order))
    .filter(F.col("to_station_id").isNotNull())
    .join(
        stations.select(
            F.col("station_id"), F.col("latitude").alias("from_lat"), F.col("longitude").alias("from_lon")
        ),
        "station_id",
    )
    .join(
        stations.select(
            F.col("station_id").alias("to_station_id"),
            F.col("latitude").alias("to_lat"),
            F.col("longitude").alias("to_lon"),
        ),
        "to_station_id",
    )
    .select(
        "trip_id",
        "stop_sequence",
        F.col("station_id").alias("from_station_id"),
        "to_station_id",
        F.col("departure_sec"),
        F.col("to_arrival_sec").alias("arrival_sec"),
        ((F.col("to_arrival_sec") - F.col("departure_sec")) / 60.0).alias("run_min"),
        haversine_km(F.col("from_lat"), F.col("from_lon"), F.col("to_lat"), F.col("to_lon")).alias("distance_km"),
    )
)

save_table(
    silver_trip_segments_df,
    "silver_trip_segments",
    "SILVER - run between two consecutive stops of a trip, with running time and straight-line distance",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Trips
# MAGIC One row per train run defined in the timetable, enriched with its commercial service, its origin and destination,
# MAGIC its duration, its length and its commercial speed.

# COMMAND ----------

# DBTITLE 1,silver_trips
trip_endpoints = (
    spark.table(table_name("silver_stop_times"))
    .groupBy("trip_id")
    .agg(
        F.count("*").alias("stops"),
        F.min_by("station_id", "stop_sequence").alias("origin_station_id"),
        F.max_by("station_id", "stop_sequence").alias("destination_station_id"),
        F.min_by("departure_sec", "stop_sequence").alias("departure_sec"),
        F.max_by("arrival_sec", "stop_sequence").alias("arrival_sec"),
    )
)
trip_lengths = (
    spark.table(table_name("silver_trip_segments"))
    .groupBy("trip_id")
    .agg(F.sum("distance_km").alias("distance_km"))
)
routes = spark.table(table_name("bronze_gtfs_routes")).select("route_id", "route_short_name")
services = F.broadcast(spark.table(table_name("dim_service")))

silver_trips_df = (
    spark.table(table_name("bronze_gtfs_trips"))
    .select(
        "trip_id",
        "service_id",
        "route_id",
        F.col("trip_short_name").alias("train_number"),
        F.col("trip_headsign").alias("headsign"),
    )
    .join(routes, "route_id", "left")
    .join(services, "route_short_name", "left")
    .join(trip_endpoints, "trip_id", "left")
    .join(trip_lengths, "trip_id", "left")
    .join(
        stations.select(
            F.col("station_id").alias("origin_station_id"), F.col("station_name").alias("origin_station")
        ),
        "origin_station_id",
        "left",
    )
    .join(
        stations.select(
            F.col("station_id").alias("destination_station_id"),
            F.col("station_name").alias("destination_station"),
        ),
        "destination_station_id",
        "left",
    )
    .withColumn("duration_min", (F.col("arrival_sec") - F.col("departure_sec")) / 60.0)
    .withColumn("speed_kmh", F.try_divide(F.col("distance_km"), F.col("duration_min") / 60.0))
    .withColumn(
        "timetable_version",
        F.expr("CAST(try_to_timestamp(regexp_extract(service_id, '_([0-9]{8})$', 1), 'yyyyMMdd') AS DATE)"),
    )
    .select(
        "trip_id",
        "service_id",
        "train_number",
        "route_short_name",
        "service_name",
        "service_group",
        "service_order",
        "origin_station_id",
        "origin_station",
        "destination_station_id",
        "destination_station",
        "headsign",
        "departure_sec",
        "arrival_sec",
        "duration_min",
        "stops",
        "distance_km",
        "speed_kmh",
        "timetable_version",
    )
)

save_table(
    silver_trips_df,
    "silver_trips",
    "SILVER - one row per train run of the timetable, with service, endpoints, duration, distance and speed",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Operating days: expanding the calendar
# MAGIC This is the step that turns a compact timetable into an explicit fact table. For every service, all dates of its
# MAGIC validity period are generated with `sequence` and `explode`, the weekday flags select the days on which it runs,
# MAGIC and the exceptions of `calendar_dates` are applied (type 2 removes a date, type 1 adds one).
# MAGIC The result has one row for **each day on which each train runs**.

# COMMAND ----------

# DBTITLE 1,silver_service_days
calendar_df = spark.table(table_name("bronze_gtfs_calendar")).select(
    "service_id",
    F.to_date("start_date", "yyyyMMdd").alias("start_date"),
    F.to_date("end_date", "yyyyMMdd").alias("end_date"),
    *[F.col(day).cast("int").alias(day) for day in
      ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")],
)
exceptions_df = spark.table(table_name("bronze_gtfs_calendar_dates")).select(
    "service_id",
    F.to_date("date", "yyyyMMdd").alias("service_date"),
    F.col("exception_type").cast("int").alias("exception_type"),
)

regular_days = (
    calendar_df.withColumn("service_date", F.explode(F.sequence("start_date", "end_date")))
    .withColumn(
        "runs",
        F.when(F.dayofweek("service_date") == 1, F.col("sunday"))
        .when(F.dayofweek("service_date") == 2, F.col("monday"))
        .when(F.dayofweek("service_date") == 3, F.col("tuesday"))
        .when(F.dayofweek("service_date") == 4, F.col("wednesday"))
        .when(F.dayofweek("service_date") == 5, F.col("thursday"))
        .when(F.dayofweek("service_date") == 6, F.col("friday"))
        .otherwise(F.col("saturday")),
    )
    .filter(F.col("runs") == 1)
    .select("service_id", "service_date")
)
removed_days = exceptions_df.filter(F.col("exception_type") == 2).select("service_id", "service_date")
added_days = exceptions_df.filter(F.col("exception_type") == 1).select("service_id", "service_date")

silver_service_days_df = (
    regular_days.join(removed_days, ["service_id", "service_date"], "left_anti")
    .union(added_days)
    .distinct()
)

save_table(
    silver_service_days_df,
    "silver_service_days",
    "SILVER - one row per service and date on which it runs (calendar expanded, exceptions applied)",
)

# COMMAND ----------

# DBTITLE 1,How much data does the timetable really describe?
n_trips = row_count("silver_trips")
n_stop_times = row_count("silver_stop_times")
n_service_days = row_count("silver_service_days")
n_stop_events = (
    spark.table(table_name("silver_service_days"))
    .join(spark.table(table_name("silver_trips")).select("service_id", "stops"), "service_id")
    .agg(F.sum("stops"))
    .first()[0]
)
feed_period = spark.table(table_name("silver_service_days")).agg(
    F.min("service_date").alias("first_day"), F.max("service_date").alias("last_day")
).first()

print(f"Period covered by the feed          : {feed_period['first_day']} to {feed_period['last_day']}")
print(f"Train runs defined in the timetable : {n_trips:>12,}")
print(f"Stop times defined in the timetable : {n_stop_times:>12,}")
print(f"Train runs after calendar expansion : {n_service_days:>12,}  (one per train and operating day)")
print(f"Stop events they represent          : {n_stop_events:>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Eurostat: from a wide text file to a tidy table
# MAGIC The raw lines are parsed with Spark only:
# MAGIC 1. the header line gives the list of periods;
# MAGIC 2. `posexplode` turns the values of each line into rows and joins them to the periods **by position** (unpivot);
# MAGIC 3. the first column is split into frequency, unit and country;
# MAGIC 4. each cell is separated into a numeric value and a status flag (`:` means not available, `C` confidential,
# MAGIC    `b` break in series).

# COMMAND ----------

# DBTITLE 1,silver_rail_passengers
def parse_eurostat(bronze_table: str, dataset: str) -> DataFrame:
    """Unpivot a raw Eurostat TSV (one text line per row) into one row per series and period."""
    lines = spark.table(table_name(bronze_table)).select("value")
    is_header = F.col("value").startswith("freq")

    periods = lines.filter(is_header).select(
        F.posexplode(F.split("value", "\t")).alias("position", "period")
    ).select("position", F.trim("period").alias("period"))

    cells = lines.filter(~is_header).select(
        F.split("value", "\t").getItem(0).alias("series_key"),
        F.posexplode(F.split("value", "\t")).alias("position", "cell"),
    )

    return (
        cells.filter(F.col("position") > 0)
        .join(F.broadcast(periods), "position")
        .select(
            F.lit(dataset).alias("dataset"),
            F.split("series_key", ",").getItem(0).alias("frequency"),
            F.split("series_key", ",").getItem(1).alias("unit"),
            F.split("series_key", ",").getItem(2).alias("country_code"),
            "period",
            F.substring("period", 1, 4).cast("int").alias("year"),
            F.when(F.col("period").contains("-Q"), F.substring("period", 7, 1).cast("int")).alias("quarter"),
            F.expr("try_cast(nullif(regexp_extract(cell, '^([0-9.]+)', 1), '') AS DOUBLE)").alias("value"),
            F.expr("nullif(trim(regexp_replace(cell, '[0-9.:@]', '')), '')").alias("flag"),
        )
    )


silver_rail_passengers_df = parse_eurostat("bronze_eurostat_rail_pa_total", "rail_pa_total").unionByName(
    parse_eurostat("bronze_eurostat_rail_pa_quartal", "rail_pa_quartal")
)

save_table(
    silver_rail_passengers_df,
    "silver_rail_passengers",
    "SILVER - Eurostat rail passengers, one row per dataset, unit, country and period (value NULL when not available)",
)

display(
    spark.table(table_name("silver_rail_passengers"))
    .filter((F.col("country_code") == COUNTRY) & (F.col("frequency") == "A") & (F.col("year") >= 2019))
    .orderBy("unit", "year")
)

# COMMAND ----------

# DBTITLE 1,dim_country
dim_country_df = spark.createDataFrame(
    [
        ("AT", "Austria"), ("BA", "Bosnia and Herzegovina"), ("BE", "Belgium"), ("BG", "Bulgaria"),
        ("CH", "Switzerland"), ("CZ", "Czechia"), ("DE", "Germany"), ("DK", "Denmark"), ("EE", "Estonia"),
        ("EL", "Greece"), ("ES", "Spain"), ("FI", "Finland"), ("FR", "France"), ("HR", "Croatia"),
        ("HU", "Hungary"), ("IE", "Ireland"), ("IT", "Italy"), ("LI", "Liechtenstein"), ("LT", "Lithuania"),
        ("LU", "Luxembourg"), ("LV", "Latvia"), ("ME", "Montenegro"), ("MK", "North Macedonia"),
        ("NL", "Netherlands"), ("NO", "Norway"), ("PL", "Poland"), ("PT", "Portugal"), ("RO", "Romania"),
        ("SE", "Sweden"), ("SI", "Slovenia"), ("SK", "Slovakia"), ("TR", "Turkiye"), ("UA", "Ukraine"),
        ("UK", "United Kingdom"), ("XK", "Kosovo"),
    ],
    "country_code STRING, country_name STRING",
)

save_table(dim_country_df, "dim_country", "SILVER dimension - Eurostat country codes (aggregates such as EU27 excluded)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 Data quality report
# MAGIC Each check counts the records that **violate** a rule. A check passes when the count is zero.
# MAGIC The checks cover referential integrity between the files, the validity of times and coordinates, the mapping of
# MAGIC services, and the reconciliation between Bronze and Silver.

# COMMAND ----------

# DBTITLE 1,Data quality checks
trips_t = spark.table(table_name("silver_trips"))
stop_times_t = spark.table(table_name("silver_stop_times"))
segments_t = spark.table(table_name("silver_trip_segments"))
stations_t = spark.table(table_name("dim_station"))
service_days_t = spark.table(table_name("silver_service_days"))
calendar_ids = spark.table(table_name("bronze_gtfs_calendar")).select("service_id").distinct()

quality_checks = [
    ("Stations with a duplicated identifier",
     stations_t.groupBy("station_id").count().filter("count > 1").count()),
    ("Stations with a duplicated name",
     stations_t.groupBy("station_name").count().filter("count > 1").count()),
    ("Stations without coordinates or outside mainland Portugal",
     stations_t.filter(
         F.col("latitude").isNull() | F.col("longitude").isNull()
         | ~F.col("latitude").between(*PORTUGAL_LAT_RANGE)
         | ~F.col("longitude").between(*PORTUGAL_LON_RANGE)
     ).count()),
    ("Trips with a duplicated identifier",
     trips_t.groupBy("trip_id").count().filter("count > 1").count()),
    ("Trips whose route has no commercial service mapped",
     trips_t.filter(F.col("service_name").isNull()).count()),
    ("Trips without stop times",
     trips_t.filter(F.col("stops").isNull()).count()),
    ("Trips whose service is missing from the calendar",
     trips_t.join(calendar_ids, "service_id", "left_anti").count()),
    ("Stop times of an unknown trip",
     stop_times_t.join(trips_t.select("trip_id"), "trip_id", "left_anti").count()),
    ("Stop times at an unknown station",
     stop_times_t.join(stations_t.select("station_id"), "station_id", "left_anti").count()),
    ("Stop times with a missing time",
     stop_times_t.filter(F.col("arrival_sec").isNull() | F.col("departure_sec").isNull()).count()),
    ("Stop times where the train departs before it arrives",
     stop_times_t.filter(F.col("departure_sec") < F.col("arrival_sec")).count()),
    ("Duplicated stop sequence inside a trip",
     stop_times_t.groupBy("trip_id", "stop_sequence").count().filter("count > 1").count()),
    ("Segments with zero or negative running time",
     segments_t.filter(F.col("run_min") <= 0).count()),
    (f"Segments faster than {MAX_PLAUSIBLE_SPEED_KMH} km/h in a straight line",
     segments_t.filter(
         F.try_divide(F.col("distance_km"), F.col("run_min") / 60.0) > MAX_PLAUSIBLE_SPEED_KMH
     ).count()),
    ("Same train number running twice on the same day",
     service_days_t.join(trips_t.select("service_id", "train_number"), "service_id")
     .groupBy("service_date", "train_number").count().filter("count > 1").count()),
    ("Bronze rows of stop_times not present in Silver",
     row_count("bronze_gtfs_stop_times") - row_count("silver_stop_times")),
    ("Bronze rows of trips not present in Silver",
     row_count("bronze_gtfs_trips") - row_count("silver_trips")),
    ("Bronze rows of stops not present in Silver",
     row_count("bronze_gtfs_stops") - row_count("dim_station")),
]

print(f"{'Data quality check':<62}{'Violations':>11}  Result")
print("-" * 82)
for description, violations in quality_checks:
    print(f"{description:<62}{violations:>11,}  {'PASS' if violations == 0 else 'REVIEW'}")

n_checks_failed = sum(1 for _, violations in quality_checks if violations != 0)
print("-" * 82)
print(f"{len(quality_checks) - n_checks_failed} of {len(quality_checks)} checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold layer, the present: what CP runs today
# MAGIC
# MAGIC ### 7.1 A veracity trap: the feed is a rolling snapshot
# MAGIC Before aggregating anything, we look at how many trains the feed contains for each month of its period.

# COMMAND ----------

# DBTITLE 1,Trains per day in the feed, by month and service group
monthly_coverage_df = spark.sql(
    f"""
    SELECT date_format(d.service_date, 'yyyy-MM')                            AS month,
           ROUND(COUNT(*) / COUNT(DISTINCT d.service_date), 0)               AS trains_per_day,
           ROUND(COUNT(CASE WHEN t.service_group = 'Long distance' THEN 1 END) / COUNT(DISTINCT d.service_date), 0)
               AS long_distance,
           ROUND(COUNT(CASE WHEN t.service_group = 'Regional' THEN 1 END) / COUNT(DISTINCT d.service_date), 0)
               AS regional,
           ROUND(COUNT(CASE WHEN t.service_name = 'Urbanos de Lisboa' THEN 1 END) / COUNT(DISTINCT d.service_date), 0)
               AS urbanos_lisboa,
           ROUND(COUNT(CASE WHEN t.service_name = 'Urbanos do Porto' THEN 1 END) / COUNT(DISTINCT d.service_date), 0)
               AS urbanos_porto,
           ROUND(COUNT(CASE WHEN t.route_short_name = 'Linha de Cascais' THEN 1 END) / COUNT(DISTINCT d.service_date), 0)
               AS of_which_linha_de_cascais
    FROM {table_name('silver_service_days')} d
    JOIN {table_name('silver_trips')} t ON d.service_id = t.service_id
    GROUP BY date_format(d.service_date, 'yyyy-MM')
    ORDER BY month
    """
)

display(monthly_coverage_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **What this table shows, and why it matters.**
# MAGIC * **The past is incomplete.** For the months before the download the feed contains far fewer trains, and some lines
# MAGIC   are missing altogether (the Cascais line, one of the busiest in the country, shows no trains for most of 2026).
# MAGIC   The feed is published to tell passengers what runs from now on, so timetable versions that have already expired
# MAGIC   are removed from it. Reading the apparent growth during 2026 as an increase of service would be wrong.
# MAGIC * **The future is partially published.** After the annual timetable change of 13 December 2026 some services are
# MAGIC   fully present and others are not there yet (section 10 quantifies this).
# MAGIC
# MAGIC **Decision.** "The present" is measured on the **reference week of 16 to 22 November 2026**, the first full week
# MAGIC after the download, for which the feed is complete. Dates before the download are never used.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Gold tables
# MAGIC The function below returns every train run of a reference week (one row per train and day). It is used for the
# MAGIC present here and for the future in section 10, so both periods are measured in exactly the same way.

# COMMAND ----------

# DBTITLE 1,Train runs of a reference week
def train_runs_of_week(week_start: str) -> DataFrame:
    """One row per train and operating day for the 7 days starting on `week_start` (a Monday)."""
    return (
        spark.table(table_name("silver_service_days"))
        .filter(
            (F.col("service_date") >= F.lit(week_start).cast("date"))
            & (F.col("service_date") < F.date_add(F.lit(week_start).cast("date"), 7))
        )
        .join(spark.table(table_name("silver_trips")), "service_id")
        .withColumn("day_of_week", F.dayofweek("service_date"))   # 1 = Sunday ... 7 = Saturday
        .withColumn("day_name", F.date_format("service_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("service_date").isin(1, 7))
    )


def weekly_supply(week_start: str, period: str) -> DataFrame:
    """Supply indicators of a reference week per commercial service."""
    return (
        train_runs_of_week(week_start)
        .groupBy("service_order", "service_group", "service_name")
        .agg(
            F.count("*").alias("trains_per_week"),
            F.round(F.count(F.when(~F.col("is_weekend"), 1)) / 5, 1).alias("trains_per_weekday"),
            F.round(F.count(F.when(F.col("is_weekend"), 1)) / 2, 1).alias("trains_per_weekend_day"),
            F.round(F.sum("distance_km"), 0).alias("train_km_per_week"),
            F.round(F.avg("distance_km"), 1).alias("avg_distance_km"),
            F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
            F.round(F.avg("stops"), 1).alias("avg_stops"),
            F.round(F.sum("distance_km") / (F.sum("duration_min") / 60.0), 1).alias("commercial_speed_kmh"),
        )
        .withColumn("period", F.lit(period))
        .withColumn("week_start", F.lit(week_start).cast("date"))
    )


gold_weekly_supply_df = weekly_supply(PRESENT_WEEK_START, "Present").unionByName(
    weekly_supply(FUTURE_WEEK_START, "Future")
)

save_table(
    gold_weekly_supply_df,
    "gold_weekly_supply",
    "GOLD - supply indicators per commercial service for the present and the future reference weeks",
)

# COMMAND ----------

# DBTITLE 1,gold_daily_supply: trains and train-km per day from the download date onwards
gold_daily_supply_df = spark.sql(
    f"""
    SELECT d.service_date,
           t.service_group,
           t.service_name,
           COUNT(*)                     AS trains,
           ROUND(SUM(t.distance_km), 0) AS train_km
    FROM {table_name('silver_service_days')} d
    JOIN {table_name('silver_trips')} t ON d.service_id = t.service_id
    WHERE d.service_date >= DATE'{SNAPSHOT_DATE}'
    GROUP BY d.service_date, t.service_group, t.service_name
    """
)

save_table(
    gold_daily_supply_df,
    "gold_daily_supply",
    "GOLD - scheduled trains and train-km per day and commercial service, from the download date onwards",
)

# COMMAND ----------

# DBTITLE 1,gold_station_activity: train calls per station in the present week
present_runs = train_runs_of_week(PRESENT_WEEK_START)

gold_station_activity_df = (
    present_runs.select("trip_id", "service_date", "is_weekend", "service_name", "route_short_name")
    .join(spark.table(table_name("silver_stop_times")), "trip_id")
    .groupBy("station_id")
    .agg(
        F.count("*").alias("train_calls_per_week"),
        F.round(F.count(F.when(~F.col("is_weekend"), 1)) / 5, 1).alias("train_calls_per_weekday"),
        F.countDistinct("service_name").alias("services"),
        F.concat_ws(", ", F.sort_array(F.collect_set("service_name"))).alias("service_list"),
    )
    .join(spark.table(table_name("dim_station")), "station_id")
    .select(
        "station_id", "station_name", "latitude", "longitude",
        "train_calls_per_week", "train_calls_per_weekday", "services", "service_list",
    )
)

save_table(
    gold_station_activity_df,
    "gold_station_activity",
    "GOLD - trains calling at each station in the present reference week",
)

# COMMAND ----------

# DBTITLE 1,gold_hourly_departures: departures from all stations per hour of an average weekday
gold_hourly_departures_df = (
    present_runs.filter(~F.col("is_weekend"))
    .select("trip_id", "service_group")
    .join(spark.table(table_name("silver_stop_times")).filter(~F.col("is_last_stop")), "trip_id")
    .withColumn("hour", F.floor(F.col("departure_sec") / 3600) % 24)
    .groupBy("hour", "service_group")
    .agg(F.round(F.count("*") / 5, 1).alias("departures_per_weekday"))
)

save_table(
    gold_hourly_departures_df,
    "gold_hourly_departures",
    "GOLD - station departures per hour of the day and service group, average weekday of the present week",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Q1: How much service does CP run today?

# COMMAND ----------

# DBTITLE 1,Supply per commercial service in the present week
present_supply_df = spark.sql(
    f"""
    SELECT service_group,
           service_name,
           trains_per_week,
           ROUND(100.0 * trains_per_week / SUM(trains_per_week) OVER (), 1)     AS pct_of_trains,
           trains_per_weekday,
           trains_per_weekend_day,
           train_km_per_week,
           ROUND(100.0 * train_km_per_week / SUM(train_km_per_week) OVER (), 1) AS pct_of_train_km,
           avg_distance_km,
           avg_duration_min,
           avg_stops,
           commercial_speed_kmh
    FROM {table_name('gold_weekly_supply')}
    WHERE period = 'Present'
    ORDER BY service_order
    """
)

display(present_supply_df)

# COMMAND ----------

# DBTITLE 1,Trains per day of the week and service group
trains_by_day_df = spark.sql(
    f"""
    SELECT service_date,
           date_format(service_date, 'EEEE')                               AS day_name,
           SUM(trains)                                                     AS trains,
           SUM(CASE WHEN service_group = 'Long distance' THEN trains END)  AS long_distance,
           SUM(CASE WHEN service_group = 'Regional' THEN trains END)       AS regional,
           SUM(CASE WHEN service_group = 'Urban' THEN trains END)          AS urban,
           SUM(train_km)                                                   AS train_km
    FROM {table_name('gold_daily_supply')}
    WHERE service_date >= DATE'{PRESENT_WEEK_START}'
      AND service_date < date_add(DATE'{PRESENT_WEEK_START}', 7)
    GROUP BY service_date
    ORDER BY service_date
    """
)

display(trains_by_day_df)

# COMMAND ----------

# DBTITLE 1,Chart: trains per day of the present week
trains_by_day_pd = trains_by_day_df.toPandas()

fig, ax = plt.subplots(figsize=(11, 4.5))
bottom = [0.0] * len(trains_by_day_pd)
for column, label in (("urban", "Urban"), ("regional", "Regional"), ("long_distance", "Long distance")):
    values = trains_by_day_pd[column].astype(float).tolist()
    ax.bar(trains_by_day_pd["day_name"], values, bottom=bottom, label=label)
    bottom = [b + v for b, v in zip(bottom, values)]
ax.set_title(f"Scheduled trains per day, week of {PRESENT_WEEK_START}")
ax.set_ylabel("Trains")
ax.grid(axis="y", alpha=0.3)
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Departures per hour of an average weekday
hourly_departures_df = spark.sql(
    f"""
    SELECT hour,
           SUM(departures_per_weekday)                                                    AS all_services,
           SUM(CASE WHEN service_group = 'Urban' THEN departures_per_weekday ELSE 0 END)         AS urban,
           SUM(CASE WHEN service_group = 'Regional' THEN departures_per_weekday ELSE 0 END)      AS regional,
           SUM(CASE WHEN service_group = 'Long distance' THEN departures_per_weekday ELSE 0 END) AS long_distance
    FROM {table_name('gold_hourly_departures')}
    GROUP BY hour
    ORDER BY hour
    """
)

display(hourly_departures_df)

# COMMAND ----------

# DBTITLE 1,Chart: daily profile of departures
hourly_departures_pd = hourly_departures_df.toPandas().fillna(0)

fig, ax = plt.subplots(figsize=(11, 4.5))
for column, label in (("urban", "Urban"), ("regional", "Regional"), ("long_distance", "Long distance")):
    ax.plot(hourly_departures_pd["hour"], hourly_departures_pd[column].astype(float), marker="o", label=label)
ax.set_title("Station departures per hour, average weekday of the present week")
ax.set_xlabel("Hour of the day")
ax.set_ylabel("Departures from all stations")
ax.set_xticks(range(0, 24))
ax.grid(alpha=0.3)
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,The 15 busiest stations
display(
    spark.table(table_name("gold_station_activity"))
    .select("station_name", "train_calls_per_week", "train_calls_per_weekday", "services", "service_list")
    .orderBy(F.desc("train_calls_per_week"), "station_name")
    .limit(15)
)

# COMMAND ----------

# DBTITLE 1,Chart: map of the network, stations sized by number of trains
station_activity_pd = (
    spark.table(table_name("gold_station_activity"))
    .select("station_name", "latitude", "longitude", "train_calls_per_week")
    .orderBy(F.desc("train_calls_per_week"), "station_name")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(6.5, 10))
scatter = ax.scatter(
    station_activity_pd["longitude"],
    station_activity_pd["latitude"],
    s=station_activity_pd["train_calls_per_week"].astype(float) / 12 + 4,
    c=station_activity_pd["train_calls_per_week"].astype(float),
    cmap="viridis",
    alpha=0.75,
)
labelled_stations = ["Lisboa Oriente", "Porto Campanha", "Coimbra-B", "Braga", "Faro", "Entroncamento"]
for _, station in station_activity_pd[station_activity_pd["station_name"].isin(labelled_stations)].iterrows():
    ax.annotate(station["station_name"], (station["longitude"], station["latitude"]),
                xytext=(6, 4), textcoords="offset points", fontsize=8)
fig.colorbar(scatter, ax=ax, label="Train calls per week")
ax.set_title(f"CP stations, week of {PRESENT_WEEK_START}")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.set_aspect(1.3)
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Structured Streaming: live station departure indicators
# MAGIC
# MAGIC **Q2: can station departure indicators be kept up to date as events arrive, with the same results as batch?**
# MAGIC
# MAGIC A station information system does not receive a finished table: it receives a continuous flow of small files with
# MAGIC departure events. We reproduce this scenario with the trains of **Wednesday 18 November 2026**:
# MAGIC
# MAGIC 1. An upstream system is simulated by exporting the departure events of that day as **JSON files** to a Volume
# MAGIC    (the landing zone) in **two drops**: the departures before 12:00 first, the remaining ones later.
# MAGIC 2. A chain of three Structured Streaming queries moves the events through the medallion layers:
# MAGIC
# MAGIC | Query | Source | Processing | Sink | Output mode |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | JSON files in the Volume | add file name and ingestion time | `bronze_departures_stream` | append |
# MAGIC | 2 | `bronze_departures_stream` | validation, stateful deduplication, stream-static join with `dim_station` | `silver_departures_stream` | append |
# MAGIC | 3 | `silver_departures_stream` | 1-hour tumbling window aggregation per station | `gold_station_departures_stream` | complete |
# MAGIC
# MAGIC > **Scope note.** CP does not publish an open real-time feed, so the events are the **scheduled** departures replayed
# MAGIC > as a stream. The pipeline is the one a real-time feed would use; only the origin of the events is simulated.
# MAGIC
# MAGIC Key concepts applied:
# MAGIC * **Checkpointing.** Each query stores its progress in the Volume. Started again, it processes **only the new
# MAGIC   data**, exactly once: this is what makes the pipeline incremental and fault tolerant.
# MAGIC * **Micro-batches.** `maxFilesPerTrigger` limits how many files each micro-batch reads.
# MAGIC * **Trigger.** Serverless compute supports the `availableNow` trigger: the query processes everything available,
# MAGIC   in several micro-batches, and stops. Scheduling it periodically gives near real-time processing without an
# MAGIC   always-on cluster.

# COMMAND ----------

# DBTITLE 1,Simulate the upstream system: departure events of one day, split in two drops
stream_day_events_df = (
    spark.table(table_name("silver_service_days"))
    .filter(F.col("service_date") == F.lit(STREAM_DAY).cast("date"))
    .join(spark.table(table_name("silver_trips")).select("service_id", "trip_id", "train_number", "service_name"),
          "service_id")
    .join(spark.table(table_name("silver_stop_times")).filter(~F.col("is_last_stop")), "trip_id")
    .select(
        F.concat_ws("|", F.col("service_date").cast("string"), "trip_id", F.col("stop_sequence").cast("string"))
        .alias("event_id"),
        F.col("service_date").cast("string").alias("service_date"),
        "trip_id",
        "train_number",
        "service_name",
        "station_id",
        "stop_sequence",
        # seconds after midnight are added to the service day, so trains after 24:00 fall on the next day
        F.timestamp_seconds(
            F.unix_timestamp(F.col("service_date").cast("timestamp")) + F.col("departure_sec")
        ).alias("scheduled_departure"),
    )
)

split_ts = f"{STREAM_DAY} 12:00:00"
drop_1_df = stream_day_events_df.filter(F.col("scheduled_departure") < F.lit(split_ts).cast("timestamp"))
drop_2_df = stream_day_events_df.filter(F.col("scheduled_departure") >= F.lit(split_ts).cast("timestamp"))


def land_events(df: DataFrame) -> None:
    """Write a set of departure events to the landing zone as new JSON files."""
    df.repartition(FILES_PER_DROP).write.mode("append").json(LANDING_PATH)


print(f"Departure events of {STREAM_DAY}: {stream_day_events_df.count():,}")
print(f"  drop 1 (before 12:00) : {drop_1_df.count():,}")
print(f"  drop 2 (from 12:00)   : {drop_2_df.count():,}")

# COMMAND ----------

# DBTITLE 1,Definition of the three streaming queries
# File sources need an explicit schema in streaming: there is no schema inference at run time.
departure_event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("service_date", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("train_number", StringType(), True),
        StructField("service_name", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("stop_sequence", IntegerType(), True),
        StructField("scheduled_departure", TimestampType(), True),
    ]
)


def clean_departures(events: DataFrame, dim_station: DataFrame) -> DataFrame:
    """Silver logic for departure events. Works for batch and for streaming DataFrames."""
    return (
        events.filter(
            F.col("event_id").isNotNull()
            & F.col("station_id").isNotNull()
            & F.col("scheduled_departure").isNotNull()
        )
        .dropDuplicates(["event_id"])
        .join(F.broadcast(dim_station.select("station_id", "station_name")), "station_id", "left")
        .select(
            "event_id",
            F.col("service_date").cast("date").alias("service_date"),
            "trip_id",
            "train_number",
            "service_name",
            "station_id",
            "station_name",
            "stop_sequence",
            "scheduled_departure",
        )
    )


def hourly_station_departures(departures: DataFrame) -> DataFrame:
    """Gold logic: departures per station in 1-hour tumbling windows. Batch or streaming."""
    return (
        departures.groupBy(
            F.window("scheduled_departure", "1 hour").alias("time_window"), "station_id", "station_name"
        )
        .agg(
            F.count("*").alias("departures"),
            F.min("scheduled_departure").alias("first_departure"),
            F.max("scheduled_departure").alias("last_departure"),
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "station_id",
            "station_name",
            "departures",
            "first_departure",
            "last_departure",
        )
    )


def start_bronze_stream():
    """Query 1: JSON files in the landing zone -> bronze_departures_stream (append)."""
    events = (
        spark.readStream.format("json")
        .schema(departure_event_schema)
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(LANDING_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return (
        events.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze_departures_stream")
        .trigger(availableNow=True)
        .toTable(table_name("bronze_departures_stream"))
    )


def start_silver_stream():
    """Query 2: bronze_departures_stream -> silver_departures_stream (append)."""
    bronze_stream = spark.readStream.table(table_name("bronze_departures_stream"))
    silver_stream = clean_departures(bronze_stream, spark.table(table_name("dim_station")))
    return (
        silver_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/silver_departures_stream")
        .trigger(availableNow=True)
        .toTable(table_name("silver_departures_stream"))
    )


def start_gold_stream():
    """Query 3: silver_departures_stream -> gold_station_departures_stream (complete)."""
    silver_stream = spark.readStream.table(table_name("silver_departures_stream"))
    return (
        hourly_station_departures(silver_stream)
        .writeStream.format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/gold_station_departures_stream")
        .trigger(availableNow=True)
        .toTable(table_name("gold_station_departures_stream"))
    )


def run_streaming_pipeline(label: str) -> None:
    """Run the three queries in sequence until all available data is processed."""
    for start_query in (start_bronze_stream, start_silver_stream, start_gold_stream):
        start_query().awaitTermination()
    print(f"[{label}] streaming tables after the run:")
    for name in ("bronze_departures_stream", "silver_departures_stream", "gold_station_departures_stream"):
        print(f"  {name:<32}: {row_count(name):>8,} rows")


def show_departure_board() -> None:
    """Departures per hour at the reference station, read from the streaming Gold table."""
    display(
        spark.table(table_name("gold_station_departures_stream"))
        .filter(F.col("station_name") == DEPARTURE_BOARD_STATION)
        .select(
            "station_name",
            F.date_format("window_start", "yyyy-MM-dd HH:mm").alias("hour_starting"),
            "departures",
            F.date_format("first_departure", "HH:mm").alias("first_departure"),
            F.date_format("last_departure", "HH:mm").alias("last_departure"),
        )
        .orderBy("hour_starting")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 First drop of events: the morning

# COMMAND ----------

# DBTITLE 1,Drop 1 arrives and the streaming pipeline runs
land_events(drop_1_df)
run_streaming_pipeline("after drop 1")

bronze_stream_version_after_drop_1 = (
    spark.sql(f"DESCRIBE HISTORY {table_name('bronze_departures_stream')}")
    .agg(F.max("version"))
    .first()[0]
)

# COMMAND ----------

# DBTITLE 1,Departure indicator of Lisboa Oriente after drop 1
show_departure_board()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Second drop of events: incremental processing
# MAGIC New files arrive in the landing zone and the **same** queries are started again. Thanks to the checkpoints they do
# MAGIC not reprocess the morning: only the new files are read, and the indicator is extended with the afternoon and evening.

# COMMAND ----------

# DBTITLE 1,Drop 2 arrives and the streaming pipeline runs again
land_events(drop_2_df)
run_streaming_pipeline("after drop 2")

# COMMAND ----------

# DBTITLE 1,Departure indicator of Lisboa Oriente after drop 2
show_departure_board()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Evidence of micro-batches
# MAGIC Every streaming micro-batch is one ACID commit in the Delta transaction log. The history of the streaming Bronze
# MAGIC table shows how the files were consumed: several `STREAMING UPDATE` commits for each drop.

# COMMAND ----------

# DBTITLE 1,Delta history of the streaming Bronze table: one commit per micro-batch
display(
    spark.sql(f"DESCRIBE HISTORY {table_name('bronze_departures_stream')}")
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
# MAGIC ### 8.4 Reconciliation: streaming vs batch
# MAGIC The same Silver and Gold logic (`clean_departures` and `hourly_station_departures`) is now applied **in batch**
# MAGIC to the events of the day. If the streaming pipeline is correct, both paths must give exactly the same result.

# COMMAND ----------

# DBTITLE 1,Compare the streaming tables with a batch computation
# The batch results are saved as tables, so both sides of the comparison are read from Delta.
save_table(
    clean_departures(stream_day_events_df, spark.table(table_name("dim_station"))),
    "silver_departures_batch",
    "SILVER - departure events of the replayed day computed in batch (control for the streaming pipeline)",
)
batch_silver = spark.table(table_name("silver_departures_batch"))
save_table(
    hourly_station_departures(batch_silver),
    "gold_station_departures_batch",
    "GOLD - departures per station and hour computed in batch (control for the streaming pipeline)",
)
batch_gold = spark.table(table_name("gold_station_departures_batch"))
stream_silver = spark.table(table_name("silver_departures_stream"))
stream_gold = spark.table(table_name("gold_station_departures_stream"))

n_batch_events = batch_silver.count()
n_stream_events = stream_silver.count()
only_in_batch = batch_silver.join(stream_silver, "event_id", "left_anti").count()
only_in_stream = stream_silver.join(batch_silver, "event_id", "left_anti").count()

gold_keys = ["window_start", "station_id", "departures", "first_departure", "last_departure"]
gold_differences = (
    batch_gold.select(*gold_keys).exceptAll(stream_gold.select(*gold_keys)).count()
    + stream_gold.select(*gold_keys).exceptAll(batch_gold.select(*gold_keys)).count()
)

print(f"Silver events   batch = {n_batch_events:,}   streaming = {n_stream_events:,}")
print(f"Events only in the batch result     : {only_in_batch:,}")
print(f"Events only in the streaming result : {only_in_stream:,}")
print(f"Gold rows       batch = {batch_gold.count():,}   streaming = {stream_gold.count():,}")
print(f"Gold rows that differ               : {gold_differences:,}")

streaming_matches_batch = (
    n_batch_events == n_stream_events and only_in_batch == 0 and only_in_stream == 0 and gold_differences == 0
)
print(f"\nStreaming results identical to batch results : {'YES' if streaming_matches_batch else 'NO'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Graph analytics: the network of stations
# MAGIC
# MAGIC **Q3: which stations are the hubs, and which sections carry the most trains?**
# MAGIC
# MAGIC A railway is a graph by nature:
# MAGIC * **Vertices** are the stations served in the present week.
# MAGIC * **Edges** are directed sections between two **consecutive stops** of a train, **weighted** by the number of
# MAGIC   trains that run them in the week.
# MAGIC
# MAGIC Vertices and edges are stored as two DataFrames with the `id` / `src` / `dst` convention used by GraphFrames.
# MAGIC Serverless compute cannot install the GraphFrames JVM library, so the graph algorithms are written directly with
# MAGIC **DataFrame joins and aggregations**, which is also what a graph library does internally.

# COMMAND ----------

# DBTITLE 1,Build the graph: vertices and edges
graph_edges_df = (
    train_runs_of_week(PRESENT_WEEK_START)
    .select("trip_id", "service_name")
    .join(spark.table(table_name("silver_trip_segments")), "trip_id")
    .groupBy(F.col("from_station_id").alias("src"), F.col("to_station_id").alias("dst"))
    .agg(
        F.count("*").alias("weight"),
        F.countDistinct("service_name").alias("services"),
        F.round(F.avg("distance_km"), 2).alias("distance_km"),
        F.round(F.avg("run_min"), 1).alias("avg_run_min"),
    )
)

save_table(
    graph_edges_df,
    "gold_graph_edges",
    "GOLD graph - directed section between consecutive stops, weight = trains in the present week",
)
edges = spark.table(table_name("gold_graph_edges"))

graph_vertices_df = (
    edges.select(F.col("src").alias("id"))
    .union(edges.select(F.col("dst").alias("id")))
    .distinct()
    .join(
        spark.table(table_name("dim_station")).select(
            F.col("station_id").alias("id"), "station_name", "latitude", "longitude"
        ),
        "id",
    )
)

save_table(graph_vertices_df, "gold_graph_vertices", "GOLD graph - one vertex per station served in the present week")
vertices = spark.table(table_name("gold_graph_vertices"))

n_vertices = vertices.count()
n_edges = edges.count()
print(f"\nVertices (stations)        : {n_vertices:,}")
print(f"Directed edges (sections)  : {n_edges:,}")
print(f"Density                    : {n_edges / (n_vertices * (n_vertices - 1)):.3%} of all possible connections")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Degrees: terminals, through stations and junctions
# MAGIC * **Neighbours**: number of distinct stations directly connected to a station, in either direction.
# MAGIC   One neighbour is the end of a line, two is a through station, three or more is a **junction** where lines meet.
# MAGIC * **Weighted degree**: number of trains that depart from (out) or arrive at (in) the station per week.
# MAGIC
# MAGIC Because an edge joins consecutive *stops*, a fast train that does not call at intermediate stations creates a direct
# MAGIC edge between distant stations. A high number of neighbours therefore identifies the stations where many
# MAGIC different services meet.

# COMMAND ----------

# DBTITLE 1,Degree metrics per station
neighbour_pairs = edges.select(F.col("src").alias("id"), F.col("dst").alias("neighbour")).union(
    edges.select(F.col("dst").alias("id"), F.col("src").alias("neighbour"))
)
neighbours = neighbour_pairs.groupBy("id").agg(F.countDistinct("neighbour").alias("neighbours"))
out_degrees = edges.groupBy(F.col("src").alias("id")).agg(F.sum("weight").alias("trains_out"))
in_degrees = edges.groupBy(F.col("dst").alias("id")).agg(F.sum("weight").alias("trains_in"))

station_degrees_df = (
    vertices.join(neighbours, "id", "left")
    .join(out_degrees, "id", "left")
    .join(in_degrees, "id", "left")
    .fillna(0, subset=["neighbours", "trains_out", "trains_in"])
    .withColumn(
        "station_type",
        F.when(F.col("neighbours") <= 1, "Terminal")
        .when(F.col("neighbours") == 2, "Through station")
        .otherwise("Junction"),
    )
)

display(
    station_degrees_df.groupBy("station_type")
    .agg(F.count("*").alias("stations"), F.round(F.avg("trains_out"), 0).alias("avg_trains_out_per_week"))
    .orderBy(F.desc("stations"))
)

# COMMAND ----------

# DBTITLE 1,Stations where the most services meet (largest number of neighbours)
display(
    station_degrees_df.select("station_name", "neighbours", "trains_out", "trains_in", "station_type")
    .orderBy(F.desc("neighbours"), F.desc("trains_out"), "station_name")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 PageRank: the most important hubs
# MAGIC Counting trains measures local activity only. **PageRank** measures the importance of a station inside the whole
# MAGIC network: a station is important if it receives many trains **from stations that are themselves important**.
# MAGIC It can be read as the long-run probability of finding, at that station, a traveller who keeps boarding trains at random.
# MAGIC
# MAGIC We implement the weighted PageRank iteratively with DataFrames:
# MAGIC
# MAGIC `rank(v) = (1 - d) / N  +  d * ( SUM over edges u->v of rank(u) * weight(u,v) / trains_out(u)  +  dangling / N )`
# MAGIC
# MAGIC where `d` is the damping factor, `N` the number of stations and `dangling` the total rank of stations without
# MAGIC outgoing trains (redistributed uniformly so that the ranks always sum to 1).
# MAGIC
# MAGIC The iterations stop when the ranks change by less than the tolerance. A railway is a sparse graph made of long
# MAGIC chains of stations, so rank spreads slowly and more iterations are needed than in a dense graph.
# MAGIC
# MAGIC Each iteration is a distributed join and aggregation over the edges. The rank vector has one row per station, so
# MAGIC it is small: it is collected to the driver and turned into a new DataFrame, which keeps the query plan of every
# MAGIC iteration short instead of letting the lineage grow with the number of iterations.

# COMMAND ----------

# DBTITLE 1,Weighted PageRank implemented with DataFrames
def weighted_pagerank(vertices: DataFrame, edges: DataFrame, damping: float, max_iter: int, tolerance: float):
    """Iterative weighted PageRank. Returns (ranks DataFrame [id, pagerank], iterations run)."""
    vertex_ids = [row["id"] for row in vertices.select("id").distinct().collect()]
    n = len(vertex_ids)

    # Transition probability of each edge: share of the trains leaving `src` that go to `dst`
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

        # Distributed step: every station sends its rank along its outgoing edges
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

# DBTITLE 1,Save the station ranking and show the top 15 hubs
station_pagerank_df = (
    station_degrees_df.join(pagerank_df, "id")
    .withColumn("pagerank", F.round("pagerank", 6))
    .select(
        "id", "station_name", "latitude", "longitude", "station_type",
        "pagerank", "neighbours", "trains_out", "trains_in",
    )
)

save_table(
    station_pagerank_df,
    "gold_station_pagerank",
    "GOLD graph - PageRank, neighbours and weekly trains of each station",
)

top_hubs_df = spark.sql(
    f"""
    SELECT RANK() OVER (ORDER BY pagerank DESC)    AS pagerank_position,
           RANK() OVER (ORDER BY trains_out DESC)  AS train_volume_position,
           station_name,
           station_type,
           pagerank,
           neighbours,
           trains_out,
           trains_in
    FROM {table_name('gold_station_pagerank')}
    ORDER BY pagerank DESC, station_name
    LIMIT 15
    """
)
display(top_hubs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the ranking.** The two columns of positions tell different stories. Porto Campanha and Lisboa Oriente lead
# MAGIC on both, but stations such as Pinhal Novo, Entroncamento and Coimbra-B are among the top PageRank positions while
# MAGIC being far from the top in number of trains. They are the junctions where lines meet, which is exactly what
# MAGIC PageRank captures and a simple count of trains does not.

# COMMAND ----------

# DBTITLE 1,Chart: top 15 stations by PageRank
top_hubs_pd = top_hubs_df.toPandas().iloc[::-1]

fig, ax = plt.subplots(figsize=(11, 6))
ax.barh(top_hubs_pd["station_name"], top_hubs_pd["pagerank"].astype(float), color="steelblue")
ax.set_title("Top 15 stations by PageRank, present week")
ax.set_xlabel("PageRank")
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 The busiest sections of the network
# MAGIC The edges with the largest weight are the sections of track that carry the most trains. Joining the edge table
# MAGIC with itself (a graph *motif* query: A -> B and B -> A) gives the traffic in both directions.

# COMMAND ----------

# DBTITLE 1,Sections with the most trains per week, both directions
busiest_sections_df = spark.sql(
    f"""
    SELECT a.station_name                                AS station_a,
           b.station_name                                AS station_b,
           ab.weight                                     AS trains_a_to_b,
           ba.weight                                     AS trains_b_to_a,
           ab.weight + ba.weight                         AS trains_per_week,
           ROUND((ab.weight + ba.weight) / 7.0, 0)       AS trains_per_day,
           ab.distance_km
    FROM {table_name('gold_graph_edges')} ab
    JOIN {table_name('gold_graph_edges')} ba ON ab.src = ba.dst AND ab.dst = ba.src
    JOIN {table_name('gold_graph_vertices')} a ON ab.src = a.id
    JOIN {table_name('gold_graph_vertices')} b ON ab.dst = b.id
    WHERE a.station_name < b.station_name
    ORDER BY trains_per_week DESC, station_a, station_b
    LIMIT 10
    """
)

display(busiest_sections_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. The future of supply: the 2027 timetable
# MAGIC
# MAGIC **Q4: what does the timetable already published for 2027 change, and how complete is it?**
# MAGIC
# MAGIC European railways change their timetable every year in mid-December. The feed downloaded in November 2026 already
# MAGIC contains the timetable that starts on **13 December 2026** and runs until December 2027. The present week
# MAGIC (16 to 22 November 2026) is compared with a normal week of the new timetable (**18 to 24 January 2027**), using
# MAGIC the same function and the same indicators.

# COMMAND ----------

# DBTITLE 1,Scheduled trains per day, from the download date to the end of the feed
daily_trains_df = spark.sql(
    f"""
    SELECT service_date,
           SUM(trains)                                                       AS trains,
           SUM(CASE WHEN service_group = 'Long distance' THEN trains END)    AS long_distance,
           SUM(CASE WHEN service_group = 'Regional' THEN trains END)         AS regional,
           SUM(CASE WHEN service_name = 'Urbanos de Lisboa' THEN trains END) AS urbanos_lisboa,
           SUM(CASE WHEN service_name IN ('Urbanos do Porto', 'Urbanos de Coimbra') THEN trains END)
               AS urbanos_porto_coimbra
    FROM {table_name('gold_daily_supply')}
    GROUP BY service_date
    ORDER BY service_date
    """
)
daily_trains_pd = daily_trains_df.toPandas().fillna(0)

fig, ax = plt.subplots(figsize=(11, 4.5))
for column, label in (
    ("urbanos_lisboa", "Urbanos de Lisboa"),
    ("regional", "Regional and InterRegional"),
    ("urbanos_porto_coimbra", "Urbanos do Porto and Coimbra"),
    ("long_distance", "Long distance"),
):
    ax.plot(daily_trains_pd["service_date"], daily_trains_pd[column].astype(float), linewidth=1, label=label)
ax.set_title("Scheduled trains per day in the feed, from the download date onwards")
ax.set_ylabel("Trains per day")
ax.grid(alpha=0.3)
ax.legend()
fig.autofmt_xdate()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 How complete is the published 2027 timetable?
# MAGIC The chart shows a sudden drop of some services on the day of the timetable change. A service that disappears
# MAGIC completely from one day to the next has not been cancelled: **its new timetable has not been published yet**.
# MAGIC The next table classifies each service by comparing the two reference weeks:
# MAGIC
# MAGIC * **Published**: the future week has at least 80% of the trains of the present week.
# MAGIC * **Partially published**: it has some trains, but fewer than 80%.
# MAGIC * **Not published yet**: it has no trains.
# MAGIC
# MAGIC The 80% threshold is an assumption of this project, chosen to separate normal timetable adjustments from missing data.

# COMMAND ----------

# DBTITLE 1,Publication status of the 2027 timetable per service
publication_status_df = spark.sql(
    f"""
    WITH present AS (
        SELECT * FROM {table_name('gold_weekly_supply')} WHERE period = 'Present'
    ),
    future AS (
        SELECT * FROM {table_name('gold_weekly_supply')} WHERE period = 'Future'
    )
    SELECT p.service_order,
           p.service_group,
           p.service_name,
           p.trains_per_week                                             AS trains_present_week,
           COALESCE(f.trains_per_week, 0)                                AS trains_future_week,
           ROUND(100.0 * COALESCE(f.trains_per_week, 0) / p.trains_per_week, 1) AS future_as_pct_of_present,
           CASE
               WHEN COALESCE(f.trains_per_week, 0) = 0 THEN 'Not published yet'
               WHEN f.trains_per_week < {PARTIAL_PUBLICATION_THRESHOLD} * p.trains_per_week THEN 'Partially published'
               ELSE 'Published'
           END                                                           AS publication_status
    FROM present p
    LEFT JOIN future f ON p.service_name = f.service_name
    ORDER BY p.service_order
    """
)

display(publication_status_df.drop("service_order"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Like-for-like comparison of the published services
# MAGIC Only the services whose 2027 timetable is **published** are compared. For the others any difference would measure
# MAGIC missing data, not a change of service.

# COMMAND ----------

# DBTITLE 1,Present vs future supply for the published services
published_services = [
    row["service_name"]
    for row in publication_status_df.filter(F.col("publication_status") == "Published").collect()
]
published_list = ", ".join(f"'{name}'" for name in published_services)
print(f"Services compared: {published_services}")

supply_comparison_df = spark.sql(
    f"""
    WITH present AS (
        SELECT * FROM {table_name('gold_weekly_supply')} WHERE period = 'Present'
    ),
    future AS (
        SELECT * FROM {table_name('gold_weekly_supply')} WHERE period = 'Future'
    )
    SELECT p.service_name,
           p.trains_per_week                       AS trains_week_present,
           f.trains_per_week                       AS trains_week_future,
           f.trains_per_week - p.trains_per_week   AS trains_week_change,
           p.train_km_per_week                     AS train_km_present,
           f.train_km_per_week                     AS train_km_future,
           ROUND(100.0 * (f.train_km_per_week - p.train_km_per_week) / p.train_km_per_week, 1) AS train_km_change_pct,
           p.avg_duration_min                      AS avg_duration_present,
           f.avg_duration_min                      AS avg_duration_future,
           p.commercial_speed_kmh                  AS speed_present_kmh,
           f.commercial_speed_kmh                  AS speed_future_kmh
    FROM present p
    JOIN future f ON p.service_name = f.service_name
    WHERE p.service_name IN ({published_list})
    ORDER BY p.service_order
    """
)

display(supply_comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 Journey times on the main long-distance connections
# MAGIC For a passenger the timetable change is felt in the **journey time** and in the **number of direct trains**.
# MAGIC The function below finds, for a reference week, every train that calls at both stations of a connection in the
# MAGIC right order (a self-join of the stop times) and measures the time between departure and arrival.
# MAGIC Only the long-distance services are included, because their 2027 timetable is published.

# COMMAND ----------

# DBTITLE 1,gold_journey_times: direct trains and journey time per connection
CONNECTIONS = [
    ("Lisboa Santa Apolonia", "Porto Campanha"),
    ("Porto Campanha", "Lisboa Santa Apolonia"),
    ("Lisboa Santa Apolonia", "Braga"),
    ("Lisboa Santa Apolonia", "Coimbra-B"),
    ("Lisboa Oriente", "Faro"),
    ("Porto Campanha", "Faro"),
]
connections_df = spark.createDataFrame(CONNECTIONS, "origin_station STRING, destination_station STRING")


def journey_times(week_start: str, period: str) -> DataFrame:
    """Direct long-distance trains and journey time per connection for the weekdays of a reference week."""
    station_names = spark.table(table_name("dim_station")).select("station_id", "station_name")
    calls = (
        train_runs_of_week(week_start)
        .filter(~F.col("is_weekend"))
        .filter(F.col("service_group") == "Long distance")
        .select("trip_id", "service_date", "service_name")
        .join(spark.table(table_name("silver_stop_times")), "trip_id")
        .join(station_names, "station_id")
    )
    origin_calls = calls.select(
        "trip_id", "service_date", "service_name",
        F.col("station_name").alias("origin_station"),
        F.col("stop_sequence").alias("origin_sequence"),
        F.col("departure_sec").alias("origin_departure_sec"),
    )
    destination_calls = calls.select(
        "trip_id", "service_date",
        F.col("station_name").alias("destination_station"),
        F.col("stop_sequence").alias("destination_sequence"),
        F.col("arrival_sec").alias("destination_arrival_sec"),
    )
    return (
        origin_calls.join(destination_calls, ["trip_id", "service_date"])
        .filter(F.col("origin_sequence") < F.col("destination_sequence"))
        .join(F.broadcast(connections_df), ["origin_station", "destination_station"])
        .withColumn("journey_min", (F.col("destination_arrival_sec") - F.col("origin_departure_sec")) / 60.0)
        .groupBy("origin_station", "destination_station", "service_name")
        .agg(
            F.round(F.count("*") / 5, 1).alias("direct_trains_per_weekday"),
            F.round(F.min("journey_min"), 0).cast("int").alias("fastest_min"),
            F.round(F.avg("journey_min"), 0).cast("int").alias("average_min"),
            F.round(F.max("journey_min"), 0).cast("int").alias("slowest_min"),
        )
        .withColumn("period", F.lit(period))
    )


gold_journey_times_df = journey_times(PRESENT_WEEK_START, "Present").unionByName(
    journey_times(FUTURE_WEEK_START, "Future")
)

save_table(
    gold_journey_times_df,
    "gold_journey_times",
    "GOLD - direct trains per weekday and journey time on the main long-distance connections, present and future",
)

# COMMAND ----------

# DBTITLE 1,Journey times: present vs 2027 timetable
journey_comparison_df = spark.sql(
    f"""
    WITH present AS (
        SELECT * FROM {table_name('gold_journey_times')} WHERE period = 'Present'
    ),
    future AS (
        SELECT * FROM {table_name('gold_journey_times')} WHERE period = 'Future'
    )
    SELECT COALESCE(p.origin_station, f.origin_station)           AS origin,
           COALESCE(p.destination_station, f.destination_station) AS destination,
           COALESCE(p.service_name, f.service_name)               AS service,
           p.direct_trains_per_weekday                            AS trains_weekday_present,
           f.direct_trains_per_weekday                            AS trains_weekday_future,
           p.fastest_min                                          AS fastest_present_min,
           f.fastest_min                                          AS fastest_future_min,
           f.fastest_min - p.fastest_min                          AS fastest_change_min,
           p.average_min                                          AS average_present_min,
           f.average_min                                          AS average_future_min,
           f.average_min - p.average_min                          AS average_change_min
    FROM present p
    FULL OUTER JOIN future f
      ON p.origin_station = f.origin_station
     AND p.destination_station = f.destination_station
     AND p.service_name = f.service_name
    ORDER BY origin, destination, service
    """
)

display(journey_comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. The future of demand: passenger projection to 2030
# MAGIC
# MAGIC **Q5: where is passenger demand heading until 2030?**
# MAGIC
# MAGIC The timetable describes supply. Demand comes from the Eurostat statistics of passengers transported by rail in
# MAGIC Portugal, available per year and per quarter since 2004. The annual and the quarterly datasets are published
# MAGIC separately, so the sum of four quarters can differ slightly from the annual figure.

# COMMAND ----------

# DBTITLE 1,Rail passengers in Portugal per year
annual_passengers_df = spark.sql(
    f"""
    WITH annual AS (
        SELECT year,
               MAX(CASE WHEN unit = 'THS_PAS' THEN value END) / 1000.0 AS passengers_million,
               MAX(CASE WHEN unit = 'MIO_PKM' THEN value END)          AS passenger_km_million
        FROM {table_name('silver_rail_passengers')}
        WHERE dataset = 'rail_pa_total' AND country_code = '{COUNTRY}'
        GROUP BY year
    )
    SELECT year,
           ROUND(passengers_million, 1)                                           AS passengers_million,
           ROUND(100.0 * (passengers_million / LAG(passengers_million) OVER (ORDER BY year) - 1), 1)
               AS growth_pct,
           passenger_km_million,
           ROUND(passenger_km_million / passengers_million, 1)                    AS avg_trip_km
    FROM annual
    WHERE passengers_million IS NOT NULL
    ORDER BY year
    """
)

display(annual_passengers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 Projection method
# MAGIC The quarterly series has three very different regimes: a long period of slow change until 2019, the collapse and
# MAGIC recovery of the pandemic (2020 to 2022), and a strong growth since 2023. A model fitted on the whole series would
# MAGIC average three different worlds, so the projection is fitted **only on the quarters from 2023 onwards**.
# MAGIC
# MAGIC The model is deliberately simple and fully written in Spark SQL:
# MAGIC 1. **Trend**: least-squares straight line of passengers over time, with the aggregates `regr_slope` and `regr_intercept`.
# MAGIC 2. **Seasonality**: for each quarter of the year, the average ratio between the observed value and the trend.
# MAGIC 3. **Projection**: trend multiplied by the seasonal factor of the quarter.
# MAGIC
# MAGIC Before trusting it, the model is **backtested**: it is fitted with the data until 2024 only and its projection
# MAGIC for the following quarters is compared with what really happened.

# COMMAND ----------

# DBTITLE 1,Quarterly series and the trend + seasonality model
quarterly_source_df = spark.sql(
    f"""
    SELECT year,
           quarter,
           year * 4 + quarter      AS time_index,
           value / 1000.0          AS passengers_million
    FROM {table_name('silver_rail_passengers')}
    WHERE dataset = 'rail_pa_quartal' AND country_code = '{COUNTRY}' AND unit = 'THS_PAS'
      AND value IS NOT NULL
    """
)
save_table(
    quarterly_source_df,
    "silver_quarterly_passengers",
    f"SILVER - quarterly rail passengers (million) of country {COUNTRY}, input of the projection model",
)
quarterly_passengers_df = spark.table(table_name("silver_quarterly_passengers"))
QUARTERLY_TABLE = table_name("silver_quarterly_passengers")

last_actual = quarterly_passengers_df.orderBy(F.desc("time_index")).first()
print(f"Quarterly observations : {quarterly_passengers_df.count()}")
print(f"Last observed quarter  : {last_actual['year']}-Q{last_actual['quarter']}"
      f" ({last_actual['passengers_million']:.1f} million passengers)")

recent_quarters_df = spark.sql(
    f"""
    SELECT year,
           quarter,
           ROUND(passengers_million, 2) AS passengers_million,
           ROUND(100.0 * (passengers_million / LAG(passengers_million, 4) OVER (ORDER BY time_index) - 1), 1)
               AS growth_vs_same_quarter_last_year_pct
    FROM {table_name('silver_quarterly_passengers')}
    """
).filter(F.col("year") >= TREND_START_YEAR + 1).orderBy("year", "quarter")

display(recent_quarters_df)


def trend_seasonal_projection(first_train_year: int, last_train_year: int, end_year: int) -> DataFrame:
    """Fit trend + seasonality on [first_train_year, last_train_year] and project every quarter until end_year."""
    return spark.sql(
        f"""
        WITH training AS (
            SELECT * FROM {QUARTERLY_TABLE}
            WHERE year BETWEEN {first_train_year} AND {last_train_year}
        ),
        trend AS (
            SELECT regr_slope(passengers_million, time_index)     AS slope,
                   regr_intercept(passengers_million, time_index) AS intercept
            FROM training
        ),
        seasonality AS (
            SELECT t.quarter,
                   AVG(t.passengers_million / (trend.intercept + trend.slope * t.time_index)) AS seasonal_factor
            FROM training t CROSS JOIN trend
            GROUP BY t.quarter
        ),
        periods AS (
            SELECT y.year, q.quarter, y.year * 4 + q.quarter AS time_index
            FROM (SELECT explode(sequence({first_train_year}, {end_year})) AS year) y
            CROSS JOIN (SELECT explode(sequence(1, 4)) AS quarter) q
        )
        SELECT p.year,
               p.quarter,
               p.time_index,
               a.passengers_million                                                    AS actual_million,
               (trend.intercept + trend.slope * p.time_index) * s.seasonal_factor      AS projected_million,
               trend.slope * 4                                                         AS trend_million_per_year,
               p.year <= {last_train_year}                                             AS used_for_fitting
        FROM periods p
        CROSS JOIN trend
        JOIN seasonality s ON p.quarter = s.quarter
        LEFT JOIN {QUARTERLY_TABLE} a ON p.time_index = a.time_index
        """
    )

# COMMAND ----------

# DBTITLE 1,Backtest: fit until 2024, compare with what happened afterwards
backtest_df = (
    trend_seasonal_projection(TREND_START_YEAR, BACKTEST_LAST_TRAIN_YEAR, last_actual["year"])
    .filter(~F.col("used_for_fitting") & F.col("actual_million").isNotNull())
    .select(
        "year",
        "quarter",
        F.round("actual_million", 2).alias("actual_million"),
        F.round("projected_million", 2).alias("projected_million"),
        F.round(100 * (F.col("projected_million") - F.col("actual_million")) / F.col("actual_million"), 1)
        .alias("error_pct"),
    )
    .orderBy("year", "quarter")
)

display(backtest_df)

backtest_mape = backtest_df.agg(F.avg(F.abs("error_pct"))).first()[0]
backtest_bias = backtest_df.agg(F.avg("error_pct")).first()[0]
print(f"Backtest quarters                     : {backtest_df.count()}")
print(f"Mean absolute percentage error (MAPE) : {backtest_mape:.1f}%")
print(f"Mean error (bias)                     : {backtest_bias:+.1f}%  (positive = the model projected too high)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the backtest.** A model that only knew the data until 2024 projected the following six quarters with an
# MAGIC error of a few percent, which is acceptable for a two-parameter model. The error is, however, systematically
# MAGIC positive and grows with the horizon: the model projected **too high**, because growth has been slowing down
# MAGIC (the table of recent quarters above shows that year-on-year growth in the first half of 2026 is clearly below
# MAGIC that of 2024 and 2025).
# MAGIC A linear continuation of the recent trend must therefore be read as an **upper bound**.
# MAGIC
# MAGIC ### 11.2 Projection to 2030 in two scenarios
# MAGIC A straight line cannot go on for ever: trains, track capacity and population are finite. The projection is
# MAGIC therefore presented as a range between two scenarios, not as a single forecast.
# MAGIC
# MAGIC * **Scenario A, the recent trend continues** (upper bound): the trend + seasonality model fitted on all quarters
# MAGIC   since 2023.
# MAGIC * **Scenario B, growth returns to its long-run pace** (lower bound): the last complete year grows at the average
# MAGIC   annual rate observed between 2004 and 2019, the whole period available before the pandemic.

# COMMAND ----------

# DBTITLE 1,gold_passenger_forecast: quarterly projection, scenario A
gold_passenger_forecast_df = trend_seasonal_projection(
    TREND_START_YEAR, last_actual["year"], FORECAST_END_YEAR
).select(
    "year",
    "quarter",
    F.round("actual_million", 3).alias("actual_million"),
    F.round("projected_million", 3).alias("projected_million"),
    F.round("trend_million_per_year", 3).alias("trend_million_per_year"),
    F.col("actual_million").isNull().alias("is_projection"),
)

save_table(
    gold_passenger_forecast_df,
    "gold_passenger_forecast",
    "GOLD - quarterly rail passengers in Portugal: observed values and trend + seasonality projection (scenario A)",
)

# COMMAND ----------

# DBTITLE 1,Annual passengers until 2030: scenarios A and B
pre_pandemic = spark.sql(
    f"""
    SELECT MAX(CASE WHEN year = {LONG_RUN_FIRST_YEAR} THEN value END) / 1000.0 AS passengers_first,
           MAX(CASE WHEN year = {LONG_RUN_LAST_YEAR} THEN value END) / 1000.0  AS passengers_last
    FROM {table_name('silver_rail_passengers')}
    WHERE dataset = 'rail_pa_total' AND country_code = '{COUNTRY}' AND unit = 'THS_PAS'
    """
).first()
pre_pandemic_growth = (pre_pandemic["passengers_last"] / pre_pandemic["passengers_first"]) ** (
    1 / (LONG_RUN_LAST_YEAR - LONG_RUN_FIRST_YEAR)
) - 1

last_complete_year = spark.sql(
    f"SELECT MAX(year) FROM (SELECT year FROM {QUARTERLY_TABLE} GROUP BY year HAVING COUNT(*) = 4)"
).first()[0]
last_complete_year_total = spark.sql(
    f"SELECT SUM(passengers_million) FROM {QUARTERLY_TABLE} WHERE year = {last_complete_year}"
).first()[0]

print(f"Average annual growth {LONG_RUN_FIRST_YEAR}-{LONG_RUN_LAST_YEAR} : {pre_pandemic_growth:.2%}")
print(f"Last complete year              : {last_complete_year} ({last_complete_year_total:.1f} million passengers,"
      " sum of the four quarters)")

annual_scenarios_df = spark.sql(
    f"""
    SELECT year,
           ROUND(SUM(COALESCE(actual_million, projected_million)), 1) AS scenario_a_recent_trend,
           ROUND({last_complete_year_total} * POWER(1 + {pre_pandemic_growth}, year - {last_complete_year}), 1)
               AS scenario_b_long_run_pace,
           SUM(CASE WHEN is_projection THEN 1 ELSE 0 END)             AS projected_quarters
    FROM {table_name('gold_passenger_forecast')}
    WHERE year > {last_complete_year}
    GROUP BY year
    ORDER BY year
    """
)

display(annual_scenarios_df)

# COMMAND ----------

# DBTITLE 1,Chart: observed passengers and projection to 2030
history_pd = (
    quarterly_passengers_df.select("year", "quarter", "passengers_million").orderBy("year", "quarter").toPandas()
)
projection_pd = (
    spark.table(table_name("gold_passenger_forecast"))
    .filter(F.col("is_projection"))
    .orderBy("year", "quarter")
    .toPandas()
)
scenarios_pd = annual_scenarios_df.toPandas()

history_x = history_pd["year"] + (history_pd["quarter"] - 1) / 4.0
projection_x = projection_pd["year"] + (projection_pd["quarter"] - 1) / 4.0

fig, (ax_quarterly, ax_annual) = plt.subplots(1, 2, figsize=(13, 4.8), gridspec_kw={"width_ratios": [3, 2]})

ax_quarterly.plot(history_x, history_pd["passengers_million"].astype(float), color="steelblue", label="Observed")
ax_quarterly.plot(projection_x, projection_pd["projected_million"].astype(float), color="darkorange",
                  linestyle="--", label="Scenario A projection")
ax_quarterly.axvspan(PANDEMIC_YEARS[0], PANDEMIC_YEARS[-1] + 1, color="grey", alpha=0.15, label="Pandemic years")
ax_quarterly.set_title("Rail passengers in Portugal per quarter")
ax_quarterly.set_xlabel("Year")
ax_quarterly.set_ylabel("Million passengers")
ax_quarterly.grid(alpha=0.3)
ax_quarterly.legend(loc="upper left")

positions = range(len(scenarios_pd))
bar_width = 0.4
ax_annual.bar([p - bar_width / 2 for p in positions], scenarios_pd["scenario_a_recent_trend"].astype(float),
              bar_width, color="darkorange", label="A: recent trend")
ax_annual.bar([p + bar_width / 2 for p in positions], scenarios_pd["scenario_b_long_run_pace"].astype(float),
              bar_width, color="seagreen", label="B: long-run pace")
ax_annual.axhline(float(last_complete_year_total), color="steelblue", linestyle=":",
                  label=f"{last_complete_year} observed")
ax_annual.set_xticks(list(positions))
ax_annual.set_xticklabels(scenarios_pd["year"].astype(int))
ax_annual.set_title("Annual passengers by scenario")
ax_annual.set_ylabel("Million passengers")
ax_annual.grid(axis="y", alpha=0.3)
ax_annual.legend(loc="upper left")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.3 Portugal in the European context
# MAGIC Is the growth of rail demand in Portugal exceptional? The annual Eurostat file has the same indicator for the other
# MAGIC European countries, so the recovery from the pre-pandemic level (2019) can be ranked.

# COMMAND ----------

# DBTITLE 1,gold_passenger_growth_by_country: latest year compared with 2019
gold_passenger_growth_df = spark.sql(
    f"""
    WITH passengers AS (
        SELECT p.country_code, c.country_name, p.year, p.value
        FROM {table_name('silver_rail_passengers')} p
        JOIN {table_name('dim_country')} c ON p.country_code = c.country_code
        WHERE p.dataset = 'rail_pa_total' AND p.unit = 'THS_PAS' AND p.value IS NOT NULL
    ),
    latest AS (
        SELECT country_code, MAX(year) AS latest_year
        FROM passengers
        GROUP BY country_code
    )
    SELECT b.country_code,
           b.country_name,
           l.latest_year,
           ROUND(b.value / 1000.0, 1)                       AS passengers_2019_million,
           ROUND(n.value / 1000.0, 1)                       AS passengers_latest_million,
           ROUND(100.0 * (n.value / b.value - 1), 1)        AS growth_since_2019_pct,
           RANK() OVER (ORDER BY n.value / b.value DESC)    AS growth_rank
    FROM passengers b
    JOIN latest l ON b.country_code = l.country_code
    JOIN passengers n ON n.country_code = b.country_code AND n.year = l.latest_year
    WHERE b.year = 2019 AND l.latest_year >= 2024
    """
)

save_table(
    gold_passenger_growth_df,
    "gold_passenger_growth_by_country",
    "GOLD - rail passengers per country: latest available year (2024 or later) compared with 2019",
)

display(spark.table(table_name("gold_passenger_growth_by_country")).orderBy("growth_rank").limit(12))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Delta Lake: audit history and time travel
# MAGIC Every table of the project is a Delta table, so each write is an ACID transaction recorded in a log.
# MAGIC This gives two capabilities that plain files in a data lake do not have:
# MAGIC * **Audit**: what changed and when (`DESCRIBE HISTORY`, already used in section 8.3).
# MAGIC * **Time travel**: query a table as it was at a previous version (`VERSION AS OF`), which is useful to reproduce
# MAGIC   a report or to recover from a wrong load.

# COMMAND ----------

# DBTITLE 1,Time travel: the streaming Bronze table before and after the second drop
rows_after_drop_1 = spark.sql(
    f"SELECT COUNT(*) FROM {table_name('bronze_departures_stream')} "
    f"VERSION AS OF {bronze_stream_version_after_drop_1}"
).first()[0]
rows_now = row_count("bronze_departures_stream")

print(f"Version {bronze_stream_version_after_drop_1} (after drop 1) : {rows_after_drop_1:>8,} events")
print(f"Current version           : {rows_now:>8,} events")
print(f"Events added by drop 2    : {rows_now - rows_after_drop_1:>8,}")

# COMMAND ----------

# DBTITLE 1,Catalogue of the tables created by the project
display(
    spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
    .filter(~F.col("isTemporary"))
    .select("tableName")
    .withColumn(
        "layer",
        F.when(F.col("tableName").startswith("bronze"), "1 - Bronze")
        .when(F.col("tableName").startswith("silver") | F.col("tableName").startswith("dim"), "2 - Silver")
        .otherwise("3 - Gold"),
    )
    .orderBy("layer", "tableName")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Conclusions
# MAGIC The next cell computes the headline numbers directly from the Gold tables, so that the conclusions always reflect
# MAGIC the data that was actually processed.

# COMMAND ----------

# DBTITLE 1,Key findings computed from the Gold tables
present_totals = spark.sql(
    f"""
    SELECT SUM(trains_per_week)   AS trains,
           SUM(train_km_per_week) AS train_km,
           SUM(CASE WHEN service_group = 'Urban' THEN trains_per_week END)           AS urban_trains,
           SUM(CASE WHEN service_group = 'Long distance' THEN trains_per_week END)   AS long_distance_trains,
           SUM(CASE WHEN service_group = 'Long distance' THEN train_km_per_week END) AS long_distance_km
    FROM {table_name('gold_weekly_supply')}
    WHERE period = 'Present'
    """
).first()
weekday_trains = trains_by_day_df.filter(F.dayofweek("service_date").between(2, 6)).agg(F.avg("trains")).first()[0]
weekend_trains = trains_by_day_df.filter(F.dayofweek("service_date").isin(1, 7)).agg(F.avg("trains")).first()[0]
peak_hour = hourly_departures_df.orderBy(F.desc("all_services"), "hour").first()
busiest_station = (
    spark.table(table_name("gold_station_activity")).orderBy(F.desc("train_calls_per_week"), "station_name").first()
)
top_hub = spark.table(table_name("gold_station_pagerank")).orderBy(F.desc("pagerank"), "station_name").first()
n_junctions = spark.table(table_name("gold_station_pagerank")).filter("station_type = 'Junction'").count()
busiest_section = busiest_sections_df.first()
status_counts = {
    row["publication_status"]: row["services"]
    for row in publication_status_df.groupBy("publication_status")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_list("service_name"))).alias("services"))
    .collect()
}
lisbon_porto = (
    journey_comparison_df.filter(
        (F.col("origin") == "Lisboa Santa Apolonia")
        & (F.col("destination") == "Porto Campanha")
        & (F.col("service") == "Alfa Pendular")
    ).first()
)
forecast_end = annual_scenarios_df.filter(F.col("year") == FORECAST_END_YEAR).first()
portugal_growth = (
    spark.table(table_name("gold_passenger_growth_by_country")).filter(F.col("country_code") == COUNTRY).first()
)
n_countries_ranked = row_count("gold_passenger_growth_by_country")

print("DATA")
print(f"  {n_trips:,} timetable train runs expand to {n_service_days:,} train runs and {n_stop_events:,} stop events.")
print(f"  Data quality: {len(quality_checks) - n_checks_failed} of {len(quality_checks)} checks passed.")
print("\nQ1 - PRESENT: how much service does CP run?")
print(f"  {present_totals['trains']:,} trains and {present_totals['train_km']:,.0f} train-km in the week of {PRESENT_WEEK_START}.")
print(f"  {weekday_trains:,.0f} trains on an average weekday, {weekend_trains:,.0f} on an average weekend day.")
print(f"  Urban services are {present_totals['urban_trains'] / present_totals['trains']:.1%} of the trains;"
      f" long distance is {present_totals['long_distance_trains'] / present_totals['trains']:.1%} of the trains"
      f" but {present_totals['long_distance_km'] / present_totals['train_km']:.1%} of the train-km.")
print(f"  Peak hour: {peak_hour['hour']:02d}:00 with {peak_hour['all_services']:,.0f} departures from all stations.")
print(f"  Busiest station: {busiest_station['station_name']} ({busiest_station['train_calls_per_week']:,} train calls per week).")
print("\nQ2 - PRESENT: can the indicators be maintained incrementally?")
print(f"  Streaming results identical to batch results: {'YES' if streaming_matches_batch else 'NO'}"
      f" ({n_stream_events:,} departure events processed in two drops).")
print("\nQ3 - PRESENT: the network")
print(f"  {n_vertices:,} stations, {n_edges:,} directed sections, {n_junctions:,} junctions.")
print(f"  Most important hub (PageRank): {top_hub['station_name']} ({top_hub['pagerank']}).")
print(f"  Busiest section: {busiest_section['station_a']} - {busiest_section['station_b']}"
      f" ({busiest_section['trains_per_week']:,} trains per week, both directions).")
print("\nQ4 - FUTURE: the 2027 timetable")
for status in ("Published", "Partially published", "Not published yet"):
    print(f"  {status:<20}: {status_counts.get(status, '-')}")
print(f"  Alfa Pendular Lisboa Santa Apolonia - Porto Campanha:"
      f" {lisbon_porto['trains_weekday_present']} -> {lisbon_porto['trains_weekday_future']} direct trains per weekday,"
      f" average journey {lisbon_porto['average_present_min']} -> {lisbon_porto['average_future_min']} min,"
      f" fastest {lisbon_porto['fastest_present_min']} -> {lisbon_porto['fastest_future_min']} min.")
print("\nQ5 - FUTURE: passenger demand")
print(f"  {last_complete_year}: {last_complete_year_total:.1f} million passengers;"
      f" {portugal_growth['growth_since_2019_pct']:+.1f}% since 2019,"
      f" rank {portugal_growth['growth_rank']} of {n_countries_ranked} European countries.")
latest_growth = recent_quarters_df.orderBy(F.desc("year"), F.desc("quarter")).first()
print(f"  Latest quarter ({latest_growth['year']}-Q{latest_growth['quarter']}):"
      f" {latest_growth['growth_vs_same_quarter_last_year_pct']:+.1f}% compared with the same quarter of the previous year.")
print(f"  Backtest error of the projection model (MAPE): {backtest_mape:.1f}%, bias {backtest_bias:+.1f}%.")
print(f"  {FORECAST_END_YEAR}: between {forecast_end['scenario_b_long_run_pace']:.1f} (scenario B)"
      f" and {forecast_end['scenario_a_recent_trend']:.1f} (scenario A) million passengers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What we built
# MAGIC An end-to-end lakehouse on Databricks Free Edition that turns the public timetable of CP and the Eurostat passenger
# MAGIC statistics into decision-ready indicators, using four processing styles of Apache Spark on one engine and one copy
# MAGIC of the data: batch DataFrames, Spark SQL, Structured Streaming and graph analytics.
# MAGIC
# MAGIC ### Answers to the business questions
# MAGIC * **Q1, the present supply.** CP's offer is dominated in number of trains by the urban services of Lisbon and Porto,
# MAGIC   while the long-distance services, few in number, account for a much larger share of the distance run. Supply
# MAGIC   falls markedly at the weekend and has two clear weekday peaks, in the morning and in the late afternoon
# MAGIC   (section 7.3).
# MAGIC * **Q2, real time.** The streaming pipeline consumed the departure events in two instalments, processed only the
# MAGIC   new files in the second run, and reached exactly the same result as the batch computation (section 8.4).
# MAGIC * **Q3, the network.** PageRank and the degree metrics identify the stations that hold the network together:
# MAGIC   the two main stations of Porto and Lisbon and the junctions where lines meet. The edge weights identify the
# MAGIC   sections with the most trains, which are all in the Lisbon urban network, led by the Sintra line (section 9).
# MAGIC   These are the places where a disruption affects the most trains.
# MAGIC * **Q4, the future supply.** The 2027 timetable is published for the long-distance services and for the Lisbon
# MAGIC   urban services, and it is still missing or incomplete for the remaining ones (section 10.1). Where it is
# MAGIC   published, the number of trains is practically unchanged, and the scheduled journey between Lisbon and Porto
# MAGIC   becomes about ten minutes longer, for Alfa Pendular and for Intercidades (section 10.3).
# MAGIC * **Q5, the future demand.** Rail demand in Portugal is far above its pre-pandemic level, and its growth since
# MAGIC   2019 is among the strongest in Europe (section 11.3). The growth is slowing down, so the projection for 2030 is
# MAGIC   given as a range between a lower and an upper scenario (section 11.2); in both of them demand is higher than today.
# MAGIC
# MAGIC ### The main message: demand is growing faster than supply
# MAGIC Putting Q4 and Q5 together gives the central conclusion of the project. On the services that can be compared,
# MAGIC the timetable published for 2027 offers essentially the same number of trains as today, with slightly longer
# MAGIC long-distance journeys, while demand has grown every year since 2021 and is projected to keep growing. If both
# MAGIC tendencies continue, the same trains will carry more passengers, and the pressure will be felt first where the
# MAGIC network is already the busiest: the urban lines and the hubs identified in section 9.
# MAGIC
# MAGIC ### What we learned about Big Data processing
# MAGIC * **Veracity comes first.** The most important step of the project was noticing that the feed is a rolling
# MAGIC   snapshot (section 7.1). Without it, the apparent growth of service during 2026 and the apparent cut of service
# MAGIC   in 2027 would both have been reported as facts.
# MAGIC * **A small file can describe a large dataset.** A few megabytes of timetable expand into hundreds of thousands
# MAGIC   of train runs and millions of stop events, generated by Spark with `sequence` and `explode`.
# MAGIC * **One engine, one set of rules.** The same functions computed the present and the future
# MAGIC   (`train_runs_of_week`), and the batch and the streaming results (`clean_departures`).
# MAGIC * **Incremental processing is a property of the checkpoint**, not of the code: the same query, started again,
# MAGIC   continues where it stopped.
# MAGIC * **Graph algorithms are joins.** PageRank was written as a repeated join and aggregation that Spark distributes
# MAGIC   like any other query.
# MAGIC
# MAGIC ### Limitations
# MAGIC * The timetable describes **scheduled** service. Delays, cancellations and strikes are not in the data.
# MAGIC * Distances are **straight lines** between consecutive stations, so distances and speeds are lower bounds.
# MAGIC * The comparison with 2027 is limited to the services already published on the download date.
# MAGIC * Eurostat counts the passengers of **all** rail operators in Portugal, and supply is measured in trains, not in
# MAGIC   seats, so supply and demand are compared as tendencies and not as a load factor.
# MAGIC * The projection is a trend with seasonality fitted on a short and exceptional period. It is a scenario, not a
# MAGIC   forecast: it ignores prices, capacity limits, new infrastructure and the economy.
# MAGIC * Streaming used simulated file drops and the `availableNow` trigger. The Gold stream uses *complete* output mode;
# MAGIC   with an unbounded stream a **watermark** would be added so that old windows are finalised and their state released.
# MAGIC
# MAGIC ### Future work
# MAGIC * **Machine learning with Spark ML**: a demand model with explanatory variables (fares, fuel prices, population,
# MAGIC   tourism) instead of a pure trend.
# MAGIC * Repeat the comparison when the complete 2027 timetable is published, and store successive downloads of the feed
# MAGIC   to build the history that a single snapshot cannot give.
# MAGIC * Add rolling stock capacity (seats per train) to measure supply in seat-kilometres, and real-time operational
# MAGIC   data to measure punctuality.
# MAGIC
# MAGIC ### References
# MAGIC * CP - Comboios de Portugal, public timetable in GTFS format (downloaded on 16 November 2026).
# MAGIC * Eurostat, *Passengers transported* (`rail_pa_total`) and quarterly passengers transported (`rail_pa_quartal`),
# MAGIC   downloaded on 16 November 2026.
# MAGIC * Google, *General Transit Feed Specification (GTFS) Schedule Reference*.
# MAGIC * Damji, J., Wenig, B., Das, T., Lee, D. (2020). *Learning Spark: Lightning-fast Data Analysis*. O'Reilly.
# MAGIC * Chambers, B., Zaharia, M. (2018). *Spark: The Definitive Guide*. O'Reilly.
