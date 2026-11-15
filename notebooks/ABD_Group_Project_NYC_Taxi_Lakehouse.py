# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Mobility Lakehouse
# MAGIC ## Batch, Streaming and Graph Analytics with Apache Spark on Databricks Free Edition
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
# MAGIC 4. Data source exploration
# MAGIC 5. **Bronze** layer: raw ingestion (batch)
# MAGIC 6. **Silver** layer: cleaning, data quality and enrichment
# MAGIC 7. **Gold** layer: business aggregates and analysis with Spark SQL
# MAGIC 8. **Structured Streaming**: incremental ingestion of trip events
# MAGIC 9. **Graph analytics**: the city as a network of zones
# MAGIC 10. Delta Lake: audit history and time travel
# MAGIC 11. Conclusions
# MAGIC
# MAGIC > **How to run:** attach the notebook to *Serverless* compute and choose **Run all**.
# MAGIC > The notebook is self-contained and idempotent: it only reads the `samples.nyctaxi.trips` table that ships with
# MAGIC > every Databricks workspace and (re)creates its own schema, so it can be re-run safely.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Problem definition and motivation
# MAGIC
# MAGIC ### Context
# MAGIC Taxi and ride-hailing fleets generate one event per trip: where and when it started, where and when it ended,
# MAGIC how far it went and how much it cost. In a city like New York this is a continuous, high-volume flow of events,
# MAGIC and its value depends on how quickly it can be turned into decisions. This is a typical Big Data problem:
# MAGIC
# MAGIC * **Volume**: trip records accumulate without end, so storage and processing must scale horizontally.
# MAGIC * **Velocity**: events arrive continuously and operational indicators must be refreshed incrementally,
# MAGIC   not by recomputing everything from scratch.
# MAGIC * **Variety**: the same data arrives as curated tables (structured) and as JSON event files (semi-structured).
# MAGIC * **Veracity**: meter and GPS errors produce impossible trips (zero distance, negative fares, absurd speeds)
# MAGIC   that must be detected and isolated before any analysis.
# MAGIC
# MAGIC ### Problem
# MAGIC A taxi operator (or a city mobility department) wants a single, trustworthy data platform that answers
# MAGIC three groups of questions:
# MAGIC
# MAGIC | # | Business question | Processing type | Section |
# MAGIC |---|---|---|---|
# MAGIC | Q1 | **When** is demand highest? How do weekdays differ from weekends? | Batch, Spark SQL | 7 |
# MAGIC | Q2 | **Where** does demand and revenue come from, and which routes dominate? | Batch, Spark SQL | 7 |
# MAGIC | Q3 | Can the same indicators be kept **up to date as new trip events arrive**, with exactly the same results as the batch computation? | Structured Streaming | 8 |
# MAGIC | Q4 | Seen as a **network**, which zones are the most important hubs, and which zones export or import vehicles? | Graph analytics | 9 |
# MAGIC
# MAGIC ### Why it matters
# MAGIC * Knowing *when* and *where* demand concentrates lets an operator position drivers before the demand appears.
# MAGIC * Zones that receive many more trips than they originate accumulate idle vehicles, while the opposite zones
# MAGIC   run short. Quantifying this imbalance is the starting point for fleet rebalancing.
# MAGIC * Indicators that are refreshed incrementally support operational decisions that a nightly report cannot.
# MAGIC
# MAGIC ### Objective
# MAGIC Build an end-to-end **lakehouse pipeline** with Apache Spark that ingests trip data in batch and in streaming,
# MAGIC organises it in a **medallion architecture** (Bronze, Silver, Gold), and answers Q1 to Q4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Big Data technologies and solution architecture
# MAGIC
# MAGIC | Technology | How it is used in this project |
# MAGIC |---|---|
# MAGIC | **Databricks Free Edition** (serverless compute) | Execution environment for the whole notebook |
# MAGIC | **Apache Spark DataFrame API** (PySpark) | Ingestion, cleaning, enrichment, joins, aggregations |
# MAGIC | **Spark SQL** | Gold layer and analysis: CTEs, window functions, conditional aggregation |
# MAGIC | **Spark Structured Streaming** | Incremental ingestion of JSON trip events with checkpoints, stream-static join, stateful deduplication and windowed aggregation |
# MAGIC | **Graph analytics on Spark** | Zone network: degrees, net flow, reciprocity and an iterative **PageRank** written with DataFrames |
# MAGIC | **Delta Lake** | ACID storage format of every table, transaction log for audit and time travel |
# MAGIC | **Unity Catalog** | Data organisation: `catalog.schema.table` namespace, table comments, and a managed **Volume** as landing zone |
# MAGIC
# MAGIC ### Data organisation: medallion architecture
# MAGIC
# MAGIC ```
# MAGIC                                    Unity Catalog schema: <catalog>.abd_nyc_taxi
# MAGIC
# MAGIC  SOURCES                     BRONZE (raw)              SILVER (clean)                GOLD (business)
# MAGIC  -------                     ------------              --------------                ---------------
# MAGIC  samples.nyctaxi.trips  -->  bronze_trips         -->  silver_trips             -->  gold_hourly_demand
# MAGIC  (structured table)   batch                            silver_trips_quarantine       gold_borough_kpis
# MAGIC                                                        dim_borough (reference)       gold_top_routes
# MAGIC                                                                                      gold_graph_vertices
# MAGIC                                                                                      gold_graph_edges
# MAGIC                                                                                      gold_zone_pagerank
# MAGIC
# MAGIC  Volume /landing/trips_json  bronze_trips_stream  -->  silver_trips_stream      -->  gold_hourly_demand_stream
# MAGIC  (JSON event files)  streaming      (append)                 (append)                    (complete)
# MAGIC                                 checkpoint                  checkpoint                   checkpoint
# MAGIC ```
# MAGIC
# MAGIC * **Bronze** keeps the data exactly as received, plus ingestion metadata. Nothing is filtered, so any later
# MAGIC   rule can be changed and replayed.
# MAGIC * **Silver** holds typed, deduplicated, validated and enriched trips. Rejected records are not deleted: they go to a
# MAGIC   **quarantine** table together with the reason for rejection.
# MAGIC * **Gold** holds small aggregated tables that directly answer the business questions.
# MAGIC * The batch and the streaming paths share **the same cleaning function**, so the business rules are written once.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Environment setup
# MAGIC All names used by the pipeline are defined once in the next cell. The project lives in its own schema,
# MAGIC created inside the current catalog of the workspace (`workspace` in Databricks Free Edition).

# COMMAND ----------

# DBTITLE 1,Imports and configuration
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

import matplotlib.pyplot as plt

# ---- Source -----------------------------------------------------------------
SOURCE_TABLE = "samples.nyctaxi.trips"

# ---- Target namespace (Unity Catalog) ---------------------------------------
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "abd_nyc_taxi"
VOLUME = "landing"

# ---- Business rules used by the Silver layer --------------------------------
MAX_TRIP_DURATION_MIN = 180   # trips longer than 3 hours are treated as meter errors
MAX_TRIP_DISTANCE_MI = 100    # longer than any plausible city trip
MAX_FARE_USD = 500
MAX_SPEED_MPH = 80
AIRPORT_ZIPS = [11430, 11371]  # JFK and LaGuardia

# ---- Streaming simulation ---------------------------------------------------
FILES_PER_DROP = 6            # JSON files written to the landing zone per drop
MAX_FILES_PER_TRIGGER = 2     # files consumed by each streaming micro-batch

# ---- Graph analytics --------------------------------------------------------
PAGERANK_DAMPING = 0.85
PAGERANK_MAX_ITER = 15
PAGERANK_TOLERANCE = 1e-6


def table_name(name: str) -> str:
    """Return the fully qualified Unity Catalog name of a project table."""
    return f"{CATALOG}.{SCHEMA}.{name}"


print(f"Source table   : {SOURCE_TABLE}")
print(f"Project schema : {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create the project schema and the landing volume
# Start from a clean state so that the notebook is idempotent (safe to run again).
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
spark.sql(
    f"CREATE SCHEMA {CATALOG}.{SCHEMA} "
    "COMMENT 'ABD group project: NYC taxi mobility lakehouse (bronze, silver and gold tables)'"
)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

LANDING_PATH = f"{VOLUME_ROOT}/trips_json"        # landing zone for streaming JSON events
CHECKPOINT_PATH = f"{VOLUME_ROOT}/_checkpoints"   # one sub-folder per streaming query

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data source exploration
# MAGIC The source is `samples.nyctaxi.trips`, a sample of the New York City Taxi and Limousine Commission (TLC)
# MAGIC yellow taxi trip records, available in the `samples` catalog of every Databricks workspace.
# MAGIC Each row is one trip. Before designing the pipeline we look at its schema, size, time span and obvious
# MAGIC quality problems.

# COMMAND ----------

# DBTITLE 1,Schema and sample rows
source_df = spark.table(SOURCE_TABLE)

source_df.printSchema()
display(source_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Size, time span and cardinalities
display(
    source_df.agg(
        F.count("*").alias("rows"),
        F.min("tpep_pickup_datetime").alias("first_pickup"),
        F.max("tpep_pickup_datetime").alias("last_pickup"),
        F.countDistinct("pickup_zip").alias("distinct_pickup_zips"),
        F.countDistinct("dropoff_zip").alias("distinct_dropoff_zips"),
    )
)

# COMMAND ----------

# DBTITLE 1,Descriptive statistics of the numeric columns
display(source_df.select("trip_distance", "fare_amount").summary())

# COMMAND ----------

# DBTITLE 1,First look at data quality problems
display(
    source_df.agg(
        F.count(F.when(F.col("trip_distance") <= 0, 1)).alias("distance_zero_or_negative"),
        F.count(F.when(F.col("fare_amount") <= 0, 1)).alias("fare_zero_or_negative"),
        F.count(
            F.when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), 1)
        ).alias("dropoff_not_after_pickup"),
        F.count(
            F.when(F.col("pickup_zip").isNull() | F.col("dropoff_zip").isNull(), 1)
        ).alias("missing_zip"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Observations.** The table has six columns: pick-up and drop-off timestamps, distance (miles), fare (USD) and the
# MAGIC ZIP codes of origin and destination. The checks above show that some records cannot be real trips, which
# MAGIC justifies an explicit data quality step (Silver layer) instead of analysing the raw data directly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze layer: raw ingestion (batch)
# MAGIC The Bronze table is a faithful copy of the source. The only additions are two technical columns that record
# MAGIC **where** each row came from and **when** it was ingested, which is what makes the pipeline auditable.

# COMMAND ----------

# DBTITLE 1,Ingest the source table into Bronze
bronze_df = (
    spark.table(SOURCE_TABLE)
    .withColumn("_source", F.lit(SOURCE_TABLE))
    .withColumn("_ingested_at", F.current_timestamp())
)

save_table(
    bronze_df,
    "bronze_trips",
    "BRONZE - raw NYC yellow taxi trips ingested in batch, unmodified, with ingestion metadata",
)

display(spark.table(table_name("bronze_trips")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver layer: cleaning, data quality and enrichment
# MAGIC
# MAGIC ### 6.1 Reference data: borough dimension
# MAGIC The trips only carry ZIP codes. To analyse demand geographically we add the borough, using a small reference
# MAGIC table keyed by the first three digits of the ZIP code (the USPS sectional centre).
# MAGIC
# MAGIC > **Assumption.** This prefix mapping is an approximation that is accurate at borough level for New York City.
# MAGIC > ZIP codes with any other prefix (New Jersey, Long Island, Westchester, ...) are labelled *Outside NYC*.

# COMMAND ----------

# DBTITLE 1,Create the borough dimension
dim_borough_df = spark.createDataFrame(
    [
        (100, "Manhattan"),
        (101, "Manhattan"),
        (102, "Manhattan"),
        (103, "Staten Island"),
        (104, "Bronx"),
        (111, "Queens"),
        (113, "Queens"),
        (114, "Queens"),
        (116, "Queens"),
        (112, "Brooklyn"),
    ],
    "zip3 INT, borough STRING",
)

save_table(
    dim_borough_df,
    "dim_borough",
    "SILVER reference - maps the 3-digit ZIP prefix to the NYC borough",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Cleaning and validation rules
# MAGIC One function implements all Silver logic. It works on **any** DataFrame with the raw trip columns, so the very
# MAGIC same code is applied to the batch Bronze table here and to the streaming Bronze table in section 8.
# MAGIC
# MAGIC | Step | Rule |
# MAGIC |---|---|
# MAGIC | Standardise | Rename columns and enforce data types |
# MAGIC | Identify | `trip_id` = SHA-256 hash of the business columns (natural key of a trip) |
# MAGIC | Deduplicate | Keep one record per `trip_id` |
# MAGIC | Derive | Duration (min), average speed (mph), fare per mile, calendar attributes |
# MAGIC | Enrich | Pick-up and drop-off borough (broadcast join with `dim_borough`), airport flag |
# MAGIC | Validate | Every record gets a `reject_reason`; it is empty for valid trips |
# MAGIC
# MAGIC Validation rules, evaluated in this order (the first one that fails is recorded):
# MAGIC
# MAGIC 1. `missing_values`: any of the six source fields is null
# MAGIC 2. `invalid_time_range`: drop-off is not after pick-up
# MAGIC 3. `duration_too_long`: longer than 180 minutes
# MAGIC 4. `invalid_distance`: distance is not in (0, 100] miles
# MAGIC 5. `invalid_fare`: fare is not in (0, 500] USD
# MAGIC 6. `implausible_speed`: average speed above 80 mph
# MAGIC
# MAGIC Divisions use `try_divide`, which returns `NULL` instead of failing when the divisor is zero.

# COMMAND ----------

# DBTITLE 1,Silver transformation shared by batch and streaming
def clean_trips(raw_df: DataFrame, dim_borough: DataFrame) -> DataFrame:
    """Standardise, deduplicate, enrich and validate raw trip records.

    Works for both batch and streaming DataFrames. Returns every input trip once,
    with a `reject_reason` column that is NULL for valid trips.
    """
    # 1) Standardise names and types
    trips = raw_df.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("pickup_zip").cast("int").alias("pickup_zip"),
        F.col("dropoff_zip").cast("int").alias("dropoff_zip"),
    )

    # 2) Natural key and deduplication
    trips = trips.withColumn(
        "trip_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.coalesce(F.col("pickup_ts").cast("string"), F.lit("")),
                F.coalesce(F.col("dropoff_ts").cast("string"), F.lit("")),
                F.coalesce(F.col("trip_distance").cast("string"), F.lit("")),
                F.coalesce(F.col("fare_amount").cast("string"), F.lit("")),
                F.coalesce(F.col("pickup_zip").cast("string"), F.lit("")),
                F.coalesce(F.col("dropoff_zip").cast("string"), F.lit("")),
            ),
            256,
        ),
    ).dropDuplicates(["trip_id"])

    # 3) Derived measures and calendar attributes
    trips = (
        trips.withColumn(
            "duration_min",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0,
        )
        .withColumn(
            "speed_mph",
            F.try_divide(F.col("trip_distance"), F.col("duration_min") / 60.0),
        )
        .withColumn(
            "fare_per_mile", F.try_divide(F.col("fare_amount"), F.col("trip_distance"))
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("day_of_week", F.dayofweek("pickup_ts"))  # 1 = Sunday ... 7 = Saturday
        .withColumn("day_name", F.date_format("pickup_ts", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("pickup_ts").isin(1, 7))
    )

    # 4) Enrichment: borough of origin and destination (small dimension -> broadcast join)
    pickup_dim = F.broadcast(
        dim_borough.select(
            F.col("zip3").alias("pickup_zip3"), F.col("borough").alias("pickup_borough")
        )
    )
    dropoff_dim = F.broadcast(
        dim_borough.select(
            F.col("zip3").alias("dropoff_zip3"), F.col("borough").alias("dropoff_borough")
        )
    )
    trips = (
        trips.join(
            pickup_dim, F.floor(F.col("pickup_zip") / 100) == F.col("pickup_zip3"), "left"
        )
        .join(
            dropoff_dim, F.floor(F.col("dropoff_zip") / 100) == F.col("dropoff_zip3"), "left"
        )
        .withColumn("pickup_borough", F.coalesce("pickup_borough", F.lit("Outside NYC")))
        .withColumn("dropoff_borough", F.coalesce("dropoff_borough", F.lit("Outside NYC")))
        .withColumn(
            "is_airport_trip",
            F.coalesce(
                F.col("pickup_zip").isin(AIRPORT_ZIPS) | F.col("dropoff_zip").isin(AIRPORT_ZIPS),
                F.lit(False),
            ),
        )
        .drop("pickup_zip3", "dropoff_zip3")
    )

    # 5) Validation: the first rule that fails is recorded
    reject_reason = (
        F.when(
            F.col("pickup_ts").isNull()
            | F.col("dropoff_ts").isNull()
            | F.col("trip_distance").isNull()
            | F.col("fare_amount").isNull()
            | F.col("pickup_zip").isNull()
            | F.col("dropoff_zip").isNull(),
            "missing_values",
        )
        .when(F.col("dropoff_ts") <= F.col("pickup_ts"), "invalid_time_range")
        .when(F.col("duration_min") > MAX_TRIP_DURATION_MIN, "duration_too_long")
        .when(
            (F.col("trip_distance") <= 0) | (F.col("trip_distance") > MAX_TRIP_DISTANCE_MI),
            "invalid_distance",
        )
        .when(
            (F.col("fare_amount") <= 0) | (F.col("fare_amount") > MAX_FARE_USD),
            "invalid_fare",
        )
        .when(F.col("speed_mph") > MAX_SPEED_MPH, "implausible_speed")
    )

    return trips.withColumn("reject_reason", reject_reason).select(
        "trip_id",
        "pickup_ts",
        "dropoff_ts",
        "pickup_date",
        "pickup_hour",
        "day_of_week",
        "day_name",
        "is_weekend",
        "pickup_zip",
        "dropoff_zip",
        "pickup_borough",
        "dropoff_borough",
        "is_airport_trip",
        "trip_distance",
        "fare_amount",
        "duration_min",
        "speed_mph",
        "fare_per_mile",
        "reject_reason",
    )

# COMMAND ----------

# DBTITLE 1,Build the Silver tables: valid trips and quarantine
dim_borough = spark.table(table_name("dim_borough"))
validated_df = clean_trips(spark.table(table_name("bronze_trips")), dim_borough)

# The validated set is written once and then split, so both tables come from the same computation.
save_table(
    validated_df,
    "silver_trips_validated",
    "SILVER (staging) - every deduplicated trip with its validation result",
)
validated = spark.table(table_name("silver_trips_validated"))

save_table(
    validated.filter(F.col("reject_reason").isNull()).drop("reject_reason"),
    "silver_trips",
    "SILVER - clean, deduplicated and enriched taxi trips (one row per valid trip)",
)
save_table(
    validated.filter(F.col("reject_reason").isNotNull()),
    "silver_trips_quarantine",
    "SILVER - trips rejected by the data quality rules, with the reason for rejection",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Data quality report
# MAGIC Every Bronze row must end up in exactly one place: removed as a duplicate, accepted in Silver, or quarantined.

# COMMAND ----------

# DBTITLE 1,Reconciliation of Bronze, Silver and Quarantine
n_bronze = row_count("bronze_trips")
n_validated = row_count("silver_trips_validated")
n_silver = row_count("silver_trips")
n_quarantine = row_count("silver_trips_quarantine")
n_duplicates = n_bronze - n_validated

print(f"Bronze rows                : {n_bronze:>8,}")
print(f"  duplicates removed       : {n_duplicates:>8,}")
print(f"  valid trips (Silver)     : {n_silver:>8,}  ({n_silver / n_bronze:.2%} of Bronze)")
print(f"  rejected (Quarantine)    : {n_quarantine:>8,}  ({n_quarantine / n_bronze:.2%} of Bronze)")

balanced = n_bronze == n_duplicates + n_silver + n_quarantine
print(f"\nBronze = duplicates + Silver + Quarantine : {'OK' if balanced else 'MISMATCH'}")

# COMMAND ----------

# DBTITLE 1,Rejected trips by reason
display(
    spark.sql(
        f"""
        SELECT reject_reason,
               COUNT(*)                                           AS trips,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_rejected
        FROM {table_name('silver_trips_quarantine')}
        GROUP BY reject_reason
        ORDER BY trips DESC
        """
    )
)

# COMMAND ----------

# DBTITLE 1,Sample of the clean Silver table
display(spark.table(table_name("silver_trips")).orderBy("pickup_ts").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold layer: business aggregates and analysis with Spark SQL
# MAGIC The Gold tables are small, aggregated and ready to be consumed by a dashboard or a report.
# MAGIC They are written in **Spark SQL** on top of the Silver table.
# MAGIC
# MAGIC ### 7.1 Gold tables

# COMMAND ----------

# DBTITLE 1,gold_hourly_demand: demand and service indicators per day, hour and borough
gold_hourly_demand_df = spark.sql(
    f"""
    SELECT pickup_date,
           pickup_hour,
           pickup_borough,
           COUNT(*)                    AS trips,
           ROUND(SUM(fare_amount), 2)  AS revenue_usd,
           ROUND(AVG(fare_amount), 2)  AS avg_fare_usd,
           ROUND(AVG(trip_distance), 2) AS avg_distance_mi,
           ROUND(AVG(duration_min), 2) AS avg_duration_min,
           ROUND(AVG(speed_mph), 2)    AS avg_speed_mph
    FROM {table_name('silver_trips')}
    GROUP BY pickup_date, pickup_hour, pickup_borough
    """
)

save_table(
    gold_hourly_demand_df,
    "gold_hourly_demand",
    "GOLD - trips, revenue and service indicators per pickup date, hour and borough",
)

# COMMAND ----------

# DBTITLE 1,gold_borough_kpis: where demand and revenue come from
gold_borough_kpis_df = spark.sql(
    f"""
    SELECT pickup_borough,
           COUNT(*)                                             AS trips,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)   AS pct_of_trips,
           ROUND(SUM(fare_amount), 2)                           AS revenue_usd,
           ROUND(100.0 * SUM(fare_amount) / SUM(SUM(fare_amount)) OVER (), 2) AS pct_of_revenue,
           ROUND(AVG(fare_amount), 2)                           AS avg_fare_usd,
           ROUND(AVG(trip_distance), 2)                         AS avg_distance_mi,
           ROUND(AVG(duration_min), 2)                          AS avg_duration_min,
           ROUND(SUM(fare_amount) / SUM(trip_distance), 2)      AS revenue_per_mile_usd,
           ROUND(100.0 * AVG(CASE WHEN is_airport_trip THEN 1 ELSE 0 END), 2) AS pct_airport_trips,
           DENSE_RANK() OVER (ORDER BY COUNT(*) DESC)           AS demand_rank
    FROM {table_name('silver_trips')}
    GROUP BY pickup_borough
    """
)

save_table(
    gold_borough_kpis_df,
    "gold_borough_kpis",
    "GOLD - demand, revenue and service indicators per pickup borough",
)

# COMMAND ----------

# DBTITLE 1,gold_top_routes: the five busiest routes that start in each borough
gold_top_routes_df = spark.sql(
    f"""
    WITH routes AS (
        SELECT pickup_borough,
               pickup_zip,
               dropoff_zip,
               dropoff_borough,
               COUNT(*)                     AS trips,
               ROUND(AVG(fare_amount), 2)   AS avg_fare_usd,
               ROUND(AVG(trip_distance), 2) AS avg_distance_mi,
               ROUND(AVG(duration_min), 2)  AS avg_duration_min
        FROM {table_name('silver_trips')}
        GROUP BY pickup_borough, pickup_zip, dropoff_zip, dropoff_borough
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY pickup_borough
                   ORDER BY trips DESC, pickup_zip, dropoff_zip
               ) AS route_rank
        FROM routes
    )
    SELECT *
    FROM ranked
    WHERE route_rank <= 5
    """
)

save_table(
    gold_top_routes_df,
    "gold_top_routes",
    "GOLD - top 5 routes (pickup ZIP to dropoff ZIP) by number of trips for each pickup borough",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Q1: When is demand highest?
# MAGIC To compare weekdays with weekends fairly, the number of trips in each hour is divided by the number of
# MAGIC weekdays (or weekend days) present in the data, giving the **average number of trips per hour of a typical day**.

# COMMAND ----------

# DBTITLE 1,Average trips per hour of the day: weekdays vs weekends
hourly_profile_df = spark.sql(
    f"""
    WITH day_counts AS (
        SELECT is_weekend, COUNT(DISTINCT pickup_date) AS n_days
        FROM {table_name('silver_trips')}
        GROUP BY is_weekend
    ),
    hour_counts AS (
        SELECT is_weekend, pickup_hour, COUNT(*) AS trips
        FROM {table_name('silver_trips')}
        GROUP BY is_weekend, pickup_hour
    )
    SELECT h.pickup_hour,
           ROUND(MAX(CASE WHEN NOT h.is_weekend THEN h.trips / d.n_days END), 2) AS weekday_avg_trips,
           ROUND(MAX(CASE WHEN h.is_weekend THEN h.trips / d.n_days END), 2)     AS weekend_avg_trips
    FROM hour_counts h
    JOIN day_counts d ON h.is_weekend = d.is_weekend
    GROUP BY h.pickup_hour
    ORDER BY h.pickup_hour
    """
)

display(hourly_profile_df)

# COMMAND ----------

# DBTITLE 1,Chart: hourly demand profile
hourly_profile_pd = hourly_profile_df.toPandas().fillna(0)

fig, ax = plt.subplots(figsize=(11, 4.5))
ax.plot(hourly_profile_pd["pickup_hour"], hourly_profile_pd["weekday_avg_trips"], marker="o", label="Weekday")
ax.plot(hourly_profile_pd["pickup_hour"], hourly_profile_pd["weekend_avg_trips"], marker="s", label="Weekend")
ax.set_title("Average number of trips per hour of the day")
ax.set_xlabel("Pick-up hour")
ax.set_ylabel("Average trips per day")
ax.set_xticks(range(0, 24))
ax.grid(alpha=0.3)
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Demand by day of the week
display(
    spark.sql(
        f"""
        SELECT day_of_week,
               day_name,
               COUNT(DISTINCT pickup_date)                       AS days_in_data,
               COUNT(*)                                          AS trips,
               ROUND(COUNT(*) / COUNT(DISTINCT pickup_date), 1)  AS avg_trips_per_day,
               ROUND(AVG(fare_amount), 2)                        AS avg_fare_usd,
               ROUND(AVG(speed_mph), 2)                          AS avg_speed_mph
        FROM {table_name('silver_trips')}
        GROUP BY day_of_week, day_name
        ORDER BY day_of_week
        """
    )
)

# COMMAND ----------

# DBTITLE 1,Daily trend with a 7-day moving average (window function)
daily_trend_df = spark.sql(
    f"""
    WITH daily AS (
        SELECT pickup_date,
               SUM(trips)       AS trips,
               SUM(revenue_usd) AS revenue_usd
        FROM {table_name('gold_hourly_demand')}
        GROUP BY pickup_date
    )
    SELECT pickup_date,
           trips,
           ROUND(revenue_usd, 2) AS revenue_usd,
           ROUND(AVG(trips) OVER (ORDER BY pickup_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1)
               AS trips_7d_moving_avg
    FROM daily
    ORDER BY pickup_date
    """
)

display(daily_trend_df)

# COMMAND ----------

# DBTITLE 1,Chart: daily trips and 7-day moving average
daily_trend_pd = daily_trend_df.toPandas()

fig, ax = plt.subplots(figsize=(11, 4.5))
ax.bar(daily_trend_pd["pickup_date"], daily_trend_pd["trips"], color="lightsteelblue", label="Trips per day")
ax.plot(
    daily_trend_pd["pickup_date"],
    daily_trend_pd["trips_7d_moving_avg"],
    color="darkblue",
    linewidth=2,
    label="7-day moving average",
)
ax.set_title("Daily number of trips")
ax.set_xlabel("Pick-up date")
ax.set_ylabel("Trips")
ax.grid(axis="y", alpha=0.3)
ax.legend()
fig.autofmt_xdate()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Q2: Where does demand and revenue come from?

# COMMAND ----------

# DBTITLE 1,Indicators per pickup borough
borough_kpis_df = spark.table(table_name("gold_borough_kpis")).orderBy("demand_rank")
display(borough_kpis_df)

# COMMAND ----------

# DBTITLE 1,Chart: share of trips and of revenue per borough
borough_kpis_pd = borough_kpis_df.select("pickup_borough", "pct_of_trips", "pct_of_revenue").toPandas()
borough_kpis_pd["pct_of_trips"] = borough_kpis_pd["pct_of_trips"].astype(float)
borough_kpis_pd["pct_of_revenue"] = borough_kpis_pd["pct_of_revenue"].astype(float)

positions = range(len(borough_kpis_pd))
bar_width = 0.4

fig, ax = plt.subplots(figsize=(11, 4.5))
ax.bar([p - bar_width / 2 for p in positions], borough_kpis_pd["pct_of_trips"], bar_width, label="% of trips")
ax.bar([p + bar_width / 2 for p in positions], borough_kpis_pd["pct_of_revenue"], bar_width, label="% of revenue")
ax.set_xticks(list(positions))
ax.set_xticklabels(borough_kpis_pd["pickup_borough"])
ax.set_title("Share of trips and of revenue by pickup borough")
ax.set_ylabel("Percentage")
ax.grid(axis="y", alpha=0.3)
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Top 5 routes starting in each borough
display(
    spark.table(table_name("gold_top_routes")).orderBy(F.col("pickup_borough"), F.col("route_rank"))
)

# COMMAND ----------

# DBTITLE 1,Airport trips compared with all other trips
display(
    spark.sql(
        f"""
        SELECT CASE WHEN is_airport_trip THEN 'Airport (JFK or LaGuardia)' ELSE 'Other trips' END AS trip_type,
               COUNT(*)                                            AS trips,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS pct_of_trips,
               ROUND(100.0 * SUM(fare_amount) / SUM(SUM(fare_amount)) OVER (), 2) AS pct_of_revenue,
               ROUND(AVG(fare_amount), 2)                          AS avg_fare_usd,
               ROUND(AVG(trip_distance), 2)                        AS avg_distance_mi,
               ROUND(AVG(duration_min), 2)                         AS avg_duration_min
        FROM {table_name('silver_trips')}
        GROUP BY is_airport_trip
        ORDER BY trips DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Structured Streaming: incremental ingestion of trip events
# MAGIC
# MAGIC **Q3: can the indicators be kept up to date as new trip events arrive, with the same results as batch?**
# MAGIC
# MAGIC In production, trips do not arrive as one finished table. A dispatch system continuously drops small
# MAGIC **JSON files** with new trip events into cloud storage. We reproduce this scenario:
# MAGIC
# MAGIC 1. An upstream system is simulated by exporting the trips as JSON files to a Unity Catalog **Volume**
# MAGIC    (the landing zone) in **two drops**: first the older half of the period, later the newer half.
# MAGIC 2. A chain of three Structured Streaming queries moves the events through the medallion layers:
# MAGIC
# MAGIC | Query | Source | Processing | Sink | Output mode |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | JSON files in the Volume | add file name and ingestion time | `bronze_trips_stream` | append |
# MAGIC | 2 | `bronze_trips_stream` | `clean_trips()`: stream-static join and stateful deduplication | `silver_trips_stream` | append |
# MAGIC | 3 | `silver_trips_stream` | 1-hour tumbling window aggregation per borough | `gold_hourly_demand_stream` | complete |
# MAGIC
# MAGIC Key concepts applied:
# MAGIC * **Checkpointing.** Each query stores its progress in the Volume. When the query is started again it processes
# MAGIC   **only the new data**, exactly once, which is what makes the pipeline incremental and fault tolerant.
# MAGIC * **Micro-batches.** `maxFilesPerTrigger` limits how many files each micro-batch reads.
# MAGIC * **Trigger.** Serverless compute supports the `availableNow` trigger: the query processes everything that is
# MAGIC   available, in several micro-batches, and then stops. Scheduling it periodically gives near real-time
# MAGIC   processing without paying for an always-on cluster.
# MAGIC * **Unified API.** Query 2 calls the same `clean_trips()` function used by the batch pipeline.

# COMMAND ----------

# DBTITLE 1,Simulate the upstream system: split the events in two drops
bounds = spark.table(SOURCE_TABLE).agg(
    F.min("tpep_pickup_datetime").alias("first_ts"),
    F.max("tpep_pickup_datetime").alias("last_ts"),
).first()

# Midpoint of the period covered by the data
split_ts = bounds["first_ts"] + (bounds["last_ts"] - bounds["first_ts"]) / 2
print(f"Period covered : {bounds['first_ts']}  to  {bounds['last_ts']}")
print(f"Drop 1         : pick-ups before {split_ts}")
print(f"Drop 2         : pick-ups from   {split_ts}")

events_df = spark.table(SOURCE_TABLE)
drop_1_df = events_df.filter(F.col("tpep_pickup_datetime") < F.lit(split_ts))
drop_2_df = events_df.filter(
    (F.col("tpep_pickup_datetime") >= F.lit(split_ts)) | F.col("tpep_pickup_datetime").isNull()
)


def land_events(df: DataFrame) -> None:
    """Write a set of trip events to the landing zone as new JSON files."""
    df.repartition(FILES_PER_DROP).write.mode("append").json(LANDING_PATH)

# COMMAND ----------

# DBTITLE 1,Definition of the three streaming queries
# File sources need an explicit schema in streaming: there is no schema inference at run time.
trip_event_schema = StructType(
    [
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("pickup_zip", IntegerType(), True),
        StructField("dropoff_zip", IntegerType(), True),
    ]
)


def start_bronze_stream():
    """Query 1: JSON files in the landing zone -> bronze_trips_stream (append)."""
    events = (
        spark.readStream.format("json")
        .schema(trip_event_schema)
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(LANDING_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return (
        events.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze_trips_stream")
        .trigger(availableNow=True)
        .toTable(table_name("bronze_trips_stream"))
    )


def start_silver_stream():
    """Query 2: bronze_trips_stream -> silver_trips_stream (append), same rules as batch."""
    bronze_stream = spark.readStream.table(table_name("bronze_trips_stream"))
    silver_stream = (
        clean_trips(bronze_stream, spark.table(table_name("dim_borough")))
        .filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
    )
    return (
        silver_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/silver_trips_stream")
        .trigger(availableNow=True)
        .toTable(table_name("silver_trips_stream"))
    )


def start_gold_stream():
    """Query 3: silver_trips_stream -> gold_hourly_demand_stream (1-hour windows, complete)."""
    hourly = (
        spark.readStream.table(table_name("silver_trips_stream"))
        .groupBy(F.window("pickup_ts", "1 hour").alias("time_window"), "pickup_borough")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("fare_amount"), 2).alias("revenue_usd"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "pickup_borough",
            "trips",
            "revenue_usd",
            "avg_fare_usd",
            "avg_distance_mi",
        )
    )
    return (
        hourly.writeStream.format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/gold_hourly_demand_stream")
        .trigger(availableNow=True)
        .toTable(table_name("gold_hourly_demand_stream"))
    )


def run_streaming_pipeline(label: str) -> None:
    """Run the three queries in sequence until all available data is processed."""
    for start_query in (start_bronze_stream, start_silver_stream, start_gold_stream):
        start_query().awaitTermination()
    print(f"[{label}] streaming tables after the run:")
    for name in ("bronze_trips_stream", "silver_trips_stream", "gold_hourly_demand_stream"):
        print(f"  {name:<28}: {row_count(name):>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 First drop of events

# COMMAND ----------

# DBTITLE 1,Drop 1 arrives and the streaming pipeline runs
land_events(drop_1_df)
run_streaming_pipeline("after drop 1")

bronze_stream_version_after_drop_1 = (
    spark.sql(f"DESCRIBE HISTORY {table_name('bronze_trips_stream')}")
    .agg(F.max("version"))
    .first()[0]
)

# COMMAND ----------

# DBTITLE 1,Streaming Gold table after drop 1
display(
    spark.sql(
        f"""
        SELECT MIN(window_start) AS first_window,
               MAX(window_end)   AS last_window,
               COUNT(*)          AS hourly_windows,
               SUM(trips)        AS trips,
               ROUND(SUM(revenue_usd), 2) AS revenue_usd
        FROM {table_name('gold_hourly_demand_stream')}
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Second drop of events: incremental processing
# MAGIC New files arrive in the landing zone and the **same** queries are started again. Thanks to the checkpoints they
# MAGIC do not reprocess drop 1: only the new files are read, and the Gold aggregates are updated.

# COMMAND ----------

# DBTITLE 1,Drop 2 arrives and the streaming pipeline runs again
land_events(drop_2_df)
run_streaming_pipeline("after drop 2")

# COMMAND ----------

# DBTITLE 1,Streaming Gold table after drop 2
display(
    spark.sql(
        f"""
        SELECT MIN(window_start) AS first_window,
               MAX(window_end)   AS last_window,
               COUNT(*)          AS hourly_windows,
               SUM(trips)        AS trips,
               ROUND(SUM(revenue_usd), 2) AS revenue_usd
        FROM {table_name('gold_hourly_demand_stream')}
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Evidence of micro-batches
# MAGIC Every streaming micro-batch is one ACID commit in the Delta transaction log. The history of the streaming
# MAGIC Bronze table therefore shows how the files were consumed: several small `STREAMING UPDATE` commits for each drop.

# COMMAND ----------

# DBTITLE 1,Delta history of the streaming Bronze table: one commit per micro-batch
display(
    spark.sql(f"DESCRIBE HISTORY {table_name('bronze_trips_stream')}")
    .select(
        "version",
        "timestamp",
        "operation",
        F.expr("try_element_at(operationMetrics, 'numOutputRows')").alias("rows_written"),
    )
    .orderBy("version")
)

# COMMAND ----------

# DBTITLE 1,Events ingested from each landed JSON file
display(
    spark.table(table_name("bronze_trips_stream"))
    .groupBy(F.element_at(F.split("_source_file", "/"), -1).alias("source_file"))
    .agg(
        F.count("*").alias("events"),
        F.min("tpep_pickup_datetime").alias("first_pickup"),
        F.max("tpep_pickup_datetime").alias("last_pickup"),
        F.min("_ingested_at").alias("ingested_at"),
    )
    .orderBy("ingested_at", "source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Reconciliation: streaming vs batch
# MAGIC The streaming path received the same events as the batch path, only in a different format (JSON files) and in
# MAGIC two instalments. If the pipeline is correct, both paths must produce **exactly** the same Silver trips and the
# MAGIC same Gold totals.

# COMMAND ----------

# DBTITLE 1,Compare the streaming tables with the batch tables
batch_silver = spark.table(table_name("silver_trips"))
stream_silver = spark.table(table_name("silver_trips_stream"))

n_batch = batch_silver.count()
n_stream = stream_silver.count()
only_in_batch = batch_silver.join(stream_silver, "trip_id", "left_anti").count()
only_in_stream = stream_silver.join(batch_silver, "trip_id", "left_anti").count()

batch_totals = spark.table(table_name("gold_hourly_demand")).agg(
    F.sum("trips").alias("trips"), F.round(F.sum("revenue_usd"), 2).alias("revenue")
).first()
stream_totals = spark.table(table_name("gold_hourly_demand_stream")).agg(
    F.sum("trips").alias("trips"), F.round(F.sum("revenue_usd"), 2).alias("revenue")
).first()

print(f"Silver trips    batch = {n_batch:,}   streaming = {n_stream:,}")
print(f"Trips only in the batch table     : {only_in_batch:,}")
print(f"Trips only in the streaming table : {only_in_stream:,}")
print(f"Gold trips      batch = {batch_totals['trips']:,}   streaming = {stream_totals['trips']:,}")
print(f"Gold revenue    batch = {batch_totals['revenue']:,.2f}   streaming = {stream_totals['revenue']:,.2f}")

streaming_matches_batch = (
    n_batch == n_stream
    and only_in_batch == 0
    and only_in_stream == 0
    and batch_totals["trips"] == stream_totals["trips"]
    and abs(batch_totals["revenue"] - stream_totals["revenue"]) < 0.01
)
print(f"\nStreaming results identical to batch results : {'YES' if streaming_matches_batch else 'NO'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Graph analytics: the city as a network of zones
# MAGIC
# MAGIC **Q4: which zones are the most important hubs, and which zones export or import vehicles?**
# MAGIC
# MAGIC Trips have a natural graph structure:
# MAGIC * **Vertices** are the zones (ZIP codes).
# MAGIC * **Edges** are directed routes from the pick-up zone to the drop-off zone, **weighted** by the number of trips.
# MAGIC
# MAGIC Vertices and edges are stored as two DataFrames with the `id` / `src` / `dst` column convention used by GraphFrames.
# MAGIC Serverless compute cannot install the GraphFrames JVM library, so the graph algorithms are written
# MAGIC directly with **DataFrame joins and aggregations**, which is also what a graph library does internally.

# COMMAND ----------

# DBTITLE 1,Build the graph: vertices and edges
silver = spark.table(table_name("silver_trips"))

edges_df = silver.groupBy(
    F.col("pickup_zip").alias("src"), F.col("dropoff_zip").alias("dst")
).agg(
    F.count("*").alias("weight"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
)

vertices_df = (
    silver.select(F.col("pickup_zip").alias("id"), F.col("pickup_borough").alias("borough"))
    .union(silver.select(F.col("dropoff_zip").alias("id"), F.col("dropoff_borough").alias("borough")))
    .distinct()
    .withColumn("is_airport", F.col("id").isin(AIRPORT_ZIPS))
)

save_table(vertices_df, "gold_graph_vertices", "GOLD graph - one vertex per ZIP code (zone)")
save_table(
    edges_df,
    "gold_graph_edges",
    "GOLD graph - directed edge from pickup ZIP (src) to dropoff ZIP (dst), weight = number of trips",
)

vertices = spark.table(table_name("gold_graph_vertices"))
edges = spark.table(table_name("gold_graph_edges"))

# COMMAND ----------

# DBTITLE 1,Size and density of the network
n_vertices = vertices.count()
n_edges = edges.count()
n_self_loops = edges.filter(F.col("src") == F.col("dst")).count()
total_weight = edges.agg(F.sum("weight")).first()[0]
self_loop_weight = edges.filter(F.col("src") == F.col("dst")).agg(F.coalesce(F.sum("weight"), F.lit(0))).first()[0]

print(f"Vertices (zones)                : {n_vertices:,}")
print(f"Directed edges (routes)         : {n_edges:,}")
print(f"Density (edges / possible edges): {n_edges / (n_vertices * n_vertices):.2%}")
print(f"Routes inside a single zone     : {n_self_loops:,} routes, {self_loop_weight / total_weight:.2%} of all trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Degrees and net flow
# MAGIC * **Out-degree / in-degree**: number of distinct zones a zone sends trips to / receives trips from.
# MAGIC * **Weighted out / in degree**: number of trips that start / end in the zone.
# MAGIC * **Net flow** = trips in - trips out. A positive value means the zone **accumulates vehicles** (more arrivals
# MAGIC   than departures); a negative value means the zone **loses vehicles** and has to be resupplied.

# COMMAND ----------

# DBTITLE 1,Degree metrics per zone
out_degrees = edges.groupBy(F.col("src").alias("id")).agg(
    F.count("*").alias("out_degree"), F.sum("weight").alias("trips_out")
)
in_degrees = edges.groupBy(F.col("dst").alias("id")).agg(
    F.count("*").alias("in_degree"), F.sum("weight").alias("trips_in")
)

zone_degrees_df = (
    vertices.join(out_degrees, "id", "left")
    .join(in_degrees, "id", "left")
    .fillna(0, subset=["out_degree", "trips_out", "in_degree", "trips_in"])
    .withColumn("net_flow", F.col("trips_in") - F.col("trips_out"))
)

display(zone_degrees_df.orderBy(F.desc("trips_out"), "id").limit(10))

# COMMAND ----------

# DBTITLE 1,Zones that accumulate vehicles (largest positive net flow)
display(
    zone_degrees_df.select("id", "borough", "is_airport", "trips_in", "trips_out", "net_flow")
    .orderBy(F.desc("net_flow"), "id")
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Zones that lose vehicles (largest negative net flow)
display(
    zone_degrees_df.select("id", "borough", "is_airport", "trips_in", "trips_out", "net_flow")
    .orderBy(F.asc("net_flow"), "id")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 PageRank: the most important hubs
# MAGIC Counting trips only measures local activity. **PageRank** measures the importance of a zone inside the whole
# MAGIC network: a zone is important if it receives many trips **from zones that are themselves important**.
# MAGIC It can be read as the probability of finding, in the long run, a taxi that keeps following passenger trips.
# MAGIC
# MAGIC We implement the weighted PageRank iteratively with DataFrames:
# MAGIC
# MAGIC `rank(v) = (1 - d) / N  +  d * ( SUM over edges u->v of rank(u) * weight(u,v) / trips_out(u)  +  dangling / N )`
# MAGIC
# MAGIC where `d` is the damping factor, `N` the number of zones and `dangling` the total rank of zones without
# MAGIC outgoing trips (redistributed uniformly so that the ranks always sum to 1).
# MAGIC
# MAGIC Each iteration is a distributed join and aggregation over the edges. The resulting rank vector has one row per
# MAGIC zone, so it is small: it is collected to the driver and turned into a new DataFrame, which keeps the query plan
# MAGIC of every iteration short instead of letting the lineage grow with the number of iterations.

# COMMAND ----------

# DBTITLE 1,Weighted PageRank implemented with DataFrames
def weighted_pagerank(vertices: DataFrame, edges: DataFrame, damping: float, max_iter: int, tolerance: float):
    """Iterative weighted PageRank. Returns (ranks DataFrame [id, pagerank], iterations run)."""
    vertex_ids = [row["id"] for row in vertices.select("id").distinct().collect()]
    n = len(vertex_ids)

    # Transition probability of each edge: share of the trips leaving `src` that go to `dst`
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
        ranks_df = spark.createDataFrame(list(ranks.items()), "id INT, rank DOUBLE")

        # Distributed step: every zone sends its rank along its outgoing edges
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
        print(f"  iteration {iteration:>2}: total change in ranks = {change:.8f}")
        if change < tolerance:
            break

    ranks_df = spark.createDataFrame(list(ranks.items()), "id INT, pagerank DOUBLE")
    return ranks_df, iterations


pagerank_df, pagerank_iterations = weighted_pagerank(
    vertices, edges, PAGERANK_DAMPING, PAGERANK_MAX_ITER, PAGERANK_TOLERANCE
)
print(f"\nPageRank finished after {pagerank_iterations} iterations")
print(f"Sum of all ranks = {pagerank_df.agg(F.sum('pagerank')).first()[0]:.6f} (expected 1.0)")

# COMMAND ----------

# DBTITLE 1,Save the zone ranking and show the top 15 hubs
zone_pagerank_df = (
    zone_degrees_df.join(pagerank_df, "id")
    .withColumn("pagerank", F.round("pagerank", 6))
    .select(
        "id", "borough", "is_airport", "pagerank",
        "out_degree", "in_degree", "trips_out", "trips_in", "net_flow",
    )
)

save_table(
    zone_pagerank_df,
    "gold_zone_pagerank",
    "GOLD graph - PageRank, degrees and net flow of each zone (ZIP code)",
)

top_hubs_df = spark.sql(
    f"""
    SELECT RANK() OVER (ORDER BY pagerank DESC)     AS pagerank_position,
           RANK() OVER (ORDER BY trips_out DESC)    AS pickup_volume_position,
           id                                       AS zip_code,
           borough,
           is_airport,
           pagerank,
           trips_out,
           trips_in,
           net_flow
    FROM {table_name('gold_zone_pagerank')}
    ORDER BY pagerank DESC, id
    LIMIT 15
    """
)
display(top_hubs_df)

# COMMAND ----------

# DBTITLE 1,Chart: top 15 zones by PageRank
top_hubs_pd = top_hubs_df.toPandas().iloc[::-1]
bar_labels = [f"{zip_code} ({borough})" for zip_code, borough in zip(top_hubs_pd["zip_code"], top_hubs_pd["borough"])]
bar_colors = ["darkorange" if is_airport else "steelblue" for is_airport in top_hubs_pd["is_airport"]]

fig, ax = plt.subplots(figsize=(11, 6))
ax.barh(bar_labels, top_hubs_pd["pagerank"].astype(float), color=bar_colors)
ax.set_title("Top 15 zones by PageRank (orange = airport)")
ax.set_xlabel("PageRank")
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 Strongest connections and reciprocity
# MAGIC A graph *motif* query finds pairs of zones connected in **both** directions (A -> B and B -> A) by joining the
# MAGIC edge table with itself. Comparing the two directions shows whether a connection is balanced or one-way.

# COMMAND ----------

# DBTITLE 1,Busiest connections between two different zones, both directions
display(
    spark.sql(
        f"""
        SELECT ab.src                                   AS zone_a,
               ab.dst                                   AS zone_b,
               ab.weight                                AS trips_a_to_b,
               ba.weight                                AS trips_b_to_a,
               ab.weight + ba.weight                    AS total_trips,
               ROUND(ab.weight / (ab.weight + ba.weight), 2) AS share_a_to_b
        FROM {table_name('gold_graph_edges')} ab
        JOIN {table_name('gold_graph_edges')} ba
          ON ab.src = ba.dst AND ab.dst = ba.src
        WHERE ab.src < ab.dst
        ORDER BY total_trips DESC, zone_a, zone_b
        LIMIT 10
        """
    )
)

# COMMAND ----------

# DBTITLE 1,How much of the network is reciprocal?
reciprocity = spark.sql(
    f"""
    SELECT COUNT(*)                                             AS routes_between_different_zones,
           COUNT(ba.src)                                        AS routes_with_a_return_route,
           ROUND(100.0 * COUNT(ba.src) / COUNT(*), 2)           AS pct_reciprocal_routes
    FROM {table_name('gold_graph_edges')} ab
    LEFT JOIN {table_name('gold_graph_edges')} ba
      ON ab.src = ba.dst AND ab.dst = ba.src
    WHERE ab.src <> ab.dst
    """
)
display(reciprocity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Delta Lake: audit history and time travel
# MAGIC Every table of the project is a Delta table, so each write is an ACID transaction recorded in a log.
# MAGIC This gives two capabilities that plain files in a data lake do not have:
# MAGIC * **Audit**: who changed what and when (`DESCRIBE HISTORY`).
# MAGIC * **Time travel**: query a table as it was at a previous version (`VERSION AS OF`), which is useful to reproduce
# MAGIC   a report or to recover from a wrong load.

# COMMAND ----------

# DBTITLE 1,Time travel: the streaming Bronze table before and after the second drop
rows_after_drop_1 = spark.sql(
    f"SELECT COUNT(*) FROM {table_name('bronze_trips_stream')} VERSION AS OF {bronze_stream_version_after_drop_1}"
).first()[0]
rows_now = row_count("bronze_trips_stream")

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
# MAGIC ## 11. Conclusions
# MAGIC The next cell computes the headline numbers directly from the Gold tables, so that the conclusions always
# MAGIC reflect the data that was actually processed.

# COMMAND ----------

# DBTITLE 1,Key findings computed from the Gold tables
peak = spark.sql(
    f"""
    SELECT pickup_hour, SUM(trips) AS trips
    FROM {table_name('gold_hourly_demand')}
    GROUP BY pickup_hour
    ORDER BY trips DESC, pickup_hour
    LIMIT 1
    """
).first()
quietest = spark.sql(
    f"""
    SELECT pickup_hour, SUM(trips) AS trips
    FROM {table_name('gold_hourly_demand')}
    GROUP BY pickup_hour
    ORDER BY trips ASC, pickup_hour
    LIMIT 1
    """
).first()
busiest_day = spark.sql(
    f"""
    SELECT day_name, COUNT(*) / COUNT(DISTINCT pickup_date) AS avg_trips
    FROM {table_name('silver_trips')}
    GROUP BY day_name
    ORDER BY avg_trips DESC, day_name
    LIMIT 1
    """
).first()
top_borough = spark.table(table_name("gold_borough_kpis")).orderBy("demand_rank", "pickup_borough").first()
airport = spark.sql(
    f"""
    SELECT ROUND(100.0 * AVG(CASE WHEN is_airport_trip THEN 1 ELSE 0 END), 2) AS pct_trips,
           ROUND(100.0 * SUM(CASE WHEN is_airport_trip THEN fare_amount ELSE 0 END) / SUM(fare_amount), 2)
               AS pct_revenue
    FROM {table_name('silver_trips')}
    """
).first()
top_hub = spark.table(table_name("gold_zone_pagerank")).orderBy(F.desc("pagerank"), "id").first()
top_importer = spark.table(table_name("gold_zone_pagerank")).orderBy(F.desc("net_flow"), "id").first()
top_exporter = spark.table(table_name("gold_zone_pagerank")).orderBy(F.asc("net_flow"), "id").first()
pct_reciprocal = reciprocity.first()["pct_reciprocal_routes"]

print("DATA QUALITY")
print(f"  {n_bronze:,} raw trips -> {n_silver:,} valid trips ({n_silver / n_bronze:.2%});"
      f" {n_quarantine:,} quarantined, {n_duplicates:,} duplicates removed.")
print("\nQ1 - WHEN is demand highest?")
print(f"  Peak hour     : {peak['pickup_hour']:02d}:00 ({peak['trips']:,} trips in the period)")
print(f"  Quietest hour : {quietest['pickup_hour']:02d}:00 ({quietest['trips']:,} trips in the period)")
print(f"  Busiest day   : {busiest_day['day_name']} ({busiest_day['avg_trips']:.1f} trips per day on average)")
print("\nQ2 - WHERE does demand and revenue come from?")
print(f"  {top_borough['pickup_borough']} originates {top_borough['pct_of_trips']}% of the trips"
      f" and {top_borough['pct_of_revenue']}% of the revenue.")
print(f"  Airport trips are {airport['pct_trips']}% of the trips and {airport['pct_revenue']}% of the revenue.")
print("\nQ3 - Can the indicators be maintained incrementally?")
print(f"  Streaming results identical to batch results: {'YES' if streaming_matches_batch else 'NO'}"
      f" ({n_stream:,} trips processed from JSON events in two drops).")
print("\nQ4 - Which zones are the hubs of the network?")
print(f"  Network of {n_vertices:,} zones and {n_edges:,} routes; {pct_reciprocal}% of the routes have a return route.")
print(f"  Most important hub (PageRank)   : ZIP {top_hub['id']} ({top_hub['borough']}), PageRank {top_hub['pagerank']}")
print(f"  Largest accumulation of vehicles: ZIP {top_importer['id']} ({top_importer['borough']}),"
      f" net flow {top_importer['net_flow']:+,} trips")
print(f"  Largest loss of vehicles        : ZIP {top_exporter['id']} ({top_exporter['borough']}),"
      f" net flow {top_exporter['net_flow']:+,} trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What we built
# MAGIC An end-to-end lakehouse on Databricks Free Edition that takes raw taxi trips to decision-ready indicators,
# MAGIC using four processing styles of Apache Spark on a single engine and a single copy of the data:
# MAGIC batch DataFrames, Spark SQL, Structured Streaming and graph analytics.
# MAGIC
# MAGIC ### Answers to the business questions
# MAGIC * **Q1 (when).** The hourly profile for weekdays and weekends, the demand per day of the week and the daily trend
# MAGIC   (section 7.2) show when demand concentrates. The peak hour and the busiest day reported above tell an operator
# MAGIC   when to have more vehicles on the street.
# MAGIC * **Q2 (where).** The borough indicators and the top routes (section 7.3) show where trips and revenue originate.
# MAGIC   Comparing the share of trips with the share of revenue, per borough and for airport trips, shows which segments
# MAGIC   are worth more per trip than their volume suggests.
# MAGIC * **Q3 (incremental).** Yes. The streaming pipeline consumed the events in two instalments, processed only the new
# MAGIC   files in the second run, and reached exactly the same Silver trips and Gold totals as the batch pipeline (section 8.4).
# MAGIC * **Q4 (network).** PageRank identifies the hubs of the network, and the net flow identifies the zones that
# MAGIC   accumulate or lose vehicles (section 9). These are the zones where rebalancing the fleet has the most effect.
# MAGIC
# MAGIC ### What we learned about Big Data processing
# MAGIC * **Medallion architecture pays off.** Keeping the raw data in Bronze and quarantining rejected records instead of
# MAGIC   deleting them made every number traceable: Bronze = duplicates + Silver + Quarantine.
# MAGIC * **One engine, one set of rules.** The same `clean_trips()` function ran in batch and in streaming. Writing the
# MAGIC   business rules once is what guarantees that both paths agree.
# MAGIC * **Incremental processing is a property of the checkpoint**, not of the code: the same query, started again,
# MAGIC   continues where it stopped.
# MAGIC * **Graph algorithms are joins.** PageRank was expressed as a repeated join and aggregation, which Spark distributes
# MAGIC   like any other query.
# MAGIC * **Delta Lake** turns files into reliable tables: ACID commits per micro-batch, history and time travel.
# MAGIC
# MAGIC ### Limitations
# MAGIC * The source is a **sample** of the TLC data. The volume is small, but nothing in the pipeline depends on it: the
# MAGIC   same code runs unchanged on the full multi-year dataset, where Spark distributes the work over more executors.
# MAGIC * The borough is derived from the **ZIP prefix**, which is an approximation at borough level.
# MAGIC * Streaming was demonstrated with the `availableNow` trigger and simulated file drops, not with a live event source.
# MAGIC   The Gold stream uses *complete* output mode, which recomputes all windows; with an unbounded stream we would
# MAGIC   add a **watermark** so that old windows are finalised and their state is released.
# MAGIC
# MAGIC ### Future work
# MAGIC * **Machine learning with Spark ML**: predict the fare and the duration of a trip, and forecast hourly demand per zone
# MAGIC   using the Gold tables as features.
# MAGIC * Ingest the full TLC dataset and an external weather source to explain demand peaks.
# MAGIC * Schedule the streaming pipeline as a job and publish the Gold tables in a dashboard.
# MAGIC
# MAGIC ### References
# MAGIC * Damji, J., Wenig, B., Das, T., Lee, D. (2020). *Learning Spark: Lightning-fast Data Analysis*. O'Reilly.
# MAGIC * Chambers, B., Zaharia, M. (2018). *Spark: The Definitive Guide*. O'Reilly.
# MAGIC * NYC Taxi and Limousine Commission, TLC Trip Record Data (source of `samples.nyctaxi.trips`).
