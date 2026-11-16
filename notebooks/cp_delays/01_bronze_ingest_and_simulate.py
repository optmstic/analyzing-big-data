# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: ingest the CP timetable and simulate the movement log
# MAGIC
# MAGIC **Project question.** Where do CP train delays originate, and how do they propagate from station to station?
# MAGIC
# MAGIC This notebook builds the **Bronze** layer of the lakehouse in two steps:
# MAGIC
# MAGIC | Step | Input | Output |
# MAGIC |---|---|---|
# MAGIC | 1. Ingest | Six GTFS files of CP in the Volume `/Volumes/workspace/default/cp_raw/` | Six tables `workspace.cp_bronze.gtfs_*`, exactly as received |
# MAGIC | 2. Simulate | The real timetable from step 1 | `workspace.cp_bronze.movements_raw`: one row per train stop and day, six months, about 3.6 million rows |
# MAGIC
# MAGIC > **The delays are simulated.** CP publishes its timetable but no open record of how late its trains actually
# MAGIC > ran. Stations, trains, stop sequences and scheduled times in this project are **real** (official CP timetable);
# MAGIC > the *actual* arrival and departure times are **generated** by the model described in section 4. The model's
# MAGIC > parameters are assumptions chosen to give plausible magnitudes. They are **not calibrated** to CP's real
# MAGIC > punctuality, so the results show how the pipeline measures delay propagation, not how punctual CP really is.
# MAGIC
# MAGIC **Run order:** `00_setup_unity_catalog` (once), upload the GTFS files, then this notebook.
# MAGIC The notebook is idempotent: it overwrites its tables on every run and always produces the same data.
# MAGIC
# MAGIC **Platform rules followed** (Databricks Free Edition, serverless): only the PySpark DataFrame API and Spark SQL;
# MAGIC no RDDs, no `SparkContext`, no Python UDFs, no `dbfs:/` paths; files in a Unity Catalog Volume and tables in
# MAGIC Unity Catalog schemas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC Every name and every assumption of the simulation is in this cell, so that it can be explained and changed in one place.

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import math
import os

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

# ---- Unity Catalog ------------------------------------------------------------
RAW_PATH = "/Volumes/workspace/default/cp_raw"
BRONZE = "workspace.cp_bronze"
MOVEMENTS_TABLE = f"{BRONZE}.movements_raw"

GTFS_FILES = ["routes", "trips", "stops", "stop_times", "calendar", "calendar_dates"]

# ---- Period simulated: six full months ---------------------------------------
SIM_START_DATE = "2026-05-01"
SIM_END_DATE = "2026-10-31"

# The CP feed is a rolling snapshot: it is only complete from its download date onwards.
# The timetable of this complete week (Monday to Sunday) is replayed over the simulated period.
REFERENCE_WEEK_START = "2026-11-16"

# ---- Simulator ----------------------------------------------------------------
SIMULATOR_NAME = "cp_delay_simulator_v1"
SEED = 20261117                       # same seed = same data on every run

RUSH_HOURS = [7, 8, 9, 17, 18, 19]    # weekdays only

# Per service tier, in minutes:
#   origin_scale : average delay with which a train leaves its first station
#   drift        : average delay gained on each section between two stops
#   sigma        : random variation of the delay gained on each section
TIER_PARAMETERS = [
    # tier,            origin_scale, drift, sigma
    ("Alfa Pendular",  1.6,          0.04,  0.50),
    ("Intercidades",   2.0,          0.08,  0.60),
    ("InterRegional",  1.4,          0.05,  0.45),
    ("Regional",       1.2,          0.03,  0.40),
    ("Urban",          0.5,          0.01,  0.25),
]

RUSH_ORIGIN_FACTOR = 1.6              # trains start later in rush hours
RUSH_DRIFT_MIN = 0.05                 # extra delay per section in rush hours
CONGESTION_DRIFT_MIN = 0.04           # extra delay per section when arriving at the busiest station (scaled 0 to 1)

BAD_DAY_PROBABILITY = 0.06            # share of days with network-wide problems (weather, strikes, failures)
BAD_DAY_ORIGIN_FACTOR = 1.8
BAD_DAY_DRIFT_MIN = 0.06

INCIDENT_PROBABILITY = 0.003          # chance that a train suffers an incident on a section
INCIDENT_MIN_DELAY = 6.0              # minutes
INCIDENT_MEAN_EXTRA = 12.0            # minutes, on top of the minimum

DISRUPTION_PROBABILITY = 0.002        # chance that a station is disrupted during one hour of one day
DISRUPTION_MIN_DELAY = 4.0            # minutes added to every train arriving in that hour
DISRUPTION_MEAN_EXTRA = 8.0

MAX_EARLY_ARRIVAL_MIN = 2.0           # a train never arrives more than this ahead of the timetable

# ---- Imperfections of an operational source, for the Silver layer to handle -----
MISSING_READING_PROBABILITY = 0.002   # stop events whose actual times were not recorded
DUPLICATE_PROBABILITY = 0.003         # stop events delivered twice

print(f"Raw files       : {RAW_PATH}")
print(f"Bronze schema   : {BRONZE}")
print(f"Period simulated: {SIM_START_DATE} to {SIM_END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest the GTFS files into Bronze
# MAGIC Bronze keeps the files **exactly as received**: every column stays text and nothing is filtered. Two columns are
# MAGIC added so that every row can be traced: the file it came from and when it was loaded.
# MAGIC Interpreting the data (types, joins, rules) is the job of the Silver layer.

# COMMAND ----------

# DBTITLE 1,Check that the files and the Bronze table exist
missing_files = [f"{name}.txt" for name in GTFS_FILES if not os.path.exists(f"{RAW_PATH}/{name}.txt")]
if missing_files:
    raise FileNotFoundError(
        f"Missing files in {RAW_PATH}: {missing_files}. Upload them to the Volume and run the notebook again."
    )

if not spark.catalog.tableExists(MOVEMENTS_TABLE):
    raise RuntimeError(f"{MOVEMENTS_TABLE} does not exist. Run the notebook 00_setup_unity_catalog first.")

print("All GTFS files found and the Bronze schema is ready.")

# COMMAND ----------

# DBTITLE 1,Load each GTFS file as a Bronze Delta table
gtfs_log = []

for name in GTFS_FILES:
    raw_df = (
        spark.read.option("header", True)
        .option("inferSchema", False)          # Bronze keeps the text as it is
        .csv(f"{RAW_PATH}/{name}.txt")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    table = f"{BRONZE}.gtfs_{name}"
    raw_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    spark.sql(f"COMMENT ON TABLE {table} IS 'BRONZE - CP GTFS file {name}.txt as received, all columns as text'")
    gtfs_log.append((table, spark.table(table).count(), len(raw_df.columns) - 2))

display(spark.createDataFrame(gtfs_log, "bronze_table STRING, rows LONG, source_columns INT"))

# COMMAND ----------

# DBTITLE 1,A look at the timetable: one row per stop of each train
display(
    spark.table(f"{BRONZE}.gtfs_stop_times")
    .select("trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time", "_source_file")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The reference timetable
# MAGIC The simulation needs to know **which trains run on each day of the week**. GTFS does not list dates: `calendar`
# MAGIC gives a validity period and seven weekday flags per service, and `calendar_dates` lists the exceptions.
# MAGIC
# MAGIC The CP feed only describes the service from its download date onwards (older timetable versions are removed from
# MAGIC it), so the past six months cannot be read from it directly. We take one **complete week** of the feed
# MAGIC (16 to 22 November 2026) and assume that this weekly timetable also ran during the simulated period.
# MAGIC
# MAGIC > **Assumption.** The same weekly timetable is replayed for all 26 weeks. Public holidays and seasonal timetable
# MAGIC > changes are not modelled.

# COMMAND ----------

# DBTITLE 1,Trains that run on each day of the reference week
calendar = spark.table(f"{BRONZE}.gtfs_calendar").select(
    "service_id",
    F.to_date("start_date", "yyyyMMdd").alias("start_date"),
    F.to_date("end_date", "yyyyMMdd").alias("end_date"),
    *[F.col(day).cast("int").alias(day) for day in
      ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")],
)
removed_dates = (
    spark.table(f"{BRONZE}.gtfs_calendar_dates")
    .filter(F.col("exception_type") == "2")
    .select("service_id", F.to_date("date", "yyyyMMdd").alias("ref_date"))
)
added_dates = (
    spark.table(f"{BRONZE}.gtfs_calendar_dates")
    .filter(F.col("exception_type") == "1")
    .select("service_id", F.to_date("date", "yyyyMMdd").alias("ref_date"))
)

# The seven dates of the reference week
reference_days = spark.sql(
    f"SELECT explode(sequence(DATE'{REFERENCE_WEEK_START}', date_add(DATE'{REFERENCE_WEEK_START}', 6))) AS ref_date"
).withColumn("day_of_week", F.dayofweek("ref_date"))   # 1 = Sunday ... 7 = Saturday

weekday_flag = (
    F.when(F.col("day_of_week") == 1, F.col("sunday"))
    .when(F.col("day_of_week") == 2, F.col("monday"))
    .when(F.col("day_of_week") == 3, F.col("tuesday"))
    .when(F.col("day_of_week") == 4, F.col("wednesday"))
    .when(F.col("day_of_week") == 5, F.col("thursday"))
    .when(F.col("day_of_week") == 6, F.col("friday"))
    .otherwise(F.col("saturday"))
)

regular_services = (
    calendar.crossJoin(F.broadcast(reference_days))
    .filter(F.col("ref_date").between(F.col("start_date"), F.col("end_date")) & (weekday_flag == 1))
    .select("service_id", "ref_date")
)
services_by_weekday = (
    regular_services.join(removed_dates, ["service_id", "ref_date"], "left_anti")
    .union(added_dates.join(F.broadcast(reference_days.select("ref_date")), "ref_date").select("service_id", "ref_date"))
    .distinct()
    .withColumn("day_of_week", F.dayofweek("ref_date"))
    .select("service_id", "day_of_week")
)

# Service tier of each train, from the GTFS route short name
routes = spark.table(f"{BRONZE}.gtfs_routes").select("route_id", "route_short_name")
service_tier = (
    F.when(F.col("route_short_name") == "AP", "Alfa Pendular")
    .when(F.col("route_short_name") == "IC", "Intercidades")
    .when(F.col("route_short_name") == "IR", "InterRegional")
    .when(F.col("route_short_name") == "R", "Regional")
    .otherwise("Urban")
)

trains_by_weekday = (
    spark.table(f"{BRONZE}.gtfs_trips")
    .select("trip_id", "service_id", "route_id", F.col("trip_short_name").alias("train_number"))
    .join(routes, "route_id")
    .withColumn("service_tier", service_tier)
    .join(services_by_weekday, "service_id")
    .select("day_of_week", "trip_id", "train_number", "service_tier")
)

display(
    trains_by_weekday.groupBy("day_of_week")
    .pivot("service_tier", [row[0] for row in TIER_PARAMETERS])
    .count()
    .withColumn("day", F.element_at(F.array(*[F.lit(d) for d in
                ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")]), F.col("day_of_week")))
    .orderBy("day_of_week")
)

# COMMAND ----------

# DBTITLE 1,Stop times with the scheduled times in seconds after midnight
def time_to_seconds(column_name: str):
    """Convert a GTFS 'HH:MM:SS' text into seconds after midnight. Hours can exceed 23 for trains past midnight."""
    parts = F.split(F.col(column_name), ":")
    return (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )


stop_times = spark.table(f"{BRONZE}.gtfs_stop_times").select(
    "trip_id",
    F.col("stop_sequence").cast("int").alias("stop_sequence"),
    "stop_id",
    time_to_seconds("arrival_time").alias("arrival_sec"),
    time_to_seconds("departure_time").alias("departure_sec"),
)

# How busy is each station? Number of train calls in the reference week, scaled from 0 (quietest) to 1 (busiest).
station_congestion = (
    trains_by_weekday.join(stop_times, "trip_id")
    .groupBy("stop_id")
    .agg(F.count("*").alias("weekly_calls"))
    .withColumn("congestion", F.percent_rank().over(Window.orderBy("weekly_calls")))
    .select("stop_id", "congestion")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The delay simulator
# MAGIC
# MAGIC ### How a delay is built
# MAGIC Each train run (one train on one day) carries one number along its route: **its current delay in minutes**.
# MAGIC
# MAGIC | Step | What happens | Why it is realistic |
# MAGIC |---|---|---|
# MAGIC | **1. Origin** | The train leaves its first station with a small random delay. | Late preparation or late arrival of the previous service. Larger in rush hours and on bad days. |
# MAGIC | **2. Each section** | Between two stops the delay changes by a small random amount, on average slightly positive. | Traffic, signals, boarding time. The average gain is larger in **rush hours** and when arriving at **busy stations**. |
# MAGIC | **3. Incidents** | Rarely, a train suffers an incident and gains 6 minutes or more at once. | Breakdowns, door problems, an occupied track. |
# MAGIC | **4. Station disruptions** | Rarely, a station is disrupted for one hour and **every** train arriving in that hour gains delay. | A signal failure or an accident affects all trains at that place and time: this is what creates bottlenecks. |
# MAGIC | **5. Propagation** | The delay of a stop is the **running total** of everything that happened before it. | A train that is late stays late: delay gained at one station is carried to all the following ones. |
# MAGIC
# MAGIC Step 5 is the heart of the project. It is computed with a **window function**: a cumulative sum over the stops of
# MAGIC each train run, ordered by stop sequence. Spark distributes it because every train run is an independent partition
# MAGIC of the window.
# MAGIC
# MAGIC ### Reproducible randomness without UDFs
# MAGIC The random numbers are not drawn with `rand()`. They are computed with a **hash** of the row's own keys
# MAGIC (`xxhash64` of date, train and stop, with a seed). The same row always gets the same number, on any cluster and
# MAGIC whatever the partitioning, so the notebook generates exactly the same 3.6 million rows on every run.

# COMMAND ----------

# DBTITLE 1,Random number helpers built on hashes
def uniform(salt: str, *key_columns):
    """Reproducible uniform number in (0, 1), derived from a hash of the key columns."""
    hashed = F.xxhash64(F.lit(SEED), F.lit(salt), *[F.col(c) for c in key_columns])
    return (F.pmod(hashed, F.lit(1000000)) + F.lit(0.5)) / F.lit(1000000.0)


def exponential(salt: str, mean, *key_columns):
    """Reproducible positive number with the given average: many small values, a few large ones."""
    return -F.log(uniform(salt, *key_columns)) * mean


def standard_normal(salt: str, *key_columns):
    """Reproducible bell-shaped number with average 0 and standard deviation 1 (Box-Muller transform)."""
    return F.sqrt(-2.0 * F.log(uniform(salt + "_a", *key_columns))) * F.cos(
        F.lit(2.0 * math.pi) * uniform(salt + "_b", *key_columns)
    )

# COMMAND ----------

# DBTITLE 1,Step A: every scheduled stop of every train for six months
simulated_days = (
    spark.sql(f"SELECT explode(sequence(DATE'{SIM_START_DATE}', DATE'{SIM_END_DATE}')) AS service_date")
    .withColumn("day_of_week", F.dayofweek("service_date"))
    # A bad day affects the whole network (same value for all trains of the date)
    .withColumn("is_bad_day", uniform("bad_day", "service_date") < BAD_DAY_PROBABILITY)
)

tier_parameters = spark.createDataFrame(
    TIER_PARAMETERS, "service_tier STRING, origin_scale DOUBLE, drift DOUBLE, sigma DOUBLE"
)

scheduled_stops = (
    simulated_days.join(trains_by_weekday, "day_of_week")                 # trains of each simulated day
    .join(stop_times, "trip_id")                                          # their stops
    .join(F.broadcast(tier_parameters), "service_tier")
    .join(F.broadcast(station_congestion), "stop_id")
    .withColumn("scheduled_hour", F.floor(F.col("arrival_sec") / 3600) % 24)
    .withColumn(
        "is_rush_hour",
        F.col("day_of_week").between(2, 6) & F.col("scheduled_hour").isin(RUSH_HOURS),
    )
)

# COMMAND ----------

# DBTITLE 1,Step B: the delay gained at each stop
train_run = Window.partitionBy("service_date", "trip_id")

rush = F.when(F.col("is_rush_hour"), 1.0).otherwise(0.0)
bad_day = F.when(F.col("is_bad_day"), 1.0).otherwise(0.0)

# 1. Origin: delay with which the train leaves its first station
origin_delay = exponential(
    "origin",
    F.col("origin_scale")
    * (1.0 + (RUSH_ORIGIN_FACTOR - 1.0) * rush)
    * (1.0 + (BAD_DAY_ORIGIN_FACTOR - 1.0) * bad_day),
    "service_date", "trip_id",
)

# 2. Section: small random change, with an average that grows with rush hour, congestion and bad days
section_delay = (
    F.col("drift")
    + RUSH_DRIFT_MIN * rush
    + CONGESTION_DRIFT_MIN * F.col("congestion")
    + BAD_DAY_DRIFT_MIN * bad_day
    + F.col("sigma") * standard_normal("section", "service_date", "trip_id", "stop_sequence")
)

# 3. Incident: rare and large, affects one train (twice as likely on a bad day)
incident_delay = F.when(
    uniform("incident", "service_date", "trip_id", "stop_sequence") < INCIDENT_PROBABILITY * (1.0 + bad_day),
    INCIDENT_MIN_DELAY
    + exponential("incident_size", INCIDENT_MEAN_EXTRA, "service_date", "trip_id", "stop_sequence"),
).otherwise(0.0)

# 4. Station disruption: rare, affects every train arriving at the station in that hour (more likely at busy stations)
disruption_delay = F.when(
    uniform("disruption", "service_date", "stop_id", "scheduled_hour")
    < DISRUPTION_PROBABILITY * (0.5 + 1.5 * F.col("congestion")),
    DISRUPTION_MIN_DELAY
    + exponential("disruption_size", DISRUPTION_MEAN_EXTRA, "service_date", "stop_id", "scheduled_hour"),
).otherwise(0.0)

is_first_stop = F.col("stop_sequence") == F.min("stop_sequence").over(train_run)

stops_with_increments = scheduled_stops.withColumn("is_first_stop", is_first_stop).withColumn(
    "delay_gained_min",
    F.when(F.col("is_first_stop"), origin_delay).otherwise(section_delay + incident_delay + disruption_delay),
)

# COMMAND ----------

# DBTITLE 1,Step C: propagation, the running total of delay along each train run
along_the_route = train_run.orderBy("stop_sequence").rowsBetween(Window.unboundedPreceding, Window.currentRow)

simulated_stops = (
    stops_with_increments
    # 5. The delay at a stop is the sum of everything gained up to that stop
    .withColumn("running_delay_min", F.sum("delay_gained_min").over(along_the_route))
    # A train can arrive slightly early, but never leaves before its scheduled time
    .withColumn("arrival_delay_sec",
                F.round(F.greatest(F.col("running_delay_min"), F.lit(-MAX_EARLY_ARRIVAL_MIN)) * 60).cast("int"))
    .withColumn("departure_delay_sec",
                F.round(F.greatest(F.col("running_delay_min"), F.lit(0.0)) * 60).cast("int"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Shape the data as an operational source would deliver it
# MAGIC The Bronze table should look like a real feed from an operations system, so that the Silver notebook has real
# MAGIC cleaning work to do:
# MAGIC * times are **text in Lisbon local time**, not timestamps;
# MAGIC * a small share of readings is **missing** (the actual times are null);
# MAGIC * a small share of rows is **delivered twice**.
# MAGIC
# MAGIC Times are built as wall-clock values (`TIMESTAMP_NTZ`): the scheduled seconds after midnight are added to the
# MAGIC service date. This does not depend on the time zone of the session, and it places trains that run past midnight
# MAGIC on the following day.

# COMMAND ----------

# DBTITLE 1,Build the movement log
TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"


def local_time_text(seconds_after_midnight):
    """Service date + seconds after midnight, as text in local wall-clock time."""
    wall_clock = F.col("service_date").cast("timestamp_ntz") + F.make_dt_interval(secs=seconds_after_midnight)
    return F.date_format(wall_clock, TIME_FORMAT)


reading_is_missing = (
    uniform("missing", "service_date", "trip_id", "stop_sequence") < MISSING_READING_PROBABILITY
)

movements = simulated_stops.select(
    F.concat_ws("|", F.col("service_date").cast("string"), "trip_id", F.col("stop_sequence").cast("string"))
    .alias("event_id"),
    F.col("service_date").cast("string").alias("service_date"),
    "trip_id",
    "train_number",
    "stop_sequence",
    "stop_id",
    local_time_text(F.col("arrival_sec")).alias("scheduled_arrival"),
    local_time_text(F.col("departure_sec")).alias("scheduled_departure"),
    F.when(~reading_is_missing, local_time_text(F.col("arrival_sec") + F.col("arrival_delay_sec")))
    .alias("actual_arrival"),
    F.when(~reading_is_missing, local_time_text(F.col("departure_sec") + F.col("departure_delay_sec")))
    .alias("actual_departure"),
    F.lit(True).alias("is_simulated"),
    F.lit(SIMULATOR_NAME).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
    F.date_format("service_date", "yyyy-MM").alias("service_month"),
)

# Rows delivered twice: an exact copy of a small, reproducible sample
duplicated_rows = movements.filter(
    uniform("duplicate", "service_date", "trip_id", "stop_sequence") < DUPLICATE_PROBABILITY
)
movements_raw = movements.unionByName(duplicated_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write the Bronze Delta table
# MAGIC The table is **partitioned by month**. Each month is stored in its own folder, so a query that filters on
# MAGIC `service_month` reads only the files of that month (*partition pruning*). Six partitions of about 600,000 rows
# MAGIC each is a sensible size: partitioning by day would create hundreds of very small files, which is slower.

# COMMAND ----------

# DBTITLE 1,Write workspace.cp_bronze.movements_raw
(
    movements_raw.write.format("delta")
    .mode("overwrite")
    .partitionBy("service_month")
    .saveAsTable(MOVEMENTS_TABLE)
)

n_movements = spark.table(MOVEMENTS_TABLE).count()
print(f"{MOVEMENTS_TABLE}: {n_movements:,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation
# MAGIC Free Edition has no Spark UI, so the result is checked with queries and with the query plan.

# COMMAND ----------

# DBTITLE 1,Rows, trains and imperfections per month (one partition per month)
# MAGIC %sql
# MAGIC SELECT service_month,
# MAGIC        COUNT(*)                                                  AS rows_in_bronze,
# MAGIC        COUNT(DISTINCT event_id)                                  AS distinct_stop_events,
# MAGIC        COUNT(*) - COUNT(DISTINCT event_id)                       AS duplicated_rows,
# MAGIC        COUNT(DISTINCT service_date)                              AS days,
# MAGIC        COUNT(DISTINCT service_date, trip_id)                     AS train_runs,
# MAGIC        COUNT(DISTINCT stop_id)                                   AS stations,
# MAGIC        SUM(CASE WHEN actual_arrival IS NULL THEN 1 ELSE 0 END)   AS missing_readings
# MAGIC FROM workspace.cp_bronze.movements_raw
# MAGIC GROUP BY service_month
# MAGIC ORDER BY service_month

# COMMAND ----------

# DBTITLE 1,Storage of the Delta table: partition column, number of files and size
display(
    spark.sql(f"DESCRIBE DETAIL {MOVEMENTS_TABLE}").select(
        "format",
        "partitionColumns",
        "numFiles",
        F.round(F.col("sizeInBytes") / 1024 / 1024, 1).alias("size_mb"),
    )
)

# COMMAND ----------

# DBTITLE 1,Partition pruning: a query for one month reads only that partition
# In the plan, look for "PartitionFilters" in the scan of the table: it shows the filter on service_month
# being applied to the folders, before any file is read.
spark.table(MOVEMENTS_TABLE).filter(F.col("service_month") == "2026-07").groupBy("service_date").count().explain(
    "formatted"
)

# COMMAND ----------

# DBTITLE 1,One train run: the delay is carried from stop to stop
sample_run = (
    spark.table(MOVEMENTS_TABLE)
    .filter((F.col("service_month") == "2026-07") & (F.col("train_number") == "121"))
    .filter(F.col("service_date") == "2026-07-15")
    .dropDuplicates(["event_id"])
    .join(
        spark.table(f"{BRONZE}.gtfs_stops").select("stop_id", "stop_name"),
        "stop_id",
    )
    .select(
        "stop_sequence",
        "stop_name",
        "scheduled_arrival",
        "actual_arrival",
        F.round(
            (F.unix_timestamp("actual_arrival", TIME_FORMAT) - F.unix_timestamp("scheduled_arrival", TIME_FORMAT)) / 60,
            1,
        ).alias("arrival_delay_min"),
    )
    .orderBy("stop_sequence")
)

display(sample_run)

# COMMAND ----------

# DBTITLE 1,Sanity check of the simulated delays per service tier
# A first look only: the proper metrics are built in the Silver and Gold notebooks.
arrival_delay_min = (
    F.unix_timestamp("actual_arrival", TIME_FORMAT) - F.unix_timestamp("scheduled_arrival", TIME_FORMAT)
) / 60

trip_tiers = (
    spark.table(f"{BRONZE}.gtfs_trips")
    .select("trip_id", "route_id")
    .join(routes, "route_id")
    .withColumn("service_tier", service_tier)
    .select("trip_id", "service_tier")
)

delay_check_df = (
    spark.table(MOVEMENTS_TABLE)
    .filter(F.col("actual_arrival").isNotNull())
    .dropDuplicates(["event_id"])
    .join(F.broadcast(trip_tiers), "trip_id")
    .withColumn("arrival_delay_min", arrival_delay_min)
    .groupBy("service_tier")
    .agg(
        F.count("*").alias("stop_events"),
        F.round(F.avg("arrival_delay_min"), 2).alias("avg_delay_min"),
        F.round(F.percentile_approx("arrival_delay_min", 0.5), 1).alias("median_delay_min"),
        F.round(F.percentile_approx("arrival_delay_min", 0.9), 1).alias("p90_delay_min"),
        F.round(F.max("arrival_delay_min"), 1).alias("max_delay_min"),
        F.round(100 * F.avg(F.when(F.col("arrival_delay_min") <= 3, 1.0).otherwise(0.0)), 1).alias("otp_3min_pct"),
        F.round(100 * F.avg(F.when(F.col("arrival_delay_min") <= 5, 1.0).otherwise(0.0)), 1).alias("otp_5min_pct"),
    )
    .orderBy("otp_5min_pct")
)

display(delay_check_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC * The six GTFS files of CP are in Bronze as received (`workspace.cp_bronze.gtfs_*`).
# MAGIC * `workspace.cp_bronze.movements_raw` holds six months of stop-level movements, partitioned by month, with
# MAGIC   simulated actual times, text timestamps, missing readings and duplicated rows.
# MAGIC * Delay propagation was generated with a cumulative window function over each train run.
# MAGIC
# MAGIC **Next:** `02_silver_transform` cleans the log (duplicates, missing readings, timestamps in `Europe/Lisbon`),
# MAGIC computes `arrival_delay_min`, `departure_delay_min` and, with `lag()` over each train run, the delay gained
# MAGIC since the previous stop.
