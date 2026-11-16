-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Unity Catalog setup: CP rail punctuality lakehouse
-- MAGIC
-- MAGIC Run this notebook **once** (it is safe to run again: every statement uses `IF NOT EXISTS`).
-- MAGIC
-- MAGIC | Object | Name | Purpose |
-- MAGIC |---|---|---|
-- MAGIC | Volume | `workspace.default.cp_raw` | Raw GTFS files of CP, uploaded with the Databricks interface |
-- MAGIC | Schema | `workspace.cp_bronze` | Raw data as received: GTFS tables and the stop-level movement log |
-- MAGIC | Schema | `workspace.cp_silver` | Cleaned, typed and enriched stop events (one row per train stop) |
-- MAGIC | Schema | `workspace.cp_gold` | Small aggregate tables that answer the business questions |
-- MAGIC
-- MAGIC After running it, upload these six files of the CP GTFS feed to the Volume
-- MAGIC (Catalog > `workspace` > `default` > Volumes > `cp_raw` > *Upload to this volume*):
-- MAGIC `routes.txt`, `trips.txt`, `stops.txt`, `stop_times.txt`, `calendar.txt`, `calendar_dates.txt`.
-- MAGIC
-- MAGIC > **Important.** CP publishes its timetable but no open record of real delays. The movement log
-- MAGIC > `cp_bronze.movements_raw` is therefore **simulated** by notebook 01 on top of the real timetable.
-- MAGIC > Every table that derives from it carries this note in its comment.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Schemas and Volume

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.cp_bronze
COMMENT 'CP rail punctuality - BRONZE: raw GTFS tables and the simulated stop-level movement log, as received';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.cp_silver
COMMENT 'CP rail punctuality - SILVER: cleaned, typed and enriched stop events and reference tables';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.cp_gold
COMMENT 'CP rail punctuality - GOLD: presentation-ready aggregates (reliability, delay propagation, bottlenecks)';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.default.cp_raw
COMMENT 'Raw GTFS files of CP - Comboios de Portugal (routes, trips, stops, stop_times, calendar, calendar_dates)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Bronze tables
-- MAGIC
-- MAGIC **GTFS tables.** Notebook 01 creates one table per file, `cp_bronze.gtfs_routes`, `gtfs_trips`, `gtfs_stops`,
-- MAGIC `gtfs_stop_times`, `gtfs_calendar` and `gtfs_calendar_dates`. They keep **every column of the file as `STRING`**
-- MAGIC (nothing is interpreted in Bronze) plus two metadata columns: `_source_file STRING` and `_ingested_at TIMESTAMP`.
-- MAGIC
-- MAGIC **Movement log.** One row per train stop and day, as an operational system would deliver it: timestamps are
-- MAGIC text in Lisbon local time, some readings are missing and some rows are duplicated. It is partitioned by month,
-- MAGIC so a query for one month reads only that month's files.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.cp_bronze.movements_raw (
  event_id             STRING    COMMENT 'Identifier of the stop event: service_date|trip_id|stop_sequence. Not unique in Bronze (duplicates exist)',
  service_date         STRING    COMMENT 'Day of operation of the train, text yyyy-MM-dd',
  trip_id              STRING    COMMENT 'GTFS trip identifier',
  train_number         STRING    COMMENT 'Commercial train number (GTFS trip_short_name)',
  stop_sequence        INT       COMMENT 'Order of the stop within the trip, as in GTFS stop_times',
  stop_id              STRING    COMMENT 'GTFS stop identifier of the station',
  scheduled_arrival    STRING    COMMENT 'Timetable arrival, text yyyy-MM-dd HH:mm:ss, Europe/Lisbon local time',
  scheduled_departure  STRING    COMMENT 'Timetable departure, text yyyy-MM-dd HH:mm:ss, Europe/Lisbon local time',
  actual_arrival       STRING    COMMENT 'Recorded arrival, same format. NULL when the reading is missing',
  actual_departure     STRING    COMMENT 'Recorded departure, same format. NULL when the reading is missing',
  is_simulated         BOOLEAN   COMMENT 'TRUE: the actual times were produced by the simulator of notebook 01',
  _source_file         STRING    COMMENT 'Origin of the row (name and version of the simulator)',
  _ingested_at         TIMESTAMP COMMENT 'When the row was written to Bronze',
  service_month        STRING    COMMENT 'Partition column: month of service_date, text yyyy-MM'
)
USING DELTA
PARTITIONED BY (service_month)
COMMENT 'BRONZE - stop-level movement log of CP trains. SIMULATED actual times on top of the real CP timetable';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Silver tables
-- MAGIC Written by notebook `02_silver_transform`. The fact table has **one row per valid stop event**, with real
-- MAGIC timestamps, the delay in minutes and the change of delay since the previous stop of the same train.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.cp_silver.stations (
  station_id     STRING  COMMENT 'GTFS stop identifier',
  station_name   STRING  COMMENT 'Station name',
  latitude       DOUBLE  COMMENT 'Latitude in decimal degrees',
  longitude      DOUBLE  COMMENT 'Longitude in decimal degrees'
)
USING DELTA
COMMENT 'SILVER reference - CP stations';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.cp_silver.trips (
  trip_id              STRING  COMMENT 'GTFS trip identifier',
  train_number         STRING  COMMENT 'Commercial train number',
  route_short_name     STRING  COMMENT 'GTFS route short name (AP, IC, IR, R, U or the name of an urban line)',
  service_tier         STRING  COMMENT 'Alfa Pendular, Intercidades, InterRegional, Regional, Urbanos de Lisboa, Urbanos do Porto or Urbanos de Coimbra',
  service_group        STRING  COMMENT 'Long distance, Regional or Urban',
  line_name            STRING  COMMENT 'Urban services: name of the line. Other services: origin - destination of the train',
  origin_station       STRING  COMMENT 'Name of the first station of the trip',
  destination_station  STRING  COMMENT 'Name of the last station of the trip',
  stops                INT     COMMENT 'Number of stops of the trip'
)
USING DELTA
COMMENT 'SILVER reference - one row per train of the timetable, with its service tier and line';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.cp_silver.stop_events (
  event_id                    STRING    COMMENT 'Unique identifier of the stop event: service_date|trip_id|stop_sequence',
  service_date                DATE      COMMENT 'Day of operation of the train',
  trip_id                     STRING    COMMENT 'GTFS trip identifier',
  train_number                STRING    COMMENT 'Commercial train number',
  service_tier                STRING    COMMENT 'Commercial service of the train',
  service_group               STRING    COMMENT 'Long distance, Regional or Urban',
  line_name                   STRING    COMMENT 'Line (urban services) or origin - destination (other services)',
  stop_sequence               INT       COMMENT 'Order of the stop within the trip',
  station_id                  STRING    COMMENT 'Station of the stop',
  station_name                STRING    COMMENT 'Name of the station',
  prev_station_id             STRING    COMMENT 'Station of the previous stop of the same train (NULL at the first stop)',
  prev_station_name           STRING    COMMENT 'Name of the previous station',
  is_first_stop               BOOLEAN   COMMENT 'TRUE at the origin of the trip',
  is_last_stop                BOOLEAN   COMMENT 'TRUE at the terminus of the trip',
  scheduled_arrival_ts        TIMESTAMP COMMENT 'Timetable arrival as an instant (local Europe/Lisbon time converted to UTC)',
  scheduled_departure_ts      TIMESTAMP COMMENT 'Timetable departure as an instant',
  actual_arrival_ts           TIMESTAMP COMMENT 'Recorded arrival as an instant',
  actual_departure_ts         TIMESTAMP COMMENT 'Recorded departure as an instant',
  scheduled_hour              INT       COMMENT 'Hour of the day (0-23, Lisbon local time) of the timetable arrival',
  day_of_week                 INT       COMMENT '1 = Sunday ... 7 = Saturday',
  is_weekend                  BOOLEAN   COMMENT 'TRUE on Saturdays and Sundays',
  is_rush_hour                BOOLEAN   COMMENT 'TRUE on weekdays from 07:00 to 09:59 and from 17:00 to 19:59',
  arrival_delay_min           DOUBLE    COMMENT 'Actual minus scheduled arrival, in minutes (negative = early)',
  departure_delay_min         DOUBLE    COMMENT 'Actual minus scheduled departure, in minutes',
  delay_delta_from_prev_stop  DOUBLE    COMMENT 'arrival_delay_min minus the arrival_delay_min of the previous stop of the same train: delay gained (+) or recovered (-) on this section',
  is_simulated                BOOLEAN   COMMENT 'TRUE: the actual times come from the simulator',
  service_month               STRING    COMMENT 'Partition column: month of service_date, text yyyy-MM'
)
USING DELTA
PARTITIONED BY (service_month)
COMMENT 'SILVER - one row per valid train stop with delays in minutes. Actual times are SIMULATED';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.cp_silver.stop_events_quarantine (
  event_id             STRING  COMMENT 'Identifier of the rejected stop event',
  service_date         STRING  COMMENT 'As received in Bronze',
  trip_id              STRING  COMMENT 'As received in Bronze',
  stop_sequence        INT     COMMENT 'As received in Bronze',
  stop_id              STRING  COMMENT 'As received in Bronze',
  scheduled_arrival    STRING  COMMENT 'As received in Bronze',
  scheduled_departure  STRING  COMMENT 'As received in Bronze',
  actual_arrival       STRING  COMMENT 'As received in Bronze',
  actual_departure     STRING  COMMENT 'As received in Bronze',
  reject_reason        STRING  COMMENT 'Data rule that rejected the row (missing_reading, invalid_timestamp, unknown_station, unknown_trip)',
  service_month        STRING  COMMENT 'Month of service_date, text yyyy-MM'
)
USING DELTA
COMMENT 'SILVER - stop events rejected by the data rules, kept with the reason for rejection';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Gold tables
-- MAGIC Written by notebook `03_gold_analytics`. Each one answers one question.
-- MAGIC
-- MAGIC | Table | Question it answers |
-- MAGIC |---|---|
-- MAGIC | `gold_service_reliability` | How punctual is each service and line, month by month? |
-- MAGIC | `gold_delay_propagation` | On which sections do trains gain or recover delay, and how does delay build up along a line? |
-- MAGIC | `gold_station_bottlenecks` | At which stations and hours do delays concentrate? |
-- MAGIC
-- MAGIC *On-Time Performance (OTP)* is the share of arrivals with a delay of at most 3 or 5 minutes.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.cp_gold.gold_service_reliability (
  service_month          STRING  COMMENT 'Month, text yyyy-MM',
  service_group          STRING  COMMENT 'Long distance, Regional or Urban',
  service_tier           STRING  COMMENT 'Commercial service',
  line_name              STRING  COMMENT 'Line (urban services) or origin - destination (other services)',
  trains                 BIGINT  COMMENT 'Number of train runs (one per train and day)',
  arrivals               BIGINT  COMMENT 'Number of arrivals measured (stop events, excluding the first stop)',
  otp_3min_pct           DOUBLE  COMMENT 'Percentage of arrivals at most 3 minutes late',
  otp_5min_pct           DOUBLE  COMMENT 'Percentage of arrivals at most 5 minutes late',
  avg_arrival_delay_min  DOUBLE  COMMENT 'Average arrival delay in minutes',
  p90_arrival_delay_min  DOUBLE  COMMENT '90th percentile of the arrival delay: 9 out of 10 arrivals are at most this late',
  avg_terminus_delay_min DOUBLE  COMMENT 'Average arrival delay at the last stop of the trip'
)
USING DELTA
COMMENT 'GOLD - On-Time Performance and average delay per month, service and line. Based on SIMULATED delays';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.cp_gold.gold_delay_propagation (
  service_group             STRING  COMMENT 'Long distance, Regional or Urban',
  service_tier              STRING  COMMENT 'Commercial service',
  line_name                 STRING  COMMENT 'Line (urban services) or origin - destination (other services)',
  from_station_id           STRING  COMMENT 'Station where the section starts',
  from_station_name         STRING  COMMENT 'Name of the station where the section starts',
  to_station_id             STRING  COMMENT 'Station where the section ends',
  to_station_name           STRING  COMMENT 'Name of the station where the section ends',
  avg_stop_sequence         DOUBLE  COMMENT 'Average position of the section within the trips, to order the sections along the line',
  train_runs                BIGINT  COMMENT 'Number of train runs over the section',
  avg_delay_at_from_min     DOUBLE  COMMENT 'Average arrival delay at the start of the section',
  avg_delay_at_to_min       DOUBLE  COMMENT 'Average arrival delay at the end of the section',
  avg_delay_delta_min       DOUBLE  COMMENT 'Average delay gained (+) or recovered (-) on the section',
  pct_runs_delay_increased  DOUBLE  COMMENT 'Percentage of runs that left the section more delayed than they entered it',
  pct_late_runs_still_late  DOUBLE  COMMENT 'Of the runs that entered more than 5 minutes late, percentage still more than 5 minutes late at the end: how much delay is carried forward'
)
USING DELTA
COMMENT 'GOLD - station-to-station delay propagation per service and line. Based on SIMULATED delays';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.cp_gold.gold_station_bottlenecks (
  station_id             STRING   COMMENT 'Station',
  station_name           STRING   COMMENT 'Name of the station',
  day_type               STRING   COMMENT 'Weekday or Weekend',
  scheduled_hour         INT      COMMENT 'Hour of the day (0-23, Lisbon local time)',
  is_rush_hour           BOOLEAN  COMMENT 'TRUE for weekday hours 7-9 and 17-19',
  arrivals               BIGINT   COMMENT 'Number of arrivals measured',
  avg_arrival_delay_min  DOUBLE   COMMENT 'Average arrival delay in minutes',
  otp_5min_pct           DOUBLE   COMMENT 'Percentage of arrivals at most 5 minutes late',
  avg_delay_added_min    DOUBLE   COMMENT 'Average delay gained on the section that ends at this station',
  delay_vs_station_avg   DOUBLE   COMMENT 'Average delay of this hour divided by the average delay of the station over the whole day: above 1 is a spike',
  bottleneck_rank        INT      COMMENT 'Rank of the station within the hour and day type by delay added (1 = worst)'
)
USING DELTA
COMMENT 'GOLD - hourly delay profile per station, for rush-hour bottleneck analysis. Based on SIMULATED delays';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Check

-- COMMAND ----------

SHOW TABLES IN workspace.cp_bronze;

-- COMMAND ----------

SHOW TABLES IN workspace.cp_silver;

-- COMMAND ----------

SHOW TABLES IN workspace.cp_gold;
