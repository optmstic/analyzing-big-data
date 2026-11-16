---
name: run-analyzing-big-data
description: Run, test and drive the Comboios de Portugal (CP) Databricks notebook project locally. Use to run the notebook, start the pipeline, build the PySpark/Delta environment, check that the notebook still works after an edit, or look at its charts (screenshots).
---

# Run the project notebooks locally

The submitted project is one Databricks notebook (`notebooks/ABD_Group_Project_Portugal_Air_Connectivity.py`,
Databricks source format) that reads 49 files from `data_air/` (Eurostat air route and airport statistics for
2016 to 2026, and an airport registry). It cannot be run on Databricks from this container, so `local_test/run_air.py` executes **every cell** on a
local Spark + Delta Lake session. `driver.sh` in this directory sets that up and runs it. Paths below are relative to
the repository root.

Two earlier projects are kept in the repository and run the same way with `ABD_PROJECT=cp` (CP timetable,
`local_test/run_local.py`) or `ABD_PROJECT=cp_delays` (`local_test/run_cp_delays.py`).

## Prerequisites

Java 17+ (this container has OpenJDK 21) and `python3` with `venv`. First run also downloads the Delta Lake jar from
Maven Central (`spark.jars.packages`), so the network must allow `repo1.maven.org` (it did in this container).

## Run (agent path)

```bash
.claude/skills/run-analyzing-big-data/driver.sh setup    # once, ~30 s: virtualenv in ~/.venvs/abd (ABD_VENV to change)
.claude/skills/run-analyzing-big-data/driver.sh classic  # ~3 min: classic Spark
.claude/skills/run-analyzing-big-data/driver.sh connect  # ~3 min: Spark Connect, the protocol of Databricks serverless
.claude/skills/run-analyzing-big-data/driver.sh all      # both
```

Each run prints the control lines and writes the full log and the charts:

```
16 of 16 checks passed
Sum of all ranks = 1.000000 (expected 1.0)
Streaming results identical to batch results : YES
LOCAL TEST PASSED: 95 cells executed (16 of them %sql), 8 figures rendered.
```

* Log: `/tmp/abd_<project>_<mode>/run.log` (grep it for `^CELL ` to find a cell, or for the output of a section).
* Charts: `/tmp/abd_<project>_<mode>/out/figure_1.png` to `figure_8.png`. Open them to check the notebook's plots.
* On failure the driver prints the last 25 lines of the log. The Java stack trace is long; the first
  `Error` / `Exception` line and the `CELL n:` line above it are what matter.

If the notebook changes, run `connect` (the mode closest to Databricks) before pushing.

## What the local run replaces

`run_local.py` runs the notebook cells unchanged, except for four string substitutions at the top of its
`SUBSTITUTIONS` list (catalog `workspace` becomes `spark_catalog`, the class Volume becomes a local folder, the
`data/` folder replaces `cp_rail`, and `CREATE VOLUME` becomes a no-op), plus small stand-ins for `dbutils.fs`,
`display()`, `DataFrame.display()` and `%sql` cells. It does **not** prove anything about Databricks itself (Unity
Catalog Volumes, serverless limits, the `availableNow` trigger on serverless).

## Gotchas

* **Substitutions are exact.** Each one asserts that its target text occurs exactly once in the notebook. If you edit
  `CATALOG = "workspace"`, `VOLUME_PATH = ...`, `SOURCE_PATH = ...` or the `CREATE VOLUME` line, update `SUBSTITUTIONS`
  or the run stops with `substitution target not found exactly once`.
* **Expected numbers are asserted** at the end of each harness (for the air project: 12,473 routes, 602
  airports, 331 / 22,312 / 1,350,767 itineraries from Portugal, 8 figures). A change to the data files or to the number of charts fails the run on
  purpose; update the assertions after checking the new numbers.
* **`%sql` cells are written as `# MAGIC %sql` with the table names `workspace.<schema>.<table>`**; the harness rewrites
  `workspace.` to `spark_catalog.`. A `%sql` cell that uses another catalog name is not rewritten.
* **Streaming batch/stream comparison needs saved tables.** Comparing a streaming result with a batch DataFrame built
  in the same cell failed on classic Spark with `[INTERNAL_ERROR] Couldn't find station_id ...`; the notebook saves the batch result
  as `SV_Departures_Batch` / `GD_Station_Departures_Batch` first.
* **Each Eurostat file has its own header.** The United Kingdom file stops in 2019 and the Bosnian one covers
  2021 to 2023, so a value position means a different year per file. The parser joins values to periods by file
  name and position; joining by position alone silently shifts years.
* **Division by zero under ANSI mode.** Some route reports have zero seats. Plain `/` fails with `DIVIDE_BY_ZERO`
  (Databricks serverless and Spark 4 run in ANSI mode); the notebook uses `try_divide` everywhere.
* **Closed airports are missing from the registry.** Berlin-Tegel, Doncaster Sheffield and Scatsta reported traffic
  but have no code in `airports.csv`; they are added from `CLOSED_AIRPORTS` in the configuration cell.
* **The air run needs memory.** `run_air.py` sets `spark.driver.memory` to 3g for the 1.5 million itinerary join.

## Troubleshooting

* `AttributeError: install_layout` while installing pyspark: the system `setuptools` is too old to build it. The
  driver upgrades `pip setuptools wheel` inside the virtualenv first; do not `pip install pyspark` outside it.
* `run 'driver.sh setup' first`: the virtualenv in `$ABD_VENV` does not exist.
* `CONNECT tunnel failed, response 403` to a host (for example `ec.europa.eu`, `publico.cp.pt`): the environment
  network allowlist does not include it. The run itself needs none of them (the data is in `data/`); only refreshing
  the data does.

## Refreshing the data (not part of a normal run)

The Eurostat files come from the dissemination API on host `ec.europa.eu`, with a key filter and `startPeriod=2016`:
`.../sdmx/2.1/data/avia_par_<cc>/A..<measures>.?format=TSV&startPeriod=2016` and
`.../sdmx/2.1/data/avia_paoa/M..<measure>..TOTAL.<coverage>?format=TSV&startPeriod=2016`. A request that is too large
returns a small XML document (`SUBMITTED` or `EXTRACTION_TOO_BIG`) instead of data: split it by measure, as was done
for Germany. The expected numbers in `run_air.py`, `README.md` and `report/one_page_report.html` are tied to these
files, so a refresh means updating all three.
