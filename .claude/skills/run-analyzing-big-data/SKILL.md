---
name: run-analyzing-big-data
description: Run, test and drive the Comboios de Portugal (CP) Databricks notebook project locally. Use to run the notebook, start the pipeline, build the PySpark/Delta environment, check that the notebook still works after an edit, or look at its charts (screenshots).
---

# Run the project notebooks locally

The submitted project is one Databricks notebook (`notebooks/ABD_Group_Project_Portugal_Air_Connectivity.py`,
Databricks source format) that reads four files from `data_air/` (world airline routes and Eurostat air passenger
statistics). It cannot be run on Databricks from this container, so `local_test/run_air.py` executes **every cell** on a
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
18 of 18 checks passed
Sum of all ranks = 1.000000 (expected 1.0)
Streaming results identical to batch results : YES
LOCAL TEST PASSED: 81 cells executed (16 of them %sql), 6 figures rendered.
```

* Log: `/tmp/abd_<project>_<mode>/run.log` (grep it for `^CELL ` to find a cell, or for the output of a section).
* Charts: `/tmp/abd_<project>_<mode>/out/figure_1.png` to `figure_6.png`. Open them to check the notebook's plots.
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
* **Expected numbers are asserted** at the end of each harness (for the air project: 36,759 connections, 3,391
  airports, 296 / 21,972 / 1,479,733 itineraries from Portugal, 6 figures). A change to the data files or to the number of charts fails the run on
  purpose; update the assertions after checking the new numbers.
* **`%sql` cells are written as `# MAGIC %sql` with the table names `workspace.<schema>.<table>`**; the harness rewrites
  `workspace.` to `spark_catalog.`. A `%sql` cell that uses another catalog name is not rewritten.
* **Streaming batch/stream comparison needs saved tables.** Comparing a streaming result with a batch DataFrame built
  in the same cell failed on classic Spark with `[INTERNAL_ERROR] Couldn't find station_id ...`; the notebook saves the batch result
  as `SV_Departures_Batch` / `GD_Station_Departures_Batch` first.
* **Summing a Eurostat series after renaming a code.** Mapping the old Madeira airport code `LPFU` to `LPMA` created
  duplicated keys for 2006; the two half-year values must be added, not deduplicated (the data quality check on
  duplicated observations catches this).
* **The air run needs memory.** `run_air.py` sets `spark.driver.memory` to 3g for the 1.5 million itinerary join.

## Troubleshooting

* `AttributeError: install_layout` while installing pyspark: the system `setuptools` is too old to build it. The
  driver upgrades `pip setuptools wheel` inside the virtualenv first; do not `pip install pyspark` outside it.
* `run 'driver.sh setup' first`: the virtualenv in `$ABD_VENV` does not exist.
* `CONNECT tunnel failed, response 403` to a host (for example `ec.europa.eu`, `publico.cp.pt`): the environment
  network allowlist does not include it. The run itself needs none of them (the data is in `data/`); only refreshing
  the data does.

## Refreshing the data (not part of a normal run)

`data_air/avia_par_pt.tsv.gz` comes from the Eurostat dissemination API
(`.../sdmx/2.1/data/avia_par_pt?format=TSV&compressed=true`, host `ec.europa.eu`). The three CSV files come from the
repository `optmstic/airport-network-route-analysis`. The expected numbers in `run_air.py`, `README.md` and
`report/one_page_report.html` are tied to these files, so a refresh means updating all three.
