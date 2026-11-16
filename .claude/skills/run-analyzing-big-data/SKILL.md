---
name: run-analyzing-big-data
description: Run, test and drive the Comboios de Portugal (CP) Databricks notebook project locally. Use to run the notebook, start the pipeline, build the PySpark/Delta environment, check that the notebook still works after an edit, or look at its charts (screenshots).
---

# Run the CP rail notebook locally

The project is one Databricks notebook (`notebooks/ABD_Group_Project_CP_Rail_Present_and_Future.py`, Databricks
source format) that reads nine files from `data/` (CP GTFS timetable and Eurostat passenger statistics). It cannot be
run on Databricks from this container, so `local_test/run_local.py` executes **every cell** on a local Spark + Delta Lake
session. `driver.sh` in this directory sets that up and runs it. Paths below are relative to the repository root.

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
19 of 19 checks passed
Streaming results identical to batch results : YES
Sum of all ranks = 1.000000 (expected 1.0)
LOCAL TEST PASSED: 76 cells executed (6 of them %sql), 6 figures rendered.
```

* Log: `/tmp/abd_local_<mode>/run.log` (grep it for `^CELL ` to find a cell, or for the output of a section).
* Charts: `/tmp/abd_local_<mode>/out/figure_1.png` to `figure_6.png`. Open them to check the notebook's plots.
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
* **Expected numbers are asserted** at the end of `run_local.py` (548,614 train runs, 8,954,555 stop events, 19
  data quality checks passed, 6 figures). A change to the data files or to the number of charts fails the run on
  purpose; update the assertions after checking the new numbers.
* **`%sql` cells are written as `# MAGIC %sql` with the table names `workspace.DL_CP.<table>`**; the harness rewrites
  `workspace.` to `spark_catalog.`. A `%sql` cell that uses another catalog name is not rewritten.
* **Streaming batch/stream comparison needs saved tables.** Comparing a streaming result with a batch DataFrame built
  in the same cell failed on classic Spark with `[INTERNAL_ERROR] Couldn't find station_id ...`; the notebook saves the batch result
  as `SV_Departures_Batch` / `GD_Station_Departures_Batch` first.
* **The feed is a rolling snapshot.** Numbers depend on the files in `data/` and on the fixed reference weeks
  (2026-11-16 and 2027-01-18) in the notebook configuration cell, not on today's date.

## Troubleshooting

* `AttributeError: install_layout` while installing pyspark: the system `setuptools` is too old to build it. The
  driver upgrades `pip setuptools wheel` inside the virtualenv first; do not `pip install pyspark` outside it.
* `run 'driver.sh setup' first`: the virtualenv in `$ABD_VENV` does not exist.
* `CONNECT tunnel failed, response 403` to a host (for example `ec.europa.eu`, `publico.cp.pt`): the environment
  network allowlist does not include it. The run itself needs none of them (the data is in `data/`); only refreshing
  the data does.

## Refreshing the data (not part of a normal run)

`data/*.txt` is the unzipped `https://publico.cp.pt/gtfs/gtfs.zip`; `data/rail_pa_total.tsv` and
`data/rail_pa_quartal.tsv` come from the Eurostat dissemination API (`.../sdmx/2.1/data/<dataset>?format=TSV`).
The notebook's reference weeks, the expected numbers in `run_local.py` and the values in `README.md` are tied to the
16 November 2026 snapshot, so a refresh means updating all three.
