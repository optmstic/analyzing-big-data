---
name: run-analyzing-big-data
description: Run, test and drive the Portugal air connectivity Databricks notebook locally. Use to run the notebook, start the pipeline, build the PySpark/Delta environment, check that the notebook still works after an edit, or look at its charts (screenshots).
---

# Run the project notebook locally

The project is one Databricks notebook (`notebooks/ABD_Project_Portugal_Air_Connectivity.py`, Databricks source
format) that reads `air_project_data.zip` (Eurostat air route and airport statistics, 42 files). It cannot be run on
Databricks from this container, so `local_test/run_local.py` executes **every cell** on a local Spark + Delta Lake
session. `driver.sh` in this directory sets that up and runs it. Paths below are relative to the repository root.

`archive/` holds earlier, larger versions of the project. Their test harnesses were moved with them and their paths
were not updated, so they do not run from there.

## Prerequisites

Java 17+ (this container has OpenJDK 21) and `python3` with `venv`. First run also downloads the Delta Lake jar from
Maven Central (`spark.jars.packages`), so the network must allow `repo1.maven.org` (it did in this container).

## Run (agent path)

```bash
.claude/skills/run-analyzing-big-data/driver.sh setup    # once, ~30 s: virtualenv in ~/.venvs/abd (ABD_VENV to change)
.claude/skills/run-analyzing-big-data/driver.sh classic  # ~2 min: classic Spark
.claude/skills/run-analyzing-big-data/driver.sh connect  # ~2 min: Spark Connect, the protocol of Databricks serverless
.claude/skills/run-analyzing-big-data/driver.sh all      # both
```

Each run prints the control lines and writes the full log and the charts:

```
All quality checks passed: YES
Streaming result identical to batch: YES
LOCAL TEST PASSED: 31 cells executed (6 of them %sql), 3 figures rendered.
```

* Log: `/tmp/abd_local_<mode>/run.log` (grep it for `^CELL ` to find a cell).
* Charts: `/tmp/abd_local_<mode>/out/figure_1.png` to `figure_3.png`. Open them to check the notebook's plots.
* On failure the driver prints the last 25 lines of the log. The Java stack trace is long; the first
  `Error` / `Exception` line and the `CELL n:` line above it are what matter.

If the notebook changes, run `connect` (the mode closest to Databricks) before pushing.

## What the local run replaces

`run_local.py` runs the notebook cells unchanged except for three text substitutions in its `localise` function: the
Volume path becomes a local folder, `CREATE VOLUME` becomes a no-op, and the catalog `workspace.` becomes
`spark_catalog.`. It also gives `dbutils.fs.rm`, `display()` and `%sql` cells small stand-ins. It does **not** prove
anything about Databricks itself (Unity Catalog Volumes, unzipping into a Volume, the `availableNow` trigger on
serverless).

## Gotchas

* **The substitutions are exact text.** The harness asserts that `"/Volumes/workspace/abd/abd"` and
  `CREATE VOLUME IF NOT EXISTS` each occur once in the notebook. Editing those lines needs a matching edit in
  `localise`.
* **`workspace.` is replaced everywhere**, including inside `%sql` cells. Do not use the word followed by a dot for
  anything other than the catalog.
* **The data zip must match `data_air/`.** After changing a file in `data_air/`, rebuild it:
  `(cd data_air && zip -q -j ../air_project_data.zip *)`.
* **Each Eurostat file has its own header.** The United Kingdom file stops in 2019 and the Bosnian one covers 2021 to
  2023, so a value position means a different year per file. The parser joins values to periods by file name and
  position; joining by position alone silently shifts years.
* **Division by zero under ANSI mode.** Databricks serverless and Spark 4 run in ANSI mode, where `/` by zero fails
  with `DIVIDE_BY_ZERO`. Some routes of earlier years report zero seats; the notebook only divides by seats for
  Portuguese routes of 2024, where there are none. Use `try_divide` if that is widened.
* **`DESCRIBE HISTORY` is not used as a subquery.** It is read with `spark.sql(...)` and then selected from, because
  using it inside `FROM (...)` is not portable.

## Troubleshooting

* `AttributeError: install_layout` while installing pyspark: the system `setuptools` is too old to build it. The
  driver upgrades `pip setuptools wheel` inside the virtualenv first; do not `pip install pyspark` outside it.
* `run 'driver.sh setup' first`: the virtualenv in `$ABD_VENV` does not exist.
* `CONNECT tunnel failed, response 403` to `ec.europa.eu`: the environment network allowlist does not include it. The
  run itself does not need it (the data is in the zip); only refreshing the data does.

## Refreshing the data (not part of a normal run)

The Eurostat files come from the dissemination API on host `ec.europa.eu`, with a key filter and `startPeriod=2016`:
`.../sdmx/2.1/data/avia_par_<cc>/A..<measures>.?format=TSV&startPeriod=2016` and
`.../sdmx/2.1/data/avia_paoa/M..PAS_CRD..TOTAL.<coverage>?format=TSV&startPeriod=2016`. A request that is too large
returns a small XML document (`SUBMITTED` or `EXTRACTION_TOO_BIG`) instead of data: split it by measure, as was done
for Germany. The numbers in `README.md`, `report/one_page_report.html` and `report/presentation_script.md` are tied
to these files.
