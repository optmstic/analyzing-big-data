# Analyzing Big Data 2026-2027: Group Project

**Portugal's air connectivity, 2016-2026: the European route network and the passengers who use it.**
A lakehouse with Spark SQL, graph analytics and Structured Streaming on Apache Spark (Databricks Free Edition).

| Path | What it is |
|---|---|
| `notebooks/ABD_Group_Project_Portugal_Air_Connectivity.py` | **The project notebook**, in Databricks source format (import it into Databricks) |
| `data_air/` | The 49 source files the notebook reads |
| `report/one_page_report.pdf` | The optional one-page document (source: `report/one_page_report.html`) |
| `local_test/run_air.py` | Local test that executes every cell on Spark + Delta Lake with the same data files |

## Data

Every traffic figure is an official Eurostat statistic, downloaded on 16 November 2026 with the Eurostat data API,
filtered to the period from 2016 onwards, and stored unchanged.

| Files | Eurostat dataset | Content | Period |
|---|---|---|---|
| `routes_annual_<country>.tsv` (38 files, 35 countries; Germany in 4 files) | `avia_par_<country>` | Passengers, seats and flights of every main route, by direction | Annual, 2016 to 2024 |
| `routes_pt_monthly_<measure>.tsv` (4 files) | `avia_par_pt` | The same measures for the routes of the Portuguese airports | Monthly, 2016-01 to 2024-12 |
| `airports_monthly_<measure>_<coverage>.tsv` (6 files) | `avia_paoa` | Passengers and flights of each main European airport: total, national, international | Monthly, 2016-01 to 2026 (2026-04 for Portugal) |
| `airports.csv` | | Public airport registry (Kaggle), used only for names and coordinates | |

The route statistics end in 2024 because Eurostat had not yet published 2025. Czechia publishes no route dataset; the
United Kingdom reported until 2019.

## How to produce the deliverable (HTML notebook)

The syllabus requires a *working Databricks Notebook in html format*, which has to be exported from Databricks.

1. Sign in to Databricks Free Edition and **import** the notebook (Workspace > Import > File).
2. In the first cell, replace the placeholders with the group number, student names and numbers.
3. Attach **Serverless** compute and choose **Run all** once. The notebook creates the folder `air_routes` in the Volume
   of the classes (`/Volumes/workspace/abd/abd`), then stops in section 4 with a message saying that the source files
   are missing. This is expected.
4. Open **Catalog** > `workspace` > `abd` > Volumes > `abd` > `air_routes` > **Upload to this volume**, and upload the
   49 files of `data_air/` into that folder (select them all at once; no sub-folders).
5. Choose **Run all** again. Check these three control lines in the output:
   - `16 of 16 checks passed` (section 6.6)
   - `Sum of all ranks = 1.000000` (section 8.5)
   - `Streaming results identical to batch results : YES` (section 11.4)
6. **File > Export > HTML** and submit that file in Moodle by **17 November, 24:00**.
7. Put the group number and names in `report/one_page_report.html` (the line with *Group XX*), print it to PDF from a
   browser, and submit it as the optional one-page document.

The notebook is idempotent: every run drops and rebuilds its schema (`workspace.DL_AIR`) and its data lake folder
(`/Volumes/workspace/abd/abd/DL_AIR`), and never touches the uploaded files.

## Expected results

With the files in `data_air/`, the notebook should print these values (obtained in the local test):

| Indicator | Value |
|---|---|
| Route reports, flows, monthly airport rows | 145,912, 101,398 and 162,217 |
| Network of 2024 | 602 airports, 12,473 routes, 1,103 million passengers |
| Two declarations of the same flow | differ by 0.49% (median) |
| One-stop itineraries of the whole network | 922,420 |
| Itineraries from the Portuguese airports | 331 direct, 22,312 with one stop, 1,350,767 with two stops |
| Reach of Lisbon in 2024 | 106 airports direct, 562 within one stop, 598 within two stops |
| Lisbon, direct destinations (comparable panel) | 99 in 2019, 71 in 2020, 106 in 2024 |
| Passengers of Portugal in 2025 | 67.2 million, +22.1% compared with 2019, rank 7 of 33 countries |
| Load factor of the Portuguese routes in 2024 | 85.7% (Lisbon - Porto 73.0%, Lisbon - Faro 71.7%) |
| Seasonality ratio in 2025 | LIS 1.48, OPO 1.86, FAO 4.37, FNC 1.65, PDL 2.81 |
| Routes 2019 to 2024 | 211 kept, 52 gained, 26 lost, 24 not comparable |
| Streaming | 21,672 monthly records, identical to batch |
| Growth | 5.0% in 2025, 3.7% in January to April 2026 |
| Passengers in 2030 | 85.7 million (scenario A) to 90.1 million (scenario B) |

The itinerary counts were cross-checked with an independent pandas computation and are asserted by the local test.

## The project as a pipeline of the classes

| Class | What the project takes from it |
|---|---|
| ABD02, ABD03 | The `workspace` catalog, the Volume `/Volumes/workspace/abd/abd`, `dbutils.fs`, `display` |
| ABD04, ABD05 | Reading CSV and text files into DataFrames, schemas, tables |
| ABD06 | Aggregations, pivot, explode, window functions, a UDF, PageRank with DataFrames |
| ABD07 | Medallion layout: data rules and Parquet in dated Bronze folders, business rules in Silver (`SV_` Delta tables), Gold (`GD_` Delta tables), `%sql` exploration, MERGE, history and time travel |
| ABD08 | Structured Streaming: source, transformation, sink, output modes, checkpoint (with the `availableNow` trigger that serverless supports) |
| ABD09 | Graph operations of GraphFrames rebuilt with DataFrames: degrees, motif, subgraph, breadth-first search, PageRank |

## How the project meets the syllabus

| Requirement | Where |
|---|---|
| Use Apache Spark and/or Databricks Free Edition | Whole notebook (PySpark, Spark SQL, serverless compute) |
| Problem and data source related to the course topics | Section 1; sources in section 4 |
| Combine structured and/or unstructured and/or streaming data | A structured CSV file, 48 text files parsed with Spark (sections 5 and 6), streaming JSON records (section 11) |
| Use the data organisation concepts covered in class | The medallion lakehouse of class ABD07, in the class Volume (sections 2, 5 to 7, 13) |
| Working Databricks notebook in HTML | Steps above |
| One-page document (optional) | `report/one_page_report.pdf` |

| Evaluation item (points) | Where |
|---|---|
| Problem definition and motivation (3) | Executive summary and section 1 |
| Big Data technologies use (4) | Section 2, applied in sections 5 to 13 |
| Notebook overall presentation and structure (4) | Table of contents, numbered sections, titled cells |
| Programming standards, style and readability (3) | Central configuration, documented functions, logic shared between batch and streaming |
| Execution and output clarity (3) | Printed reconciliations, tables, eight charts |
| Conclusion (3) | Section 14: answers, recommendations, limitations |

Spark ML is taught after the delivery date and is listed as future work.

## Local test (optional)

```bash
pip install "pyspark[connect]==4.0.1" delta-spark==4.0.0 pandas pyarrow matplotlib   # needs Java 17+
python local_test/run_air.py                 # classic Spark
ABD_CONNECT=1 python local_test/run_air.py   # Spark Connect, the protocol used by Databricks serverless
```

The test reads the same files in `data_air/`. It replaces the Volume with a local folder and uses small stand-ins for
`dbutils.fs`, `display()` and `%sql` cells.

## Earlier alternatives kept in the repository

These were built before the air connectivity theme was chosen. They are not part of the submission.

| Path | What it is |
|---|---|
| `notebooks/ABD_Group_Project_CP_Rail_Present_and_Future.py`, `data/`, `local_test/run_local.py` | Comboios de Portugal timetable project (real data) |
| `notebooks/cp_delays/`, `local_test/run_cp_delays.py` | CP delays project: setup SQL and Bronze notebook only, with **simulated** delays |
