# Analyzing Big Data 2026-2027: Group Project

**Portugal's air connectivity: the global route network and the passengers who use it.**
A lakehouse with Spark SQL, graph analytics and Structured Streaming on Apache Spark (Databricks Free Edition).

| Path | What it is |
|---|---|
| `notebooks/ABD_Group_Project_Portugal_Air_Connectivity.py` | **The project notebook**, in Databricks source format (import it into Databricks) |
| `data_air/` | The four source files the notebook reads |
| `report/one_page_report.pdf` | The optional one-page document (source: `report/one_page_report.html`) |
| `local_test/run_air.py` | Local test that executes every cell on Spark + Delta Lake with the same data files |

## Data

| Files | Source |
|---|---|
| `airports.csv`, `routes.csv`, `airlines.csv` | Public static snapshot of the world's airports, airline routes and airlines (Kaggle), the same files as the earlier pandas project `optmstic/airport-network-route-analysis` |
| `avia_par_pt.tsv.gz` | Eurostat dataset `avia_par_pt`: passengers, seats and flights between the main Portuguese airports and their partner airports (TSV export, unchanged, downloaded on 16 November 2026) |

## How to produce the deliverable (HTML notebook)

The syllabus requires a *working Databricks Notebook in html format*, which has to be exported from Databricks.

1. Sign in to Databricks Free Edition and **import** the notebook (Workspace > Import > File).
2. In the first cell, replace the placeholders with the group number, student names and numbers.
3. Attach **Serverless** compute and choose **Run all** once. The notebook creates the folder `air_routes` in the Volume
   of the classes (`/Volumes/workspace/abd/abd`), then stops in section 4 with a message saying that the source files
   are missing. This is expected.
4. Open **Catalog** > `workspace` > `abd` > Volumes > `abd` > `air_routes` > **Upload to this volume**, and upload the
   four files of `data_air/` into that folder (keep `avia_par_pt.tsv.gz` compressed).
5. Choose **Run all** again. Check these three control lines in the output:
   - `18 of 18 checks passed` (section 6.4)
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
| Airline routes, connections, airports | 66,378, 36,759 and 3,391 |
| Eurostat observations | 809,988 |
| One-stop itineraries of the whole world | 2,293,298 |
| Itineraries from the Portuguese airports | 296 direct, 21,972 with one stop, 1,479,733 with two stops |
| Reach of Lisbon | 101 airports direct, 1,165 within one stop, 2,631 within two stops |
| Airports not reachable from Lisbon | 47; every other airport is at most 8 flights away |
| Passengers handled in 2024 | 64.1 million (Lisbon 51.5%), +19.2% compared with 2019 |
| Load factor in 2024 | 85.7% |
| Seasonality ratio | LIS 1.48, OPO 1.65, FAO 4.06, FNC 1.51, PDL 2.6 |
| Streaming | 25,857 monthly records, identical to batch |
| Passengers in 2030 | 82.6 million (scenario A) to 92.3 million (scenario B) |

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
| Combine structured and/or unstructured and/or streaming data | Structured CSV files, a text file parsed with Spark (sections 5 and 6), streaming JSON records (section 11) |
| Use the data organisation concepts covered in class | The medallion lakehouse of class ABD07, in the class Volume (sections 2, 5 to 7, 13) |
| Working Databricks notebook in HTML | Steps above |
| One-page document (optional) | `report/one_page_report.pdf` |

| Evaluation item (points) | Where |
|---|---|
| Problem definition and motivation (3) | Executive summary and section 1 |
| Big Data technologies use (4) | Section 2, applied in sections 5 to 13 |
| Notebook overall presentation and structure (4) | Table of contents, numbered sections, titled cells |
| Programming standards, style and readability (3) | Central configuration, documented functions, logic shared between batch and streaming |
| Execution and output clarity (3) | Printed reconciliations, tables, six charts |
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
