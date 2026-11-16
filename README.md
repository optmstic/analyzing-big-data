# Analyzing Big Data 2026-2027: Group Project

**Comboios de Portugal: the rail network today and tomorrow.**
A lakehouse with batch, streaming and graph analytics on Apache Spark (Databricks Free Edition).

| Path | What it is |
|---|---|
| `notebooks/ABD_Group_Project_CP_Rail_Present_and_Future.py` | The project notebook, in Databricks source format (import it into Databricks) |
| `data/` | The nine source files the notebook reads (snapshot of 16 November 2026) |
| `local_test/run_local.py` | Local test that executes every cell on Spark + Delta Lake with the same data files |

## The project as a pipeline of the classes

The notebook follows the ABD class notebooks in order and uses their conventions:

| Class | What the project takes from it |
|---|---|
| ABD02, ABD03 | The `workspace` catalog, the Volume `/Volumes/workspace/abd/abd`, `dbutils.fs`, `display` |
| ABD04, ABD05 | Reading CSV and text files into DataFrames, schemas, tables |
| ABD06 | Aggregations, pivot, explode, date functions, window functions, a UDF, PageRank with DataFrames |
| ABD07 | Medallion layout: data rules and Parquet in dated Bronze folders, business rules in Silver (`SV_` Delta tables), Gold (`GD_` Delta tables), `%sql` exploration, MERGE, history and time travel |
| ABD08 | Structured Streaming: source, transformation, sink, output modes, checkpoint (with the `availableNow` trigger that serverless supports) |
| ABD09 | Graph operations of GraphFrames rebuilt with DataFrames: degrees, motif, subgraph, breadth-first search, PageRank |

## Data

| Files | Source |
|---|---|
| `agency.txt`, `routes.txt`, `stops.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, `calendar_dates.txt` | Official CP timetable in GTFS format, `https://publico.cp.pt/gtfs/gtfs.zip` (unzipped, unchanged) |
| `rail_pa_total.tsv`, `rail_pa_quartal.tsv` | Eurostat, passengers transported by rail, annual and quarterly (TSV export, unchanged) |

The notebook is tied to this snapshot: its reference weeks (16 November 2026 for the present, 18 January 2027 for the
future) were chosen for it. Use these files, not a newer download.

## How to produce the deliverable (HTML notebook)

The syllabus requires a *working Databricks Notebook in html format*, which has to be exported from Databricks.

1. Sign in to Databricks Free Edition and **import** the notebook (Workspace > Import > File).
2. In the first cell, replace the placeholders with the group number, student names and numbers.
3. Attach **Serverless** compute and choose **Run all** once. The notebook creates the folder `cp_rail` in the Volume
   of the classes (`/Volumes/workspace/abd/abd`), then stops in section 4 with a message saying that the source files
   are missing. This is expected.
4. Open **Catalog** > `workspace` > `abd` > Volumes > `abd` > `cp_rail` > **Upload to this volume**, and upload the
   nine files of `data/` into that folder.
5. Choose **Run all** again. Check these three control lines in the output:
   - `19 of 19 checks passed` (section 6.6)
   - `Streaming results identical to batch results : YES` (section 8.4)
   - `Sum of all ranks = 1.000000` (section 9.2)
6. **File > Export > HTML** and submit that file in Moodle by **17 November, 24:00**.

The notebook is idempotent: every run drops and rebuilds its schema (`workspace.DL_CP`) and its data lake folder
(`/Volumes/workspace/abd/abd/DL_CP`), and never touches the uploaded files.

## Expected results

With the data snapshot in `data/`, the notebook should print these values (obtained in the local test):

| Indicator | Value |
|---|---|
| Train runs after calendar expansion | 548,614 |
| Stop events they represent | 8,954,555 |
| Trains in the week of 16 November 2026 | 8,503 (1,355 on an average weekday, 864 on an average weekend day) |
| Busiest station | Lisboa Oriente, 2,271 train calls per week |
| Most important hub (PageRank) | Porto Campanha |
| Stations not reachable from Lisboa Oriente | 35 (the Cascais line and the Vouga line) |
| 2027 timetable published for | Alfa Pendular, Intercidades, Urbanos de Lisboa |
| Alfa Pendular Lisboa Santa Apolonia to Porto Campanha | fastest 168 min today, 179 min in the 2027 timetable |
| Rail passengers in Portugal, 2025 | 240.0 million (sum of quarters), +37.2% since 2019 |
| Passengers projected for 2030 | 251.3 million (scenario B) to 327.9 million (scenario A) |

## How the project meets the syllabus

| Requirement | Where |
|---|---|
| Use Apache Spark and/or Databricks Free Edition | Whole notebook (PySpark, Spark SQL, serverless compute) |
| Problem and data source related to the course topics | Section 1; sources in section 4 |
| Combine structured and/or unstructured and/or streaming data | Structured CSV tables, a raw text file parsed with Spark (sections 5 and 6), streaming JSON events (section 8) |
| Use the data organisation concepts covered in class | The medallion lakehouse of class ABD07, in the class Volume (sections 2, 5 to 7, 12) |
| Working Databricks notebook in HTML | Steps above |

| Evaluation item (points) | Where |
|---|---|
| Problem definition and motivation (3) | Section 1 |
| Big Data technologies use (4) | Section 2, applied in sections 5 to 12 |
| Notebook overall presentation and structure (4) | Table of contents, numbered sections, titled cells |
| Programming standards, style and readability (3) | Central configuration, documented functions, logic shared between present/future and batch/streaming |
| Execution and output clarity (3) | Printed reconciliations, tables, six charts |
| Conclusion (3) | Section 13 |

Course topics covered: Spark basics, Spark SQL, data pipelines and the data lake, Spark Streaming, Spark graph
analytics. Spark ML is taught after the delivery date and is listed as future work.

## Local test (optional)

```bash
pip install "pyspark[connect]==4.0.1" delta-spark==4.0.0 pandas pyarrow matplotlib   # needs Java 17+
python local_test/run_local.py                 # classic Spark
ABD_CONNECT=1 python local_test/run_local.py   # Spark Connect, the protocol used by Databricks serverless
```

The test reads the same files in `data/`. It replaces the Volume with a local folder and uses small stand-ins for
`dbutils.fs`, `display()` and `%sql` cells.
