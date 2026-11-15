# Analyzing Big Data 2026-2027: Group Project

**NYC Taxi Mobility Lakehouse: batch, streaming and graph analytics with Apache Spark on Databricks Free Edition.**

| File | What it is |
|---|---|
| `notebooks/ABD_Group_Project_NYC_Taxi_Lakehouse.py` | The project notebook, in Databricks source format (import it into Databricks) |
| `local_test/run_local.py` | Local smoke test that executes every cell on Spark + Delta Lake with synthetic stand-in data |

## How to produce the deliverable (HTML notebook)

The syllabus requires a *working Databricks Notebook in html format*. The HTML has to be exported from Databricks
after running the notebook there:

1. Sign in to Databricks Free Edition and open **Workspace**.
2. **Import** `notebooks/ABD_Group_Project_NYC_Taxi_Lakehouse.py` (Workspace > Import > File).
3. In the first cell, replace the placeholders with the group number, student names and numbers.
4. Attach **Serverless** compute and choose **Run all**. No libraries, uploads or internet access are needed:
   the only input is `samples.nyctaxi.trips`, which ships with every workspace.
5. Check that the two control lines read `Bronze = duplicates + Silver + Quarantine : OK` and
   `Streaming results identical to batch results : YES`.
6. Read section 11 against the numbers that were printed and adjust the wording if you want to quote specific values.
7. **File > Export > HTML** and submit that file in Moodle by **17 November, 24:00**.

The notebook is idempotent: it drops and recreates its own schema (`<current catalog>.abd_nyc_taxi`), so it can be
re-run at any time.

## How the project meets the syllabus

| Requirement | Where |
|---|---|
| Use Apache Spark and/or Databricks Free Edition | Whole notebook (PySpark, Spark SQL, serverless compute) |
| Problem and data source related to the course topics | Section 1; source `samples.nyctaxi.trips` (section 4) |
| Combine structured and/or unstructured and/or streaming data | Structured table (sections 5 to 7) and streaming JSON events (section 8) |
| Use the data organisation concepts covered in class | Medallion architecture on Delta Lake and Unity Catalog (sections 2, 5 to 7, 10) |
| Working Databricks notebook in HTML | Steps above |

| Evaluation item (points) | Where |
|---|---|
| Problem definition and motivation (3) | Section 1 |
| Big Data technologies use (4) | Section 2, applied in sections 5 to 10 |
| Notebook overall presentation and structure (4) | Table of contents, numbered sections, titled cells |
| Programming standards, style and readability (3) | Central configuration, documented functions, one shared cleaning function |
| Execution and output clarity (3) | Printed reconciliations, tables, four charts |
| Conclusion (3) | Section 11 |

Course topics covered: Spark basics, Spark SQL, data pipelines and the data lake, Spark Streaming, Spark graph analytics.
Spark ML is taught after the delivery date and is listed as future work.

## Local smoke test (optional)

```bash
pip install "pyspark[connect]==4.0.1" delta-spark==4.0.0 pandas pyarrow matplotlib   # needs Java 17+
python local_test/run_local.py                 # classic Spark
ABD_CONNECT=1 python local_test/run_local.py   # Spark Connect, the protocol used by Databricks serverless
```

The test replaces the source table with **synthetic** data of the same schema and the Volume with a local folder.
It proves that the code runs and that the reconciliations hold; its numbers are not the project results.
