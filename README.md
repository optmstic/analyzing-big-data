# Analyzing Big Data 2026-2027: Project

**Portugal's air connectivity, 2016-2026.** A small lakehouse on Apache Spark and Databricks Free Edition that answers
three questions with official Eurostat statistics: where can you fly from Portugal, where are the passengers, and
where is demand heading.

| File | What it is |
|---|---|
| `notebooks/ABD_Project_Portugal_Air_Connectivity.py` | **The project notebook** (Databricks source format, 31 code cells) |
| `air_project_data.zip` | **The one data file to upload** (42 small files, 6.6 MB) |
| `report/one_page_report.pdf` | The optional one-page document (editable source: `report/one_page_report.html`) |
| `report/presentation_script.md` | A 10-minute speaking script, section by section, with likely questions and answers |
| `local_test/run_local.py` | Local test that runs every cell on Spark + Delta Lake with the same zip |
| `data_air/` | The 42 files inside the zip, uncompressed |
| `archive/` | Earlier, larger versions of the project. Not part of the submission |

## How to produce the HTML notebook to submit

1. In Databricks Free Edition, open **Catalog** > `workspace` > `abd` > Volumes > `abd` and choose
   **Upload to this volume**. Upload `air_project_data.zip` (do not unzip it).
2. **Import** the notebook (Workspace > Import > File).
3. In the first cell, write your name and student number.
4. Attach **Serverless** compute and choose **Run all**.
5. Check the two control lines: `All quality checks passed: YES` (section 4) and
   `Streaming result identical to batch: YES` (section 7.1).
6. **File > Export > HTML** and submit that file in Moodle by **17 November, 24:00**.
7. For the one-page document, write your name in `report/one_page_report.html` (the line with *Student name and
   number*), open it in a browser and print it to PDF.

If the Volume `workspace.abd.abd` does not exist yet, run the notebook once: it creates the Volume and then stops,
asking for the zip. Upload the zip and run it again.

The notebook can be run again at any time: it rebuilds its schema (`workspace.DL_AIR`) and its folder
(`/Volumes/workspace/abd/abd/DL_AIR`) on every run.

## What the notebook should print

| Indicator | Value |
|---|---|
| European network against 2019 | -49% routes in 2020, +8% in 2024 |
| Airports and edges of the 2024 graph | 602 and 12,478 |
| One-stop journeys | 22,318 from the Portuguese airports, 923,384 in the whole network |
| Lisbon | 106 airports direct, 562 within one stop (94%) |
| Porto | 77 airports direct, 544 within one stop (91%) |
| Main hub for Lisbon | Istanbul, a possible stop for 40% of the one-stop destinations |
| Portugal in 2025 | 67.2 million passengers, +22% against 2019 |
| Growth | 5.0% in 2025, 3.7% in January to April 2026 |
| 2030 | 86 to 91 million passengers |

## Data

Official Eurostat statistics, downloaded on 16 November 2026 and stored unchanged:

| Files | Eurostat dataset | Content |
|---|---|---|
| `routes_annual_<country>.tsv` (38 files, 35 countries) | `avia_par_<country>` | Passengers, seats and flights of each air route, per year, 2016 to 2024 |
| `airports_monthly_PAS_CRD_<type>.tsv` (3 files) | `avia_paoa` | Monthly passengers of each European airport (total, national, international), 2016 to 2026 |
| `airports.csv` | | OurAirports (ourairports.com), for airport names and coordinates |

Route statistics end in 2024 because Eurostat had not published 2025 yet. Airport statistics reach April 2026 for
Portugal.

## How the project meets the syllabus

| Requirement or evaluation item | Where |
|---|---|
| Apache Spark and Databricks Free Edition | Whole notebook |
| Structured, unstructured and streaming data | CSV file, text files parsed with Spark (sections 3 and 4), streaming JSON (section 7.1) |
| Data organisation concepts of the classes | Medallion architecture, Volume, Delta tables (sections 1 to 4) |
| Problem definition and motivation | First cell |
| Big Data technologies use | Section 1, applied in sections 3 to 7 |
| Presentation and structure | Eight numbered sections, titled cells |
| Programming standards and readability | One configuration cell, two helpers, one parser, commented code |
| Execution and output clarity | Tables, three charts, two control lines |
| Conclusion | Section 8 |

## Local test (optional)

```bash
pip install "pyspark[connect]==4.0.1" delta-spark==4.0.0 pandas pyarrow matplotlib   # needs Java 17+
python local_test/run_local.py                 # classic Spark
ABD_CONNECT=1 python local_test/run_local.py   # Spark Connect, the protocol used by Databricks serverless
```
