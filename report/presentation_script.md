# Presentation script (about 10 minutes, 16 slides)

The same text is in the speaker notes of each slide of the deck. Read the "main conclusion" slide (13) slowly: it is
the one sentence to remember.

| Slide | Title | What to say |
|---|---|---|
| 1 | Portugal’s air connectivity, 2016–2026 | Introduce yourself and the course. Then: Portugal depends on its air links, for tourism, for the islands and for the diaspora. This project asks three questions about them, and answers them with official Eurostat statistics on Databricks. |
| 2 | Three questions about Portugal’s air links | The big idea in three sentences. I took official European statistics about flights. I used them to answer three questions about Portugal: where you can fly, where the passengers are, and how traffic is changing. Spark was needed because one step, combining every route with every other route, creates almost a million rows. |
| 3 | Official Eurostat statistics, from 2016 to 2026 | Eurostat gives one file per country, listing its air routes: for each route, how many passengers, seats and flights per year. There is also a file with each airport’s passengers per month, up to 2026. In total 42 files, uploaded as one zip. |
| 4 | A medallion lakehouse on Databricks Free Edition | Bronze, Silver and Gold are just three shelves. Bronze: the files exactly as downloaded. Silver: the same data cleaned into normal tables, one row per route per year. Gold: small summary tables that answer the questions. Everything runs on Databricks Free Edition, and the tables are stored in Delta format. |
| 5 | The files are not normal tables, so a parser cuts them into rows | The files are not normal tables. Each line packs several pieces of information into one field: here, annual figures, flights, passenger flights arriving at Faro from Vienna. Then there is one column per year. The parser cuts each line into normal rows. Then six simple quality tests: no duplicates, no negative numbers, no impossible months. All pass. |
| 6 | Routes halved in 2020, then came back larger | Europe had about 5,800 air routes in 2019. Covid cut that to about 3,000 in 2020. By 2024 it was back above 6,200, 8% more than before the pandemic. |
| 7 | A journey with one stop is a join of the routes with themselves | This is the key idea, and the Big Data part. If Lisbon flies to Istanbul, and Istanbul flies to Tokyo, then Lisbon reaches Tokyo with one stop. To find all such trips, the program matches every route’s destination with every other route’s origin. That is the join, and it turns about 6,200 routes into 923,384 possible trips. |
| 8 | With one stop, Porto reaches almost as far as Lisbon | Lisbon flies directly to 106 airports and reaches 562 with one stop: 94% of all the airports in the data. Porto has fewer direct flights, 77, but reaches almost as many, 544. So once you accept one stop, Porto is almost as well connected as Lisbon. And of the places Lisbon cannot reach directly, 40% can be reached by changing planes in Istanbul. Honest limit: the data only covers routes that touch Europe, so 94% means 94% of the airports connected to Europe, not of the whole world. |
| 9 | 67 million passengers in 2025, half of them in Lisbon | Portugal had 67.2 million air passengers in 2025, 22% more than in 2019. Lisbon handles half and Porto a quarter. Someone flying Lisbon to Porto is counted at both airports, so domestic passengers are divided by two to count each person once. |
| 10 | Faro’s summers, and empty seats on domestic routes | The seasonality ratio is the busiest month’s passengers divided by the quietest month’s. Faro’s 4.4 means July has 4.4 times more passengers than January; Lisbon’s 1.5 means it is busy all year. And most big routes fly 80 to 89% full, but Lisbon to Porto and Lisbon to Faro fly only about 72% full: more than a quarter of their seats are empty. |
| 11 | A stream adds only the new months | Streaming in plain words: new monthly data arrives as files. The program reads 2025 first, then 2026. The second time it reads only the new files, because it keeps a record of what it has already read: the checkpoint. The final check shows the result is exactly the same as recalculating everything from scratch. |
| 12 | Growth has settled at about 5% a year | Growth is about 5% a year in 2024 and 2025, and 3.7% in early 2026. The two 2030 lines: A assumes growth stays at the 2025 rate; B assumes the same yearly increase as 2016 to 2019. They give 86 to 91 million passengers. They are simple reference lines, not a forecast. |
| 13 | Portugal’s air traffic has fully recovered and keeps growing, but more slowly | If you are asked for the main conclusion: Portugal’s air traffic has fully recovered and keeps growing, but more slowly. Lisbon is the hub, yet the regional airports are almost as well connected with one stop. The weak points are Faro’s winter and the half-empty domestic flights. |
| 14 | Three things the numbers suggest | My three recommendations. Promote Porto, Faro and the islands by their one-stop reach. Review the domestic routes that fly with empty seats. And work on Faro’s winter. |
| 15 | What Spark made possible, and the limits | What Spark made possible: the self-join, reading many files with a wildcard, and one engine for files, SQL, a graph and a stream. The main limitation is that route data ends in 2024, because Eurostat had not published 2025 yet. |
| 16 | Questions? | Thank the audience and invite questions. Likely questions and short answers are in presentation_script.md. |

## Questions you may get, and short answers

| Question | Answer |
|---|---|
| Why is this Big Data if the files are small? | The files are small, but the one-stop join produces more than 900,000 rows from about 6,000 routes, and the same code would run unchanged on monthly data for all countries, which is about a hundred times larger. |
| Why Spark and not pandas? | Reading 42 files in two statements, the self-join for one-stop journeys, and streaming with checkpoints all come built in. |
| Is the data real? | Yes. Every number is an official Eurostat statistic. Only the arrival of the files in the streaming part is simulated. |
| Why does the route data stop in 2024? | Eurostat had not published the 2025 routes yet. The airport data goes to April 2026. |
| What does "one stop" mean exactly? | Both routes existed in the same year. I do not have timetables, so I cannot say the connection is practical on a given day. |
| Why "international + national / 2"? | A domestic passenger is counted at the departure airport and again at the arrival airport, so national passengers are halved to count each person once. |
| Why did you not use GraphFrames? | It needs a library that cannot be installed on serverless compute. A one-stop journey is a join, so DataFrames are enough. |
| What is the checkpoint for? | It stores which files the stream has already read, so a restart continues where it stopped and no file is counted twice. |
