# Presentation script (10 minutes)

Open the exported HTML notebook and scroll through it while you talk. Each block below says where to stop, what to
show and what to say. The times add up to 9 minutes 30 seconds, which leaves a small margin.

| # | Time | Where in the notebook | What to say |
|---|---|---|---|
| 1 | 1:00 | Title cell, "The project in one minute" | "Portugal depends on its air links: tourism, the islands, the diaspora. I asked three questions. Where can you fly from Portugal? Where are the passengers? And where is demand heading? I answer them with official Eurostat statistics: the air routes of 35 countries from 2016 to 2024, and the monthly passengers of about 500 airports up to 2026." |
| 2 | 1:00 | Section 1, the architecture diagram | "Everything runs on Databricks Free Edition. The data follows the medallion architecture from class. Bronze keeps the files as received. Silver has the clean tables. Gold has the small tables that answer the questions. Files are in a Unity Catalog Volume and tables are in Delta format." |
| 3 | 1:00 | Section 3, "What a Eurostat file looks like" | "The files are not tables. The first field packs four things: frequency, unit, measure and route. Then there is one column per year, and a colon when there is no value. So I read them as text. I read 38 files in one statement with a wildcard." |
| 4 | 1:00 | Section 4, the parser and the quality checks | "One function turns these lines into rows, with split and posexplode. Each file has its own header, because the United Kingdom stopped reporting in 2019, so each value is matched with the header of its own file. A pivot puts passengers, seats and flights side by side. Then six quality checks, all zero." |
| 5 | 1:00 | Section 5.1, the network table | "First result. The European network lost half of its routes in 2020 and three quarters of its passengers. By 2023 it had recovered, and in 2024 it has 8% more routes than before the pandemic." |
| 6 | 2:00 | Section 5.2, the diagram, the reach table and the chart | "This is the central idea. The network is a graph, and a journey with one stop is a join of the routes with themselves. That join turns about 6,000 routes into more than 900,000 one-stop journeys, which is why I use Spark. The result: Lisbon flies directly to 106 airports, but with one stop it reaches 562, which is 94% of the network. Porto has 77 direct flights and reaches 91%. So a regional airport is almost as well connected as the capital. And one hub stands out: Istanbul is a possible stop for 40% of the places Lisbon cannot reach directly." |
| 7 | 1:30 | Section 6, the airports table, seasonality and empty seats | "Demand. Portugal had 67 million air passengers in 2025, 22% more than in 2019. Lisbon has half of them. Faro is very seasonal: more than four times as many passengers in July as in January. And planes are full, except on two domestic routes: Lisbon to Porto and Lisbon to Faro fly with more than a quarter of the seats empty." |
| 8 | 1:00 | Section 7.1, the two deliveries and the control | "New months arrive every month, so I built a stream. The first delivery has the months of 2025, the second the months of 2026. The checkpoint remembers what was already read, so the second run only processes the new files. The control shows that the result is identical to a normal batch calculation." |
| 9 | 1:00 | Section 7.2 and section 8 | "Growth has settled at about 5% a year, and 3.7% so far in 2026. Two simple scenarios give between 86 and 91 million passengers in 2030. My three recommendations: promote Porto, Faro and the islands by their one-stop reach; review the domestic routes with empty seats; and work on Faro's winter. The main limitation is that route data ends in 2024." |

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
