<img src="assets/cassandra_logo.png" width="25%" align="right" alt="" title="Cassandra logo" />

#### Udacity Data Engineering Nanodegree
### Project 2: Data Modeling with Cassandra


##### &nbsp;

<!--- [//]: # (_Photo credit: []_) --->

<!--- For instructions on how to setup and run this project go to the [starter code section.](https://github.com/tommytracey/udacity_data_engineering/p1_data_modeling_postgres#project-starter-code)
##### &nbsp; --->

<!-- The write-up below is also available [here as a blog post](https://medium.com/@thomastracey/training-two-agents-to-play-tennis-8285ebfaec5f). -->

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data. This will inform future projects that improve the Sparkify music service.

Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team at Sparkify.


##### &nbsp;

## Goals
In this project, we apply what we've learned on data modeling with Apache Cassandra to build an ETL pipeline using Python.

To complete the project, you need to:
1. Model your data by creating tables in Apache Cassandra to run queries
2. Write an ETL pipeline that transfers data from a set of CSV files and inserts it into Apache Cassandra tables.


##### &nbsp;

## Project Setup & Instructions

##### &nbsp;

### Datasets
For this project, you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

##### &nbsp;

### Project Template
To get started with the project, go to the workspace on the next page, where you'll find the project template (a Jupyter notebook file). You can work on your project and submit your work through this workspace.

The project template includes one Jupyter Notebook file, in which:

- you will process the `event_datafile_new.csv` dataset to create a denormalized dataset
- you will model the data tables keeping in mind the queries you need to run
- you have been provided queries that you will need to model your data tables for
- you will load the data into tables you create in Apache Cassandra and run your queries

##### &nbsp;

### Project Steps
Below are steps you can follow to complete each component of this project.

#### Modeling your NoSQL database or Apache Cassandra database
1. Design tables to answer the queries outlined in the project template
1. Write Apache Cassandra `CREATE KEYSPACE` and `SET KEYSPACE` statements
1. Develop your CREATE statement for each of the tables to address each question
1. Load the data with `INSERT` statement for each of the tables
1. Include `IF NOT EXISTS` clauses in your `CREATE` statements to create tables only if the tables do not already exist. We recommend you also include `DROP TABLE` statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
1. Test by running the proper select statements with the correct `WHERE` clause

#### Build ETL Pipeline
1. Implement the logic in section Part I of the notebook template to iterate through each event file in `event_data` to process and create a new CSV file in Python
1. Make necessary edits to Part II of the notebook template to include Apache Cassandra `CREATE` and `INSERT` statements to load processed records into relevant tables in your data model
1. Test by running `SELECT` statements after running the queries on your database

##### &nbsp;

## My Implementation

(in progress)

<!--
### Schema Design

<img src="images/sparkify_erd.png" width="100%" align="top-left" alt="" title="Sparkify ERD" />

* (diagram created using [Lucid Chart](https://www.lucidchart.com)) *


#### Schema Notes:
- Some of the identity fields are VARCHAR data types, while others are INT. For example, the source data for *song_id* and *artist_id* contain non numeric characters, so these are VARCHAR. Whereas, *user_id* only contains real numbers, so it is setup as an INT data type, and specifically the BIGINT data type to allow for a larger number of users.
- The timestamp data in the log files is recorded as a BIGINT data type. I decided to store it in its original format in the *start_time* field of the `songplays` table. However, it is converted to a timestamp in the *start_time* field of the `time` table &mdash; which makes it easier to derive the other time values (*hour, day, week* etc.)
- The *songs.duration, artists.latitude*, and *artists.longitude* data types are all set to FLOAT. But, for some reason when I export the schema information they show as data type DOUBLE. I couldn't figure out why this happens. Perhaps it's a bug in psycopg2. Regardless, it does not negatively affect the pipeline (other than allotting more space than is needed).



### Example Queries
Here are some examples of queries I used in my implementation.

```python
## create `songplays` fact table

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    PRIMARY KEY (songplay_id),
    songplay_id  SERIAL,
    start_time   BIGINT          NOT NULL,
    user_id      INT             NOT NULL,
    level        VARCHAR(50)     NOT NULL,
    song_id      VARCHAR(100)    NOT NULL,
    artist_id    VARCHAR(100)    NOT NULL,
    session_id   BIGINT          NOT NULL,
    location     TEXT,
    user_agent   TEXT)
""")
```

```python
# find songs and artists that match records extracted from log files

song_select = ("""SELECT songs.song_id, artists.artist_id FROM songs
JOIN artists ON songs.artist_id=artists.artist_id
WHERE songs.title=%s
AND artists.name=%s
AND songs.duration=%s
""")
```

```python
# insert the results from the `song_select` query above into the `songplays` table

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id)
    DO UPDATE
    SET start_time=EXCLUDED.start_time, user_id=EXCLUDED.user_id, level=EXCLUDED.level, song_id=EXCLUDED.song_id, artist_id=EXCLUDED.artist_id, session_id=EXCLUDED.session_id, location=EXCLUDED.location, user_agent=EXCLUDED.user_agent
""")
```



### ETL Output
Here is the output after running `etl.py`. You can see that there's only one record inserted into the `songplays` table. This means, there's only one song within all 30 of the log files that matches any of the records in the `songs` table. Although this is the correct result, it's a bit disappointing. I wish Udacity had setup the project so there are hundreds of matches. It would make the analysis more interesting.  

```
songplay row inserted:
[483] (1542837407796, '15', 'paid', 'SOZCTXZ12AB0182364', 'AR5KOSW1187FB35FF4', 818, 'Chicago-Naperville-Elgin, IL-IN-WI', '"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"')

---------
total log records checked in this file = 437
total rows inserted = 1
26/30 files processed.
```

##### &nbsp;

-->
