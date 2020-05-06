# Project: Sparkify Data Lakes

## Introduction
A music streaming startup, Sparkify, has grown their user base and song
database even more and want to move their data warehouse to a data lake.
Their data resides in S3, in a directory of JSON logs on user activity
on the app, as well as a directory with JSON metadata on the songs
in their app.

As their data engineer, you are tasked with building an ETL pipeline
that extracts their data from S3, processes them using Spark, and loads
the data back into S3 as a set of dimensional tables. This will allow
their analytics team to continue finding insights in what songs their
users are listening to.

You'll be able to test your database and ETL pipeline by
running queries given to you by the analytics team from Sparkify
and compare your results with their expected results.

## Resolution
In order to resolve the problem I loaded the data from S3
buckets onto Spark. I processed the data using spark and
filtered, joined and select data accordingly. Furthermore,
I also created parquet files for better performance and
smaller storage.
 
## Data Modeling
The database Star Schema has the following fact and dimension tables:

### Fact Table
songplay

`songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`

### Dimensions Tables
users

`user_id, first_name, last_name, gender, level`

songs

`song_id, title, artist_id, year, duration`

artists

`artist_id, name, location, latitude, longitude`

time

`start_time, hour, day, week, month, year, weekday`

## Project Structure

```
├── README.md
├── dl.cfg
├── etl.py

0 directories, 4 files
```
`etl.py` loads data from S3 into Spark, then process that data into
parquet files.

## Run the Project
Install Spark on your local machine or have it on an
EMR AWS cluster.

Run etl.py file:

    python etl.py

## Greetings
Made with Love by a Brazilian in Malta
