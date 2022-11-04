# Project: Data Modeling with Postgres for Sparkify Startup

## Project Description

This Project will allow Sparkify Startup analyze the data they've been collecting on songs and user activity on their new music streaming app. They will be able to know what songs users are listening to based on their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The Project carried out several tasks including a Postgres database with tables designed to optimize queries on song play analysis and also provided an ETL pipeline that makes the process efficient.

![Sparkify ER Diagram!](/sparkifydb_erd.png)

***

## Scripts execution with python from the project's root

### Create Postgres Database and Tables

    py create_tables.py

### Execute ETL Pipeline

    py etl.py

***

## Files in repository

1. **test.ipynb** displays the first few rows of each table to let you check your database.
2. **create_tables.py** drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. **etl.py** reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. **sql_queries.py** contains all your sql queries, and is imported into the last three files above.


