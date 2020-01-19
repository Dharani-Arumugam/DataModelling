# Data Modeling with Apache Cassandra

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

### Data :

*Event Data :* collection of JSON files that describes the songs such as title, artist name, year, etc.

*Logs data :* collection of JSON files where each file covers the users activities over a given day.

### Run the scripts :

There are two python scripts that should be run in the same order below :

  -  `python create_table.py`
      sql_queries.py which has all the queries to create/delete the tables in database. Create_table.py will connect to database and internally calls sql_queries.py
  -  `python etl.py`
      This builds the pipeline to extract data from JSON files and inserts the data into corresponding tables.
