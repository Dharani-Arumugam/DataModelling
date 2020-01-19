# Data Modeling with postgresql

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a Data engineer, the task is to create a database star schema and ETL pipeline that will insert all the data which is stored as .json in local to the database.

### Data :
*Songs metadata :* collection of JSON files that describes the songs such as title, artist name, year, etc.

*Logs data :* collection of JSON files where each file covers the users activities over a given day.

### Run the scripts :

There are two python scripts that should be run in the same order below :

  -  `python create_table.py`
      sql_queries.py which has all the queries to create/delete the tables in database. Create_table.py will connect to database and internally calls sql_queries.py
  -  `python etl.py`
      This builds the pipeline to extract data from JSON files and inserts the data into corresponding tables.
