# Data Modeling with Apache Cassandra

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. The task is to create a database for this analysis.

### Data :

*Event Data :* collection of CSV files that describes the songs such as title, artist name, year, etc.


#### Sample Data :

event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv

### Run the notebook :

  -  `DataModellingwithApacheCassandra.ipynb`
      It is a jupyter notebook which has code to extract data from .csv files in event_data folder. The extracted data is written into a new csv file named `event_datafile_new.csv`

  -   From the extracted data the data is inserted into the new tables created as part of Data Modelling.

##### Queries :

_Query 1_: Give me the artist, song title and song's length in the music app history
	 that was heard during sessionId = 338, itemInSession = 4
_Query 2_: Give me only the following: name of artist, song (sorted by itemInSession) and
	 user (first and last name) for userid = 10, sessionid = 182
_Query 3_: Give me every user name (first and last) in my music app history who
	 listened to the song 'All Hands Against His Own'
