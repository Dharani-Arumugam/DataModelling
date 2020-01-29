# Cloud Data Warehouse

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to built an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

Infrastructure as a code(Iaac) is used to build cluster

### Datasets :
*Song Dataset :* collection of JSON files that describes the songs such as title, artist name, year, etc.
Song data: `s3://udacity-dend/song_data`

*Log Dataset :* collection of JSON files where each file covers the users activities over a given day.
Log data: `s3://udacity-dend/log_data`
Log data json path: `s3://udacity-dend/log_json_path.json`

### Star Schema :
Create a star schema optimized for queries on song play analysis. This includes the following tables :

*Fact Table :*
- songplays - records in event data associated with song plays i.e. records with page `NextSong`
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

*Dimension Tables :*
  - users - users in the app
*user_id, first_name, last_name, gender, level*
  - songs - songs in music database
*song_id, title, artist_id, year, duration*
  - artists - artists in music database
*artist_id, name, location, latitude, longitude*
  - time - timestamps of records in songplays broken down into specific units
*start_time, hour, day, week, month, year, weekday*

### ETL pipeline :

 - Load from data from s3 buckets to staging tables named as `staging_songs` and `staging_events`
 - Data from staging tables are moved to AWS Redshift cluster

 ### Run the scripts in the given order :

 1. `create_cluster.py` - builds the cluster. After running create_cluster get 'Endpoint' and 'Role_Arn' value and store it in dwf.cfg file as 'Host' and 'ROLE_ARN'
 2. `check_cluster_status.py` - check the status of cluster. Run this folder to find if the status of cluster is 'available'
 3. `create_tables.py`- create the fact and dimension table in the newly created cluster.
 4. `etl.py` - builds the etl process to load the data from s3 to staging tables and extract them to AWS Redshift Cluster
 5. `analytics_test.ipynb` - Notebook which tests the loaded data in the cluster
 6. `delete_cluster.py`- delete the cluster
