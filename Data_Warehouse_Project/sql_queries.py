import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop       = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop        = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop             = "DROP TABLE IF EXISTS songplays;"
user_table_drop                 = "DROP TABLE IF EXISTS users;"
song_table_drop                 = "DROP TABLE IF EXISTS songs;"
artist_table_drop               = "DROP TABLE IF EXISTS artists;"
time_table_drop                 = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create = ("""
    CREATE TABLE staging_events (
          event_id               INTEGER  IDENTITY(1,1)  PRIMARY KEY
         ,artist_name            VARCHAR(500)
         ,auth                   VARCHAR
         ,user_first_name        VARCHAR
         ,gender                 CHAR(1)
         ,item_in_session        INTEGER
         ,user_last_name         VARCHAR
         ,song_length            DOUBLE PRECISION
         ,user_level             VARCHAR
         ,artist_location        VARCHAR(500)
         ,method                 VARCHAR
         ,page                   VARCHAR
         ,registration           DOUBLE PRECISION
         ,session_id             INTEGER
         ,song_title             VARCHAR(500)
         ,status                 INTEGER
         ,ts_timestamp           BIGINT
         ,user_agent             VARCHAR
         ,user_id                VARCHAR
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
         song_id             VARCHAR  PRIMARY KEY
        ,num_songs           INTEGER
        ,artist_id           VARCHAR
        ,artist_latitude     DOUBLE PRECISION
        ,artist_longitude    DOUBLE PRECISION
        ,artist_location     VARCHAR(500)
        ,artist_name         VARCHAR(500)
        ,title               VARCHAR(500)
        ,duration            DOUBLE PRECISION
        ,year                INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
         songplay_id         INTEGER  IDENTITY(1,1)  PRIMARY KEY   sortkey
        ,start_time          TIMESTAMP
        ,user_id             VARCHAR
        ,level               VARCHAR
        ,song_id             VARCHAR    distkey
        ,artist_id           VARCHAR
        ,session_id          INTEGER
        ,location            VARCHAR(500)
        ,user_agent          VARCHAR
    );
""")


user_table_create = ("""
    CREATE TABLE users (
         user_id      VARCHAR  PRIMARY KEY distkey
        ,first_name   VARCHAR
        ,last_name    VARCHAR
        ,gender       CHAR(1)
        ,level        VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
         song_id      VARCHAR PRIMARY KEY
        ,title        VARCHAR(500)
        ,artist_id    VARCHAR  REFERENCES artists(artist_id) sortkey distkey
        ,year         INTEGER
        ,duration     DOUBLE PRECISION
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
         artist_id   VARCHAR PRIMARY KEY distkey
        ,name        VARCHAR(500)
        ,location    VARCHAR(500)
        ,latitude    DOUBLE PRECISION
        ,longitude   DOUBLE PRECISION
    );
""")

time_table_create = ("""
    CREATE TABLE time (
         start_time  TIMESTAMP PRIMARY KEY sortkey distkey
        ,hour        INTEGER
        ,day         INTEGER
        ,week        INTEGER
        ,month       INTEGER
        ,year        INTEGER
        ,weekday     INTEGER
    );
""")

IAM_ROLE_NAME = config.get("IAM_ROLE","ARN")
LOG_DATA      = config.get("S3","LOG_DATA")
LOG_JSONPATH  = config.get("S3","LOG_JSONPATH")
SONG_DATA     = config.get("S3","SONG_DATA")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events
        FROM            {}
        IAM_ROLE        {}
        REGION          'us-west-2'
        JSON            {}
        STATUPDATE      OFF
        COMPUPDATE      OFF
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(LOG_DATA, IAM_ROLE_NAME, LOG_JSONPATH)

staging_songs_copy = ("""
   COPY staging_songs
       FROM             {}
       IAM_ROLE         {}
       REGION           'us-west-2'
       FORMAT AS JSON   'auto'
       STATUPDATE       OFF
       COMPUPDATE       OFF
       TRUNCATECOLUMNS  BLANKSASNULL EMPTYASNULL
""").format(SONG_DATA, IAM_ROLE_NAME)

# FINAL TABLES
songplay_table_insert = ("""
        INSERT INTO songplays( start_time
                              ,user_id
                              ,level
                              ,song_id
                              ,artist_id
                              ,session_id
                              ,location
                              ,user_agent)
       SELECT DISTINCT TIMESTAMP 'epoch' + se.ts_timestamp /1000 *INTERVAL '1 second' AS start_time
              ,se.user_id           AS user_id
              ,se.user_level        AS level
              ,ss.song_id           AS song_id
              ,ss.artist_id         AS artist_id
              ,se.session_id        AS session_id
              ,se.artist_location   AS location
              ,se.user_agent        AS user_agent
       FROM   staging_songs  ss
             ,staging_events se
       WHERE  se.page        = 'NextSong'
       AND    se.song_title  =  ss.title
       AND    se.user_id   IS NOT NULL

""")
user_table_insert = ("""
        INSERT INTO users( user_id
                          ,first_name
                          ,last_name
                          ,gender
                          ,level )
        SELECT  user_id          AS   user_id
               ,user_first_name  AS   first_name
               ,user_last_name   AS   last_name
               ,gender           AS   gender
               ,user_level       AS   level
        FROM   staging_events
        WHERE  user_id IS NOT NULL
        GROUP BY user_id
                ,user_first_name
                ,user_last_name
                ,gender
                ,user_level
        ORDER BY user_id;
""")

song_table_insert = ("""
        INSERT INTO songs( song_id
                          ,title
                          ,artist_id
                          ,year
                          ,duration)
        SELECT  song_id            AS  song_id
               ,title              AS  title
               ,artist_id          AS  artist_id
               ,year               AS  year
               ,duration           AS  duration
        FROM   staging_songs
        WHERE  song_id IS NOT NULL
        GROUP BY song_id
                ,artist_id
                ,title
                ,year
                ,duration
        ORDER BY song_id;
""")

artist_table_insert = ("""
        INSERT INTO artists( artist_id
                            ,name
                            ,location
                            ,latitude
                            ,longitude)
        SELECT   artist_id           AS  artist_id
                ,artist_name         AS  name
                ,artist_location     AS  location
                ,artist_latitude     AS  latitude
                ,artist_longitude    AS  longitude
        FROM    staging_songs
        WHERE   artist_id IS NOT NULL
        GROUP BY  artist_id
                 ,artist_name
                 ,artist_location
                 ,artist_latitude
                 ,artist_longitude
        ORDER BY artist_id;
""")

time_table_insert = ("""
        INSERT INTO time( start_time
                         ,hour
                         ,day
                         ,week
                         ,month
                         ,year
                         ,weekday )
        SELECT  DISTINCT start_time
               ,date_part(hour,    start_time) AS hour
               ,date_part(day ,    start_time) AS day
               ,date_part(week,    start_time) AS week
               ,date_part(month,   start_time) AS month
               ,date_part(year,    start_time) AS year
               ,date_part(weekday, start_time) AS weekday
        FROM   songplays
        WHERE    start_time IS NOT NULL
        ORDER BY start_time;
""")

# QUERY LISTS
create_table_queries   = [ staging_events_table_create
                          ,staging_songs_table_create
                          ,user_table_create
                          ,artist_table_create
                          ,song_table_create
                          ,time_table_create
                          ,songplay_table_create ]
drop_table_queries    = [ staging_events_table_drop
                         ,staging_songs_table_drop
                         ,songplay_table_drop
                         ,user_table_drop
                         ,song_table_drop
                         ,artist_table_drop
                         ,time_table_drop ]
copy_table_queries    = [ staging_events_copy
                         ,staging_songs_copy]
insert_table_queries  = [songplay_table_insert
                         ,user_table_insert
                         ,artist_table_insert
                         ,song_table_insert
                         ,time_table_insert]
