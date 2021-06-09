# Reference for converting timestamps in ms into date details
# https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift

import configparser


# Setting Configurations through the variable after fetching the details from 'dwh.cfg'
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES queries for all the tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES queries for all the tables
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                             (artist varchar,
                              auth varchar,
                              firstName varchar,
                              gender varchar,
                              itemInSession varchar,
                              lastName varchar,
                              length varchar,
                              level varchar,
                              location varchar,
                              method varchar,
                              page varchar,
                              registration varchar,
                              sessionId INT,
                              song varchar,
                              status INT,
                              ts BIGINT,
                              userAgent varchar,
                              userId INT
                             )""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                             (num_songs INT,
                              artist_id varchar,
                              artist_latitude varchar,
                              artist_longitude varchar,
                              artist_location varchar,
                              artist_name varchar,
                              song_id varchar,
                              title varchar,
                              duration float,
                              year INT            
                             )""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                             (songplayId INT IDENTITY(0,1) PRIMARY KEY,
                              startTime timestamp SORTKEY NOT NULL,
                              userId INT,
                              level varchar,
                              songId varchar NOT NULL,
                              artistId varchar DISTKEY NOT NULL,
                              sessionId INT,
                              location varchar,
                              userAgent varchar            
                             )""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                             (userId INT SORTKEY PRIMARY KEY,
                              firstName varchar,
                              lastName varchar,
                              gender varchar,
                              level varchar
                             )""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                             (songId varchar SORTKEY PRIMARY KEY,
                              title varchar,
                              artistId varchar DISTKEY NOT NULL,
                              year INT,
                              duration float
                             )""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                             (artistId varchar DISTKEY PRIMARY KEY,
                              name varchar,
                              location varchar,
                              artistLatitude varchar,
                              artistLongitude varchar
                             )""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                             (startTime timestamp SORTKEY PRIMARY KEY,
                              hour INT NOT NULL,
                              day INT NOT NULL,
                              week INT NOT NULL,
                              month INT NOT NULL,
                              year INT NOT NULL,
                              weekday INT NOT NULL
                             )""")

# Copying data from S3 buckets of Events and Songs into staging tables.
staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
FORMAT AS json {}
compupdate off statupdate off
region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
FORMAT AS json 'auto'
compupdate off statupdate off
region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# Inserting the data with the help of Staging tables into Star Schema tables for query processing.
# Fact Table - songplays
# Domension Table - users, songs, artists, time
songplay_table_insert = ("""
                            INSERT INTO songplays (startTime, userId, level, songId, artistId, sessionId, location, userAgent)
                            SELECT  DISTINCT 
                                    TIMESTAMP 'epoch' + (SE.ts/1000) * interval '1 second' as startTime,
                                    SE.userId,
                                    SE.level,
                                    SS.song_id,
                                    SS.artist_id,
                                    SE.sessionId,
                                    SE.location,
                                    SE.userAgent
                            FROM    staging_events SE
                      INNER JOIN    staging_songs SS
                              ON    (SE.song = SS.title 
                                    AND SE.artist = SS.artist_name
                                    AND SE.length = SS.duration)
                           WHERE    SE.page = 'NextSong';
                        """)

user_table_insert = ("""
                        INSERT INTO users (userId, firstName, lastName, gender)
                        SELECT  DISTINCT
                                userId,
                                firstName,
                                lastName,
                                gender
                        FROM    staging_events
                        WHERE   userId IS NOT NULL
                          AND   page = 'NextSong';
""")

song_table_insert = ("""
                        INSERT INTO songs (songId, title, artistId, year, duration)
                        SELECT  DISTINCT
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                        FROM    staging_songs
                        WHERE   song_id IS NOT NULL;
""")

artist_table_insert = ("""
                        INSERT INTO artists (artistId, name, location, artistLatitude, artistLongitude)
                        SELECT  DISTINCT
                                SS.artist_id,
                                SS.artist_name,
                                SE.location,
                                SS.artist_latitude,
                                SS.artist_longitude
                        FROM    staging_events SE
                        JOIN    staging_songs SS
                          ON    (SE.song = SS.title AND SE.artist = SS.artist_name)
                       WHERE    SS.artist_id IS NOT NULL;
""")

time_table_insert = ("""
                        INSERT INTO time (startTime, hour, day, week, month, year, weekday)
                        SELECT  DISTINCT
                                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as startTime,
                                EXTRACT(HOUR FROM startTime) AS hour,
                                EXTRACT(DAY FROM startTime) AS day,
                                EXTRACT(WEEK FROM startTime) AS week,
                                EXTRACT(MONTH FROM startTime) AS month,
                                EXTRACT(YEAR FROM startTime) AS year,
                                EXTRACT(WEEKDAY FROM startTime) AS weekday
                        FROM    staging_events
                        WHERE   page = 'NextSong';
""")

# List of queries that will be called respectively - 
# From create_tables.py for dropping and creating the tables repeatedly
# From etl.py for loading the data into Staging tables and inserting the data into star schema tables

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
