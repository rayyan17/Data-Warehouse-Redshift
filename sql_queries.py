import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


"""Data Manipulation Queries"""


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS song_event_log;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_info;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS song_event_log \
(artist VARCHAR, \
auth VARCHAR, \
firstName VARCHAR, \
gender VARCHAR, \
itemInSession INTEGER, \
lastName VARCHAR, \
length DECIMAL, \
level VARCHAR, \
location VARCHAR, \
method VARCHAR, \
page VARCHAR, \
registration VARCHAR, \
sessionId INTEGER, \
song VARCHAR, \
status INTEGER, \
ts BIGINT, \
userAgent VARCHAR, \
userId INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_info \
(song_id VARCHAR, \
title VARCHAR, \
artist_id VARCHAR, \
artist_name VARCHAR, \
artist_location VARCHAR, \
artist_latitude DECIMAL, \
artist_longitude DECIMAL, \
year INTEGER, \
num_songs INTEGER, \
duration DECIMAL);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays \
(songplay_id INT IDENTITY(0,1) sortkey distkey, \
start_time TIMESTAMP NOT NULL, \
user_id INTEGER NOT NULL, \
level VARCHAR, \
song_id VARCHAR, \
artist_id VARCHAR, \
session_id INTEGER, \
location VARCHAR, \
user_agent CHARACTER VARYING);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users \
(user_id INTEGER PRIMARY KEY distkey, \
first_name VARCHAR sortkey, \
last_name VARCHAR, \
gender VARCHAR, \
level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs \
(song_id VARCHAR PRIMARY KEY sortkey, \
title VARCHAR, \
artist_id VARCHAR, \
year INTEGER, \
duration DECIMAL) \
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists \
(artist_id VARCHAR PRIMARY KEY sortkey, \
name VARCHAR, \
location VARCHAR, \
latitude DECIMAL, \
longitude DECIMAL) \
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time \
(start_time TIMESTAMP PRIMARY KEY sortkey distkey, \
hour INTEGER NOT NULL, \
day INTEGER NOT NULL, \
week INTEGER NOT NULL, \
month INTEGER NOT NULL, \
year INTEGER NOT NULL, \
weekda INTEGER NOT NULL);
""")


# STAGING TABLES

staging_events_copy = ("""
copy song_event_log from 's3://udacity-dend/log_data/' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 's3://udacity-dend/log_json_path.json' ;
""").format(config["IAM_ROLE"]["ARN"])

staging_songs_copy = ("""
copy songs_info from 's3://udacity-dend/song_data/' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto';
""").format(config["IAM_ROLE"]["ARN"])


# INSERTION INTO FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
(select dateadd(millisecond, ts, '1970-01-01 00:00:00'), userId, level, song_id, artist_id, sessionId, location, userAgent \
from song_event_log left outer join songs_info on \
(songs_info.title=song_event_log.song \
and songs_info.artist_name=song_event_log.artist \
and songs_info.duration=song_event_log.length) \
where page='NextSong'
);
""")

user_table_insert = ("""
insert into users \
(select userId, firstName, lastName, gender, level \
from song_event_log where page='NextSong');
""")

song_table_insert = ("""
insert into songs \
(select song_id, title, artist_id, year, duration from songs_info);
""")

artist_table_insert = ("""
insert into artists \
(select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from songs_info);
""")

time_table_insert = ("""
insert into time \
(select dateadd(millisecond, ts, '1970-01-01 00:00:00') as main_time, \
extract(hour from main_time), \
extract(day from main_time), \
extract(week from main_time), \
extract(month from main_time), \
extract(year from main_time), \
extract(dayofweek from main_time) \
from song_event_log where page='NextSong');
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
