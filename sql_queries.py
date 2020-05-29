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

