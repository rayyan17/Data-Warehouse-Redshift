"""Data Manipulation Queries"""


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS song_event_log;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_info;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

