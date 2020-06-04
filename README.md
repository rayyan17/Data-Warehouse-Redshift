# Sparkify DataWarehouse
Sparkify is keeping its data in S3 as Json files. Now they need to build an analysis on that. The goal is to build a Data Warehouse for them so they can do OLAP queries and perform business analysis to further increase there business and value.


## Warehouse Design
We are building our warehouse follwing a very simple architecture like Kimball's Bus. We will be building an ETL Pipeline that will extract all the data from different Sparkify data sources in S3 and copy them into a Staging Sever. Then we will shift this data into a Star Schema which will help Analytics team to easily perform OLAP querires.


## ETL Pipeline
### Extraction:
We extract data for songs and related logs from the following directories in S3:
```
s3://udacity-dend/log_data
s3://udacity-dend/song_data
```


### Transformation
We will copy this data into our staging tables:
1. song_event_log: All data from log_data will be copied here
2. songs_info: All data from from song_data will be copied here

### Load
Once all the data is in our staging tables, we will move it into our 5 tables in Star Schema:
1. songplays
2. users
3. songs
4. artists
5. time


## Running the project
In order to run the project from the scratch run the following commands from your terminal:
```
python3.6 create_tables.py
```

then
```
python3.6 etl.py
```

In case, you have already created your tables and just want to add new data use only the second command.
