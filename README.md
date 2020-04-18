# Data Lake Design Using AWS S3, AWS EMR & PySpark
## Quickstart:
1. Update the 'dwh.cfg' file:
- Add in your AWS KEY
- Add in your AWS SECRET
- Add in the other fields for INPUT_LOG_DATA, INPUT_SONG_DATA and OUTPUT_DATA

2. Run the following command in the CLI:
- 'python etl.py' to extract, transform and load the data from AWS S3 via AWS EMR cluster and saved back into S3.

### Requirements
- python3
- pyspark

## Overview:
In this project, Sparkify, an online music app provider, requires a distributed database specifically for it's data analytics. This project will extract the data (JSON) from an S3 bucket, process the data in a Spark Cluster and then save back to AWS S3. 
Tecnologies used includes Python, AWS S3 & AWS EMR.

## Data Source:
Current data is collected and stored in JSON format. Current data is located in AWS S3 buckets:
- data about the songs ({"artist_id":{"0":"AR8IEZO1187B99055E"},"artist_latitude":{"0":null},"artist_location":{"0":""},"artist_longitude":{"0":null},"artist_name":{"0":"Marc Shaiman"},"duration":{"0":149.86404},"num_songs":{"0":1},"song_id":{"0":"SOINLJW12A8C13314C"},"title":{"0":"City Slickers"},"year":{"0":2008}})
- event data collected ({"artist":"Sydney Youngblood","auth":"Logged In","firstName":"Jacob","gender":"M","itemInSession":53,"lastName":"Klein","length":238.07955,"level":"paid","location":"Tampa-St. Petersburg-Clearwater, FL","method":"PUT","page":"NextSong","registration":1540558108796.0,"sessionId":954,"song":"Ain\'t No Sunshine","status":200,"ts":1543449657796,"userAgent":"\\"Mozilla\\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\\/537.78.2 (KHTML, like Gecko) Version\\/7.0.6 Safari\\/537.78.2\\"","userId":"73"})

## Analytics requirements:
Sparkify requires analysis on its song plays. 

## Model suggestion:
The current star schema has been suggested, which would allow for analysis on the song plays and would allow for further segmentation and filtering based on the dimensions:
### Fact Tables:
- song_plays (event data of each song play)
### Dimension Tables:
- users (what are the demographics of their users)
- artists (which artists are more popular)
- songs (which songs are more popular)
- time (how frequently are users interacting with their app)

![SparkifyDB schema as ER Diagram](./img/database_model.png)