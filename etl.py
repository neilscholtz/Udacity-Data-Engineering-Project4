import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_sd, output_data):
    """
    Extracts, transforms and loads song data to output file
    
    Args:
        - spark: spark object
        - input_data_sd: AWS S3 input file location for song_data
        - output_data: AWS S3 output file location
        
    Returns:
        - None
    """
    # get filepath to song data file
    song_data = input_data_sd

    # read song data file
    print ('Reading in song data from {}'.format(song_data))
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    print ('Extracting song data...')
    songs_table = df.select('song_id','title','artist_id','year','duration')\
                    .dropDuplicates()
    print ('{} song entries processed.'.format(songs_table.count()))

    # write songs table to parquet files partitioned by year and artist
    song_table_path = output_data + 'songs/songs.parquet'
    print ('Writing song data to {}...'.format(song_table_path))
    songs_table.write.mode('overwrite')\
                    .parquet(song_table_path)
    print ('File {} saved.'.format(song_table_path))

    # extract columns to create artists table
    print ('Extracting artists data...')
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude')\
                        .dropDuplicates(['artist_id'])
    print ('{} artist entries processed.'.format(artists_table.count()))

    # write artists table to parquet files
    artist_table_path = output_data + 'artists/artists.parquet'
    print ('Writing artist data to {}...'.format(artist_table_path))
    artists_table.write.mode('overwrite')\
                        .parquet(artist_table_path)
    print ('File {} saved.'.format(artist_table_path))


def process_log_data(spark, input_data_ld, input_data_sd, output_data):
    """
    Extracts, transforms and loads log data to output file
    
    Args:
        - spark: spark object
        - input_data_ld: AWS S3 input file location for log_data
        - input_data_sd: AWS S3 input file location for song_data
        - output_data: AWS S3 output file location
        
    Returns:
        - None
    """
    # get filepath to log data file
    log_data = input_data_ld

    # read log data file
    print ('Reading in log data from {}'.format(log_data))
    df = spark.read.json(log_data)

    # filter by actions for song plays
    print ('Extracting "NextSong" only entries...')
    df = df.filter(df.page=='NextSong')

    # extract columns for users table
    print ('Extracting user data...')
    users_table = df.select('userId','firstName','lastName','gender','level').dropDuplicates(['userId'])
    print ('{} user entries processed.'.format(user_table.count()))

    # write users table to parquet files
    user_table_path = output_data + 'users/users.parquet'
    print ('Writing user data to {}...'.format(user_table_path))
    users_table.write.mode('overwrite')\
                    .parquet(user_table_path)
    print ('File {} saved.'.format(user_table_format))

    # create timestamp column from original timestamp column
    print ('Extracting time data...')
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000), 
                        types.TimestampType())
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000).date(), 
                        types.DateType())
    df_datetime = df.withColumn('datetime', get_datetime('ts'))

    # extract columns to create time table
    time_table = df_datetime.selectExpr(['cast(datetime as date) start_time',
                                         'hour(timestamp) hour',
                                         'day(timestamp) day',
                                         'weekofyear(timestamp) week',
                                         'month(timestamp) month',
                                         'year(timestamp) year',
                                         'dayofweek(timestamp) weekday'])\
                            .dropDuplicates('start_time')
    print ('{} time entries processed.'.format(time_table.count()))

    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + 'time/time.parquet'
    print ('Writing time data to {}...'.format(time_table_path))
    time_table.write.mode('overwrite')\
                    .partitionBy('year', 'month')\
                    .parquet(time_table_path)
    print ('File {} saved.'.format(time_table_path))
    
    
    # read in song data to use for songplays table
    print ('Reading in song data from {}'.format(input_data_sd))
    log_data_df = df
    song_data_df = spark.read.json(input_data_sd)
    log_song_df = log_data_df.join(song_data_df, log_data_df.song == song_data_df.title, 'left')
    
    
    # extract columns from joined song and log datasets to create songplays table 
    print ('Extracting songplay data...')
    songplays_table = log_song_df.select([F.monotonically_increasing_id().alias('songplay_id'),
                                          'timestamp', 
                                          F.year('timestamp').alias('year'),
                                          F.month('timestamp').alias('month'),
                                          'userId',
                                          'level',
                                          'song_id',
                                          'artist_id',
                                          'sessionId',
                                          'location',
                                          'userAgent'])
    print ('{} songplay entries processed.'.format(songplays_table.count()))

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + 'songplays/songplays.parquet'
    print ('Writing songplays data to {}...'.format(songplays_table_path))
    songplays_table.write.mode('overwrite')\
                        .partitionBy('year','month') \
                        .parquet(songplays_table_path)
    print ('File {} saved.'.format(songplays_table_path))

def main():
    """
    - Creates spark object
    - Extracts, transorms and loads song_data from and to S3 bucket
    - Extracts, transorms and loads log_data from and to S3 bucket
    """
    spark = create_spark_session()
    input_data_sd = config['DATA']['INPUT_SONG_DATA']
    input_data_ld = config['DATA']['INPUT_LOG_DATA']
    output_data = config['DATA']['OUTPUT_DATA']
    
    process_song_data(spark, input_data_sd, output_data)    
    process_log_data(spark, input_data_sd, input_data_ld, output_data)


if __name__ == "__main__":
    main()