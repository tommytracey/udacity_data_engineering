import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T


def parse_config():
    '''Parses data from configuration file'''

    # parse config file
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # set environment variables
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

    # return data locations
    return (config['DATA']['INPUT_DIR'], config['DATA']['OUTPUT_DIR'])


def create_spark_session():
    '''Creates a new Spark session or gets an existing one'''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_dir, output_data_dir):
    """ ETL process for Sparkify song data

    This function:
    1. Extracts input data from song files stored in S3
    2. Performs data transformations and creates required tables
    3. Stores the processed data in S3

    Parameters:
        spark       : Spark session
        input_data_dir  : files containing song and artist metadata (JSON format)
        output_data_dir : song and artist dimension tables (parquet format)

    """
    # get filepath to song data file
    ### TODO - update path to 'song_data/*/*/*/*.json'
    song_data = os.path.join(input_data_dir, 'song-data/A/A/A/*.json')

    # define schema
    song_schema = StructType([
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_name', StringType()),
        StructField('duration', DoubleType()),
        StructField('num_songs', IntegerType()),
        StructField('title', StringType()),
        StructField('year', IntegerType()),
    ])

    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    song_fields = ['song_id','title','artist_id','year','duration']
    songs_table = df.select(song_fields).dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_out_path = str(output_data_dir + 'songs/' + 'songs.parquet')
    songs_table.write.partitionBy('year', 'artist_id').parquet(songs_out_path)

    # extract columns to create artists table
    artist_fields = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artists_table = df.select(artist_fields) \
                    .withColumnRenamed('artist_name','artist') \
                    .withColumnRenamed('artist_location','location') \
                    .withColumnRenamed('artist_latitude','latitude') \
                    .withColumnRenamed('artist_longitude','longitude') \
                    .dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_out_path = str(output_data_dir + 'artists/' + 'artists.parquet')
    artists_table.write.parquet(artists_out_path)


def process_log_data(spark, input_data_dir, output_data_dir):
    """ ETL process for Sparkify log data

    This function:
    1. Extracts input data from log files stored in S3
    2. Performs data transformations and creates required tables
    3. Stores the output data in S3

    Parameters:
        spark           : Spark session
        input_data_dir  : files containing necessary metadata (JSON format)
        output_data_dir : user and time dimension tables + songplays fact table (parquet format)

    """
    # get filepath to log data file
    log_data =

    # read log data file
    df =

    # filter by actions for song plays
    df =

    # extract columns for users table
    users_table =

    # write users table to parquet files
    users_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df =

    # create datetime column from original timestamp column
    get_datetime = udf()
    df =

    # extract columns to create time table
    time_table =

    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df =

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    '''Executes entire ETL process'''

    # parse configuration data
    input_data_dir, output_data_dir = parse_config()

    # initiate Spark session
    spark = create_spark_session()

    # run ETL process
    process_song_data(spark, input_data_dir, output_data_dir)
    process_log_data(spark, input_data_dir, output_data_dir)


if __name__ == "__main__":
    main()
