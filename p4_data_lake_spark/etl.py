import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


def parse_config():
    '''Parses data from configuration file'''

    # parse config file
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # set environment variables
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

    # return data bucket locations
    return (config['INPUT_DATA'], config['OUTPUT_DATA'])


def create_spark_session():
    '''Creates a new Spark session or gets an existing one'''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ ETL process for Sparkify song data

    This function:
    1. Extracts input data from song files stored in S3
    2. Performs data transformations and creates required tables
    3. Stores the processed data in S3

    Parameters:
        spark       : Spark session
        input_data  : files containing song and artist metadata (JSON format)
        output_data : song and artist dimension tables (parquet format)

    """
    # get filepath to song data file
    song_data =

    # read song data file
    df =

    # extract columns to create songs table
    songs_table =

    # write songs table to parquet files partitioned by year and artist
    songs_table

    # extract columns to create artists table
    artists_table =

    # write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    """ ETL process for Sparkify log data

    This function:
    1. Extracts input data from log files stored in S3
    2. Performs data transformations and creates required tables
    3. Stores the output data in S3

    Parameters:
        spark       : Spark session
        input_data  : files containing necessary metadata (JSON format)
        output_data : user and time dimension tables + songplays fact table (parquet format)

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
    input_data, output_data = parse_config()

    # initiate Spark session
    spark = create_spark_session()

    # run ETL process
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
