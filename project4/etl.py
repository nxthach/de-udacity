from pyspark.sql import SparkSession
import os
import configparser
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql import types as T


config = configparser.ConfigParser()
#config.read('dl.cfg')
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

OUTPUT_DATA = config['AWS']['OUTPUT_DATA']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/A/A/A/*.json"
    
    # read song data file
    songdata_df = spark.read.json(song_data)
    songdata_df.printSchema()

    # create view
    songdata_df.createOrReplaceTempView("songdata_view")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songdata_view
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_out_path = output_data + "songs_table.parquet"
    #
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(songs_table_out_path)

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id        AS artist_id, 
            artist_name      AS name,
            artist_location  AS location,
            artist_latitude  AS latitude, 
            artist_longitude AS longitude 
        FROM songdata_view
    """)
    
    # write artists table to parquet files
    artists_table_out_path = output_data + "artists_table.parquet"
    #
    artists_table.write.mode("overwrite").parquet(artists_table_out_path)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/2018/11/*.json"

    # read log data file
    logdata_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    logdata_nextsong_df = logdata_df.filter(logdata_df.page == 'NextSong')

    # create view
    logdata_nextsong_df.createOrReplaceTempView("logdata_nextsong_view")

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT userId AS user_id, 
            firstName       AS first_name, 
            lastName        AS last_name, 
            gender, 
            level
        FROM logdata_nextsong_view
    """)
    
    # write users table to parquet files
    users_table_out_path = output_data + "users_table.parquet"
    #
    users_table.write.mode("overwrite").parquet(users_table_out_path)

    # Create function to convert timestamp
    @udf(T.TimestampType())
    def to_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)
    
    # Add startTime column to logdata_nextsong_df
    logdata_nextsong_df = logdata_nextsong_df.withColumn("startTime", to_timestamp("ts"))

    
    # Create view 
    logdata_nextsong_df.createOrReplaceTempView("logdata_nextsong_view")

    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT startTime AS start_time, 
                        hour(startTime) AS hour,
                        day(startTime)  AS day, 
                        weekofyear(startTime) AS week,
                        month(startTime) AS month,
                        year(startTime) AS year,
                        dayofweek(startTime) AS weekday
        FROM logdata_nextsong_view
        ORDER BY start_time
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table_out_path = output_data + "time_table.parquet"

    #
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(time_table_out_path)

    # read in song data to use for songplays table
    song_data = "s3a://udacity-dend/song_data/A/A/A/*.json"
    
    # read song data file
    songdata_df = spark.read.json(song_data)

    # Create new dataframe was joined by song data and log data
    log_song_data_join_df = logdata_nextsong_df.join(songdata_df, \
                            (logdata_nextsong_df.artist == songdata_df.artist_name) \
                          & (logdata_nextsong_df.song == songdata_df.title) \
                          & (logdata_nextsong_df.length == songdata_df.duration) \
                  )

    # Add songplay_id as auto increase
    log_song_data_join_df = log_song_data_join_df.withColumn("songplay_id", F.monotonically_increasing_id())


    # Create view
    log_song_data_join_df.createOrReplaceTempView("log_song_data_join_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT songplay_id AS songplay_id, 
            startTime   AS start_time, 
            userId      AS user_id, 
            level       AS level,
            song_id     AS song_id,
            artist_id   AS artist_id,
            sessionId   AS session_id,
            location    AS location,
            userAgent   AS user_agent
        FROM log_song_data_join_view
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_out_path = output_data + "songplays_table.parquet"

    #
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(songplays_out_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
