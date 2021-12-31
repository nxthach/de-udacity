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




def create_spark_session():
    print("Start create SparkSession.")
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

    print("End create SparkSession.")


def process_song_data(spark, input_data, output_data):
    
    #
    #=== READ SONG DATA FILE FROM S3 ===
    #

    print("Start processing song_data JSON files...")
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    #song_data = input_data + "song_data/*/*/*/*.json"
    
    
    # read song data file
    songdata_df = spark.read.json(song_data)
    songdata_df.printSchema()

    #
    #=== CREATE TABLE AND WRITE TO S3 ===
    #

    # create songdata_view
    print("Create view songdata_view")
    songdata_df.createOrReplaceTempView("songdata_view")

    #
    # 1. songs_table
    #
    
    # extract columns to create songs table
    print("Start create songs_table.")

    songs_table = spark.sql("""
        SELECT  song_id,
                title, 
                artist_id, 
                year, 
                duration
        FROM songdata_view
    """)

    print("End create songs_table.")
    
    # write songs table to parquet files partitioned by year and artist
    print("Start write songs_table.")

    songs_table_out_path = output_data + "songs_table.parquet"
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(songs_table_out_path)

    print("End write songs_table.")

    #
    # 2. artists_table
    #
    # extract columns to create artists table
    
    print("Start create artists_table.")

    artists_table = spark.sql("""
        SELECT  artist_id        AS artist_id, 
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude, 
                artist_longitude AS longitude 
        FROM songdata_view
    """)

    print("End create artists_table.")
    
    # write artists table to parquet files
    print("Start write artists_table.")

    artists_table_out_path = output_data + "artists_table.parquet"
    artists_table.write.mode("overwrite").parquet(artists_table_out_path)

    print("End write artists_table.")

    #
    print("End processing song_data JSON files!")


def process_log_data(spark, input_data, output_data):

    #
    #=== READ LOG DATA FILE FROM S3 ===
    #

    print("Start processing log_data JSON files...")

    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"
    # log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    logdata_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    logdata_nextsong_df = logdata_df.filter(logdata_df.page == 'NextSong')
    logdata_nextsong_df.printSchema()

    #
    #=== CREATE TABLE AND WRITE TO S3 ===
    #

    # create view logdata_nextsong_view
    print("Create view logdata_nextsong_view.")
    logdata_nextsong_df.createOrReplaceTempView("logdata_nextsong_view")

    #
    # 1. users_table
    #
    
    # extract columns for users table
    print("Start create users_table.")

    users_table = spark.sql("""
        SELECT DISTINCT userId AS user_id, 
            firstName       AS first_name, 
            lastName        AS last_name, 
            gender, 
            level
        FROM logdata_nextsong_view
    """)

    print("End create users_table.")
    
    # write users table to parquet files
    print("Start write users_table.")

    users_table_out_path = output_data + "users_table.parquet"
    users_table.write.mode("overwrite").parquet(users_table_out_path)

    print("End write users_table.")

    #
    # 2. time_table
    #
    
    print("Start create time_table...")

    # Create function to convert timestamp
    @udf(T.TimestampType())
    def to_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)
    
    # Add [startTime] column to logdata_nextsong_df
    print("Add [startTime] column to logdata_nextsong_df.")
    logdata_nextsong_df = logdata_nextsong_df.withColumn("startTime", to_timestamp("ts"))

    # Create view logdata_nextsong_view
    print("Create view logdata_nextsong_view.")
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

    print("End create time_table!")
    
    # write time table to parquet files partitioned by year and month
    print("Start write time_table.")

    time_table_out_path = output_data + "time_table.parquet"
    #time_table.write.partitionBy("year", "month").mode("overwrite").parquet(time_table_out_path)
    
    #
    time_table2 = time_table.limit(5)
    time_table2.write.partitionBy("year", "month").mode("overwrite").parquet(time_table_out_path)

    print("End write time_table.")

    #
    # 3. songplays_table
    #

    print("Start create time_table...")

    # read in song data to use for songplays table
    print("Start read song_data.")

    song_data = input_data + "song_data/A/A/A/*.json"
    #song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    songdata_df = spark.read.json(song_data)

    print("End read song_data.")    

    # create new dataframe was joined by song data and log data
    print("Create new dataframe (log_song_data_join_df) was joined by song data and log data.")
    log_song_data_join_df = logdata_nextsong_df.join(songdata_df, \
                            (logdata_nextsong_df.artist == songdata_df.artist_name) \
                          & (logdata_nextsong_df.song == songdata_df.title) \
                          & (logdata_nextsong_df.length == songdata_df.duration) \
                          )

    # add [songplay_id] as auto increase
    print("Add [songplay_id] as auto increase.")
    log_song_data_join_df = log_song_data_join_df.withColumn("songplay_id", F.monotonically_increasing_id())


    # Create view log_song_data_join_view
    print("Create view log_song_data_join_view.")
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

    print("End create songplays_table!")

    # write songplays table to parquet files partitioned by year and month
    print("Start write songplays_table.")

    songplays_out_path = output_data + "songplays_table.parquet"
    songplays_table.write.mode("overwrite").parquet(songplays_out_path)

    print("End write songplays_table.")
    
    #
    print("End processing log_data JSON files!")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"    
    output_data = config['AWS']['OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
