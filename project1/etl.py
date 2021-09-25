import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    
    # -------------------------------------
    # insert date to [song] table
    
    song_data_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data_df.values[0]

    cur.execute(song_table_insert, song_data)
    
    
    # -------------------------------------
    # insert date to [artist] table
    
    artist_data_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data_df.values[0] 

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    nextsong_df = df[df['page']=='NextSong']
    nextsong_df.head()

    
    # -------------------------------------
    # insert date to [time] table
        
    # convert timestamp column to datetime
    time_df = nextsong_df.loc[: , ['ts']]
    
    # insert time data records
    #Convert the ts timestamp column to datetime
    time_df['ts'] = pd.to_datetime(time_df['ts'], unit='ms')

    #extract the hour, day, week, month, year, weekday
    time_df['hour'] = time_df['ts'].apply(lambda x: x.hour)
    time_df['day'] = time_df['ts'].apply(lambda x: x.day)
    time_df['week'] = time_df['ts'].apply(lambda x: x.week)
    time_df['month'] = time_df['ts'].apply(lambda x: x.month)
    time_df['year'] = time_df['ts'].apply(lambda x: x.year)
    time_df['weekday'] = time_df['ts'].apply(lambda x: x.dayofweek)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    
    # -------------------------------------
    # insert date to [user] table
    
    # Create user dataframe
    # And rename userId -> user_id, firstName -> first_name, lastName -> last_name
    user_df = (
        df
        .loc[: , ["userId", "firstName", "lastName", "gender", "level"]]
        .rename({"userId": "user_id",
                "firstName": "first_name",
                "lastName": "last_name"},
               axis="columns")

    )

    # Filter the recodes with user_id not empty
    user_df=user_df.query("user_id != ''")
    

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

        
    # -------------------------------------
    # insert date to [songplay] table    

    # Convert value of [ts] column from string (as timestamp) to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # Filter the recodes with userId not empty
    df=df.query("userId != ''")

    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts, row.userId, row.level,
            songid, artistid, 
            row.sessionId, row.location, row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()