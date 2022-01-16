class SqlQueries:
    songplay_table_select = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_select = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_select = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_select = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_select = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    
    ###
    ### SQL for check quality
    ###
    
    ### 1. songplays
    
    # check null
    songplays_check_nulls = ("""
        SELECT COUNT(*)
        FROM songplays
        WHERE userid IS NULL
              OR songid IS NULL
              OR artistid IS NULL
              OR sessionid IS NULL;
    """)
    
    # count number of data was saved
    songplays_check_count = ("""
        SELECT COUNT(*)
        FROM songplays;
    """)
    
    
    ### 2. users ###
    
    # check null
    users_check_nulls = ("""
        SELECT COUNT(*)
        FROM users
        WHERE userid IS NULL;
    """)
    
    # count number of data was saved
    users_check_count = ("""
        SELECT COUNT(*)
        FROM users;
    """)
    
    ### 3. songs ###
    
    # check null
    songs_check_nulls = ("""
        SELECT COUNT(*)
        FROM songs
        WHERE songid IS NULL;
    """)
    
    # count number of data was saved
    songs_check_count = ("""
        SELECT COUNT(*)
        FROM songs;
    """)
    
     ### 4. artists ###
    
    # check null
    artists_check_nulls = ("""
        SELECT COUNT(*)
        FROM artists
        WHERE artistid IS NULL;
    """)
    
    # count number of data was saved
    artists_check_count = ("""
        SELECT COUNT(*)
        FROM artists;
    """)
    
    ### 5. time ###
    
    # check null
    time_check_nulls = ("""
        SELECT COUNT(*)
        FROM time
        WHERE start_time IS NULL;
    """)
    
    # count number of data was saved
    time_check_count = ("""
        SELECT COUNT(*)
        FROM time;
    """)
    
    
    
    
    