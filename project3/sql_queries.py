import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id BIGINT IDENTITY(0,1) NOT NULL,
        artist text NULL,
        auth text NULL,
        firstName text NULL,
        gender text NULL,
        itemInSession text NULL,
        lastName text NULL,
        length text NULL,
        level text NULL,
        location text NULL,
        method text NULL,
        page text NULL,
        registration text NULL,
        sessionId INTEGER NOT NULL SORTKEY DISTKEY,
        song text NULL,
        status INTEGER NULL,
        ts BIGINT NOT NULL,
        userAgent text NULL,
        userId text NULL
    );

""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id text NOT NULL SORTKEY DISTKEY,
        artist_latitude text NULL,
        artist_location text NULL,
        artist_longitude text NULL,
        artist_name text NULL,
        duration DECIMAL(9) NULL,
        num_songs INTEGER NULL,
        song_id text NOT NULL,
        title text NULL,
        year INTEGER NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY, 
        start_time timestamp NOT NULL, 
        user_id int, 
        level text, 
        song_id text, 
        artist_id text, 
        session_id int, 
        location text, 
        user_agent text);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY, 
        first_name text, 
        last_name text, 
        gender text, 
        level text);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text PRIMARY KEY, 
        title text, 
        artist_id text NOT NULL, 
        year int, 
        duration numeric);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id text PRIMARY KEY, 
        name text, 
        location text, 
        latitude numeric, 
        longitude numeric);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday text);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT se.ts   AS start_time,
            se.userId        AS user_id,
            se.level         AS level,
            ss.song_id       AS song_id,
            ss.artist_id     AS artist_id,
            se.sessionId     AS session_id,
            se.location      AS location,
            se.userAgent     AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
    
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.userId AS user_id,
           se.firstName       AS first_name,
           se.lastName        AS last_name,
           se.gender          AS gender,
           se.level           AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")


song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id AS song_id,
           ss.title            AS title,
           ss.artist_id        AS artist_id,
           ss.year             AS year,
           ss.duration         AS duration
    FROM staging_songs AS ss;
""")


artist_table_insert = ("""
    INSERT INTO artists ( artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id AS artist_id,
           ss.artist_name        AS name,
           ss.artist_location    AS location,
           ss.artist_latitude    AS latitude,
           ss.artist_longitude   AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time ( start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT se.ts                   AS start_time,
           EXTRACT(hour FROM start_time)    AS hour,
           EXTRACT(day FROM start_time)     AS day,
           EXTRACT(week FROM start_time)    AS week,
           EXTRACT(month FROM start_time)   AS month,
           EXTRACT(year FROM start_time)    AS year,
           EXTRACT(week FROM start_time)    AS weekday
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
