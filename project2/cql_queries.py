# DROP TABLES
#
song_session_table_drop = "DROP TABLE IF EXISTS song_session"
artist_session_table_drop = "DROP TABLE IF EXISTS artist_session"
song_user_table_drop = "DROP TABLE IF EXISTS song_user"


# CREATE TABLES

##
## song_session
##
song_session_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_session (
        session_id int, 
        item_in_session int, 
        artist text, 
        song text, 
        length float, 
        PRIMARY KEY(session_id, item_in_session))
""")

##
## artist_session
##
artist_session_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_session (
        user_id int, 
        session_id int, 
        artist text, 
        song text, 
        item_in_session int, 
        first_name text, 
        last_name text, 
        PRIMARY KEY((user_id, session_id), item_in_session)
    )
""")


##
## song_user
##
song_user_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_user (
        song text, 
        user_id int, 
        first_name text, 
        last_name text, 
        PRIMARY KEY(song, user_id)
    )
""")

# INSERT QUERIES


##
## song_session
##
song_session_insert = ("""
    INSERT INTO song_session (session_id, item_in_session, artist, song, length)
    VALUES (%s, %s, %s, %s, %s)
""")


##
## artist_session
##
artist_session_insert = ("""
    INSERT INTO artist_session ( user_id, session_id, artist, song, item_in_session, first_name, last_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

song_user_insert = ("""
    INSERT INTO song_user ( song, user_id, first_name, last_name)
    VALUES (%s, %s, %s, %s)
""")

# SELECT QUERIES

##
## Query-1: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
##
song_session_select = ("""
    SELECT artist, song, length
    FROM song_session
    WHERE session_id = (%s) AND item_in_session = (%s)
""")


##
## Query-2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
##
song_session_select = ("""
    SELECT artist, song, first_name, last_name
    FROM song_session \
    WHERE session_id = (%s) AND item_in_session = (%s)
""")


##
## Query-3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
## 
song_user_select = ("""
    SELECT first_name, last_name FROM song_user WHERE song = (%s)
""")


#QUERY LISTS
create_table_queries = [song_session_table_create, artist_session_table_create, song_user_table_create]
drop_table_queries = [song_session_table_drop, artist_session_table_drop, song_user_table_drop]
