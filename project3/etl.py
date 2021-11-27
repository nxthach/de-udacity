import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Load raw data (log_data, song_data) from S3 
    and insert into staging_events and staging_songs tables.
    """    

    print("Start loading data from S3 to staging tables ...")
    
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("Error in query : " + query)
            print(e)
            
    print("End loading data from S3 to staging tables!!!")


def insert_tables(cur, conn):
    
    """
    Insert data from staging tables (staging_events and staging_songs)
    into star schema tables:
        * songplays
        * users
        * songs
        * artists
        * time
    """
    
    print("Start inserting data from staging...")
    
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("Error in query : " + query)
            print(e)

    print("End inserting data from staging!!!")

def main():

    """
    Connect to AWS Redshift, and
        * Load data to staging_tables from JSON files (song_data, log_data in S3)
        * Insert data from staging_tables to tables(songplays, users, songs, artists, time)
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_ENDPOINT    = config.get('DWH', 'DWH_ENDPOINT')
    DWH_DB          = config.get('DWH', 'DWH_DB')
    DWH_DB_USER     = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
    DWH_PORT        = config.get('DWH', 'DWH_PORT')

    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                        .format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()