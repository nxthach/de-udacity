import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ENDPOINT    = config.get('DWH', 'DWH_ENDPOINT')
DWH_DB          = config.get('DWH', 'DWH_DB')
DWH_DB_USER     = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT        = config.get('DWH', 'DWH_PORT')


def drop_tables(cur, conn):
    
    """
    Drop tables if existed
    """
    
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("Error in query : " + query)
            print(e)

    print("All tables was dropped successfully.")

def create_tables(cur, conn):
    
    """
    Create the tables
    * staging_events
    * staging_songs
    * songplays
    * users
    * songs
    * artists
    * time
    
    """
    
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("Error in query : " + query)
            print(e)

    print("All tables created successfully.")

def main():
    
    """
    Connect to AWS Redshift, create new DB,
    drop any existing tables, create new tables.
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()