import cassandra
from cql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Connect to Apache Cassandra, drop any existing sparkifydb and create a new one.

    """
    
    # Connect to Apache Cassandra
    from cassandra.cluster import Cluster
    try:
        cluster = Cluster()
        session = cluster.connect()

    except Exception as e:
        print(e)


    # Drop sparkifydb keyspace if existing
    try:
        session.execute("DROP KEYSPACE IF EXISTS sparkifydb ")
    except Exception as e:
        print(e)


    # Create sparkifydb keyspace
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sparkifydb
            WITH REPLICATION =
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
    except Exception as e:
        print(e)


    # Set keyspace (sparkifydb) for Cassandra session.
    try:
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    return cluster, session


def drop_tables(session):
    """
    Drop any existing tables from sparkifydb.
    
    """
    
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print("Error: Issue dropping table.")
            print(e)

    print("Drop tables successfully.)



def create_tables(session):
    """
    Create tables
    
    """

    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print("Error: Issue creating table.")
            print(e)

    print("Create tables successfully.")

def main():
    """
    Connect to Apache Cassandra, create new keyspace (sparkifydb),
    drop any existing tables, create new tables.

    

    Output:
    * New sparkifydb is created, old tables are droppped if existing,
      and new tables (song_session, artist_session, song_user) are created.

    """

    # Connect to Apache Cassandra, create new keyspace.
    cluster, session = create_database()

    # Drop old tables from keyspace.
    drop_tables(session)

    # Create new tables to keyspace.
    create_tables(session)

    # Close the session and DB connection.
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
