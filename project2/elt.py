# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cql_queries import *

def process_event_file(session, filepath):
    """
    Process data from csv file and insert to tables:
    song_session, artist_session, and song_user.

    """
    
    print('Starting to process input CSV data file...')
    num_lines = 0


    ### 
    # Process song_session table.
    #
    # sessionId = line[8]
    # itemInSession = line[3]
    # artist = line[0]
    # song = line [9]
    # length = line [5]
    ### 
    with open(filepath, encoding = 'utf8') as f:
        num_lines = 0
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(song_session_insert, 
                            (int(line[8]), 
                             int(line[3]), 
                             line[0],
                             line[9], 
                             float(line[5])))
            num_lines += 1

        # print total lines was process
        print('Processed lines: ', num_lines)
        print('Data inserted successfully into song_session table.')


    ### 
    # Process artist_session table
    #
    # userId = line[10]
    # sessionId = line[8]
    # artist = line[0]
    # song = line[9]
    # itemInSession = line[3]
    # firstName = line[1]
    # lastName = line[4]
    ###

    with open(filepath, encoding = 'utf8') as f:
        num_lines = 0
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(artist_session_insert, 
                            (int(line[10]), 
                             int(line[8]), 
                             line[0], 
                             line[9], 
                             int(line[3]), 
                             line[1], 
                             line[4]))
            num_lines += 1

        # print total lines was process
        print('Processed lines: ', num_lines)
        print('Data inserted successfully into artist_in_session table.')


    ### 
    # Process song_user table
    #
    # song = line[9]
    # userId = line[10]
    # firstName = line[1]
    # lastName = line[4]
    ### 
    
    with open(filepath, encoding = 'utf8') as f:
        num_lines = 0
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(song_user_insert, 
                            (line[9], 
                            int(line[10]), 
                            line[1], 
                            line[4]))
            num_lines += 1
            
        # print total lines was process
        print('Processed lines: ', num_lines)
        print('Data inserted successfully into user_and_song table.')

def process_data(session, filepath, target_file, func):
    """
    Walk through the whole file in the input data directory,
    and combine input data into a new CSV file,
    then call function process to handle the combined data.

    """    
    
    ###
    # Create a for loop to create a list of files and collect each filepath
    ###
    file_path_list = []
    for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root,'*'))
            for f in files :
                file_path_list.append(os.path.abspath(f))
    
    
    ###
    # Build list of data from list of file we got [file_path_list]
    ###
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:

        # reading csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line)

    
    ###
    # Creating event_datafile_new.csv file from list of data 
    ###
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(target_file, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender',
                         'itemInSession','lastName','length',
                         'level','location','sessionId',
                         'song', 'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], 
                             row[4], row[5], row[6], 
                             row[7], row[8], row[12], 
                             row[13], row[16]))
    
        
    ###
    # Read data from event_datafile_new.csv file
    # and insert to table: song_session, artist_session, song_user
    ###
    func(session, target_file)
    print('All data was processed.')

def main():
    """
    Connect to DB and process all data from (./event_data/*.csv).

    """

    # Get current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'
    target_file = 'event_datafile_new.csv'

    # Make a connection to Cassandra cluster
    from cassandra.cluster import Cluster
    try:
        # Connect to a local Cassandra cluster
        cluster = Cluster(['127.0.0.1'])
        # Set a session execute queries.
        session = cluster.connect()
    except Exception as e:
        print(e)

    try:
        # Set keyspace (sparkifydb) for Cassandra session.
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    # Process data 
    process_data(session, filepath, target_file, func=process_event_file)

    # Close the session and DB connection.
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
