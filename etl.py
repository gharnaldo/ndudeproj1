import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Extracts the informartion from each file found in song_data folder and insert it into the songs and artists tables in the DB. 
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_columns = ['song_id','title','artist_id','year','duration']
    song_data = df[song_columns].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_columns = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data = df[artist_columns].values.tolist()[0] 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extracts the informartion from each file found in log_data folder and insert it into the time, users and songplays tables in the DB. 
    """    
    # open log file
    df = pd.read_json(filepath, lines=True) 

    # filter by NextSong action
    df = pd.DataFrame(df)
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
    time_data = ()
    #timestamp, hour, day, week of year, month, year, and weekday
    timestamp = df['ts'].values.tolist()
    hour = df['datetime'].dt.hour.values.tolist()
    day = df['datetime'].dt.day.values.tolist()
    weekofyear = df['datetime'].dt.isocalendar().week.values.tolist()
    month = df['datetime'].dt.month.values.tolist()
    year = df['datetime'].dt.year.values.tolist()
    weekday = df['datetime'].dt.weekday.values.tolist()
    #t = 
    
    # insert time data records
    time_data = zip(timestamp,hour,day,weekofyear,month,year,weekday)
    column_labels = ('ts', 'hour', 'day', 'weekofyear','month','year','weekday')
    time_df = pd.DataFrame(list(time_data),columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns = ['userId','firstName','lastName','gender','level']
    user_df = df[user_columns] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.datetime,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Reads the specified directory and calls the corresponding function to process it 
    """     
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