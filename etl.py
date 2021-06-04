import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    accepts the cursor and a filepath as its arguments
    inserts data from the song_file into the song and artist tables
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_df = df[['song_id', 'title', 'artist_id','year','duration' ]]
    song_data = song_df.values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_df = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data = artist_df.values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    accepts the cursor and a filepath as arguments
    inserts data into user, time and songplays tables
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page']=='NextSong']

    # convert timestamp column to datetime
    df.loc[:, 'ts'] = pd.to_datetime( df['ts'], unit='ms')
    
    # insert time data records
    hour = list(df['ts'].dt.hour)
    day = list(df['ts'].dt.day)
    month = list(df['ts'].dt.month)
    year = list(df['ts'].dt.year)
    weekday = list(df['ts'].dt.weekday)
    week = list(df['ts'].dt.week)
    ts = list(df['ts'])
    
    
    time_data = []

    for i in range (df['ts'].size - 1):
        time_list = [ts[i],hour[i],day[i],week[i],month[i],year[i],weekday[i]]
        time_data.append(time_list)

    column_labels = ('start_time','hour','day','week','month','year','weekday')

    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song,row.artist,row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    accepts cursor and connection objects,file path and the function to be used to process the files in that path
    gets all files and stores them in a list
    iterates over all files and the func is applies to them.
    
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