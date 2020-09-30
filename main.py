import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "zw4o439fo8y1q0j08rg1ap4eo"
TOKEN = "BQCOq2SAkH1WT5YZ-n4mXB4l-Nk4n8aXfzhK7ongWBwCgbkWKzlu_mb5pdHNvzO1IC2hl6SHIVcMuEzO2CfxA4fgRqcW0yH5ZBnQL0lh6TSRZhrriKymMbqDOU42SLSWuB83y_sOx8exQHLYwa7Y2p5ApgCrBR91Ki20uUee"



def check_if_data_valid(df: pd.DataFrame):
    #Check if the DF is empty
    if df.empty:
        print("No songs listened to. Execution finished")
        return False
    #check if song is unique
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key is a duplicate")

    #Check for null values
    if df.isnull().values.any():
        raise Exception("Null value found")

    #Check if all timestamps are from correct date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("Timestamp is not from yesterday")




if __name__ == "__main__":

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {}".format(TOKEN)

    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = 1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp),headers=headers)


    if 'json' in r.headers.get('Content-Type'):
        data = r.json()
    else:
        print('Response content is not in JSON format.')
        data = 'spam'



    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["artist_name","song_name","played_at","timestamp"])

    print(song_df)

    if check_if_data_valid(song_df):
        print("Data is valid, continiue to Load Stage")
    else:
        print("Invalid data")


    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY(played_at)
    )
    """
    cursor.execute(query)
    print("Database ceated successfuly")

    try:
        song_df.to_sql("my_played_tracks",engine,index=False,if_exists="append")
    except:
        print("Data already exists")

    conn.close()
    print("Connection closed successfuly!")