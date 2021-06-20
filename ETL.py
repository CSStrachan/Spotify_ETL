#This program downloads the data about what songs ive listened to throughout the day and everyday the table grows

#pip3 install sqlalchemy, pandas, requests #insert in command line
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATA_LOCALE = "sqlite:///my_played_tracks.sqlite"
USER_ID = "cstrachan98"
TOKEN = "BQDx3H5xW98AQm3p588rMbvI5tVqNjbYp86DLyvpzUtzapIlyxldTHGnL-GG4qQV1PpedpvCyWUeB80SW9S2P7LrqtybMZCQoEYkhJbWoTMXzKyeUfZQtP1GpC6ZM1SBNsCstNjHS7811lQtDiWvUw" #expires after a few minutes (request when ready)


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    # Primary Key Check (Unique id of each row in the data table passed on the time the song is played)
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls (checking that the data isn't altered/any nulls found terminate the process)
    if df.isnull().values.any():
        raise Exception("Null values found")
    
    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            print(timestamp)
            raise Exception("At least one of returned songs does not have a yesterday's timestamp")

    return True

if __name__ == "__main__":

# Extract part of ETL
    headers = { #Headers is somewhat of a dictionary - holds all kinds of data
        "Accept" :"application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    #Convert time to Unix timestamp in miliseconds
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    #Downloads last 24 hours of music on spotify
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played", headers = headers)
    data = r.json()

    with open('result.json', 'w') as fp:
        json.dump(data, fp)

song_names = []
artist_names = []
played_at_list = []
timestamps = []
print (data.keys())

# Extracting the parts deemed relevent from the json object
for song in data["items"]:
    timestamp = song["played_at"][0:10]
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])
    
# Prepare a dictionary in order to turn it into a pandas dataframe below
song_dict = {
    "song_name" : song_names,
    "artist_name" : artist_names,
    "played_at" : played_at_list,
    "timestamp" : timestamps
}

song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

print (song_df)
# Validate
if check_if_valid_data(song_df):
    print("Data valid, proceed to load stage")

# Load

enginge = sqlalchemy.create_engine(DATA_LOCALE)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""

cursor.execute(sql_query)
print("Opened database successfully")

try:
    song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
except:
    print("Data already in the database")

conn.close()
print("Close databse successfully")


# Job Scheduling

# For the scheduling in Airflow, refer to files in the dag folder