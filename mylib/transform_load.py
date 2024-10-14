"""
Transforms and Loads data into the databricks database

"""

import csv
import os
from dotenv import load_dotenv
from databricks import sql


# load the csv file and insert into a new databricks database
def load(dataset="data/Spotify_2023.csv"):
    """Transforms and Loads data into databricks database"""
    payload = csv.reader(open(dataset, newline="", encoding="UTF-8"), delimiter=",")
    next(payload)
    load_dotenv()
    # print(*payload)
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:

        with connection.cursor() as cursor:

            # Drop table if exists
            cursor.execute("DROP TABLE IF EXISTS spotify_top_songs")

            # Create Table
            cursor.execute(
                """
            CREATE TABLE spotify_top_songs (
                music_id INT,
                track_name STRING,
                artist_name STRING,  
                artist_count INT,
                released_year INT,
                released_month INT,
                released_day INT,
                in_spotify_playlists INT,
                in_spotify_charts INT,
                streams STRING,
                in_apple_playlists INT,
                in_apple_charts INT,
                in_deezer_playlists STRING,
                in_deezer_charts INT,
                in_shazam_charts STRING,
                bpm INT,
                key STRING,
                mode STRING,
                danceability_percent FLOAT,  
                valence_percent FLOAT,       
                energy_percent FLOAT,        
                acousticness_percent FLOAT, 
                instrumentalness_percent FLOAT, 
                liveness_percent FLOAT,      
                speechiness_percent FLOAT   
            );
            """
            )

            cursor.execute("SELECT * FROM spotify_top_songs")
            result = cursor.fetchall()
            if not result:
                string_sql = "INSERT INTO spotify_top_songs VAlUES"
                for i in payload:
                    string_sql += "\n" + str(tuple(i)) + ","
                string_sql = string_sql[:-1] + ";"
                # print(string_sql)

                cursor.execute(string_sql)

            cursor.close()
            connection.close()

    return "Load Success"


if __name__ == "__main__":
    load()
