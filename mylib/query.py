"""Query the database"""

from dotenv import load_dotenv
from databricks import sql
import os

complex_query = """
WITH songs AS (
  SELECT artist_name,
    AVG(bpm) AS avg_bpm,
    AVG(in_spotify_charts) AS avg_spot_chart_ranking
    FROM default.spotify_top_songs
    GROUP BY artist_name 
)

SELECT * FROM default.spotify_top_songs
JOIN songs
ON default.spotify_top_songs.artist_name = songs.artist_name
ORDER BY songs.avg_spot_chart_ranking DESC;

"""


def query():
    """Query the database for the top 5 rows of the GroceryDB table"""
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:

        with connection.cursor() as cursor:

            cursor.execute(complex_query)
            # result = cursor.fetchall()

            # for row in result:
            #     print(row)

            cursor.close()
            connection.close()

    return "Query Success"


if __name__ == "__main__":
    query()
