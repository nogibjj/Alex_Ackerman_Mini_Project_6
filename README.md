[![CI](https://github.com/nogibjj/Alex_Ackerman_Mini_Project_6/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Alex_Ackerman_Mini_Project_6/actions/workflows/cicd.yml)

## Alex Ackerman Mini Project 5

# Project: ETL-Queary Pipeline with Databricks

## Overview:

The goal of this project is to build an Extract, Transform, Load (ETL)-Query pipeline using Databricks. The focus was to expand the query complexity of Mini Project 5 and design complex SQL queries for a MySQL database

## Complex SQL Query:
This query analyzes song characteristics from the top Spotify songs from 2023 to create a Common Table Expression (CTE) and join it to the original table.

```sql
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

```
### Query Explanation:

- **CTE `song`**: Selects the artist name and calculates the average beats-per-minute (bpm) of their songs as well as their average ranking in the Spotify charts.
- The query then joins the CTE `song` to spotify_top_songs on the artists name

## Data

The data used for this project came from [RunCHIRON](https://github.com/RunCHIRON/dataset) and contains a csv file of the top Spotify songs from 2023

## References

- [sqlite-lab](https://github.com/nogibjj/sqlite-lab/tree/main)

- [Dataset from RunCHIRON Repo](https://github.com/RunCHIRON/dataset)

