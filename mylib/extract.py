"""
Extract a dataset from a URL
"""

import requests


def extract(
    url="https://raw.githubusercontent.com/RunCHIRON/dataset/refs/heads/main/Spotify_2023.csv",
    file_path="data/Spotify_2023.csv",
):
    """ "Extract a url to a file path"""
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path
