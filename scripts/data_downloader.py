import requests
from datetime import datetime
import time



if __name__ == "__main__":
    date_start_formatted = "2021-01-01 00:00:00"
    date_end_formatted = "2021-01-02 00:00:00"

    date_start = int(time.mktime(time.strptime(date_start_formatted, "%Y-%m-%d %H:%M:%S"))) * 1000
    date_end = int(time.mktime(time.strptime(date_end_formatted, "%Y-%m-%d %H:%M:%S"))) * 1000


    api_url = "https://lichess.org/api/games/user/luckleland?\
tags=true&clocks=false&evals=false&opening=false&since={DATE_START}&until={DATE_END}"
    api_url = api_url.format(DATE_START=str(date_start), DATE_END=str(date_end))

    resp = requests.get(api_url)
    print(resp.content)
