import requests
from datetime import datetime
import time
import pandas as pd

if __name__ == "__main__":
    date_start_formatted = "2021-01-01 00:00:00"
    date_end_formatted = "2021-01-02 00:00:00"

    date_start = int(time.mktime(time.strptime(date_start_formatted, "%Y-%m-%d %H:%M:%S"))) * 1000
    date_end = int(time.mktime(time.strptime(date_end_formatted, "%Y-%m-%d %H:%M:%S"))) * 1000

    api_url = "https://lichess.org/api/games/user/luckleland?\
tags=true&clocks=false&evals=false&opening=false&since={DATE_START}&until={DATE_END}"
    api_url = api_url.format(DATE_START=str(date_start), DATE_END=str(date_end))

    # resp = requests.get(api_url)
    # body = resp.content.decode("utf-8")
    # body = body.split("\n")

    body = open("../data/lichess_luckleland_2022-04-04.pgn", 'r')
    new_game = True
    games_list = []
    for line in body:
        if new_game:
            game_dict = {}
            new_game = False
        if line.startswith("["):
            key = line.strip("[").split()[0].lower()
            value_start_index = line.find('\"')
            if value_start_index != -1:  # i.e. if the first quote was found
                value_end_index = line.find('\"', value_start_index + 1)
                if value_start_index != -1 and value_end_index != -1:  # i.e. both quotes were found
                    value = line[value_start_index + 1:value_end_index]
            game_dict.update({key: value})
        elif line.startswith("1."):
            game_dict.update({"Gamestring": line})
            games_list.append(game_dict)
            new_game = True
    game_data = pd.DataFrame.from_dict(games_list)
    game_data["id_key"] = game_data["utcdate"].str.replace(".", "") + game_data['utctime'].str.replace(":", "")
    game_data["date"] = game_data["date"].str.replace(".", "-")
    game_data.to_parquet("s3://jcrasto-chess-analysis/lichess_api_data", partition_cols=["date"], index=False)
