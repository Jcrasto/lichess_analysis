import requests
from datetime import datetime, date, timedelta
import boto3
import pandas as pd
import logging
import argparse
import sys
import time
from utils.query_utils import run_athena_query
from utils.s3_utils import delete_objects_by_partition_value

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default=str(date.today() - timedelta(days=7)))
    parser.add_argument("--end_date", default=str(date.today()))
    date_format = "%Y-%m-%d"
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, date_format)
    end_date = datetime.strptime(args.end_date, date_format)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_stream_handler.setFormatter(formatter)
    logger.addHandler(log_stream_handler)

    session = boto3.Session()
    s3_client = session.client('s3')
    query_output_bucket = "s3://query-results-737934178320"
    athena_client = session.client("athena", region_name='us-east-1')

    date_start = int(start_date.strftime("%s")) * 1000
    date_end = int(end_date.strftime("%s")) * 1000

    api_url = "https://lichess.org/api/games/user/luckleland?\
tags=true&clocks=false&evals=false&opening=false&since={DATE_START}&until={DATE_END}"
    api_url = api_url.format(DATE_START=str(date_start), DATE_END=str(date_end))

    resp = requests.get(api_url)
    body = resp.content.decode("utf-8")
    body = body.split("\n")

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
    game_data["id_key"] = game_data["utcdate"].str.replace(".", "") + game_data['utctime'].str.replace(":", "",
                                                                                                       regex=False)
    game_data["date"] = game_data["date"].str.replace(".", "-", regex=False)

    drop_partition_query = "ALTER TABLE lichess.lichess_api_data DROP "
    add_partition_query = "ALTER TABLE lichess.lichess_api_data ADD IF NOT EXISTS "

    bucket = "jcrasto-chess-analysis"
    prefix = "lichess_api_data/"

    for date in game_data['date'].unique():
        delete_objects_by_partition_value(bucket, prefix, "date", date)
        drop_partition_query += "PARTITION (date='{DATE}'),".format(DATE=date)
        add_partition_query += "PARTITION (date = '{DATE}') LOCATION 's3://jcrasto-chess-analysis/lichess_api_data/date={DATE}' ".format(
            DATE=date)
    drop_partition_query = drop_partition_query[:-1] + ';'
    add_partition_query = add_partition_query[:-1] + ';'

    # response = run_athena_query(athena_client, drop_partition_query)
    # logger.info(response)

    logger.info("Writing dataframe to s3://jcrasto-chess-analysis/lichess_api_data/ as parquet")
    game_data.to_parquet("s3://jcrasto-chess-analysis/lichess_api_data/", partition_cols=["date"], index=False)
    logger.info("Dataframe to s3 write completed")

    response = run_athena_query(add_partition_query)
    logger.info(response)

    response = run_athena_query("MSCK REPAIR TABLE lichess.lichess_api_data")
    logger.info(response)

    sleep_interval = 100
    logger.info(f"sleeping for {str(sleep_interval)} seconds")
    time.sleep(sleep_interval)
    logger.info("Process complete, exiting")

    sys.exit(0)
