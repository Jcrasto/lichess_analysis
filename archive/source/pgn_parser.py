from utils.query_utils import athena_query_to_df
import pandas as pd
import logging
import sys
import argparse
from datetime import datetime, date, timedelta
import time
from utils.query_utils import run_athena_query
from utils.s3_utils import delete_objects_by_partition_value


def pgn_parser(row):
    game_string = row["gamestring"]
    id_key = row["id_key"]
    date = row['date']
    last_move = False
    move_number = 1
    game_dicts = []
    while not last_move:
        if move_number >= 20:
            last_move = True
        move_string = str(move_number) + "."
        next_move_string = str(move_number + 1) + "."
        running_game_str = game_string[:game_string.index(move_string) + len(move_string)]
        if next_move_string not in game_string:
            last_move = True
            move_list = game_string[game_string.index(move_string) + len(move_string):].split()
            game_dicts.append({"id_key": id_key, "date": date, "move_number": move_number, "move": 0,
                               "pgn_string": running_game_str + " " + move_list[0]})
            if move_list[1] != '0-1' and move_list[1] != '1-0' and move_list[1] != '1/2-1/2':
                game_dicts.append({"id_key": id_key, "date": date, "move_number": move_number, "move": 1,
                                   "pgn_string": running_game_str + " " + " ".join(move_list[:2])})
        else:
            move_list = game_string[
                        game_string.index(move_string) + len(move_string):game_string.index(next_move_string)].split()
            game_dicts.append({"id_key": id_key, "date": date, "move_number": move_number, "move": 0,
                               "pgn_string": running_game_str + " " + move_list[0]})
            game_dicts.append({"id_key": id_key, "date": date, "move_number": move_number, "move": 1,
                               "pgn_string": running_game_str + " " + " ".join(move_list)})
            move_number += 1
    return pd.DataFrame(game_dicts)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default=str(date.today() - timedelta(days=7)))
    parser.add_argument("--end_date", default=str(date.today()))
    date_format = "%Y-%m-%d"
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, date_format)
    end_date = datetime.strptime(args.end_date, date_format)

    start_date_string = start_date.strftime(date_format)
    end_date_string = end_date.strftime(date_format)

    games_df_query = """select "date", gamestring, id_key
    from lichess.lichess_api_data 
    where date >= '{START_DATE}' 
    and date <= '{END_DATE}'""".format(START_DATE=start_date_string, END_DATE=end_date_string)
    games_df = athena_query_to_df(games_df_query)
    games_df['id_key'] = games_df['id_key'].astype(str)

    result = games_df.apply(pgn_parser, axis=1)
    combined_df = pd.concat(result.to_list(), ignore_index=True)
    logging.info("new dataframe with running gamestring has shape: " + str(combined_df.shape))

    bucket = "jcrasto-chess-analysis"
    prefix = "running_gamestrings/"

    add_partition_query = "ALTER TABLE lichess.running_gamestrings ADD IF NOT EXISTS "

    for date in combined_df['date'].unique():
        delete_objects_by_partition_value(bucket, prefix, "date", date)
        add_partition_query += "PARTITION (date = '{DATE}') LOCATION 's3://jcrasto-chess-analysis/running_gamestrings/date={DATE}' ".format(
            DATE=date)

    s3_location = "s3://jcrasto-chess-analysis/running_gamestrings/"
    logging.info("writing parquet data to: " + s3_location)
    combined_df.to_parquet(s3_location, partition_cols=["date"], index=False)
    logging.info("Dataframe to s3 write completed")

    response = run_athena_query(add_partition_query)
    logging.info(response)

    response = run_athena_query("MSCK REPAIR TABLE lichess.running_gamestrings")
    logging.info(response)

    sleep_interval = 100
    logging.info(f"sleeping for {str(sleep_interval)} seconds")
    time.sleep(sleep_interval)
    logging.info("Process complete, exiting")

    sys.exit(0)

