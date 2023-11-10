from utils.query_utils import athena_query_to_df
import json
import pandas as pd
import logging


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
            game_dicts.append({"id_key": id_key, "date": date, "move_number": move_string[:-1] + "w",
                               "pgn_string": running_game_str + move_list[0]})
            if move_list[1] != '0-1' and move_list[1] != '1-0' and move_list[1] != '1/2-1/2':
                game_dicts.append({"id_key": id_key, "date": date, "move_number": move_string[:-1] + "b",
                                   "pgn_string": running_game_str + " ".join(move_list[:-1])})
        else:
            move_list = game_string[
                        game_string.index(move_string) + len(move_string):game_string.index(next_move_string)].split()
            game_dicts.append({"id_key": id_key, "date": date, "move_number": move_string[:-1] + "w",
                               "pgn_string": running_game_str + move_list[0]})
            game_dicts.append({"id_key": id_key, "date": date, "move_number": move_string[:-1] + "b",
                               "pgn_string": running_game_str + " ".join(move_list)})
            move_number += 1
    return pd.DataFrame(game_dicts)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    games_df_query = """select "date", gamestring, id_key
    from lichess.lichess_api_data
    order by "date" desc"""
    games_df = athena_query_to_df(games_df_query)
    logging.info("retrieved dataframe with shape: " + str(games_df.shape))
    games_df['id_key'] = games_df['id_key'].astype(str)

    result = games_df.apply(pgn_parser, axis=1)
    combined_df = pd.concat(result.to_list(), ignore_index=True)
    logging.info("new dataframe with running gamestring has shape: " + str(combined_df.shape))
    combined_df.to_parquet("s3://jcrasto-chess-analysis/running_gamestrings/", partition_cols=["date"], index=False)
