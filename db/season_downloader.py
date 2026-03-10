#!/usr/bin/env python3

import statsapi as mlbapi
import json
import os

# Download all raw game data for an entire season in json format into a single folder.
def download_season(season: int):
    games = mlbapi.schedule(season=season)

    if not games:
        print(f"No games found for season {season}.")
        return

    if not os.path.isdir(f"{season}"):
        os.mkdir(f"{season}")

    for game in games:
        game_id = game['game_id']

        if os.path.isfile(f"{season}/game_{game_id}.json"):
            # Initial investigation for duplicate game ids showed that all the game data was exactly the same for each duplicate game id.
            # This seems safe to skip for now
            print(f"Data for game {game_id} already exists. Skipping download.")
            continue

        with open(f"{season}/game_{game_id}.json", "w") as f:
            json.dump(mlbapi.get('game', {'gamePk': game_id}), f, indent=4)


if __name__ == "__main__":
    season = 2025
    download_season(season)