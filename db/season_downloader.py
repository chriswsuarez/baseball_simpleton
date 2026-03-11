#!/usr/bin/env python3

import statsapi as mlbapi
import json
import os
import argparse

def get_sports():
    sports = mlbapi.get('sports')['sports']
    for sport in sports:
        print(f"{sport['id']}: {sport['name']}")

# Download all raw game data for an entire season in json format into a single folder.
def download_season(season: int, sport_id: int = 1):
    sports = mlbapi.get('sports')['sports']

    sport = None
    for spt in sports:
        if spt['id'] == sport_id:
            print(f"Found sport {spt['id']}: {spt['name']}")
            sport = spt['name']
            break

    if not sport:
        print(f"Sport with id {sport_id} not found. Available sports:")
        get_sports()
        return

    games = mlbapi.schedule(sportId=sport_id, season=season)

    if not games:
        print(f"No games found for sport {sport} and season {season}.")
        return

    sport = sport.replace(" ", "_").replace("-", "_").lower()
    if not os.path.isdir(f"{season}/{sport}"):
        os.mkdir(f"{season}/{sport}")

    for game in games:
        game_id = game['game_id']

        if os.path.isfile(f"{season}/{sport}/game_{game_id}.json"):
            # Initial investigation for duplicate game ids showed that all the game data was exactly the same for each duplicate game id.
            # This seems safe to skip for now
            print(f"Data for game {game_id} already exists. Skipping download.")
            continue

        with open(f"{season}/{sport}/game_{game_id}.json", "w") as f:
            json.dump(mlbapi.get('game', {'gamePk': game_id}), f, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('season', type=int, help="The season to download data for.")
    parser.add_argument('--sport_id', '-s', type=int, default=0, required=False, help="The sport to download data for.")
    args = parser.parse_args()

    season = args.season
    sport_id = args.sport_id
    download_season(season, sport_id)