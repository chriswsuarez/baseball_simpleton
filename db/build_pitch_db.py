#!/usr/bin/env python3
"""
build_pitch_db.py

ETL script: walks all downloaded game JSON files and builds a SQLite database
where each row in the `pitches` table is a single pitch, with both pitcher_id
and batter_id indexed for fast player-centric lookups.

Usage:
    python db/build_pitch_db.py
    python db/build_pitch_db.py --db pitches.db --data-root db --seasons 2025
"""

import argparse
import glob
import json
import os
import sqlite3
from pathlib import Path


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

CREATE_GAMES = """
CREATE TABLE IF NOT EXISTS games (
    game_pk         INTEGER PRIMARY KEY,
    season          INTEGER,
    sport           TEXT,
    game_date       TEXT,
    game_type       TEXT,
    home_team_id    INTEGER,
    home_team_name  TEXT,
    away_team_id    INTEGER,
    away_team_name  TEXT,
    venue_id        INTEGER,
    venue_name      TEXT,
    home_final_score INTEGER,
    away_final_score INTEGER,
    status          TEXT,
    weather_condition TEXT,
    weather_temp    TEXT,
    weather_wind    TEXT
)
"""

CREATE_PLAYERS = """
CREATE TABLE IF NOT EXISTS players (
    player_id             INTEGER PRIMARY KEY,
    full_name             TEXT,
    primary_position_code TEXT,
    primary_position_name TEXT,
    bat_side              TEXT,
    pitch_hand            TEXT,
    birth_date            TEXT,
    height                TEXT,
    weight                INTEGER,
    active                INTEGER
)
"""

CREATE_PITCHES = """
CREATE TABLE IF NOT EXISTS pitches (
    -- identity
    pitch_uid       TEXT PRIMARY KEY,
    game_pk         INTEGER,
    season          INTEGER,
    sport           TEXT,
    game_date       TEXT,
    venue_id        INTEGER,
    venue_name      TEXT,

    -- game situation
    inning          INTEGER,
    inning_half     TEXT,
    at_bat_index    INTEGER,
    pitch_index     INTEGER,
    balls_before    INTEGER,
    strikes_before  INTEGER,
    outs_at_pitch   INTEGER,
    home_score      INTEGER,
    away_score      INTEGER,
    runner_on_1b    INTEGER,
    runner_on_2b    INTEGER,
    runner_on_3b    INTEGER,

    -- players
    pitcher_id      INTEGER,
    pitcher_name    TEXT,
    pitcher_hand    TEXT,
    batter_id       INTEGER,
    batter_name     TEXT,
    batter_side     TEXT,
    catcher_id      INTEGER,
    catcher_name    TEXT,

    -- teams
    batting_team_id   INTEGER,
    batting_team_name TEXT,
    fielding_team_id  INTEGER,
    fielding_team_name TEXT,
    home_team_id      INTEGER,
    away_team_id      INTEGER,

    -- pitch classification
    pitch_type_code TEXT,
    pitch_type_desc TEXT,
    pitch_type_confidence REAL,

    -- velocity / timing
    start_speed     REAL,
    end_speed       REAL,
    plate_time      REAL,
    extension       REAL,
    zone            INTEGER,
    strike_zone_top    REAL,
    strike_zone_bottom REAL,

    -- release / trajectory coordinates
    plate_x         REAL,
    plate_z         REAL,
    release_x       REAL,
    release_y       REAL,
    release_z       REAL,
    velocity_x      REAL,
    velocity_y      REAL,
    velocity_z      REAL,
    accel_x         REAL,
    accel_y         REAL,
    accel_z         REAL,
    pfx_x           REAL,
    pfx_z           REAL,
    field_x         REAL,
    field_y         REAL,

    -- movement / break
    spin_rate       INTEGER,
    spin_direction  INTEGER,
    break_angle     REAL,
    break_length    REAL,
    break_vertical  REAL,
    break_vertical_induced REAL,
    break_horizontal REAL,
    break_y         REAL,

    -- outcome
    call_code       TEXT,
    call_description TEXT,
    is_in_play      INTEGER,
    is_strike       INTEGER,
    is_ball         INTEGER,
    is_out          INTEGER,
    pitch_description TEXT,

    -- batted ball (populated when is_in_play = 1)
    launch_speed    REAL,
    launch_angle    REAL,
    total_distance  REAL,
    trajectory      TEXT,
    hardness        TEXT,
    hit_location    TEXT,
    hit_coord_x     REAL,
    hit_coord_y     REAL,

    -- at-bat result (populated on the final pitch of each at-bat)
    is_ab_final_pitch INTEGER,
    ab_event          TEXT,
    ab_rbi            INTEGER,
    ab_description    TEXT,

    -- fielding credits as JSON array [{player_id, position_code, credit}]
    fielding_credits TEXT
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pitches_pitcher ON pitches(pitcher_id)",
    "CREATE INDEX IF NOT EXISTS idx_pitches_batter  ON pitches(batter_id)",
    "CREATE INDEX IF NOT EXISTS idx_pitches_game    ON pitches(game_pk)",
    "CREATE INDEX IF NOT EXISTS idx_pitches_date    ON pitches(game_date)",
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(CREATE_GAMES)
    conn.execute(CREATE_PLAYERS)
    conn.execute(CREATE_PITCHES)
    for idx in CREATE_INDEXES:
        conn.execute(idx)
    conn.commit()
    return conn


def already_processed(conn: sqlite3.Connection, game_pk: int) -> bool:
    row = conn.execute("SELECT 1 FROM games WHERE game_pk = ?", (game_pk,)).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _get(obj, *keys, default=None):
    """Safe nested dict access."""
    for key in keys:
        if not isinstance(obj, dict):
            return default
        obj = obj.get(key)
        if obj is None:
            return default
    return obj


def extract_players(game_data: dict) -> list[dict]:
    rows = []
    for key, p in game_data.get("players", {}).items():
        rows.append({
            "player_id":             p.get("id"),
            "full_name":             p.get("fullName"),
            "primary_position_code": _get(p, "primaryPosition", "code"),
            "primary_position_name": _get(p, "primaryPosition", "name"),
            "bat_side":              _get(p, "batSide", "code"),
            "pitch_hand":            _get(p, "pitchHand", "code"),
            "birth_date":            p.get("birthDate"),
            "height":                p.get("height"),
            "weight":                p.get("weight"),
            "active":                int(bool(p.get("active"))),
        })
    return rows


def extract_game(game: dict, season: int, sport: str) -> dict:
    gd = game.get("gameData", {})
    ld = game.get("liveData", {})
    ls = ld.get("linescore", {})
    teams = ls.get("teams", {})
    weather = gd.get("weather", {})
    return {
        "game_pk":          game.get("gamePk"),
        "season":           season,
        "sport":            sport,
        "game_date":        _get(gd, "datetime", "officialDate"),
        "game_type":        _get(gd, "game", "type"),
        "home_team_id":     _get(gd, "teams", "home", "id"),
        "home_team_name":   _get(gd, "teams", "home", "name"),
        "away_team_id":     _get(gd, "teams", "away", "id"),
        "away_team_name":   _get(gd, "teams", "away", "name"),
        "venue_id":         _get(gd, "venue", "id"),
        "venue_name":       _get(gd, "venue", "name"),
        "home_final_score": _get(teams, "home", "runs"),
        "away_final_score": _get(teams, "away", "runs"),
        "status":           _get(gd, "status", "detailedState"),
        "weather_condition": weather.get("condition"),
        "weather_temp":      weather.get("temp"),
        "weather_wind":      weather.get("wind"),
    }


def _runners_at_start(runner_state: dict) -> tuple:
    """Return (runner_1b_id, runner_2b_id, runner_3b_id) from state dict."""
    return (
        runner_state.get("1B"),
        runner_state.get("2B"),
        runner_state.get("3B"),
    )


def _update_runner_state(play: dict) -> dict:
    """Compute the runner state AFTER a completed at-bat."""
    new_state = {}
    for runner in play.get("runners", []):
        movement = runner.get("movement", {})
        if not movement.get("isOut") and movement.get("end"):
            end = movement["end"]
            if end in ("1B", "2B", "3B"):
                new_state[end] = runner["details"]["runner"]["id"]
    return new_state


def _fielding_credits(play: dict) -> list[dict]:
    """Collect all fielding credits from a play's runner list."""
    credits = []
    for runner in play.get("runners", []):
        for credit in runner.get("credits", []):
            credits.append({
                "player_id":     _get(credit, "player", "id"),
                "position_code": _get(credit, "position", "code"),
                "position_name": _get(credit, "position", "name"),
                "credit":        credit.get("credit"),
            })
    return credits


def extract_pitches(game: dict, season: int, sport: str) -> list[dict]:
    gd = game.get("gameData", {})
    ld = game.get("liveData", {})
    game_pk    = game.get("gamePk")
    game_date  = _get(gd, "datetime", "officialDate")
    venue_id   = _get(gd, "venue", "id")
    venue_name = _get(gd, "venue", "name")
    home_id    = _get(gd, "teams", "home", "id")
    away_id    = _get(gd, "teams", "away", "id")
    home_name  = _get(gd, "teams", "home", "name")
    away_name  = _get(gd, "teams", "away", "name")

    all_plays = _get(ld, "plays", "allPlays") or []
    pitches = []
    runner_state: dict = {}

    for play in all_plays:
        matchup    = play.get("matchup", {})
        about      = play.get("about", {})
        result     = play.get("result", {})
        play_events = play.get("playEvents", [])

        pitcher_id   = _get(matchup, "pitcher", "id")
        pitcher_name = _get(matchup, "pitcher", "fullName")
        pitcher_hand = _get(matchup, "pitchHand", "code")
        batter_id    = _get(matchup, "batter", "id")
        batter_name  = _get(matchup, "batter", "fullName")
        batter_side  = _get(matchup, "batSide", "code")

        inning      = about.get("inning")
        inning_half = about.get("halfInning")  # "top" or "bottom"
        at_bat_idx  = about.get("atBatIndex")

        # Determine batting/fielding teams from inning half
        if inning_half == "top":
            batting_team_id, batting_team_name = away_id, away_name
            fielding_team_id, fielding_team_name = home_id, home_name
        else:
            batting_team_id, batting_team_name = home_id, home_name
            fielding_team_id, fielding_team_name = away_id, away_name

        # Runners on base at the START of this at-bat
        r1b, r2b, r3b = _runners_at_start(runner_state)

        # At-bat final result (attached to the last pitch)
        ab_event       = result.get("eventType")
        ab_rbi         = result.get("rbi")
        ab_description = result.get("description")

        # Fielding credits for this at-bat (only on in-play final pitch)
        fc_json = json.dumps(_fielding_credits(play)) if _fielding_credits(play) else None

        # Find catcher from linescore defense (end-of-game state — best available)
        defense   = _get(ld, "linescore", "defense") or {}
        catcher_id   = _get(defense, "catcher", "id")
        catcher_name = _get(defense, "catcher", "fullName")

        # Determine pitch events and which one is the final pitch
        pitch_events = [e for e in play_events if e.get("isPitch")]
        final_pitch_index = pitch_events[-1].get("index") if pitch_events else None

        prev_balls = 0
        prev_strikes = 0

        for ev in pitch_events:
            pd     = ev.get("pitchData") or {}
            hd     = ev.get("hitData") or {}
            det    = ev.get("details") or {}
            coords = pd.get("coordinates") or {}
            breaks = pd.get("breaks") or {}
            count  = ev.get("count") or {}

            pitch_number = ev.get("pitchNumber", 0)
            ev_index     = ev.get("index")

            pitch_uid = f"{game_pk}_{at_bat_idx}_{pitch_number}"
            is_final  = 1 if ev_index == final_pitch_index else 0

            row = {
                # identity
                "pitch_uid":    pitch_uid,
                "game_pk":      game_pk,
                "season":       season,
                "sport":        sport,
                "game_date":    game_date,
                "venue_id":     venue_id,
                "venue_name":   venue_name,
                # situation
                "inning":       inning,
                "inning_half":  inning_half,
                "at_bat_index": at_bat_idx,
                "pitch_index":  pitch_number,
                "balls_before":   prev_balls,
                "strikes_before": prev_strikes,
                "outs_at_pitch":  count.get("outs"),
                "home_score":   result.get("homeScore"),
                "away_score":   result.get("awayScore"),
                "runner_on_1b": r1b,
                "runner_on_2b": r2b,
                "runner_on_3b": r3b,
                # players
                "pitcher_id":    pitcher_id,
                "pitcher_name":  pitcher_name,
                "pitcher_hand":  pitcher_hand,
                "batter_id":     batter_id,
                "batter_name":   batter_name,
                "batter_side":   batter_side,
                "catcher_id":    catcher_id,
                "catcher_name":  catcher_name,
                # teams
                "batting_team_id":    batting_team_id,
                "batting_team_name":  batting_team_name,
                "fielding_team_id":   fielding_team_id,
                "fielding_team_name": fielding_team_name,
                "home_team_id":  home_id,
                "away_team_id":  away_id,
                # pitch classification
                "pitch_type_code": _get(det, "type", "code"),
                "pitch_type_desc": _get(det, "type", "description"),
                "pitch_type_confidence": pd.get("typeConfidence"),
                # velocity / timing
                "start_speed":      pd.get("startSpeed"),
                "end_speed":        pd.get("endSpeed"),
                "plate_time":       pd.get("plateTime"),
                "extension":        pd.get("extension"),
                "zone":             pd.get("zone"),
                "strike_zone_top":    pd.get("strikeZoneTop"),
                "strike_zone_bottom": pd.get("strikeZoneBottom"),
                # coordinates
                "plate_x":    coords.get("pX"),
                "plate_z":    coords.get("pZ"),
                "release_x":  coords.get("x0"),
                "release_y":  coords.get("y0"),
                "release_z":  coords.get("z0"),
                "velocity_x": coords.get("vX0"),
                "velocity_y": coords.get("vY0"),
                "velocity_z": coords.get("vZ0"),
                "accel_x":    coords.get("aX"),
                "accel_y":    coords.get("aY"),
                "accel_z":    coords.get("aZ"),
                "pfx_x":      coords.get("pfxX"),
                "pfx_z":      coords.get("pfxZ"),
                "field_x":    coords.get("x"),
                "field_y":    coords.get("y"),
                # breaks
                "spin_rate":              breaks.get("spinRate"),
                "spin_direction":         breaks.get("spinDirection"),
                "break_angle":            breaks.get("breakAngle"),
                "break_length":           breaks.get("breakLength"),
                "break_vertical":         breaks.get("breakVertical"),
                "break_vertical_induced": breaks.get("breakVerticalInduced"),
                "break_horizontal":       breaks.get("breakHorizontal"),
                "break_y":                breaks.get("breakY"),
                # outcome
                "call_code":        _get(det, "call", "code"),
                "call_description": _get(det, "call", "description"),
                "is_in_play": int(bool(det.get("isInPlay"))),
                "is_strike":  int(bool(det.get("isStrike"))),
                "is_ball":    int(bool(det.get("isBall"))),
                "is_out":     int(bool(det.get("isOut"))),
                "pitch_description": det.get("description"),
                # batted ball
                "launch_speed":   hd.get("launchSpeed"),
                "launch_angle":   hd.get("launchAngle"),
                "total_distance": hd.get("totalDistance"),
                "trajectory":     hd.get("trajectory"),
                "hardness":       hd.get("hardness"),
                "hit_location":   hd.get("location"),
                "hit_coord_x":    _get(hd, "coordinates", "coordX"),
                "hit_coord_y":    _get(hd, "coordinates", "coordY"),
                # at-bat result (only on final pitch)
                "is_ab_final_pitch": is_final,
                "ab_event":          ab_event       if is_final else None,
                "ab_rbi":            ab_rbi         if is_final else None,
                "ab_description":    ab_description if is_final else None,
                "fielding_credits":  fc_json        if is_final else None,
            }
            pitches.append(row)

            # Advance count for next pitch
            prev_balls   = count.get("balls", prev_balls)
            prev_strikes = count.get("strikes", prev_strikes)

        # Update runner state after the at-bat completes
        runner_state = _update_runner_state(play)

    return pitches


# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------

PITCH_COLUMNS = [
    "pitch_uid", "game_pk", "season", "sport", "game_date", "venue_id", "venue_name",
    "inning", "inning_half", "at_bat_index", "pitch_index",
    "balls_before", "strikes_before", "outs_at_pitch",
    "home_score", "away_score", "runner_on_1b", "runner_on_2b", "runner_on_3b",
    "pitcher_id", "pitcher_name", "pitcher_hand",
    "batter_id", "batter_name", "batter_side",
    "catcher_id", "catcher_name",
    "batting_team_id", "batting_team_name", "fielding_team_id", "fielding_team_name",
    "home_team_id", "away_team_id",
    "pitch_type_code", "pitch_type_desc", "pitch_type_confidence",
    "start_speed", "end_speed", "plate_time", "extension",
    "zone", "strike_zone_top", "strike_zone_bottom",
    "plate_x", "plate_z",
    "release_x", "release_y", "release_z",
    "velocity_x", "velocity_y", "velocity_z",
    "accel_x", "accel_y", "accel_z",
    "pfx_x", "pfx_z", "field_x", "field_y",
    "spin_rate", "spin_direction",
    "break_angle", "break_length", "break_vertical", "break_vertical_induced",
    "break_horizontal", "break_y",
    "call_code", "call_description",
    "is_in_play", "is_strike", "is_ball", "is_out", "pitch_description",
    "launch_speed", "launch_angle", "total_distance", "trajectory", "hardness",
    "hit_location", "hit_coord_x", "hit_coord_y",
    "is_ab_final_pitch", "ab_event", "ab_rbi", "ab_description",
    "fielding_credits",
]

PITCH_INSERT = (
    f"INSERT OR IGNORE INTO pitches ({', '.join(PITCH_COLUMNS)}) "
    f"VALUES ({', '.join(':' + c for c in PITCH_COLUMNS)})"
)

GAME_COLUMNS = [
    "game_pk", "season", "sport", "game_date", "game_type",
    "home_team_id", "home_team_name", "away_team_id", "away_team_name",
    "venue_id", "venue_name", "home_final_score", "away_final_score",
    "status", "weather_condition", "weather_temp", "weather_wind",
]
GAME_INSERT = (
    f"INSERT OR IGNORE INTO games ({', '.join(GAME_COLUMNS)}) "
    f"VALUES ({', '.join(':' + c for c in GAME_COLUMNS)})"
)

PLAYER_COLUMNS = [
    "player_id", "full_name", "primary_position_code", "primary_position_name",
    "bat_side", "pitch_hand", "birth_date", "height", "weight", "active",
]
PLAYER_INSERT = (
    f"INSERT OR IGNORE INTO players ({', '.join(PLAYER_COLUMNS)}) "
    f"VALUES ({', '.join(':' + c for c in PLAYER_COLUMNS)})"
)

BATCH_SIZE = 500


def process_file(conn: sqlite3.Connection, path: str, season: int, sport: str) -> int:
    """Parse one game JSON and insert rows. Returns number of pitches inserted."""
    with open(path) as f:
        game = json.load(f)

    game_pk = game.get("gamePk")
    if not game_pk:
        return 0

    if already_processed(conn, game_pk):
        return -1  # sentinel: skipped

    game_row    = extract_game(game, season, sport)
    player_rows = extract_players(game.get("gameData", {}))
    pitch_rows  = extract_pitches(game, season, sport)

    with conn:
        conn.execute(GAME_INSERT, game_row)
        conn.executemany(PLAYER_INSERT, player_rows)
        for i in range(0, len(pitch_rows), BATCH_SIZE):
            conn.executemany(PITCH_INSERT, pitch_rows[i:i + BATCH_SIZE])

    return len(pitch_rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build player-centric pitch SQLite DB")
    parser.add_argument("--db",        default="pitches.db",
                        help="Output SQLite database path (default: pitches.db)")
    parser.add_argument("--data-root", default="db",
                        help="Root directory containing season folders (default: db)")
    parser.add_argument("--seasons",   nargs="+", type=int, default=[2025],
                        help="Season year(s) to process (default: 2025)")
    args = parser.parse_args()

    db_path   = args.db
    data_root = args.data_root
    seasons   = args.seasons

    conn = init_db(db_path)

    total_games    = 0
    total_skipped  = 0
    total_pitches  = 0
    total_errors   = 0

    for season in seasons:
        season_dir = os.path.join(data_root, str(season))
        if not os.path.isdir(season_dir):
            print(f"[WARN] Season directory not found: {season_dir}")
            continue

        # Auto-discover all sport subdirectories
        sport_dirs = [
            d for d in os.scandir(season_dir)
            if d.is_dir()
        ]

        for sport_entry in sorted(sport_dirs, key=lambda d: d.name):
            sport      = sport_entry.name
            sport_path = sport_entry.path
            files      = sorted(glob.glob(os.path.join(sport_path, "game_*.json")))

            print(f"\n[{season}/{sport}] {len(files)} game files")

            for i, filepath in enumerate(files, 1):
                try:
                    n = process_file(conn, filepath, season, sport)
                except Exception as e:
                    print(f"  ERROR {os.path.basename(filepath)}: {e}")
                    total_errors += 1
                    continue

                if n == -1:
                    total_skipped += 1
                else:
                    total_games   += 1
                    total_pitches += n

                if i % 100 == 0 or i == len(files):
                    print(f"  {i}/{len(files)} files | "
                          f"{total_games} processed | "
                          f"{total_skipped} skipped | "
                          f"{total_pitches:,} pitches | "
                          f"{total_errors} errors")

    conn.close()
    print(f"\nDone. DB: {db_path}")
    print(f"  Games processed : {total_games}")
    print(f"  Games skipped   : {total_skipped}")
    print(f"  Total pitches   : {total_pitches:,}")
    print(f"  Errors          : {total_errors}")


if __name__ == "__main__":
    main()
