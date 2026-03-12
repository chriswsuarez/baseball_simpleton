# Plan: Player-Centric Pitch Database Builder

## Context

The project has 5,218 downloaded game JSON files (MLB + Triple-A, 2025 season) with full Statcast-level pitch data. Currently there is no processing layer — just raw game JSON files. The goal is to parse all games and build a SQLite database keyed by player ID so any player's complete pitch history (thrown or faced) can be retrieved instantly.

## New Files

- `db/build_pitch_db.py` — ETL script: processes all game JSONs → SQLite database
- `db/player_pitches.py` — Python API module: query functions for player lookup

---

## Database Schema

### `pitches` table (one row per pitch)

All pitch data, with both `pitcher_id` and `batter_id` indexed so lookups are fast in either direction.

#### Identity & Game Context
| Column | Source | Description |
|--------|--------|-------------|
| `pitch_uid` | computed | `{game_pk}_{at_bat_idx}_{pitch_idx}` — unique pitch ID |
| `game_pk` | `gamePk` | Game identifier |
| `season` | dirname | Season year |
| `sport` | dirname | `major_league_baseball` or `triple_a` |
| `game_date` | `gameData.datetime.officialDate` | Date of game |
| `venue_id` | `gameData.venue.id` | Ballpark ID |
| `venue_name` | `gameData.venue.name` | Ballpark name |

#### Inning & Situation
| Column | Source | Description |
|--------|--------|-------------|
| `inning` | `allPlays[].about.inning` | Inning number |
| `inning_half` | `allPlays[].about.halfInning` | `top` or `bottom` |
| `at_bat_index` | `allPlays[].about.atBatIndex` | At-bat number in game |
| `pitch_index` | `playEvents[].pitchNumber` | Pitch number within at-bat |
| `balls_before` | `allPlays[].count.balls` at pitch time | Ball count before pitch |
| `strikes_before` | `allPlays[].count.strikes` at pitch time | Strike count before pitch |
| `outs_before` | `allPlays[].count.outs` at pitch time | Out count before pitch |
| `home_score` | `allPlays[].result.homeScore` | Home team score at time of pitch |
| `away_score` | `allPlays[].result.awayScore` | Away team score at time of pitch |
| `runner_on_1b` | `allPlays[].runners` | Player ID of runner on 1B (or NULL) |
| `runner_on_2b` | `allPlays[].runners` | Player ID of runner on 2B (or NULL) |
| `runner_on_3b` | `allPlays[].runners` | Player ID of runner on 3B (or NULL) |

#### Players
| Column | Source | Description |
|--------|--------|-------------|
| `pitcher_id` | `allPlays[].matchup.pitcher.id` | **Primary lookup key** |
| `pitcher_name` | `allPlays[].matchup.pitcher.fullName` | Pitcher full name |
| `pitcher_hand` | `allPlays[].matchup.pitchHand.code` | `L` or `R` |
| `batter_id` | `allPlays[].matchup.batter.id` | **Primary lookup key** |
| `batter_name` | `allPlays[].matchup.batter.fullName` | Batter full name |
| `batter_side` | `allPlays[].matchup.batSide.code` | `L`, `R`, or `S` |
| `catcher_id` | `liveData.linescore.defense.catcher.id` | Catcher's player ID |
| `catcher_name` | `liveData.linescore.defense.catcher.fullName` | Catcher full name |

#### Teams
| Column | Source | Description |
|--------|--------|-------------|
| `batting_team_id` | derived from `allPlays[].about.halfInning` + game teams | Team at bat ID |
| `batting_team_name` | `gameData.teams` | Team at bat name |
| `fielding_team_id` | derived | Team in field ID |
| `fielding_team_name` | `gameData.teams` | Team in field name |
| `home_team_id` | `gameData.teams.home.id` | Home team ID |
| `away_team_id` | `gameData.teams.away.id` | Away team ID |

#### Pitch Physical Properties
| Column | Source | Description |
|--------|--------|-------------|
| `pitch_type_code` | `pitchData.type.code` | `FF`, `SL`, `CH`, `CU`, `SI`, `FC`, `FS`, `KC`, etc. |
| `pitch_type_desc` | `pitchData.type.description` | Full pitch type name |
| `pitch_type_confidence` | `pitchData.typeConfidence` | Classification confidence (0–1) |
| `start_speed` | `pitchData.startSpeed` | Release speed (mph) |
| `end_speed` | `pitchData.endSpeed` | Speed at plate (mph) |
| `plate_time` | `pitchData.plateTime` | Time from release to plate (seconds) |
| `extension` | `pitchData.extension` | Release point extension from rubber (feet) |
| `zone` | `pitchData.zone` | Strike zone location (1–9 in zone, 11–14 outside) |
| `strike_zone_top` | `pitchData.strikeZoneTop` | Top of batter's strike zone (feet) |
| `strike_zone_bottom` | `pitchData.strikeZoneBottom` | Bottom of batter's strike zone (feet) |

#### Pitch Location Coordinates
| Column | Source | Description |
|--------|--------|-------------|
| `plate_x` | `pitchData.coordinates.pX` | Horizontal location at plate (feet, catcher POV) |
| `plate_z` | `pitchData.coordinates.pZ` | Vertical location at plate (feet above ground) |
| `release_x` | `pitchData.coordinates.x0` | Release point X (feet) |
| `release_y` | `pitchData.coordinates.y0` | Release point Y, distance from home (feet) |
| `release_z` | `pitchData.coordinates.z0` | Release point height (feet) |
| `velocity_x` | `pitchData.coordinates.vX0` | Initial velocity X component (ft/s) |
| `velocity_y` | `pitchData.coordinates.vY0` | Initial velocity Y component (ft/s) |
| `velocity_z` | `pitchData.coordinates.vZ0` | Initial velocity Z component (ft/s) |
| `accel_x` | `pitchData.coordinates.aX` | Acceleration X (ft/s²) |
| `accel_y` | `pitchData.coordinates.aY` | Acceleration Y (ft/s²) |
| `accel_z` | `pitchData.coordinates.aZ` | Acceleration Z (ft/s²) |
| `pfx_x` | `pitchData.coordinates.pfxX` | Horizontal movement vs gravity-only (inches) |
| `pfx_z` | `pitchData.coordinates.pfxZ` | Vertical movement vs gravity-only (inches) |
| `field_x` | `pitchData.coordinates.x` | TV broadcast X coordinate |
| `field_y` | `pitchData.coordinates.y` | TV broadcast Y coordinate |

#### Pitch Movement (Breaks)
| Column | Source | Description |
|--------|--------|-------------|
| `spin_rate` | `pitchData.breaks.spinRate` | Spin rate (rpm) |
| `spin_direction` | `pitchData.breaks.spinDirection` | Spin axis direction (degrees) |
| `break_angle` | `pitchData.breaks.breakAngle` | Angle of break (degrees) |
| `break_length` | `pitchData.breaks.breakLength` | Total break distance (inches) |
| `break_vertical` | `pitchData.breaks.breakVertical` | Total vertical break (inches) |
| `break_vertical_induced` | `pitchData.breaks.breakVerticalInduced` | Induced (spin-driven) vertical break (inches) |
| `break_horizontal` | `pitchData.breaks.breakHorizontal` | Horizontal break (inches) |
| `break_y` | `pitchData.breaks.breakY` | Distance from pitcher where ball breaks (feet) |

#### Pitch Outcome
| Column | Source | Description |
|--------|--------|-------------|
| `call_code` | `pitchData.details.call.code` | `B`=Ball, `S`=Strike swinging, `C`=Strike called, `F`=Foul, `X`=In play out, `D`=In play no out, `E`=In play error, `H`=Hit by pitch |
| `call_description` | `pitchData.details.call.description` | Human-readable call |
| `is_in_play` | `pitchData.details.isInPlay` | Ball put in play |
| `is_strike` | `pitchData.details.isStrike` | Called or swinging strike |
| `is_ball` | `pitchData.details.isBall` | Ball |
| `is_out` | `pitchData.details.isOut` | Resulted in out |
| `pitch_description` | `playEvents[].details.description` | Full event description text |

#### Hit Data (populated when `is_in_play = 1`)
| Column | Source | Description |
|--------|--------|-------------|
| `launch_speed` | `hitData.launchSpeed` | Exit velocity (mph) |
| `launch_angle` | `hitData.launchAngle` | Launch angle (degrees) |
| `total_distance` | `hitData.totalDistance` | Projected distance (feet) |
| `trajectory` | `hitData.trajectory` | `ground_ball`, `line_drive`, `fly_ball`, `popup` |
| `hardness` | `hitData.hardness` | `hard`, `medium`, `soft` |
| `hit_location` | `hitData.location` | Field location number (1–9) |
| `hit_coord_x` | `hitData.coordinates.coordX` | Spray chart X coordinate |
| `hit_coord_y` | `hitData.coordinates.coordY` | Spray chart Y coordinate |

#### At-Bat Final Result (populated on the last pitch of each at-bat)
| Column | Source | Description |
|--------|--------|-------------|
| `is_ab_final_pitch` | computed | `1` if this pitch ended the at-bat |
| `ab_event` | `allPlays[].result.eventType` | `strikeout`, `walk`, `single`, `home_run`, `field_out`, etc. |
| `ab_rbi` | `allPlays[].result.rbi` | RBIs on this at-bat |
| `ab_description` | `allPlays[].result.description` | Text description of at-bat outcome |

---

### `players` table (one row per player, populated during ETL)
| Column | Source |
|--------|--------|
| `player_id` | `gameData.players.ID{id}.id` |
| `full_name` | `.fullName` |
| `primary_position_code` | `.primaryPosition.code` |
| `primary_position_name` | `.primaryPosition.name` |
| `bat_side` | `.batSide.code` |
| `pitch_hand` | `.pitchHand.code` |
| `birth_date` | `.birthDate` |
| `height` | `.height` |
| `weight` | `.weight` |
| `active` | `.active` |

### `games` table (one row per game)
| Column | Source |
|--------|--------|
| `game_pk` | `gamePk` |
| `season` | dirname |
| `sport` | dirname |
| `game_date` | `gameData.datetime.officialDate` |
| `home_team_id` | `gameData.teams.home.id` |
| `home_team_name` | `gameData.teams.home.name` |
| `away_team_id` | `gameData.teams.away.id` |
| `away_team_name` | `gameData.teams.away.name` |
| `venue_id` | `gameData.venue.id` |
| `venue_name` | `gameData.venue.name` |
| `home_final_score` | `liveData.linescore.teams.home.runs` |
| `away_final_score` | `liveData.linescore.teams.away.runs` |
| `status` | `gameData.status.detailedState` |

---

## Implementation Plan

### File: `db/build_pitch_db.py`

```
build_pitch_db.py [--db PATH] [--data-root PATH] [--seasons 2025] [--sports mlb triple_a]
```

**Algorithm:**
1. Connect to (or create) SQLite DB, create tables + indexes
2. Walk `{data_root}/{season}/{sport}/game_*.json` files for each requested season/sport
3. Skip games already in the `games` table (idempotent reruns)
4. For each game file:
   a. Parse `gameData` → insert/update `games` and `players` rows
   b. For each play in `liveData.plays.allPlays`:
      - Extract matchup, team context, score, inning
      - Determine runner positions from `runners` list at start of at-bat
      - For each `playEvent` where `isPitch == True`:
        * Extract all pitch fields from schema above
        * Extract `hitData` if present
        * Mark `is_ab_final_pitch` on the final event of each at-bat
        * Append to batch buffer
   c. Batch-insert pitch rows (500 at a time)
5. Print progress: games processed, pitches inserted, errors skipped

**Indexes created:**
- `pitches(pitcher_id)`
- `pitches(batter_id)`
- `pitches(game_pk)`
- `pitches(game_date)`

### File: `db/player_pitches.py`

```python
# Public API
db = PitchDB("pitches.db")

db.pitches_thrown(player_id)          # All pitches as pitcher → list of dicts
db.pitches_faced(player_id)           # All pitches as batter → list of dicts
db.pitches_all(player_id)             # Both thrown and faced, with a `role` field
db.player_info(player_id)             # Player metadata from players table
db.search_player(name)                # Find player_id by name (fuzzy)
```

Each method returns a list of dicts (one per pitch) with all columns from the schema above. Methods accept optional filters: `season=`, `sport=`, `date_range=`, `pitch_type=`.

---

## Critical Files

- [db/season_downloader.py](db/season_downloader.py) — reference only, not modified
- [db/scratchpad.py](db/scratchpad.py) — reference only, not modified
- `db/build_pitch_db.py` — **new file**
- `db/player_pitches.py` — **new file**
- `db/2025/major_league_baseball/` — source data (~2,961 files)
- `db/2025/triple_a/` — source data (~2,256 files)

---

## Verification

1. Run `python db/build_pitch_db.py --data-root db --seasons 2025` and confirm it completes without errors
2. Check row counts: `SELECT COUNT(*) FROM pitches` should be ~750k–1M rows
3. Pick a known player (e.g., Shohei Ohtani, id=660271): `db.pitches_thrown(660271)` and `db.pitches_faced(660271)` should return non-empty results
4. Spot-check a specific pitch — compare a few field values against the raw JSON file for that game
5. Verify `is_ab_final_pitch` correctly marks only 1 pitch per at-bat as final
6. Run builder twice — confirm second run skips already-processed games (idempotent)
