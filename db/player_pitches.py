#!/usr/bin/env python3
"""
player_pitches.py

Python API for querying the player-centric pitch database built by build_pitch_db.py.

Usage:
    from player_pitches import PitchDB

    db = PitchDB("pitches.db")

    # Find a player by name
    results = db.search_player("Shohei Ohtani")
    # [{'player_id': 660271, 'full_name': 'Shohei Ohtani', ...}]

    player_id = results[0]["player_id"]

    # Every pitch he threw as a pitcher
    thrown = db.pitches_thrown(player_id)

    # Every pitch he faced as a batter
    faced = db.pitches_faced(player_id)

    # Both, with a 'role' field ('pitcher' or 'batter')
    all_pitches = db.pitches_all(player_id)

    # Optional filters on any of the above:
    thrown = db.pitches_thrown(player_id, pitch_type="FF", season=2025)
    faced  = db.pitches_faced(player_id, sport="major_league_baseball")
    # date_range is an inclusive (start, end) tuple of "YYYY-MM-DD" strings
    subset = db.pitches_thrown(player_id, date_range=("2025-04-01", "2025-06-30"))
"""

import json
import sqlite3
from typing import Optional


class PitchDB:
    def __init__(self, db_path: str = "pitches.db"):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------
    # Internal query builder
    # ------------------------------------------------------------------

    def _query(
        self,
        where_clauses: list[str],
        params: list,
        extra_select: str = "",
        order_by: str = "game_date, at_bat_index, pitch_index",
    ) -> list[dict]:
        select = "SELECT *" + (f", {extra_select}" if extra_select else "")
        where  = " AND ".join(where_clauses) if where_clauses else "1"
        sql    = f"{select} FROM pitches WHERE {where} ORDER BY {order_by}"
        conn   = self._get_conn()
        rows   = conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            # Deserialize fielding_credits JSON blob back to a list
            if d.get("fielding_credits"):
                try:
                    d["fielding_credits"] = json.loads(d["fielding_credits"])
                except (json.JSONDecodeError, TypeError):
                    pass
            result.append(d)
        return result

    def _apply_filters(
        self,
        where: list[str],
        params: list,
        season: Optional[int],
        sport: Optional[str],
        date_range: Optional[tuple[str, str]],
        pitch_type: Optional[str],
    ):
        if season is not None:
            where.append("season = ?")
            params.append(season)
        if sport is not None:
            where.append("sport = ?")
            params.append(sport)
        if date_range is not None:
            where.append("game_date BETWEEN ? AND ?")
            params.extend(date_range)
        if pitch_type is not None:
            where.append("pitch_type_code = ?")
            params.append(pitch_type)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def pitches_thrown(
        self,
        player_id: int,
        season: Optional[int] = None,
        sport: Optional[str] = None,
        date_range: Optional[tuple[str, str]] = None,
        pitch_type: Optional[str] = None,
    ) -> list[dict]:
        """All pitches thrown BY this player (as pitcher)."""
        where  = ["pitcher_id = ?"]
        params = [player_id]
        self._apply_filters(where, params, season, sport, date_range, pitch_type)
        return self._query(where, params)

    def pitches_faced(
        self,
        player_id: int,
        season: Optional[int] = None,
        sport: Optional[str] = None,
        date_range: Optional[tuple[str, str]] = None,
        pitch_type: Optional[str] = None,
    ) -> list[dict]:
        """All pitches faced BY this player (as batter)."""
        where  = ["batter_id = ?"]
        params = [player_id]
        self._apply_filters(where, params, season, sport, date_range, pitch_type)
        return self._query(where, params)

    def pitches_all(
        self,
        player_id: int,
        season: Optional[int] = None,
        sport: Optional[str] = None,
        date_range: Optional[tuple[str, str]] = None,
        pitch_type: Optional[str] = None,
    ) -> list[dict]:
        """All pitches where this player was either pitcher OR batter.

        Each row includes a synthetic 'role' field: 'pitcher' or 'batter'.
        If the player appeared as both in the same pitch (unusual but possible
        in practice game data), the row will appear twice.
        """
        where_p  = ["pitcher_id = ?"]
        params_p = [player_id]
        self._apply_filters(where_p, params_p, season, sport, date_range, pitch_type)
        thrown = self._query(where_p, params_p, extra_select="'pitcher' AS role")

        where_b  = ["batter_id = ?"]
        params_b = [player_id]
        self._apply_filters(where_b, params_b, season, sport, date_range, pitch_type)
        faced = self._query(where_b, params_b, extra_select="'batter' AS role")

        combined = thrown + faced
        combined.sort(key=lambda r: (r.get("game_date") or "", r.get("at_bat_index") or 0, r.get("pitch_index") or 0))
        return combined

    def player_info(self, player_id: int) -> Optional[dict]:
        """Player metadata from the players table."""
        conn = self._get_conn()
        row  = conn.execute(
            "SELECT * FROM players WHERE player_id = ?", (player_id,)
        ).fetchone()
        return dict(row) if row else None

    def search_player(self, name: str) -> list[dict]:
        """Find players by name (case-insensitive substring match).

        Returns a list of matching players sorted by full_name.
        Use the returned player_id for further queries.
        """
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM players WHERE LOWER(full_name) LIKE LOWER(?) ORDER BY full_name",
            (f"%{name}%",),
        ).fetchall()
        return [dict(r) for r in rows]

    def game_info(self, game_pk: int) -> Optional[dict]:
        """Game metadata from the games table."""
        conn = self._get_conn()
        row  = conn.execute(
            "SELECT * FROM games WHERE game_pk = ?", (game_pk,)
        ).fetchone()
        return dict(row) if row else None

    def pitch_count(self, player_id: int) -> dict:
        """Quick summary: how many pitches thrown vs faced."""
        conn    = self._get_conn()
        thrown  = conn.execute(
            "SELECT COUNT(*) FROM pitches WHERE pitcher_id = ?", (player_id,)
        ).fetchone()[0]
        faced   = conn.execute(
            "SELECT COUNT(*) FROM pitches WHERE batter_id = ?", (player_id,)
        ).fetchone()[0]
        return {"pitcher": thrown, "batter": faced, "total": thrown + faced}

    # ------------------------------------------------------------------
    # Convenience stats
    # ------------------------------------------------------------------

    def pitch_mix(self, player_id: int, role: str = "pitcher", **filter_kwargs) -> list[dict]:
        """Breakdown of pitch types used (as pitcher or batter).

        Returns a list of dicts: [{pitch_type_code, pitch_type_desc, count, pct}]
        """
        if role == "pitcher":
            pitches = self.pitches_thrown(player_id, **filter_kwargs)
        else:
            pitches = self.pitches_faced(player_id, **filter_kwargs)

        from collections import Counter
        counts = Counter()
        descs  = {}
        for p in pitches:
            code = p.get("pitch_type_code") or "UN"
            counts[code] += 1
            descs[code]   = p.get("pitch_type_desc") or code

        total = sum(counts.values()) or 1
        return sorted(
            [
                {
                    "pitch_type_code": code,
                    "pitch_type_desc": descs[code],
                    "count": cnt,
                    "pct":   round(cnt / total * 100, 1),
                }
                for code, cnt in counts.items()
            ],
            key=lambda x: -x["count"],
        )
