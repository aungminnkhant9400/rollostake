"""Resolve match ids across fixture sources."""

import sqlite3

from config.paths import DB_PATH


def resolve_match_id(row: dict, statuses=None) -> str:
    """Resolve CSV row to the local SQLite match_id.

    Laptop fixtures may use football-data ids while server imports may create
    manual_* ids from the same home/away/kickoff. Prefer an existing match_id,
    but fall back to teams plus kickoff or teams only when needed.
    """
    match_id = (row.get("match_id") or "").strip()
    home_team = (row.get("home_team") or "").strip()
    away_team = (row.get("away_team") or "").strip()
    kickoff = (row.get("kickoff") or "").strip()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if match_id:
        c.execute("SELECT match_id FROM matches WHERE match_id = ?", (match_id,))
        found = c.fetchone()
        if found:
            conn.close()
            return found[0]

    clauses = ["home_team = ?", "away_team = ?"]
    params = [home_team, away_team]
    if not home_team or not away_team:
        conn.close()
        return ""

    if kickoff:
        clauses.append("kickoff = ?")
        params.append(kickoff)
    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        clauses.append(f"status IN ({placeholders})")
        params.extend(statuses)

    c.execute(
        f"""
        SELECT match_id
        FROM matches
        WHERE {' AND '.join(clauses)}
        ORDER BY kickoff
        LIMIT 1
        """,
        params,
    )
    found = c.fetchone()
    if not found and kickoff:
        clauses = ["home_team = ?", "away_team = ?"]
        params = [home_team, away_team]
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(statuses)
        c.execute(
            f"""
            SELECT match_id
            FROM matches
            WHERE {' AND '.join(clauses)}
            ORDER BY kickoff
            LIMIT 1
            """,
            params,
        )
        found = c.fetchone()

    conn.close()
    return found[0] if found else ""
