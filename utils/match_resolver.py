"""Resolve match ids across fixture sources."""

import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config.paths import DB_PATH
from config.settings import load_settings
from utils.team_normalizer import normalize_team_name


def _fixture_timezone():
    try:
        settings = load_settings()
        return ZoneInfo(settings.get("fixture_timezone", "Asia/Macau"))
    except (Exception, ZoneInfoNotFoundError):
        return timezone(timedelta(hours=8), "Asia/Macau")


def parse_kickoff_utc(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(value[:16], "%Y-%m-%d %H:%M")
        except ValueError:
            try:
                parsed = datetime.strptime(value[:10], "%Y-%m-%d")
            except ValueError:
                return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_fixture_timezone())
    return parsed.astimezone(timezone.utc)


def format_kickoff_local(value: str, fallback: str = "TBD") -> str:
    kickoff_utc = parse_kickoff_utc(value)
    if kickoff_utc is None:
        return str(value or fallback)
    return kickoff_utc.astimezone(_fixture_timezone()).strftime("%Y-%m-%d %H:%M")


def kickoff_has_started(value: str, now_utc=None) -> bool:
    kickoff_utc = parse_kickoff_utc(value)
    if kickoff_utc is None:
        return False
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    return kickoff_utc <= now_utc.astimezone(timezone.utc)


def kickoff_result_window_elapsed(value: str, now_utc=None, minutes: int = 105) -> bool:
    kickoff_utc = parse_kickoff_utc(value)
    if kickoff_utc is None:
        return False
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    return kickoff_utc + timedelta(minutes=minutes) <= now_utc.astimezone(timezone.utc)


def _closest_kickoff_match(rows: list[tuple[str, str]], kickoff: str) -> str:
    target = parse_kickoff_utc(kickoff)
    if target is None:
        return ""

    best_match_id = ""
    best_delta = None
    max_delta_seconds = 18 * 60 * 60
    for match_id, candidate_kickoff in rows:
        candidate = parse_kickoff_utc(candidate_kickoff)
        if candidate is None:
            continue
        delta = abs((candidate - target).total_seconds())
        if delta <= max_delta_seconds and (best_delta is None or delta < best_delta):
            best_match_id = match_id
            best_delta = delta
    return best_match_id


def resolve_match_id(row: dict, statuses=None) -> str:
    """Resolve CSV row to the local SQLite match_id.

    Laptop fixtures may use football-data ids while server imports may create
    manual_* ids from the same home/away/kickoff. Prefer an existing match_id,
    but fall back to teams plus kickoff or teams only when needed.
    """
    match_id = (row.get("match_id") or "").strip()
    home_team = normalize_team_name((row.get("home_team") or "").strip())
    away_team = normalize_team_name((row.get("away_team") or "").strip())
    kickoff = (row.get("kickoff") or "").strip()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if match_id:
        c.execute("SELECT match_id, status FROM matches WHERE match_id = ?", (match_id,))
        found = c.fetchone()
        if found and (not statuses or found[1] in statuses):
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
            SELECT match_id, kickoff
            FROM matches
            WHERE {' AND '.join(clauses)}
            ORDER BY kickoff
            """,
            params,
        )
        closest = _closest_kickoff_match(c.fetchall(), kickoff)
        if closest:
            conn.close()
            return closest

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
