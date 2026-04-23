#!/usr/bin/env python3
"""Manual fixture input for upcoming matches."""

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH, ensure_runtime_dirs
from models.core import init_db


def make_match_id(home_team: str, away_team: str, league: str, kickoff: str) -> str:
    """Create a stable manual fixture id from teams, league, and kickoff."""
    raw = f"manual_{league}_{home_team}_{away_team}_{kickoff}"
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", raw).strip("_").lower()
    return slug[:160]


def normalize_kickoff(kickoff: str) -> str:
    """Normalize kickoff input to the DB's YYYY-MM-DD HH:MM style."""
    value = kickoff.strip().replace("T", " ")
    if len(value) >= 16:
        value = value[:16]
    if not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", value):
        raise ValueError("Kickoff must be in YYYY-MM-DD HH:MM format")
    return value


def add_fixture(home_team: str, away_team: str, league: str, kickoff: str, match_id: str = None):
    """Insert or update one scheduled fixture."""
    ensure_runtime_dirs()
    init_db()

    kickoff = normalize_kickoff(kickoff)
    match_id = match_id or make_match_id(home_team, away_team, league, kickoff)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO matches (match_id, home_team, away_team, league, kickoff, status)
        VALUES (?, ?, ?, ?, ?, 'scheduled')
        ON CONFLICT(match_id) DO UPDATE SET
            home_team = excluded.home_team,
            away_team = excluded.away_team,
            league = excluded.league,
            kickoff = excluded.kickoff,
            status = 'scheduled'
        """,
        (match_id, home_team.strip(), away_team.strip(), league.strip(), kickoff),
    )
    conn.commit()
    conn.close()

    print(f"Saved fixture: {home_team} vs {away_team} ({league}) - {kickoff}")
    print(f"Match ID: {match_id}")
    return match_id


def list_fixtures(include_stale: bool = False):
    """Print upcoming manual/API fixtures from the DB."""
    ensure_runtime_dirs()
    init_db()

    statuses = ("scheduled", "stale") if include_stale else ("scheduled",)
    placeholders = ",".join("?" for _ in statuses)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        f"""
        SELECT match_id, home_team, away_team, league, kickoff, status
        FROM matches
        WHERE status IN ({placeholders})
        ORDER BY kickoff, league, home_team
        """,
        statuses,
    )
    rows = c.fetchall()
    conn.close()

    if not rows:
        print("No fixtures found.")
        return []

    print("\nFixtures:")
    for i, row in enumerate(rows, 1):
        match_id, home, away, league, kickoff, status = row
        print(f"  {i}. {home} vs {away} ({league}) - {kickoff} [{status}]")
        print(f"     Match ID: {match_id}")
    return rows


def mark_fixture(match_id: str, status: str):
    """Set a fixture status, usually scheduled/stale."""
    if status not in {"scheduled", "stale"}:
        raise ValueError("Status must be scheduled or stale")

    ensure_runtime_dirs()
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE matches SET status = ? WHERE match_id = ?", (status, match_id))
    changed = c.rowcount
    conn.commit()
    conn.close()

    if changed:
        print(f"Updated {match_id} -> {status}")
    else:
        print(f"No fixture found for match_id: {match_id}")


def import_csv(path: str):
    """Import fixtures from CSV with home_team,away_team,league,kickoff columns."""
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"home_team", "away_team", "league", "kickoff"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {', '.join(sorted(missing))}")

        for row in reader:
            add_fixture(
                row["home_team"],
                row["away_team"],
                row["league"],
                row["kickoff"],
                row.get("match_id") or None,
            )
            count += 1

    print(f"\nImported {count} fixtures")


def interactive_input():
    """Prompt for fixtures until the user types done."""
    print("\nMANUAL FIXTURE INPUT")
    print("Kickoff format: YYYY-MM-DD HH:MM")
    print("Type done at the home team prompt to finish.\n")

    while True:
        home = input("Home team: ").strip()
        if home.lower() == "done":
            break
        away = input("Away team: ").strip()
        league = input("League (EPL/L1/Bundesliga/SerieA/LaLiga): ").strip()
        kickoff = input("Kickoff: ").strip()

        try:
            add_fixture(home, away, league, kickoff)
        except Exception as exc:
            print(f"Error: {exc}")


def main():
    parser = argparse.ArgumentParser(description="Manual fixture input")
    parser.add_argument("--list", "-l", action="store_true", help="List scheduled fixtures")
    parser.add_argument("--include-stale", action="store_true", help="Include stale fixtures when listing")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive fixture input")
    parser.add_argument("--csv", help="Import fixtures from CSV")
    parser.add_argument("--home", help="Home team")
    parser.add_argument("--away", help="Away team")
    parser.add_argument("--league", help="League code")
    parser.add_argument("--kickoff", help="Kickoff in YYYY-MM-DD HH:MM")
    parser.add_argument("--match-id", help="Optional custom match id")
    parser.add_argument("--stale", help="Mark a fixture stale by match id")
    parser.add_argument("--restore", help="Mark a fixture scheduled by match id")

    args = parser.parse_args()

    if args.list:
        list_fixtures(include_stale=args.include_stale)
    elif args.interactive:
        interactive_input()
    elif args.csv:
        import_csv(args.csv)
    elif args.stale:
        mark_fixture(args.stale, "stale")
    elif args.restore:
        mark_fixture(args.restore, "scheduled")
    elif args.home and args.away and args.league and args.kickoff:
        add_fixture(args.home, args.away, args.league, args.kickoff, args.match_id)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
