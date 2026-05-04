#!/usr/bin/env python3
"""Fetch real upcoming fixtures from football-data.org."""

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH, ensure_runtime_dirs
from models.core import init_db
from scrapers.manual_fixtures import add_fixture
from utils.team_normalizer import normalize_team_name


API_BASE = "https://api.football-data.org/v4"
LEAGUE_CODES = {
    "EPL": "PL",
    "L1": "FL1",
    "Bundesliga": "BL1",
    "SerieA": "SA",
    "LaLiga": "PD",
}
SLATE_COLUMNS = [
    "home_team",
    "away_team",
    "league",
    "kickoff",
    "home_odds",
    "draw_odds",
    "away_odds",
    "over_0_5_odds",
    "under_0_5_odds",
    "over_1_5_odds",
    "under_1_5_odds",
    "over_2_5_odds",
    "under_2_5_odds",
    "over_3_0_odds",
    "under_3_0_odds",
    "over_3_5_odds",
    "under_3_5_odds",
    "btts_yes_odds",
    "btts_no_odds",
    "home_over_0_5_odds",
    "home_under_0_5_odds",
    "home_over_1_5_odds",
    "home_under_1_5_odds",
    "home_over_2_5_odds",
    "home_under_2_5_odds",
    "away_over_0_5_odds",
    "away_under_0_5_odds",
    "away_over_1_5_odds",
    "away_under_1_5_odds",
    "away_over_2_5_odds",
    "away_under_2_5_odds",
    "handicap_line",
    "handicap_home_odds",
    "handicap_away_odds",
]


def _date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _request_json(url: str, token: str) -> dict:
    request = Request(url, headers={"X-Auth-Token": token, "Accept": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"football-data.org returned HTTP {exc.code}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"football-data.org request failed: {exc.reason}") from exc


def _kickoff_local(utc_date: str, timezone_name: str) -> str:
    raw = utc_date.replace("Z", "+00:00")
    kickoff_utc = datetime.fromisoformat(raw)
    if kickoff_utc.tzinfo is None:
        kickoff_utc = kickoff_utc.replace(tzinfo=timezone.utc)
    return kickoff_utc.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M")


def _fixture_from_match(match: dict, league: str, timezone_name: str) -> dict:
    home_team = normalize_team_name(match["homeTeam"]["name"])
    away_team = normalize_team_name(match["awayTeam"]["name"])
    kickoff = _kickoff_local(match["utcDate"], timezone_name)
    match_id = f"football_data_{match['id']}"
    return {
        "match_id": match_id,
        "home_team": home_team,
        "away_team": away_team,
        "league": league,
        "kickoff": kickoff,
    }


def _stale_existing_fixtures(start_date: str, end_date: str, leagues: list[str]) -> int:
    ensure_runtime_dirs()
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    placeholders = ",".join("?" for _ in leagues)
    c.execute(
        f"""
        UPDATE matches
        SET status = 'stale'
        WHERE status = 'scheduled'
          AND league IN ({placeholders})
          AND date(kickoff) BETWEEN date(?) AND date(?)
        """,
        [*leagues, start_date, end_date],
    )
    changed = c.rowcount
    conn.commit()
    conn.close()
    return changed


def fetch_fixtures(
    token: str,
    leagues: list[str],
    start_date: str,
    end_date: str,
    timezone_name: str,
) -> list[dict]:
    fixtures = []
    for league in leagues:
        competition = LEAGUE_CODES[league]
        query = urlencode({"dateFrom": start_date, "dateTo": end_date, "status": "SCHEDULED"})
        url = f"{API_BASE}/competitions/{competition}/matches?{query}"
        data = _request_json(url, token)
        for match in data.get("matches", []):
            fixtures.append(_fixture_from_match(match, league, timezone_name))
    fixtures.sort(key=lambda item: (item["kickoff"], item["league"], item["home_team"]))
    return fixtures


def save_fixtures(fixtures: list[dict], stale_window: bool, start_date: str, end_date: str, leagues: list[str]) -> dict:
    if stale_window:
        stale_count = _stale_existing_fixtures(start_date, end_date, leagues)
    else:
        stale_count = 0

    saved = 0
    for fixture in fixtures:
        add_fixture(
            fixture["home_team"],
            fixture["away_team"],
            fixture["league"],
            fixture["kickoff"],
            fixture["match_id"],
        )
        saved += 1
    return {"saved": saved, "staled": stale_count}


def export_slate(path: str, fixtures: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SLATE_COLUMNS)
        writer.writeheader()
        for fixture in fixtures:
            row = {column: "" for column in SLATE_COLUMNS}
            row.update(
                {
                    "home_team": fixture["home_team"],
                    "away_team": fixture["away_team"],
                    "league": fixture["league"],
                    "kickoff": fixture["kickoff"],
                }
            )
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Fetch upcoming fixtures from football-data.org")
    parser.add_argument("--days", type=int, default=7, help="Number of days ahead to fetch")
    parser.add_argument("--from-date", dest="from_date", type=_date, help="Start date YYYY-MM-DD")
    parser.add_argument("--to-date", dest="to_date", type=_date, help="End date YYYY-MM-DD")
    parser.add_argument("--timezone", default="Asia/Macau", help="Kickoff timezone for DB/CSV output")
    parser.add_argument(
        "--leagues",
        nargs="+",
        default=list(LEAGUE_CODES.keys()),
        choices=list(LEAGUE_CODES.keys()),
        help="Local league codes to fetch",
    )
    parser.add_argument("--export", help="Optional weekly slate CSV path to create with blank odds columns")
    parser.add_argument(
        "--no-stale-window",
        action="store_true",
        help="Do not mark old scheduled fixtures stale in the fetched date window",
    )
    args = parser.parse_args()

    token = os.getenv("FOOTBALL_DATA_TOKEN")
    if not token:
        raise SystemExit("Set FOOTBALL_DATA_TOKEN before running this script.")

    start = args.from_date or datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = args.to_date or (start + timedelta(days=args.days))
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    fixtures = fetch_fixtures(token, args.leagues, start_date, end_date, args.timezone)
    summary = save_fixtures(
        fixtures,
        stale_window=not args.no_stale_window,
        start_date=start_date,
        end_date=end_date,
        leagues=args.leagues,
    )

    if args.export:
        export_slate(args.export, fixtures)

    print("\nFixture fetch complete")
    print(f"Window: {start_date} to {end_date}")
    print(f"Leagues: {', '.join(args.leagues)}")
    print(f"Fixtures saved: {summary['saved']}")
    print(f"Old scheduled fixtures marked stale: {summary['staled']}")
    if args.export:
        print(f"Blank odds slate exported: {args.export}")


if __name__ == "__main__":
    main()
