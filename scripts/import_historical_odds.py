#!/usr/bin/env python3
"""
Import historical odds from football-data.co.uk into the odds table.

Fetches CSV files for multiple seasons and leagues, extracts bookmaker odds,
and saves them to the database for backtesting.

Supported bookmakers from football-data:
  B365  = Bet365
  PS    = Pinnacle
  BF    = Betfair
  Max   = Market maximum
  Avg   = Market average
  BFE   = Betfair Exchange (closing)

Supported markets:
  1X2   = Home/Draw/Away
  OU    = Over/Under 2.5 goals
  AH    = Asian Handicap (line from AHh/AHCh, odds from *AHH/*AHA)

Usage:
    python scripts/import_historical_odds.py --seasons 2122 2223 2324 2425 2526
    python scripts/import_historical_odds.py --seasons 2526 --leagues EPL
"""

import argparse
import csv
import io
import sqlite3
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from models.core import init_db
from utils.team_normalizer import normalize_team_name


SEASONS = {
    "2122": "2021-22",
    "2223": "2022-23",
    "2324": "2023-24",
    "2425": "2024-25",
    "2526": "2025-26",
}

LEAGUE_CODES = {
    "EPL": "E0",
    "L1": "F1",
    "Bundesliga": "D1",
    "SerieA": "I1",
    "LaLiga": "SP1",
}

BASE_URL = "https://www.football-data.co.uk/mmz4281/{season}/{league}.csv"

# Odds column mappings: (csv_col, bookmaker, market, selection_template)
ODDS_COLUMNS = [
    # 1X2
    ("B365H", "B365", "1X2", "{home} Win"),
    ("B365D", "B365", "1X2", "Draw"),
    ("B365A", "B365", "1X2", "{away} Win"),
    ("PSH", "Pinnacle", "1X2", "{home} Win"),
    ("PSD", "Pinnacle", "1X2", "Draw"),
    ("PSA", "Pinnacle", "1X2", "{away} Win"),
    ("MaxH", "Max", "1X2", "{home} Win"),
    ("MaxD", "Max", "1X2", "Draw"),
    ("MaxA", "Max", "1X2", "{away} Win"),
    ("AvgH", "Avg", "1X2", "{home} Win"),
    ("AvgD", "Avg", "1X2", "Draw"),
    ("AvgA", "Avg", "1X2", "{away} Win"),
    ("BFEH", "BFE", "1X2", "{home} Win"),
    ("BFED", "BFE", "1X2", "Draw"),
    ("BFEA", "BFE", "1X2", "{away} Win"),
    # Over/Under 2.5
    ("B365>2.5", "B365", "OU", "Over 2.5"),
    ("B365<2.5", "B365", "OU", "Under 2.5"),
    ("Max>2.5", "Max", "OU", "Over 2.5"),
    ("Max<2.5", "Max", "OU", "Under 2.5"),
    ("Avg>2.5", "Avg", "OU", "Over 2.5"),
    ("Avg<2.5", "Avg", "OU", "Under 2.5"),
    ("BFE>2.5", "BFE", "OU", "Over 2.5"),
    ("BFE<2.5", "BFE", "OU", "Under 2.5"),
]

# Asian handicap odds need the separate handicap-line column. The *AHH/*AHA
# columns are prices, not handicap values.
AH_ODDS_COLUMNS = [
    # Opening odds use AHh
    ("B365AHH", "B365", "home", "AHh"),
    ("B365AHA", "B365", "away", "AHh"),
    ("PAHH", "Pinnacle", "home", "AHh"),
    ("PAHA", "Pinnacle", "away", "AHh"),
    ("MaxAHH", "Max", "home", "AHh"),
    ("MaxAHA", "Max", "away", "AHh"),
    ("AvgAHH", "Avg", "home", "AHh"),
    ("AvgAHA", "Avg", "away", "AHh"),
    ("BFEAHH", "BFE", "home", "AHh"),
    ("BFEAHA", "BFE", "away", "AHh"),
    # Closing odds use AHCh
    ("B365CAHH", "B365C", "home", "AHCh"),
    ("B365CAHA", "B365C", "away", "AHCh"),
    ("PCAHH", "PinnacleC", "home", "AHCh"),
    ("PCAHA", "PinnacleC", "away", "AHCh"),
    ("MaxCAHH", "MaxC", "home", "AHCh"),
    ("MaxCAHA", "MaxC", "away", "AHCh"),
    ("AvgCAHH", "AvgC", "home", "AHCh"),
    ("AvgCAHA", "AvgC", "away", "AHCh"),
    ("BFECAHH", "BFEC", "home", "AHCh"),
    ("BFECAHA", "BFEC", "away", "AHCh"),
]


def safe_float(value) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(str(value).strip())
    except ValueError:
        return None


def valid_odds(odds: float) -> bool:
    return 1.01 <= odds <= 100.0


def valid_handicap_line(line: float) -> bool:
    # The current model/settlement supports full and half Asian lines. Quarter
    # lines need half-win/half-loss accounting, so skip them for now.
    return -5.0 <= line <= 5.0 and abs((line * 2) - round(line * 2)) < 1e-9


def format_line(line: float) -> str:
    return f"{line:+g}"


def fetch_csv(league: str, season: str) -> list[dict]:
    url = BASE_URL.format(season=season, league=LEAGUE_CODES[league])
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()
        content = resp.content.decode("utf-8", errors="ignore")
        return list(csv.DictReader(io.StringIO(content)))
    except Exception as e:
        print(f"  ERROR fetching {league} {season}: {e}")
        return []


def parse_date(date_str: str) -> str:
    if not date_str:
        return ""
    if "/" in date_str:
        day, month, year = date_str.split("/")
        year = "20" + year if len(year) == 2 else year
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return date_str


def save_odds_rows(rows: list[dict], overwrite: bool = True, dry_run: bool = False) -> dict:
    if dry_run:
        return {"saved": 0, "skipped": 0}

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0
    skipped = 0

    for row in rows:
        if overwrite:
            c.execute(
                """
                DELETE FROM odds
                WHERE match_id = ? AND bookmaker = ? AND market = ? AND selection = ?
                """,
                (row["match_id"], row["bookmaker"], row["market"], row["selection"]),
            )
        else:
            c.execute(
                """
                SELECT id FROM odds
                WHERE match_id = ? AND bookmaker = ? AND market = ? AND selection = ?
                """,
                (row["match_id"], row["bookmaker"], row["market"], row["selection"]),
            )
            if c.fetchone():
                skipped += 1
                continue

        c.execute(
            """
            INSERT INTO odds (match_id, bookmaker, market, selection, odds, implied_prob)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (row["match_id"], row["bookmaker"], row["market"], row["selection"], row["odds"], round(1.0 / row["odds"], 4)),
        )
        saved += 1

    conn.commit()
    conn.close()
    return {"saved": saved, "skipped": skipped}


def process_season(league: str, season: str, overwrite: bool = True, dry_run: bool = False) -> dict:
    season_label = SEASONS.get(season, season)
    print(f"\nFetching {league} {season_label}...")
    rows = fetch_csv(league, season)
    if not rows:
        return {"saved": 0, "skipped": 0, "matches": 0, "odds_rows": 0}

    odds_rows = []
    match_count = 0
    invalid_odds = 0
    invalid_ah_lines = 0

    for row in rows:
        if not row.get("HomeTeam") or not row.get("AwayTeam"):
            continue
        if not row.get("FTHG") or not row.get("FTAG"):
            continue

        home = normalize_team_name(row["HomeTeam"].strip())
        away = normalize_team_name(row["AwayTeam"].strip())
        date = parse_date(row.get("Date", ""))
        if not date:
            continue

        match_id = f"{home}_vs_{away}_{date}"
        match_count += 1

        for csv_col, bookmaker, market, selection_template in ODDS_COLUMNS:
            odds = safe_float(row.get(csv_col, ""))
            if odds is None:
                continue
            if not valid_odds(odds):
                invalid_odds += 1
                continue

            selection = selection_template.format(home=home, away=away)
            odds_rows.append({
                "match_id": match_id,
                "bookmaker": bookmaker,
                "market": market,
                "selection": selection,
                "odds": round(odds, 3),
            })

        for csv_col, bookmaker, side, line_col in AH_ODDS_COLUMNS:
            odds = safe_float(row.get(csv_col, ""))
            if odds is None:
                continue
            if not valid_odds(odds):
                invalid_odds += 1
                continue

            home_line = safe_float(row.get(line_col, ""))
            if home_line is None or not valid_handicap_line(home_line):
                invalid_ah_lines += 1
                continue

            if side == "home":
                selection = f"{home} AH {format_line(home_line)}"
            else:
                selection = f"{away} AH {format_line(-home_line)}"

            odds_rows.append({
                "match_id": match_id,
                "bookmaker": bookmaker,
                "market": "AH",
                "selection": selection,
                "odds": round(odds, 3),
            })

    summary = save_odds_rows(odds_rows, overwrite=overwrite, dry_run=dry_run)
    summary["matches"] = match_count
    summary["odds_rows"] = len(odds_rows)
    summary["invalid_odds"] = invalid_odds
    summary["invalid_ah_lines"] = invalid_ah_lines
    return summary


def main():
    parser = argparse.ArgumentParser(description="Import historical odds from football-data.co.uk")
    parser.add_argument("--seasons", nargs="+", default=["2526"], help="Season codes (e.g., 2122 2223)")
    parser.add_argument("--leagues", nargs="+", default=list(LEAGUE_CODES.keys()), help="League codes")
    parser.add_argument("--no-overwrite", action="store_true", help="Skip existing odds")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate rows without writing to the database")
    args = parser.parse_args()

    print("=" * 60)
    print("HISTORICAL ODDS IMPORTER")
    print("=" * 60)

    init_db()
    total_saved = 0
    total_matches = 0

    for season in args.seasons:
        for league in args.leagues:
            if league not in LEAGUE_CODES:
                print(f"Skipping unknown league: {league}")
                continue
            result = process_season(league, season, overwrite=not args.no_overwrite, dry_run=args.dry_run)
            print(
                f"  Matches: {result['matches']} | Odds rows: {result['odds_rows']} | "
                f"Saved: {result['saved']} | Skipped: {result['skipped']} | "
                f"Bad odds: {result['invalid_odds']} | Bad AH lines: {result['invalid_ah_lines']}"
            )
            total_saved += result["saved"]
            total_matches += result["matches"]

    print("\n" + "=" * 60)
    print(f"TOTAL: {total_matches} matches processed, {total_saved} odds rows saved")
    print("=" * 60)


if __name__ == "__main__":
    main()
