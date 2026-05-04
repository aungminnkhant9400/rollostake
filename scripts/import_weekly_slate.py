#!/usr/bin/env python3
"""Import a weekly fixture slate and its odds from one CSV file."""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH, ensure_runtime_dirs
from models.core import init_db
from scrapers.manual_fixtures import add_fixture
from utils.team_normalizer import normalize_team_name


REQUIRED_FIXTURE_COLUMNS = {"home_team", "away_team", "league", "kickoff"}
WIDE_ODDS_COLUMNS = {"home_odds", "draw_odds", "away_odds"}
LONG_ODDS_COLUMNS = {"market", "selection", "odds"}
TOTAL_LINES = ("0_5", "1_5", "2_5", "3_0", "3_5")
TEAM_TOTAL_LINES = ("0_5", "1_5", "2_5")


def _field(row: dict, *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _float_or_none(value: str):
    if value is None or str(value).strip() == "":
        return None
    return float(str(value).strip())


def _display_line(line_key: str) -> str:
    return line_key.replace("_", ".")


def _opposite_handicap(line: str) -> str:
    value = -float(line)
    if value > 0:
        return f"+{value:g}"
    return f"{value:g}"


def _insert_odds(match_id: str, market: str, selection: str, odds: float, overwrite: bool) -> bool:
    if odds is None or odds <= 0:
        return False

    implied = round(1.0 / odds, 4)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if overwrite:
        c.execute(
            """
            DELETE FROM odds
            WHERE match_id = ? AND market = ? AND selection = ? AND bookmaker = 'manual'
            """,
            (match_id, market, selection),
        )
    else:
        c.execute(
            """
            SELECT id FROM odds
            WHERE match_id = ? AND market = ? AND selection = ? AND bookmaker = 'manual'
            """,
            (match_id, market, selection),
        )
        if c.fetchone():
            conn.close()
            return False

    c.execute(
        """
        INSERT INTO odds (match_id, bookmaker, market, selection, odds, implied_prob)
        VALUES (?, 'manual', ?, ?, ?, ?)
        """,
        (match_id, market, selection, odds, implied),
    )
    conn.commit()
    conn.close()
    return True


def _validate_headers(fieldnames: list) -> str:
    columns = set(fieldnames or [])
    missing = REQUIRED_FIXTURE_COLUMNS - columns
    if missing:
        raise ValueError(f"CSV missing fixture columns: {', '.join(sorted(missing))}")

    if WIDE_ODDS_COLUMNS <= columns:
        return "wide"
    if LONG_ODDS_COLUMNS <= columns:
        return "long"

    raise ValueError(
        "CSV must include either home_odds,draw_odds,away_odds "
        "or market,selection,odds columns"
    )


def import_weekly_slate(path: str, overwrite: bool = True) -> dict:
    """Import fixtures and manual bookmaker odds from one CSV file."""
    ensure_runtime_dirs()
    init_db()

    fixture_ids = set()
    odds_saved = 0
    odds_skipped = 0
    rows_imported = 0

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        mode = _validate_headers(reader.fieldnames)

        for row in reader:
            home_team = normalize_team_name(_field(row, "home_team"))
            away_team = normalize_team_name(_field(row, "away_team"))
            league = _field(row, "league")
            kickoff = _field(row, "kickoff")
            match_id = _field(row, "match_id") or None

            if not home_team or not away_team or not league or not kickoff:
                odds_skipped += 1
                continue

            match_id = add_fixture(home_team, away_team, league, kickoff, match_id)
            fixture_ids.add(match_id)
            rows_imported += 1

            if mode == "wide":
                selections = [
                    ("1X2", f"{home_team} Win", _float_or_none(row.get("home_odds"))),
                    ("1X2", "Draw", _float_or_none(row.get("draw_odds"))),
                    ("1X2", f"{away_team} Win", _float_or_none(row.get("away_odds"))),
                    ("BTTS", "BTTS Yes", _float_or_none(row.get("btts_yes_odds"))),
                    ("BTTS", "BTTS No", _float_or_none(row.get("btts_no_odds"))),
                ]

                for line in TOTAL_LINES:
                    display_line = _display_line(line)
                    selections.append(("OU", f"Over {display_line}", _float_or_none(row.get(f"over_{line}_odds"))))
                    selections.append(("OU", f"Under {display_line}", _float_or_none(row.get(f"under_{line}_odds"))))

                for line in TEAM_TOTAL_LINES:
                    display_line = _display_line(line)
                    selections.extend([
                        ("TT", f"{home_team} O{display_line}", _float_or_none(row.get(f"home_over_{line}_odds"))),
                        ("TT", f"{home_team} U{display_line}", _float_or_none(row.get(f"home_under_{line}_odds"))),
                        ("TT", f"{away_team} O{display_line}", _float_or_none(row.get(f"away_over_{line}_odds"))),
                        ("TT", f"{away_team} U{display_line}", _float_or_none(row.get(f"away_under_{line}_odds"))),
                    ])
                
                # Handicap
                handicap_line = _field(row, "handicap_line")
                if handicap_line:
                    selections.append(("AH", f"{home_team} AH {handicap_line}", _float_or_none(row.get("handicap_home_odds"))))
                    selections.append(("AH", f"{away_team} AH {_opposite_handicap(handicap_line)}", _float_or_none(row.get("handicap_away_odds"))))
            else:
                market = _field(row, "market") or "1X2"
                selection = _field(row, "selection")
                odds_value = _float_or_none(row.get("odds"))

                if market == "1X2" and selection.lower() in {"home", "1"}:
                    selection = f"{home_team} Win"
                elif market == "1X2" and selection.lower() in {"away", "2"}:
                    selection = f"{away_team} Win"
                elif market == "1X2" and selection.lower() in {"draw", "x"}:
                    selection = "Draw"
                elif market == "AH" and selection.lower() in {"home", "1"}:
                    selection = f"{home_team} AH {_field(row, 'handicap_line') or '0'}"
                elif market == "AH" and selection.lower() in {"away", "2"}:
                    line = _field(row, "handicap_line") or "0"
                    selection = f"{away_team} AH {_opposite_handicap(line)}"
                elif market == "TT":
                    line = _field(row, "line") or _field(row, "total_line")
                    direction = _field(row, "direction").lower()
                    team_ref = selection.lower()
                    if line and direction in {"over", "under", "o", "u"} and team_ref in {"home", "away"}:
                        team = home_team if team_ref == "home" else away_team
                        short = "O" if direction in {"over", "o"} else "U"
                        selection = f"{team} {short}{line}"

                selections = [(market, selection, odds_value)]

            for market, selection, odds in selections:
                if not selection or odds is None:
                    continue
                if _insert_odds(match_id, market, selection, odds, overwrite):
                    odds_saved += 1
                else:
                    odds_skipped += 1

    return {
        "rows_imported": rows_imported,
        "fixtures": len(fixture_ids),
        "odds_saved": odds_saved,
        "odds_skipped": odds_skipped,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Import a weekly slate CSV with fixtures and odds in one step."
    )
    parser.add_argument("csv_path", help="Path to weekly slate CSV")
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Skip odds rows that already exist instead of replacing them",
    )
    args = parser.parse_args()

    summary = import_weekly_slate(args.csv_path, overwrite=not args.no_overwrite)
    print("\nWeekly slate import complete")
    print(f"Rows imported: {summary['rows_imported']}")
    print(f"Fixtures upserted: {summary['fixtures']}")
    print(f"Odds saved: {summary['odds_saved']}")
    print(f"Odds skipped: {summary['odds_skipped']}")


if __name__ == "__main__":
    main()
