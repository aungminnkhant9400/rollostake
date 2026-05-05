#!/usr/bin/env python3
"""Export and import team-total odds for official Range C/D picks.

The model can price team totals without bookmaker data, but picks should only be
created after real odds are entered. This script exports the best team-total
candidates, then imports filled odds as TT market rows.
"""

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH, ensure_runtime_dirs
from models.core import init_db
from scripts.export_market_watchlist import build_watchlist


FIELDNAMES = [
    "range_hint",
    "match_id",
    "league",
    "kickoff",
    "home_team",
    "away_team",
    "team",
    "direction",
    "line",
    "selection",
    "model_prob",
    "fair_odds",
    "min_odds_for_edge",
    "odds",
]


def _parse_team_total(selection: str) -> dict:
    match = re.match(r"^(?P<team>.+)\s+(?P<direction>[OU])(?P<line>\d+(?:\.\d+)?)$", selection.strip())
    if not match:
        return {"team": "", "direction": "", "line": ""}
    direction = "Over" if match.group("direction") == "O" else "Under"
    return {
        "team": match.group("team"),
        "direction": direction,
        "line": match.group("line"),
    }


def team_total_candidates(min_edge: float, max_rows: int) -> list:
    rows = []
    for row in build_watchlist(min_edge=min_edge, max_rows=1000):
        if row["market"] != "TT":
            continue
        parsed = _parse_team_total(row["selection"])
        rows.append(
            {
                "range_hint": row["range_hint"],
                "match_id": row["match_id"],
                "league": row["league"],
                "kickoff": row["kickoff"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "team": parsed["team"],
                "direction": parsed["direction"],
                "line": parsed["line"],
                "selection": row["selection"],
                "model_prob": row["model_prob"],
                "fair_odds": row["fair_odds"],
                "min_odds_for_edge": row["min_odds_for_edge"],
                "odds": "",
            }
        )
        if len(rows) >= max_rows:
            break
    return rows


def export_template(path: str, min_edge: float, max_rows: int) -> list:
    ensure_runtime_dirs()
    init_db()
    rows = team_total_candidates(min_edge=min_edge, max_rows=max_rows)
    existing_odds = {}
    existing_path = Path(path)
    if existing_path.exists():
        with existing_path.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                odds = (row.get("odds") or "").strip()
                if odds:
                    existing_odds[(row.get("match_id"), row.get("selection"))] = odds
    for row in rows:
        row["odds"] = existing_odds.get((row.get("match_id"), row.get("selection")), row["odds"])
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def _lookup_match_id(row: dict) -> str:
    match_id = (row.get("match_id") or "").strip()
    if match_id:
        return match_id

    home_team = (row.get("home_team") or "").strip()
    away_team = (row.get("away_team") or "").strip()
    kickoff = (row.get("kickoff") or "").strip()
    if not home_team or not away_team or not kickoff:
        return ""

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        SELECT match_id
        FROM matches
        WHERE home_team = ? AND away_team = ? AND kickoff = ?
        """,
        (home_team, away_team, kickoff),
    )
    found = c.fetchone()
    conn.close()
    return found[0] if found else ""


def _float_or_none(value: str):
    if value is None or str(value).strip() == "":
        return None
    return float(str(value).strip())


def save_tt_odds(rows: list, overwrite: bool = True, bookmaker: str = "manual") -> dict:
    ensure_runtime_dirs()
    init_db()
    saved = 0
    skipped = 0
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for row in rows:
        odds = _float_or_none(row.get("odds"))
        selection = (row.get("selection") or "").strip()
        match_id = _lookup_match_id(row)
        if not match_id or not selection or odds is None or odds <= 0:
            skipped += 1
            continue

        if overwrite:
            c.execute(
                """
                DELETE FROM odds
                WHERE match_id = ? AND market = 'TT' AND selection = ? AND bookmaker = ?
                """,
                (match_id, selection, bookmaker),
            )
        else:
            c.execute(
                """
                SELECT id FROM odds
                WHERE match_id = ? AND market = 'TT' AND selection = ? AND bookmaker = ?
                """,
                (match_id, selection, bookmaker),
            )
            if c.fetchone():
                skipped += 1
                continue

        c.execute(
            """
            INSERT INTO odds (match_id, bookmaker, market, selection, odds, implied_prob)
            VALUES (?, ?, 'TT', ?, ?, ?)
            """,
            (match_id, bookmaker, selection, odds, round(1.0 / odds, 4)),
        )
        saved += 1

    conn.commit()
    conn.close()
    return {"saved": saved, "skipped": skipped}


def import_file(path: str, overwrite: bool = True, bookmaker: str = "manual") -> dict:
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return save_tt_odds(rows, overwrite=overwrite, bookmaker=bookmaker)


def interactive_input(min_edge: float, max_rows: int, overwrite: bool, bookmaker: str) -> dict:
    rows = []
    for row in team_total_candidates(min_edge=min_edge, max_rows=max_rows):
        prompt = (
            f"{row['range_hint']} {row['home_team']} vs {row['away_team']} | "
            f"{row['selection']} | model {float(row['model_prob']):.1%} | "
            f"need >= {float(row['min_odds_for_edge']):.2f} | odds: "
        )
        value = input(prompt).strip()
        if value.lower() in {"q", "quit", "exit"}:
            break
        row["odds"] = value
        rows.append(row)
    return save_tt_odds(rows, overwrite=overwrite, bookmaker=bookmaker)


def main():
    parser = argparse.ArgumentParser(description="Team-total odds workflow for Range C/D picks")
    parser.add_argument("--export-template", default=None, help="Write a team-total odds CSV template")
    parser.add_argument("--import-file", default=None, help="Import a filled team-total odds CSV")
    parser.add_argument("--interactive", action="store_true", help="Prompt for team-total odds candidate by candidate")
    parser.add_argument("--max-rows", type=int, default=60, help="Maximum candidates to export or prompt")
    parser.add_argument("--min-edge", type=float, default=0.05, help="Minimum required edge")
    parser.add_argument("--bookmaker", default="manual", help="Bookmaker label stored in SQLite")
    parser.add_argument("--no-overwrite", action="store_true", help="Keep existing identical TT odds rows")
    args = parser.parse_args()

    overwrite = not args.no_overwrite
    if args.import_file:
        summary = import_file(args.import_file, overwrite=overwrite, bookmaker=args.bookmaker)
        print(f"Imported TT odds: saved={summary['saved']} skipped={summary['skipped']}")
        return

    if args.interactive:
        summary = interactive_input(args.min_edge, args.max_rows, overwrite, args.bookmaker)
        print(f"Imported TT odds: saved={summary['saved']} skipped={summary['skipped']}")
        return

    output = args.export_template or "team_total_odds.csv"
    rows = export_template(output, args.min_edge, args.max_rows)
    print(f"Exported {len(rows)} team-total candidates to {output}")
    print(f"Fill the odds column, then run: python scripts\\team_total_odds_cli.py --import-file {output}")


if __name__ == "__main__":
    main()
