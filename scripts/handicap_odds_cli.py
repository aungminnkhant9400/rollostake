#!/usr/bin/env python3
"""Export and import Asian handicap odds for Range C/D picks.

Friend-style +0.5 selections are represented as AH +0.5. For example,
"Nott'm Forest AH +0.5" is equivalent to Nott'm Forest or draw.
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH, ensure_runtime_dirs
from models.core import init_db
from scripts.export_market_watchlist import _decision_prob, _range_for_required_odds, _score_distribution
from utils.match_resolver import resolve_match_id


HANDICAP_LINES = (-1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5)
FIELDNAMES = [
    "range_hint",
    "match_id",
    "league",
    "kickoff",
    "home_team",
    "away_team",
    "team",
    "line",
    "selection",
    "model_prob",
    "fair_odds",
    "min_odds_for_edge",
    "odds",
]


def _format_line(line: float) -> str:
    if line > 0:
        return f"+{line:g}"
    return f"{line:g}"


def _handicap_prob(dist: dict, home_side: bool, handicap: float):
    win_prob = 0.0
    loss_prob = 0.0
    for (home_goals, away_goals), prob in dist.items():
        if home_side:
            margin = home_goals + handicap - away_goals
        else:
            margin = away_goals + handicap - home_goals
        if margin > 0:
            win_prob += prob
        elif margin < 0:
            loss_prob += prob
    return _decision_prob(win_prob, loss_prob)


def _add_candidate(rows: list, fixture: dict, team: str, line: float, model_prob, min_edge: float):
    if model_prob is None or model_prob <= 0 or model_prob >= 1:
        return

    fair_odds = 1.0 / model_prob
    min_odds = (1.0 + min_edge) / model_prob
    line_label = _format_line(line)
    rows.append(
        {
            "range_hint": _range_for_required_odds(min_odds),
            "match_id": fixture["match_id"],
            "league": fixture["league"],
            "kickoff": fixture["kickoff"],
            "home_team": fixture["home_team"],
            "away_team": fixture["away_team"],
            "team": team,
            "line": line_label,
            "selection": f"{team} AH {line_label}",
            "model_prob": round(model_prob, 4),
            "fair_odds": round(fair_odds, 3),
            "min_odds_for_edge": round(min_odds, 3),
            "odds": "",
        }
    )


def handicap_candidates(min_edge: float, max_rows: int) -> list:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        """
        SELECT m.match_id, m.home_team, m.away_team, m.league, m.kickoff,
               p.lambda_h, p.lambda_a
        FROM matches m
        JOIN predictions p ON m.match_id = p.match_id
        WHERE m.status = 'scheduled'
        ORDER BY m.kickoff, m.league, m.home_team
        """
    )
    fixtures = [dict(row) for row in c.fetchall()]
    conn.close()

    rows = []
    for fixture in fixtures:
        lambda_h = max(float(fixture["lambda_h"] or 0), 0.01)
        lambda_a = max(float(fixture["lambda_a"] or 0), 0.01)
        dist = _score_distribution(lambda_h, lambda_a)
        for line in HANDICAP_LINES:
            _add_candidate(rows, fixture, fixture["home_team"], line, _handicap_prob(dist, True, line), min_edge)
            _add_candidate(rows, fixture, fixture["away_team"], line, _handicap_prob(dist, False, line), min_edge)

    rows = [row for row in rows if row["range_hint"] in {"C", "D"}]
    rows.sort(
        key=lambda row: (
            {"D": 0, "C": 1}.get(row["range_hint"], 2),
            row["min_odds_for_edge"],
            row["kickoff"],
        )
    )
    return rows[:max_rows]


def export_template(path: str, min_edge: float, max_rows: int) -> list:
    ensure_runtime_dirs()
    rows = handicap_candidates(min_edge=min_edge, max_rows=max_rows)
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


def _float_or_none(value: str):
    if value is None or str(value).strip() == "":
        return None
    return float(str(value).strip())


def save_ah_odds(rows: list, overwrite: bool = True, bookmaker: str = "manual") -> dict:
    ensure_runtime_dirs()
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0
    skipped = 0

    for row in rows:
        match_id = resolve_match_id(row, statuses=("scheduled",))
        selection = (row.get("selection") or "").strip()
        odds = _float_or_none(row.get("odds"))
        if not match_id or not selection or odds is None or odds <= 0:
            skipped += 1
            continue

        if overwrite:
            c.execute(
                """
                DELETE FROM odds
                WHERE match_id = ? AND market = 'AH' AND selection = ? AND bookmaker = ?
                """,
                (match_id, selection, bookmaker),
            )
        else:
            c.execute(
                """
                SELECT id FROM odds
                WHERE match_id = ? AND market = 'AH' AND selection = ? AND bookmaker = ?
                """,
                (match_id, selection, bookmaker),
            )
            if c.fetchone():
                skipped += 1
                continue

        c.execute(
            """
            INSERT INTO odds (match_id, bookmaker, market, selection, odds, implied_prob)
            VALUES (?, ?, 'AH', ?, ?, ?)
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
    return save_ah_odds(rows, overwrite=overwrite, bookmaker=bookmaker)


def interactive_input(min_edge: float, max_rows: int, overwrite: bool, bookmaker: str) -> dict:
    rows = []
    for row in handicap_candidates(min_edge=min_edge, max_rows=max_rows):
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
    return save_ah_odds(rows, overwrite=overwrite, bookmaker=bookmaker)


def main():
    parser = argparse.ArgumentParser(description="Asian handicap odds workflow for Range C/D picks")
    parser.add_argument("--export-template", default=None, help="Write an AH odds CSV template")
    parser.add_argument("--import-file", default=None, help="Import a filled AH odds CSV")
    parser.add_argument("--interactive", action="store_true", help="Prompt for AH odds candidate by candidate")
    parser.add_argument("--max-rows", type=int, default=60, help="Maximum candidates to export or prompt")
    parser.add_argument("--min-edge", type=float, default=0.05, help="Minimum required edge")
    parser.add_argument("--bookmaker", default="manual", help="Bookmaker label stored in SQLite")
    parser.add_argument("--no-overwrite", action="store_true", help="Keep existing identical AH odds rows")
    args = parser.parse_args()

    overwrite = not args.no_overwrite
    if args.import_file:
        summary = import_file(args.import_file, overwrite=overwrite, bookmaker=args.bookmaker)
        print(f"Imported AH odds: saved={summary['saved']} skipped={summary['skipped']}")
        return

    if args.interactive:
        summary = interactive_input(args.min_edge, args.max_rows, overwrite, args.bookmaker)
        print(f"Imported AH odds: saved={summary['saved']} skipped={summary['skipped']}")
        return

    output = args.export_template or "handicap_odds.csv"
    rows = export_template(output, args.min_edge, args.max_rows)
    print(f"Exported {len(rows)} AH candidates to {output}")
    print(f"Fill the odds column, then run: python scripts\\handicap_odds_cli.py --import-file {output}")


if __name__ == "__main__":
    main()
