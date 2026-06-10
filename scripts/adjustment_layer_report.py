#!/usr/bin/env python3
"""Show the 14-layer adjustment audit for current scheduled predictions."""

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from models.core import init_db


def main():
    parser = argparse.ArgumentParser(description="Report 14-layer model adjustments")
    parser.add_argument("--match-id", help="Optional match_id filter")
    parser.add_argument("--all", action="store_true", help="Show inactive layers too")
    parser.add_argument("--all-statuses", action="store_true", help="Include stale, finished, and completed fixtures")
    args = parser.parse_args()

    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    clauses = []
    params = []
    if not args.all_statuses:
        clauses.append("m.status = 'scheduled'")
    if args.match_id:
        clauses.append("m.match_id = ?")
        params.append(args.match_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    c.execute(
        f"""
        SELECT m.match_id, m.home_team, m.away_team, m.league, m.kickoff,
               p.lambda_h, p.lambda_a,
               SUM(CASE WHEN l.active = 1 THEN 1 ELSE 0 END) AS active_layers,
               COUNT(l.id) AS total_layers
        FROM matches m
        JOIN predictions p ON m.match_id = p.match_id
        LEFT JOIN prediction_adjustment_layers l ON m.match_id = l.match_id
        {where}
        GROUP BY m.match_id
        ORDER BY m.kickoff, m.league, m.home_team
        """,
        params,
    )
    matches = c.fetchall()
    if not matches:
        print("No prediction layer rows found. Run: python main.py --skip-scrape --no-fatigue")
        conn.close()
        return

    for match in matches:
        print("=" * 100)
        print(
            f"{match['match_id']} | {match['home_team']} vs {match['away_team']} "
            f"({match['league']}) | {match['kickoff']}"
        )
        print(
            f"lambda_h={float(match['lambda_h'] or 0):.3f} "
            f"lambda_a={float(match['lambda_a'] or 0):.3f} | "
            f"active layers {int(match['active_layers'] or 0)}/{int(match['total_layers'] or 0)}"
        )
        layer_where = "match_id = ?"
        layer_params = [match["match_id"]]
        if not args.all:
            layer_where += " AND active = 1"
        c.execute(
            f"""
            SELECT layer_no, layer_name, home_before, away_before, home_after, away_after, note, active
            FROM prediction_adjustment_layers
            WHERE {layer_where}
            ORDER BY layer_no
            """,
            layer_params,
        )
        rows = c.fetchall()
        if not rows:
            print("  No active material layer moves.")
            continue
        for row in rows:
            marker = "*" if row["active"] else "-"
            print(
                f"  {marker} L{row['layer_no']:02d} {row['layer_name']}: "
                f"H {float(row['home_before']):.3f}->{float(row['home_after']):.3f}, "
                f"A {float(row['away_before']):.3f}->{float(row['away_after']):.3f} | "
                f"{row['note']}"
            )

    conn.close()


if __name__ == "__main__":
    main()
