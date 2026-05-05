#!/usr/bin/env python3
"""Report odds coverage and missing high-priority market prices."""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from models.core import init_db


def _read_missing(path: str, limit: int) -> list:
    file_path = Path(path)
    if not file_path.exists():
        return []
    with file_path.open(newline="", encoding="utf-8-sig") as f:
        rows = []
        for row in csv.DictReader(f):
            if (row.get("odds") or "").strip():
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows


def _format_candidate(row: dict) -> str:
    needed = row.get("min_odds_for_edge") or "?"
    selection = row.get("selection") or ""
    match = f"{row.get('home_team')} vs {row.get('away_team')}"
    kickoff = row.get("kickoff") or ""
    return f"- {kickoff} | {match} | {selection} | need >= {needed}"


def build_report(limit: int = 20) -> str:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM matches WHERE status = 'scheduled'")
    scheduled = c.fetchone()[0]
    c.execute(
        """
        SELECT market, COUNT(*)
        FROM odds o
        JOIN matches m ON o.match_id = m.match_id
        WHERE m.status = 'scheduled'
        GROUP BY market
        ORDER BY market
        """
    )
    odds_by_market = c.fetchall()
    c.execute("SELECT market, COUNT(*) FROM picks GROUP BY market ORDER BY market")
    picks_by_market = c.fetchall()
    c.execute(
        """
        SELECT p.range_code, p.market, p.selection, p.odds, p.edge_pct, p.quality,
               m.home_team, m.away_team, m.kickoff
        FROM picks p
        JOIN matches m ON p.match_id = m.match_id
        ORDER BY p.range_code, m.kickoff, p.edge_pct DESC
        """
    )
    picks = c.fetchall()
    conn.close()

    lines = [
        "# Odds Coverage Report",
        "",
        f"Scheduled fixtures: {scheduled}",
        "",
        "## Odds by Market",
    ]
    if odds_by_market:
        lines.extend(f"- {market}: {count}" for market, count in odds_by_market)
    else:
        lines.append("- none")

    lines.extend(["", "## Official Picks by Market"])
    if picks_by_market:
        lines.extend(f"- {market}: {count}" for market, count in picks_by_market)
    else:
        lines.append("- none")

    lines.extend(["", "## Official Card"])
    for range_code, market, selection, odds, edge, quality, home, away, kickoff in picks:
        lines.append(
            f"- {range_code} | {kickoff} | {home} vs {away} | "
            f"{market} {selection} @{odds:.2f} | {quality} | edge {edge:.1f}%"
        )

    lines.extend(["", "## Missing Top Team Totals"])
    missing_tt = _read_missing("team_total_odds.csv", limit)
    lines.extend(_format_candidate(row) for row in missing_tt)
    if not missing_tt:
        lines.append("- none")

    lines.extend(["", "## Missing Top Handicap / +0.5"])
    missing_ah = _read_missing("handicap_odds.csv", limit)
    lines.extend(_format_candidate(row) for row in missing_ah)
    if not missing_ah:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Next Action",
            "- Fill the odds column for the missing TT/AH rows you can find.",
            "- Import with scripts/team_total_odds_cli.py or scripts/handicap_odds_cli.py.",
            "- Rerun python main.py --skip-scrape --no-fatigue.",
        ]
    )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Generate odds coverage report")
    parser.add_argument("--output", default="odds_coverage_report.md", help="Report output path")
    parser.add_argument("--limit", type=int, default=20, help="Missing candidates per market")
    args = parser.parse_args()

    report = build_report(limit=args.limit)
    Path(args.output).write_text(report, encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
