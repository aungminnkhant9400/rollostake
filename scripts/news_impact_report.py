#!/usr/bin/env python3
"""
News Impact Report
Shows before/after model probability for each pending pick,
broken down by which external factors moved the needle.
"""

import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH


def parse_adjustment_notes(reasoning: str) -> dict:
    """Extract deltas and notes from pick reasoning."""
    notes = {
        "table": "",
        "h2h": "",
        "schedule": "",
        "news": "",
        "fatigue": "",
    }
    if "External context:" not in reasoning:
        return notes

    context = reasoning.split("External context:")[-1].split(".")[0]
    segments = [s.strip() for s in context.split(";")]
    for seg in segments:
        if "standings" in seg or "table" in seg:
            notes["table"] = seg
        elif "H2H" in seg:
            notes["h2h"] = seg
        elif "fixture congestion" in seg or "schedule" in seg:
            notes["schedule"] = seg
        elif "news" in seg or "injury" in seg:
            notes["news"] = seg
        elif "fatigue" in seg:
            notes["fatigue"] = seg
    return notes


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("""
        SELECT
            m.match_id,
            m.home_team,
            m.away_team,
            m.league,
            p.selection,
            p.market,
            p.odds,
            p.model_prob,
            p.book_prob,
            p.edge_pct,
            p.quality,
            p.range_code,
            p.reasoning
        FROM picks p
        JOIN matches m ON p.match_id = m.match_id
        WHERE p.status = 'pending'
        ORDER BY p.edge_pct DESC
    """)
    rows = c.fetchall()
    conn.close()

    if not rows:
        print("No pending picks found.")
        return

    # Separate picks by whether news moved them
    with_news = []
    without_news = []
    for row in rows:
        notes = parse_adjustment_notes(row["reasoning"] or "")
        if notes["news"]:
            with_news.append((row, notes))
        else:
            without_news.append((row, notes))

    print("=" * 90)
    print("NEWS IMPACT REPORT — Pending Picks")
    print("=" * 90)
    print(f"\nTotal pending picks: {len(rows)}")
    print(f"Picks affected by team news: {len(with_news)}")
    print(f"Picks with no news factor: {len(without_news)}")

    if with_news:
        print("\n" + "-" * 90)
        print(f"{'Match':<35} {'Market':<10} {'Sel':<20} {'Prob':>6} {'Edge':>7} {'News Impact'}")
        print("-" * 90)
        for row, notes in with_news:
            match = f"{row['home_team']} vs {row['away_team']}"
            match = match[:34]
            sel = row["selection"][:19]
            news_note = notes["news"]
            # Estimate delta sign
            delta_sign = ""
            if "supports" in news_note:
                delta_sign = "+"
            elif "downgrades" in news_note:
                delta_sign = "-"
            print(f"{match:<35} {row['market']:<10} {sel:<20} {row['model_prob']*100:>5.1f}% {row['edge_pct']:>6.1f}% {delta_sign} {news_note}")

    print("\n" + "-" * 90)
    print("Picks with NO news adjustment (table/H2H/schedule only):")
    print("-" * 90)
    for row, notes in without_news:
        match = f"{row['home_team']} vs {row['away_team']}"
        match = match[:34]
        sel = row["selection"][:19]
        other = "; ".join(v for v in [notes["table"], notes["h2h"], notes["schedule"], notes["fatigue"]] if v) or "model only"
        print(f"{match:<35} {row['market']:<10} {sel:<20} {row['model_prob']*100:>5.1f}% {row['edge_pct']:>6.1f}%   {other}")


if __name__ == "__main__":
    main()
