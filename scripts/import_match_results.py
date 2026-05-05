#!/usr/bin/env python3
"""Import completed match scores and settle affected picks."""

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import update_results
from models.core import init_db


def import_results(path: str) -> dict:
    init_db()
    imported = 0
    skipped = 0
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            match_id = (row.get("match_id") or "").strip()
            if not match_id:
                skipped += 1
                continue
            try:
                home_goals = int(row["home_goals"])
                away_goals = int(row["away_goals"])
            except (KeyError, TypeError, ValueError):
                skipped += 1
                continue
            update_results(match_id, "auto", home_goals, away_goals)
            imported += 1
    return {"imported": imported, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(description="Import match results from CSV")
    parser.add_argument("csv_path", nargs="?", default="match_results.csv")
    args = parser.parse_args()
    summary = import_results(args.csv_path)
    print(f"Match results imported: {summary['imported']} skipped: {summary['skipped']}")


if __name__ == "__main__":
    main()
