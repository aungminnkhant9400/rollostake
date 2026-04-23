#!/usr/bin/env python3
"""Refresh historical results from football-data.co.uk."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import load_settings
from scrapers.football_data import FootballDataLoader, current_season_code


def main():
    settings = load_settings()

    parser = argparse.ArgumentParser(description="Update historical match results")
    parser.add_argument("--season", action="append", help="Season code, e.g. 2526. Can be repeated.")
    parser.add_argument("--league", action="append", help="League code, e.g. EPL. Can be repeated.")
    args = parser.parse_args()

    seasons = args.season or settings.get("historical_seasons") or [current_season_code()]
    leagues = args.league or settings.get("leagues")

    loader = FootballDataLoader()
    total = 0

    for season in seasons:
        for league in leagues:
            matches = loader.fetch_season(league, season)
            loader.save_to_db(matches)
            total += len(matches)

    print(f"\nHistorical update complete: {total} completed matches processed")


if __name__ == "__main__":
    main()
