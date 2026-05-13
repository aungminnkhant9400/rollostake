#!/usr/bin/env python3
"""Rebuild risk-band picks and dashboard from saved predictions and odds."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.edge_calculator import EdgeCalculator
from config.settings import load_settings
from dashboard.generator import DashboardGenerator


def rebuild_card(league: str = None) -> list:
    settings = load_settings()
    calc = EdgeCalculator(
        bankroll=float(settings.get("bankroll", 10000)),
        use_ranges=bool(settings.get("use_ranges", True)),
        staking_mode=settings.get("staking_mode", "flat"),
        flat_stake=float(settings.get("flat_stake", 200)),
        range_configs=EdgeCalculator.range_configs_from_settings(settings),
        bookmaker=settings.get("default_bookmaker", "polymarket"),
    )
    picks = calc.generate_range_picks(league=league)
    calc.save_range_picks(picks)
    DashboardGenerator().generate()
    return picks, calc


def main():
    parser = argparse.ArgumentParser(description="Rebuild official card from saved predictions and odds")
    parser.add_argument("--league", default=None, help="Optional league filter")
    args = parser.parse_args()

    picks, calc = rebuild_card(league=args.league)
    print(f"Rebuilt dashboard with {len(picks)} picks")
    for pick in picks:
        risk_name = calc.range_configs[pick.range_code].name
        print(
            f"{risk_name} {pick.market} {pick.home_team} vs {pick.away_team} | "
            f"{pick.selection} @{pick.odds:.2f} | {pick.quality} | edge {pick.edge_pct:.1f}%"
        )


if __name__ == "__main__":
    main()
