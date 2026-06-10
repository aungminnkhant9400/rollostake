"""Application settings loader."""

import json
import os

from config.paths import PROJECT_ROOT


SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.json"


def load_settings():
    """Load project settings with defaults for older config files."""
    defaults = {
        "api_football_key": None,
        "api_football_use_rapidapi": False,
        "fixture_source": "manual",
        "fixture_days_ahead": 7,
        "fixture_timezone": "Asia/Macau",
        "fixture_season": None,
        "historical_seasons": ["2526"],
        "leagues": ["EPL", "L1", "Bundesliga", "SerieA", "LaLiga"],
        "bankroll": 1000,
        "staking_mode": "kelly",
        "flat_stake": 200,
        "use_ranges": False,
        "active_ranges": ["C", "D"],
        "default_bookmaker": "polymarket",
        "min_edge": 0.05,
        "max_picks": 12,
        "use_fatigue": True,
        "adjustment_layers": {
            "enabled": True,
            "rolling_blend_threshold": 0.30,
            "lambda_min": 0.30,
            "lambda_max": 5.00,
            "dixon_coles_rho": -0.13,
            "manual_context_file": "data/team_context.json",
            "derby_pairs": [],
        },
        "ranges": {
            "C": {
                "name": "High Risk",
                "bankroll": 10000,
                "flat_stake": 200,
                "min_odds": 2.50,
                "max_odds": 5.00,
                "max_picks": 12,
                "min_edge": 0.05,
            },
            "D": {
                "name": "Low Risk",
                "bankroll": 10000,
                "flat_stake": 200,
                "min_odds": 1.70,
                "max_odds": 2.70,
                "max_picks": 12,
                "min_edge": 0.05,
            },
        },
    }

    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            loaded = json.load(f)
    else:
        loaded = {}

    settings = {**defaults, **loaded}
    settings["ranges"] = {
        **defaults["ranges"],
        **loaded.get("ranges", {}),
    }
    settings["adjustment_layers"] = {
        **defaults["adjustment_layers"],
        **loaded.get("adjustment_layers", {}),
    }
    env_key = os.getenv("API_FOOTBALL_KEY")
    if env_key:
        settings["api_football_key"] = env_key
    return settings
