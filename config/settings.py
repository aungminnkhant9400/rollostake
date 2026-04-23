"""Application settings loader."""

import json

from config.paths import PROJECT_ROOT


SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.json"


def load_settings():
    """Load project settings with defaults for older config files."""
    defaults = {
        "api_football_key": None,
        "leagues": ["EPL", "L1", "Bundesliga", "SerieA", "LaLiga"],
        "bankroll": 1000,
        "staking_mode": "kelly",
        "flat_stake": 200,
        "use_ranges": False,
        "min_edge": 0.05,
        "max_picks": 12,
        "use_fatigue": True,
        "ranges": {
            "C": {
                "name": "Range C",
                "bankroll": 10000,
                "flat_stake": 200,
                "min_odds": 2.50,
                "max_odds": 5.00,
                "max_picks": 12,
                "min_edge": 0.05,
            },
            "D": {
                "name": "Range D",
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
    return settings

