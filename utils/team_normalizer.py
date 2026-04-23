#!/usr/bin/env python3
"""Team name normalizer for football-data.co.uk naming variations."""

# Mapping of abbreviated/inconsistent names to canonical names
TEAM_NAME_MAP = {
    # La Liga
    "Ath Madrid": "Atletico Madrid",
    "Ath Bilbao": "Athletic Bilbao",
    
    # Ligue 1
    "Paris SG": "PSG",
    
    # Serie A
    "Inter": "Inter Milan",
    
    # Bundesliga
    "M'gladbach": "Borussia M'gladbach",
    "Mainz": "Mainz 05",
    "Wolfsburg": "Wolfsburg",  # Consistent, but keep for completeness
    
    # EPL (mostly consistent, but handle edge cases)
    "Man United": "Man United",  # Consistent
    "Man City": "Man City",  # Consistent
    "Newcastle": "Newcastle",
    "Wolves": "Wolves",
    "Nott'm Forest": "Nottingham Forest",
    "Spurs": "Tottenham",
    
    # Handle reverse mapping (if data has full names but fixtures use short)
    "Atletico Madrid": "Atletico Madrid",
    "Athletic Bilbao": "Athletic Bilbao",
    "PSG": "PSG",
    "Inter Milan": "Inter Milan",
    "Borussia Dortmund": "Dortmund",
    "Bayern Munich": "Bayern Munich",
}

# Reverse lookup for fixtures → historical matching
REVERSE_MAP = {v: k for k, v in TEAM_NAME_MAP.items() if k != v}


def normalize_team_name(name: str) -> str:
    """Normalize a team name to canonical form."""
    if not name:
        return name
    
    # Strip whitespace
    name = name.strip()
    
    # Direct mapping
    if name in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[name]
    
    # Check case-insensitive
    lower_name = name.lower()
    for original, canonical in TEAM_NAME_MAP.items():
        if original.lower() == lower_name:
            return canonical
    
    # If no mapping, return as-is
    return name


def normalize_match_teams(match: dict) -> dict:
    """Normalize team names in a match dictionary."""
    match = dict(match)
    if "home_team" in match:
        match["home_team"] = normalize_team_name(match["home_team"])
    if "away_team" in match:
        match["away_team"] = normalize_team_name(match["away_team"])
    return match


def get_all_variations(canonical_name: str) -> list:
    """Get all known variations of a team name."""
    variations = [canonical_name]
    
    # Find all keys that map to this canonical name
    for original, canonical in TEAM_NAME_MAP.items():
        if canonical == canonical_name:
            variations.append(original)
    
    return list(set(variations))


if __name__ == "__main__":
    # Test
    test_names = [
        "Ath Madrid",
        "Atletico Madrid",
        "Paris SG",
        "PSG",
        "Inter",
        "Inter Milan",
        "Arsenal",
        "Man United",
    ]
    
    print("Team name normalization test:")
    for name in test_names:
        normalized = normalize_team_name(name)
        print(f"  {name:20} → {normalized}")
