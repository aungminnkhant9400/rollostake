#!/usr/bin/env python3
"""Team name normalizer for football-data.co.uk naming variations."""

import unicodedata

# Mapping of abbreviated/inconsistent names to canonical names
TEAM_NAME_MAP = {
    # EPL football-data.org full names -> football-data.co.uk-style names
    "Manchester City": "Man City",
    "Manchester City FC": "Man City",
    "Manchester United": "Man United",
    "Manchester United FC": "Man United",
    "Tottenham Hotspur": "Tottenham",
    "Tottenham Hotspur FC": "Tottenham",
    "Wolverhampton Wanderers": "Wolves",
    "Newcastle United": "Newcastle",
    "Nottingham Forest": "Nott'm Forest",
    "West Ham United": "West Ham",
    "Leeds United": "Leeds",
    "Brighton & Hove Albion": "Brighton",
    "AFC Bournemouth": "Bournemouth",

    # La Liga
    "Ath Madrid": "Atletico Madrid",
    "Club Atletico de Madrid": "Atletico Madrid",
    "Atletico Madrid": "Atletico Madrid",
    "Ath Bilbao": "Ath Bilbao",
    "Athletic Bilbao": "Ath Bilbao",
    "Athletic Club": "Ath Bilbao",
    "Real Betis Balompié": "Betis",
    "Real Sociedad de Fútbol": "Sociedad",
    "Real Sociedad": "Sociedad",
    "Rayo Vallecano de Madrid": "Vallecano",
    "Rayo Vallecano": "Vallecano",
    "RC Celta de Vigo": "Celta",
    "Celta Vigo": "Celta",
    "Deportivo Alaves": "Alaves",
    "RCD Espanyol de Barcelona": "Espanol",
    "Espanyol": "Espanol",
    
    # Ligue 1
    "Paris SG": "PSG",
    "Paris Saint-Germain": "PSG",
    "Olympique de Marseille": "Marseille",
    "Olympique Lyonnais": "Lyon",
    "Stade Rennais FC 1901": "Rennes",
    "RC Lens": "Lens",
    "AS Monaco FC": "Monaco",
    "OGC Nice": "Nice",
    "Stade Brestois 29": "Brest",
    "Angers SCO": "Angers",
    "AJ Auxerre": "Auxerre",
    "FC Metz": "Metz",
    "FC Nantes": "Nantes",
    "FC Lorient": "Lorient",
    "Le Havre AC": "Le Havre",
    "RC Strasbourg Alsace": "Strasbourg",
    "Toulouse FC": "Toulouse",
    "Lille OSC": "Lille",
    
    # Serie A
    "Inter": "Inter Milan",
    "FC Internazionale Milano": "Inter Milan",
    "Internazionale": "Inter Milan",
    "Juventus FC": "Juventus",
    "ACF Fiorentina": "Fiorentina",
    "SS Lazio": "Lazio",
    "Hellas Verona": "Verona",
    "US Sassuolo Calcio": "Sassuolo",
    "US Lecce": "Lecce",
    "Bologna FC 1909": "Bologna",
    "Parma Calcio 1913": "Parma",
    "SSC Napoli": "Napoli",
    "Torino FC": "Torino",
    
    # Bundesliga
    "Borussia Dortmund": "Dortmund",
    "Bayer 04 Leverkusen": "Leverkusen",
    "Eintracht Frankfurt": "Ein Frankfurt",
    "SC Freiburg": "Freiburg",
    "1. FC Heidenheim 1846": "Heidenheim",
    "1. FC Union Berlin": "Union Berlin",
    "FC St. Pauli 1910": "St Pauli",
    "Borussia M'gladbach": "M'gladbach",
    "Borussia Monchengladbach": "M'gladbach",
    "Borussia Monchengladbach": "M'gladbach",
    "M'gladbach": "M'gladbach",
    "1. FSV Mainz 05": "Mainz",
    "Mainz 05": "Mainz",
    "Mainz": "Mainz",
    "TSG 1899 Hoffenheim": "Hoffenheim",
    "RB Leipzig": "RB Leipzig",
    "VfB Stuttgart": "Stuttgart",
    "SV Werder Bremen": "Werder Bremen",
    "FC Augsburg": "Augsburg",
    "Wolfsburg": "Wolfsburg",  # Consistent, but keep for completeness
    "VfL Wolfsburg": "Wolfsburg",
    "1. FC Koln": "FC Koln",
    "1. FC Cologne": "FC Koln",
    "FC Cologne": "FC Koln",
    "Hamburger SV": "Hamburg",
    
    # EPL (mostly consistent, but handle edge cases)
    "Man United": "Man United",  # Consistent
    "Man City": "Man City",  # Consistent
    "Newcastle": "Newcastle",
    "Wolves": "Wolves",
    "Nott'm Forest": "Nott'm Forest",
    "Spurs": "Tottenham",
    
    # Handle reverse mapping (if data has full names but fixtures use short)
    "Atletico Madrid": "Atletico Madrid",
    "Athletic Bilbao": "Ath Bilbao",
    "PSG": "PSG",
    "Inter Milan": "Inter Milan",
    "Borussia Dortmund": "Dortmund",
    "Bayern Munich": "Bayern Munich",
}

# Reverse lookup for fixtures -> historical matching
REVERSE_MAP = {v: k for k, v in TEAM_NAME_MAP.items() if k != v}


def normalize_team_name(name: str) -> str:
    """Normalize a team name to canonical form."""
    if not name:
        return name
    
    # Strip whitespace
    name = unicodedata.normalize("NFKD", name.strip())
    name = "".join(char for char in name if not unicodedata.combining(char))
    
    # Direct mapping
    if name in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[name]
    if name.endswith(" FC") and name[:-3] in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[name[:-3]]
    
    # Check case-insensitive
    lower_name = name.lower()
    for original, canonical in TEAM_NAME_MAP.items():
        if original.lower() == lower_name:
            return canonical
        if name.endswith(" FC") and original.lower() == name[:-3].lower():
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
        print(f"  {name:20} -> {normalized}")
