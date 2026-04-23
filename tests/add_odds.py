#!/usr/bin/env python3
"""
Populate realistic odds for upcoming matches.
Uses approximate market odds for demo/testing.
"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH

def add_realistic_odds():
    """Add realistic odds for all upcoming matches."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get upcoming matches
    c.execute('SELECT match_id, home_team, away_team, league, kickoff FROM matches WHERE status = "scheduled"')
    matches = c.fetchall()
    
    print(f"Found {len(matches)} upcoming matches")
    
    # Realistic odds for upcoming matches (approximate market odds)
    odds_data = {
        # EPL
        ('Man City', 'Liverpool'): {
            '1X2': {'Man City Win': 2.30, 'Draw': 3.40, 'Liverpool Win': 3.10},
            'OU': {'Over 2.5': 1.75, 'Under 2.5': 2.10},
            'BTTS': {'BTTS Yes': 1.65, 'BTTS No': 2.20},
        },
        ('Arsenal', 'Chelsea'): {
            '1X2': {'Arsenal Win': 1.85, 'Draw': 3.60, 'Chelsea Win': 4.20},
            'OU': {'Over 2.5': 1.90, 'Under 2.5': 1.95},
            'BTTS': {'BTTS Yes': 1.75, 'BTTS No': 2.00},
        },
        ('Man United', 'Tottenham'): {
            '1X2': {'Man United Win': 2.40, 'Draw': 3.40, 'Tottenham Win': 2.90},
            'OU': {'Over 2.5': 1.70, 'Under 2.5': 2.15},
            'BTTS': {'BTTS Yes': 1.60, 'BTTS No': 2.30},
        },
        # Ligue 1
        ('PSG', 'Marseille'): {
            '1X2': {'PSG Win': 1.45, 'Draw': 4.50, 'Marseille Win': 7.00},
            'OU': {'Over 2.5': 1.55, 'Under 2.5': 2.40},
            'BTTS': {'BTTS Yes': 1.90, 'BTTS No': 1.85},
        },
        # Bundesliga
        ('Bayern Munich', 'Dortmund'): {
            '1X2': {'Bayern Munich Win': 1.60, 'Draw': 4.20, 'Dortmund Win': 5.00},
            'OU': {'Over 2.5': 1.50, 'Under 2.5': 2.60},
            'BTTS': {'BTTS Yes': 1.55, 'BTTS No': 2.40},
        },
        # Serie A
        ('Inter Milan', 'Juventus'): {
            '1X2': {'Inter Milan Win': 2.20, 'Draw': 3.30, 'Juventus Win': 3.40},
            'OU': {'Over 2.5': 2.00, 'Under 2.5': 1.80},
            'BTTS': {'BTTS Yes': 1.80, 'BTTS No': 1.95},
        },
        # La Liga
        ('Real Madrid', 'Barcelona'): {
            '1X2': {'Real Madrid Win': 2.40, 'Draw': 3.50, 'Barcelona Win': 2.80},
            'OU': {'Over 2.5': 1.65, 'Under 2.5': 2.25},
            'BTTS': {'BTTS Yes': 1.50, 'BTTS No': 2.60},
        },
    }
    
    count = 0
    for match_id, home, away, league, kickoff in matches:
        # Find odds for this matchup
        matchup = None
        for (h, a), markets in odds_data.items():
            if (h == home and a == away) or (h == away and a == home):
                matchup = markets
                break
        
        if not matchup:
            # Generate generic odds
            matchup = {
                '1X2': {f'{home} Win': 2.50, 'Draw': 3.30, f'{away} Win': 2.90},
                'OU': {'Over 2.5': 1.85, 'Under 2.5': 2.00},
            }
        
        # Insert odds for each market
        for market, selections in matchup.items():
            for selection, odds in selections.items():
                implied = round(1.0 / odds, 4)
                
                c.execute('''
                    INSERT OR REPLACE INTO odds 
                    (match_id, market, selection, odds, implied_prob)
                    VALUES (?, ?, ?, ?, ?)
                ''', (match_id, market, selection, odds, implied))
                count += 1
    
    conn.commit()
    conn.close()
    
    print(f"Added {count} odds entries")
    return count

if __name__ == '__main__':
    add_realistic_odds()
