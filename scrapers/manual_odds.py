#!/usr/bin/env python3
"""
Manual Odds Input Interface
Paste odds from Stake (or any bookmaker), get edges calculated.
"""

import sqlite3
import sys
sys.path.insert(0, '/home/ubuntu/rollo-stake-model')

DB_PATH = '/home/ubuntu/rollo-stake-model/data/rollo_stake.db'

def add_manual_odds(match_id: str, market: str, selection: str, odds: float):
    """Add manually entered odds to database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    implied = round(1.0 / odds, 4)
    
    c.execute('''
        INSERT OR REPLACE INTO odds 
        (match_id, market, selection, odds, implied_prob)
        VALUES (?, ?, ?, ?, ?)
    ''', (match_id, market, selection, odds, implied))
    
    conn.commit()
    conn.close()
    
    print(f"Added: {selection} @ {odds} (implied: {implied:.1%})")

def get_upcoming_matches():
    """List upcoming matches for reference."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''
        SELECT match_id, home_team, away_team, league, kickoff
        FROM matches
        WHERE status = 'scheduled'
        ORDER BY kickoff
    ''')
    
    matches = c.fetchall()
    conn.close()
    
    print("\nUpcoming matches:")
    for i, row in enumerate(matches, 1):
        print(f"  {i}. {row[1]} vs {row[2]} ({row[3]}) - {row[4]}")
        print(f"     Match ID: {row[0]}")
    
    return matches

def interactive_input():
    """Interactive odds input."""
    matches = get_upcoming_matches()
    
    print("\n" + "="*50)
    print("MANUAL ODDS INPUT")
    print("="*50)
    print("\nPaste odds from Stake.com (or any bookmaker)")
    print("Format: <match_number> <market> <selection> <odds>")
    print("Example: 1 1X2 'Man City Win' 2.10")
    print("Markets: 1X2, OU (Over/Under), BTTS")
    print("Type 'done' when finished")
    print()
    
    while True:
        user_input = input("> ").strip()
        
        if user_input.lower() == 'done':
            break
        
        try:
            # Parse: 1 1X2 "Man City Win" 2.10
            parts = user_input.split()
            match_num = int(parts[0]) - 1
            market = parts[1]
            
            # Extract odds (last part)
            odds = float(parts[-1])
            
            # Selection is everything between market and odds
            selection = ' '.join(parts[2:-1]).strip("'\"")
            
            if 0 <= match_num < len(matches):
                match_id = matches[match_num][0]
                add_manual_odds(match_id, market, selection, odds)
            else:
                print(f"Invalid match number. Use 1-{len(matches)}")
                
        except Exception as e:
            print(f"Error: {e}")
            print("Format: <match_number> <market> <selection> <odds>")

def quick_add(match_id: str, odds_dict: dict):
    """
    Quick add odds for a match.
    
    Args:
        match_id: Match ID from database
        odds_dict: Dict like {'1X2': {'Home Win': 2.10, 'Draw': 3.40, 'Away Win': 3.60}}
    """
    for market, selections in odds_dict.items():
        for selection, odds in selections.items():
            add_manual_odds(match_id, market, selection, odds)
    
    print(f"\nAdded {sum(len(v) for v in odds_dict.values())} odds entries")

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Manual odds input')
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive mode')
    parser.add_argument('--list', '-l', action='store_true', help='List matches')
    
    args = parser.parse_args()
    
    if args.list:
        get_upcoming_matches()
    elif args.interactive:
        interactive_input()
    else:
        print("Usage:")
        print("  python3 manual_odds.py --list       # Show upcoming matches")
        print("  python3 manual_odds.py --interactive # Interactive input")
