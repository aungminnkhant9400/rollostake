#!/usr/bin/env python3
"""
Bulk Odds Input CLI
Paste multiple odds at once instead of one-by-one.

Supports formats:
- CSV: Man City vs Liverpool,2.10,3.40,2.80
- Simple: Man City 2.10 Draw 3.40 Liverpool 2.80
- Tabular: paste from spreadsheet
"""

import argparse
import csv
import io
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from utils.team_normalizer import normalize_team_name


def parse_simple_format(text: str) -> list:
    """Parse simple text format: 'Man City vs Liverpool 2.10 3.40 2.80'"""
    lines = text.strip().split('\n')
    odds = []
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        # Try to extract: TeamA vs TeamB odds1 odds2 odds3
        # Patterns: "TeamA vs TeamB 2.10 3.40 2.80" or "TeamA 2.10 Draw 3.40 TeamB 2.80"
        
        # Find decimal numbers (odds)
        numbers = re.findall(r'\d+\.\d+', line)
        if len(numbers) < 2:
            continue
        
        # Remove numbers to get team names
        text_no_numbers = re.sub(r'\d+\.\d+', '', line).strip()
        
        # Try to split by "vs" or "VS"
        if ' vs ' in text_no_numbers.lower():
            parts = re.split(r'\s+vs\s+', text_no_numbers, flags=re.IGNORECASE)
            if len(parts) == 2:
                home = parts[0].strip()
                away = parts[1].strip()
                
                if len(numbers) >= 3:
                    odds.append({
                        'home_team': normalize_team_name(home),
                        'away_team': normalize_team_name(away),
                        'home_odds': float(numbers[0]),
                        'draw_odds': float(numbers[1]),
                        'away_odds': float(numbers[2])
                    })
                elif len(numbers) >= 2:
                    # Assume home/draw/away
                    odds.append({
                        'home_team': normalize_team_name(home),
                        'away_team': normalize_team_name(away),
                        'home_odds': float(numbers[0]),
                        'draw_odds': float(numbers[1]) if len(numbers) > 2 else None,
                        'away_odds': float(numbers[-1])
                    })
    
    return odds


def parse_csv_format(text: str) -> list:
    """Parse CSV format."""
    reader = csv.DictReader(io.StringIO(text))
    odds = []
    
    for row in reader:
        try:
            entry = {
                'home_team': normalize_team_name(row.get('home_team', '').strip()),
                'away_team': normalize_team_name(row.get('away_team', '').strip()),
                'home_odds': float(row.get('home_odds', row.get('1', 0))),
                'draw_odds': float(row.get('draw_odds', row.get('X', 0))),
                'away_odds': float(row.get('away_odds', row.get('2', 0)))
            }
            if entry['home_team'] and entry['away_team']:
                odds.append(entry)
        except (ValueError, KeyError):
            continue
    
    return odds


def find_match(home_team: str, away_team: str):
    """Find a scheduled fixture and return match_id plus DB team names."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Try exact match
    c.execute('''
        SELECT match_id, home_team, away_team FROM matches
        WHERE status = 'scheduled'
        AND home_team = ? AND away_team = ?
    ''', (home_team, away_team))
    row = c.fetchone()
    
    if row:
        conn.close()
        return row
    
    # Try fuzzy match
    c.execute('''
        SELECT match_id, home_team, away_team FROM matches
        WHERE status = 'scheduled'
    ''')
    
    for match_id, db_home, db_away in c.fetchall():
        if (home_team.lower() in db_home.lower() or db_home.lower() in home_team.lower()) and \
           (away_team.lower() in db_away.lower() or db_away.lower() in away_team.lower()):
            conn.close()
            return match_id, db_home, db_away
    
    conn.close()
    return None


def find_match_id(home_team: str, away_team: str) -> str:
    """Find match_id for a scheduled fixture."""
    match = find_match(home_team, away_team)
    return match[0] if match else None


def save_odds(odds: list, overwrite: bool = False):
    """Save odds to database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    saved = 0
    skipped = 0
    not_found = []
    
    for entry in odds:
        match = find_match(entry['home_team'], entry['away_team'])
        
        if not match:
            not_found.append(f"{entry['home_team']} vs {entry['away_team']}")
            continue

        match_id, db_home, db_away = match
        
        # Save each selection as separate row
        selections = [
            ('1X2', f'{db_home} Win', entry['home_odds']),
            ('1X2', 'Draw', entry.get('draw_odds', 0)),
            ('1X2', f'{db_away} Win', entry['away_odds'])
        ]

        if overwrite:
            c.execute('''
                DELETE FROM odds
                WHERE match_id = ? AND market = '1X2' AND bookmaker = 'manual'
            ''', (match_id,))
        
        for market, selection, odd in selections:
            if not odd or odd <= 0:
                continue
            
            # Check if exists
            c.execute('''
                SELECT id FROM odds 
                WHERE match_id = ? AND market = ? AND selection = ?
            ''', (match_id, market, selection))
            existing = c.fetchone()
            
            if existing and not overwrite:
                skipped += 1
                continue
            
            implied = 1.0 / odd if odd > 0 else 0
            
            c.execute('''
                INSERT INTO odds (match_id, bookmaker, market, selection, odds, implied_prob)
                VALUES (?, 'manual', ?, ?, ?, ?)
            ''', (match_id, market, selection, odd, implied))
            
            saved += 1
    
    conn.commit()
    conn.close()
    
    return saved, skipped, not_found


def interactive_input():
    """Interactive bulk odds input."""
    print("=" * 60)
    print("BULK ODDS INPUT")
    print("=" * 60)
    print("\nPaste odds in any format:")
    print("  Simple: Man City vs Liverpool 2.10 3.40 2.80")
    print("  CSV: home_team,away_team,home_odds,draw_odds,away_odds")
    print("\nType 'done' on a new line when finished.")
    print("-" * 60)
    
    lines = []
    while True:
        try:
            line = input()
            if line.strip().lower() == 'done':
                break
            lines.append(line)
        except EOFError:
            break
    
    text = '\n'.join(lines)
    
    if not text.strip():
        print("No input provided.")
        return
    
    # Try CSV first, then simple
    if ',' in text:
        odds = parse_csv_format(text)
        print(f"\nParsed {len(odds)} entries (CSV format)")
    else:
        odds = parse_simple_format(text)
        print(f"\nParsed {len(odds)} entries (simple format)")
    
    if not odds:
        print("No valid odds found.")
        return
    
    # Preview
    print("\nPreview:")
    for i, entry in enumerate(odds[:10], 1):
        print(f"  {i}. {entry['home_team']} vs {entry['away_team']}")
        print(f"     H: {entry['home_odds']} | D: {entry['draw_odds']} | A: {entry['away_odds']}")
    
    if len(odds) > 10:
        print(f"     ... and {len(odds) - 10} more")
    
    # Save
    saved, skipped, not_found = save_odds(odds, overwrite=True)
    
    print(f"\n{'=' * 60}")
    print(f"SAVED: {saved} odds entries")
    print(f"SKIPPED: {skipped} (already existed)")
    if not_found:
        print(f"NOT FOUND: {len(not_found)} matches")
        for match in not_found[:5]:
            print(f"  - {match}")
        if len(not_found) > 5:
            print(f"  ... and {len(not_found) - 5} more")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(description="Bulk odds input")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive input")
    parser.add_argument("--csv", help="Import from CSV file")
    parser.add_argument("--text", help="Import from text file")
    
    args = parser.parse_args()
    
    if args.interactive:
        interactive_input()
    elif args.csv:
        with open(args.csv, 'r') as f:
            odds = parse_csv_format(f.read())
            saved, skipped, not_found = save_odds(odds, overwrite=True)
            print(f"Saved {saved}, skipped {skipped}, not found {len(not_found)}")
    elif args.text:
        with open(args.text, 'r') as f:
            odds = parse_simple_format(f.read())
            saved, skipped, not_found = save_odds(odds, overwrite=True)
            print(f"Saved {saved}, skipped {skipped}, not found {len(not_found)}")
    else:
        interactive_input()


if __name__ == '__main__':
    main()
