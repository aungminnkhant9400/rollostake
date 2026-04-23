"""
Football-Data.co.uk Loader
Fetches free historical match data for model training.
"""

import requests
import csv
import io
import sqlite3
import sys
from typing import List, Dict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH

# Football-Data.co.uk CSV URLs
# Season format: 2425 = 2024-25 season
SEASONS = {
    '2324': '2023-24',
    '2425': '2024-25',
}

LEAGUE_CODES = {
    'EPL': 'E0',      # England Premier League
    'Championship': 'E1',
    'L1': 'F1',       # France Ligue 1
    'Bundesliga': 'D1', # Germany Bundesliga
    'SerieA': 'I1',   # Italy Serie A
    'LaLiga': 'SP1',  # Spain La Liga
}

BASE_URL = 'https://www.football-data.co.uk/mmz4281/{season}/{league}.csv'

class FootballDataLoader:
    """
    Loads historical match data from football-data.co.uk
    Free, comprehensive, includes odds from multiple bookmakers.
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })
    
    def fetch_season(self, league: str, season: str = '2425') -> List[Dict]:
        """
        Fetch a full season of match data.
        
        Args:
            league: League code (EPL, L1, Bundesliga, etc.)
            season: Season code (e.g., '2425' for 2024-25)
        
        Returns:
            List of match dictionaries with results and odds
        """
        if league not in LEAGUE_CODES:
            print(f"Unknown league: {league}")
            return []
        
        league_code = LEAGUE_CODES[league]
        url = BASE_URL.format(season=season, league=league_code)
        
        try:
            print(f"Fetching {SEASONS.get(season, season)} {league}...")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV
            content = response.content.decode('utf-8', errors='ignore')
            reader = csv.DictReader(io.StringIO(content))
            
            matches = []
            for row in reader:
                try:
                    # Skip incomplete rows
                    if not row.get('HomeTeam') or not row.get('AwayTeam'):
                        continue
                    if not row.get('FTHG') or not row.get('FTAG'):
                        continue
                    
                    # Parse date
                    date_str = row.get('Date', '')
                    if '/' in date_str:
                        day, month, year = date_str.split('/')
                        year = '20' + year if len(year) == 2 else year
                        date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    else:
                        date = date_str
                    
                    match = {
                        'match_id': f"{row['HomeTeam']}_vs_{row['AwayTeam']}_{date}",
                        'home_team': row['HomeTeam'].strip(),
                        'away_team': row['AwayTeam'].strip(),
                        'home_goals': int(row['FTHG']),
                        'away_goals': int(row['FTAG']),
                        'date': date,
                        'league': league,
                        'result': row.get('FTR', ''),  # H/D/A
                        'season': season,
                    }
                    
                    # Include odds if available (B365 = Bet365)
                    if row.get('B365H'):
                        match['odds_home'] = float(row['B365H'])
                    if row.get('B365D'):
                        match['odds_draw'] = float(row['B365D'])
                    if row.get('B365A'):
                        match['odds_away'] = float(row['B365A'])
                    
                    matches.append(match)
                    
                except (ValueError, KeyError) as e:
                    continue
            
            print(f"  Loaded {len(matches)} matches")
            return matches
            
        except requests.RequestException as e:
            print(f"  Error: {e}")
            return []
    
    def fetch_multiple_seasons(self, league: str, seasons: List[str]) -> List[Dict]:
        """Fetch multiple seasons for a league."""
        all_matches = []
        for season in seasons:
            matches = self.fetch_season(league, season)
            all_matches.extend(matches)
        return all_matches
    
    def save_to_db(self, matches: List[Dict]):
        """Save matches to SQLite database."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        count = 0
        for match in matches:
            c.execute('''
                INSERT OR IGNORE INTO matches 
                (match_id, home_team, away_team, league, kickoff, 
                 home_goals, away_goals, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                match['match_id'],
                match['home_team'],
                match['away_team'],
                match['league'],
                match['date'],
                match['home_goals'],
                match['away_goals'],
                'completed'
            ))
            
            if c.rowcount > 0:
                count += 1
        
        conn.commit()
        conn.close()
        print(f"Saved {count} new matches to database")
    
    def load_all_data(self, leagues: List[str] = None, seasons: List[str] = None):
        """
        Load all available data.
        
        Args:
            leagues: List of leagues to load (default: all)
            seasons: List of seasons to load (default: ['2425', '2324'])
        """
        if leagues is None:
            leagues = list(LEAGUE_CODES.keys())
        if seasons is None:
            seasons = ['2425', '2324']
        
        total = 0
        for league in leagues:
            matches = self.fetch_multiple_seasons(league, seasons)
            self.save_to_db(matches)
            total += len(matches)
        
        print(f"\nTotal matches loaded: {total}")
        return total

if __name__ == '__main__':
    loader = FootballDataLoader()
    
    # Test with one season of EPL
    matches = loader.fetch_season('EPL', '2425')
    loader.save_to_db(matches)
    
    print(f"\nDatabase now has {len(matches)} matches")
