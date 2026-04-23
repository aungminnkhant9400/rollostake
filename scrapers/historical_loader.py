"""
Historical Data Loader
Fetches and loads historical match results for model training.
Supports multiple data sources.
"""

import sqlite3
import requests
import csv
import io
from typing import List, Dict, Optional
from datetime import datetime
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH

# Football-Data.co.uk CSV URLs for major leagues
FOOTBALL_DATA_URLS = {
    'EPL': 'https://www.football-data.co.uk/mmz4281/2526/E0.csv',
    'L1': 'https://www.football-data.co.uk/mmz4281/2526/F1.csv',
    'Bundesliga': 'https://www.football-data.co.uk/mmz4281/2526/D1.csv',
    'SerieA': 'https://www.football-data.co.uk/mmz4281/2526/I1.csv',
    'LaLiga': 'https://www.football-data.co.uk/mmz4281/2526/SP1.csv',
}

# CSV column mappings (football-data.co.uk format)
COLUMN_MAP = {
    'HomeTeam': 'home_team',
    'AwayTeam': 'away_team',
    'FTHG': 'home_goals',  # Full Time Home Goals
    'FTAG': 'away_goals',  # Full Time Away Goals
    'FTR': 'result',       # Full Time Result (H/D/A)
    'Date': 'date',
    'Div': 'division',
}

class HistoricalDataLoader:
    """
    Loads historical match data from various sources.
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })
    
    def fetch_football_data(self, league: str, season: str = '2526') -> List[Dict]:
        """
        Fetch historical data from football-data.co.uk.
        
        Args:
            league: League code (EPL, L1, etc.)
            season: Season code (e.g., '2526' for 2025-26)
        
        Returns:
            List of match dictionaries
        """
        if league not in FOOTBALL_DATA_URLS:
            print(f"Unknown league: {league}")
            return []
        
        url = FOOTBALL_DATA_URLS[league].replace('2526', season)
        
        try:
            print(f"Fetching {league} data from {url}...")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV
            content = response.content.decode('utf-8', errors='ignore')
            reader = csv.DictReader(io.StringIO(content))
            
            matches = []
            for row in reader:
                try:
                    # Skip rows with missing data
                    if not row.get('HomeTeam') or not row.get('AwayTeam'):
                        continue
                    if not row.get('FTHG') or not row.get('FTAG'):
                        continue
                    
                    # Parse date
                    date_str = row.get('Date', '')
                    if '/' in date_str:
                        # Format: DD/MM/YY
                        day, month, year = date_str.split('/')
                        year = '20' + year if len(year) == 2 else year
                        date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    else:
                        date = date_str
                    
                    match = {
                        'home_team': row['HomeTeam'].strip(),
                        'away_team': row['AwayTeam'].strip(),
                        'home_goals': int(row['FTHG']),
                        'away_goals': int(row['FTAG']),
                        'date': date,
                        'league': league,
                        'result': row.get('FTR', ''),  # H/D/A
                    }
                    
                    matches.append(match)
                    
                except (ValueError, KeyError) as e:
                    continue
            
            print(f"Loaded {len(matches)} matches for {league}")
            return matches
            
        except requests.RequestException as e:
            print(f"Error fetching {league}: {e}")
            return []
    
    def save_matches(self, matches: List[Dict]):
        """Save matches to database."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        count = 0
        for match in matches:
            match_id = f"{match['home_team']}_vs_{match['away_team']}_{match['date']}"
            
            c.execute('''
                INSERT INTO matches
                (match_id, home_team, away_team, league, kickoff, home_goals, away_goals, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    home_goals = excluded.home_goals,
                    away_goals = excluded.away_goals,
                    status = 'completed'
            ''', (
                match_id,
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
        
        print(f"Upserted {count} completed matches to database")
    
    def load_all_leagues(self, season: str = '2526'):
        """Load data for all configured leagues."""
        all_matches = []
        
        for league in FOOTBALL_DATA_URLS.keys():
            matches = self.fetch_football_data(league, season)
            all_matches.extend(matches)
        
        self.save_matches(all_matches)
        return all_matches
    
    def load_sample_data(self):
        """Load built-in sample data for testing."""
        sample_matches = [
            # EPL 2025-26 season - more comprehensive
            {'home_team': 'Man City', 'away_team': 'Liverpool', 'home_goals': 2, 'away_goals': 1, 'date': '2025-08-15', 'league': 'EPL'},
            {'home_team': 'Liverpool', 'away_team': 'Man City', 'home_goals': 1, 'away_goals': 1, 'date': '2025-11-02', 'league': 'EPL'},
            {'home_team': 'Arsenal', 'away_team': 'Chelsea', 'home_goals': 3, 'away_goals': 0, 'date': '2025-08-16', 'league': 'EPL'},
            {'home_team': 'Chelsea', 'away_team': 'Arsenal', 'home_goals': 1, 'away_goals': 2, 'date': '2025-11-09', 'league': 'EPL'},
            {'home_team': 'Man City', 'away_team': 'Arsenal', 'home_goals': 2, 'away_goals': 0, 'date': '2025-09-20', 'league': 'EPL'},
            {'home_team': 'Arsenal', 'away_team': 'Man City', 'home_goals': 1, 'away_goals': 1, 'date': '2026-01-15', 'league': 'EPL'},
            {'home_team': 'Liverpool', 'away_team': 'Arsenal', 'home_goals': 2, 'away_goals': 2, 'date': '2025-10-05', 'league': 'EPL'},
            {'home_team': 'Chelsea', 'away_team': 'Liverpool', 'home_goals': 0, 'away_goals': 3, 'date': '2025-08-23', 'league': 'EPL'},
            {'home_team': 'Man City', 'away_team': 'Chelsea', 'home_goals': 4, 'away_goals': 1, 'date': '2025-12-01', 'league': 'EPL'},
            {'home_team': 'Liverpool', 'away_team': 'Chelsea', 'home_goals': 2, 'away_goals': 1, 'date': '2026-02-10', 'league': 'EPL'},
            {'home_team': 'Arsenal', 'away_team': 'Liverpool', 'home_goals': 1, 'away_goals': 0, 'date': '2025-09-12', 'league': 'EPL'},
            {'home_team': 'Chelsea', 'away_team': 'Man City', 'home_goals': 0, 'away_goals': 2, 'date': '2025-10-18', 'league': 'EPL'},
            {'home_team': 'Liverpool', 'away_team': 'Chelsea', 'home_goals': 3, 'away_goals': 1, 'date': '2025-12-26', 'league': 'EPL'},
            {'home_team': 'Man City', 'away_team': 'Liverpool', 'home_goals': 1, 'away_goals': 2, 'date': '2026-03-01', 'league': 'EPL'},
            {'home_team': 'Arsenal', 'away_team': 'Chelsea', 'home_goals': 2, 'away_goals': 1, 'date': '2026-04-05', 'league': 'EPL'},
            
            # Ligue 1
            {'home_team': 'PSG', 'away_team': 'Marseille', 'home_goals': 3, 'away_goals': 1, 'date': '2025-08-17', 'league': 'L1'},
            {'home_team': 'Marseille', 'away_team': 'PSG', 'home_goals': 0, 'away_goals': 2, 'date': '2025-11-10', 'league': 'L1'},
            {'home_team': 'PSG', 'away_team': 'Lyon', 'home_goals': 2, 'away_goals': 0, 'date': '2025-09-15', 'league': 'L1'},
            {'home_team': 'Lyon', 'away_team': 'PSG', 'home_goals': 1, 'away_goals': 3, 'date': '2025-12-20', 'league': 'L1'},
            {'home_team': 'Marseille', 'away_team': 'Lyon', 'home_goals': 2, 'away_goals': 2, 'date': '2025-10-25', 'league': 'L1'},
            
            # Bundesliga
            {'home_team': 'Bayern Munich', 'away_team': 'Dortmund', 'home_goals': 2, 'away_goals': 1, 'date': '2025-08-16', 'league': 'Bundesliga'},
            {'home_team': 'Dortmund', 'away_team': 'Bayern Munich', 'home_goals': 1, 'away_goals': 3, 'date': '2025-11-03', 'league': 'Bundesliga'},
            {'home_team': 'Bayern Munich', 'away_team': 'Leverkusen', 'home_goals': 1, 'away_goals': 1, 'date': '2025-09-22', 'league': 'Bundesliga'},
            {'home_team': 'Leverkusen', 'away_team': 'Bayern Munich', 'home_goals': 0, 'away_goals': 2, 'date': '2025-12-15', 'league': 'Bundesliga'},
            {'home_team': 'Dortmund', 'away_team': 'Leverkusen', 'home_goals': 3, 'away_goals': 2, 'date': '2025-10-30', 'league': 'Bundesliga'},
            
            # Serie A
            {'home_team': 'Inter Milan', 'away_team': 'Juventus', 'home_goals': 1, 'away_goals': 1, 'date': '2025-08-18', 'league': 'SerieA'},
            {'home_team': 'Juventus', 'away_team': 'Inter Milan', 'home_goals': 0, 'away_goals': 2, 'date': '2025-11-12', 'league': 'SerieA'},
            {'home_team': 'Inter Milan', 'away_team': 'AC Milan', 'home_goals': 2, 'away_goals': 0, 'date': '2025-10-01', 'league': 'SerieA'},
            {'home_team': 'AC Milan', 'away_team': 'Inter Milan', 'home_goals': 1, 'away_goals': 1, 'date': '2025-12-28', 'league': 'SerieA'},
            {'home_team': 'Juventus', 'away_team': 'AC Milan', 'home_goals': 1, 'away_goals': 0, 'date': '2025-09-18', 'league': 'SerieA'},
            
            # La Liga
            {'home_team': 'Real Madrid', 'away_team': 'Barcelona', 'home_goals': 2, 'away_goals': 1, 'date': '2025-08-20', 'league': 'LaLiga'},
            {'home_team': 'Barcelona', 'away_team': 'Real Madrid', 'home_goals': 1, 'away_goals': 1, 'date': '2025-11-15', 'league': 'LaLiga'},
            {'home_team': 'Real Madrid', 'away_team': 'Atletico Madrid', 'home_goals': 3, 'away_goals': 0, 'date': '2025-09-28', 'league': 'LaLiga'},
            {'home_team': 'Atletico Madrid', 'away_team': 'Real Madrid', 'home_goals': 1, 'away_goals': 2, 'date': '2025-12-22', 'league': 'LaLiga'},
            {'home_team': 'Barcelona', 'away_team': 'Atletico Madrid', 'home_goals': 2, 'away_goals': 2, 'date': '2025-10-15', 'league': 'LaLiga'},
        ]
        
        self.save_matches(sample_matches)
        return sample_matches

def get_historical_matches(league: str = None, limit: int = 1000) -> List[Dict]:
    """
    Get historical matches from database.
    
    Args:
        league: Filter by league (optional)
        limit: Max matches to return
    
    Returns:
        List of match dictionaries
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    query = '''
        SELECT match_id, home_team, away_team, home_goals, away_goals, 
               league, kickoff as date
        FROM matches
        WHERE status = 'completed' AND home_goals IS NOT NULL
    '''
    
    if league:
        query += f" AND league = '{league}'"
    
    query += f' ORDER BY kickoff DESC LIMIT {limit}'
    
    c.execute(query)
    matches = [dict(row) for row in c.fetchall()]
    conn.close()
    
    return matches

if __name__ == '__main__':
    loader = HistoricalDataLoader()
    
    # Load sample data
    matches = loader.load_sample_data()
    print(f"\nLoaded {len(matches)} sample matches")
    
    # Try to fetch real data
    # loader.load_all_leagues('2526')
