"""
Fixtures Fetcher
Gets upcoming matches from API-Football (free tier available).
"""

import requests
import sqlite3
from typing import List, Dict
from datetime import datetime, timedelta

DB_PATH = '/home/ubuntu/rollo-stake-model/data/rollo_stake.db'

# API-Football configuration
# Free tier: 100 requests/day
# Get API key from: https://www.api-football.com/
API_FOOTBALL_KEY = None  # Will use demo data if not set
API_FOOTBALL_URL = 'https://v3.football.api-sports.io'

# League IDs for API-Football
LEAGUE_IDS = {
    'EPL': 39,        # Premier League
    'L1': 61,         # Ligue 1
    'Bundesliga': 78,  # Bundesliga
    'SerieA': 135,    # Serie A
    'LaLiga': 140,    # La Liga
}

class FixturesFetcher:
    """
    Fetches upcoming fixtures for supported leagues.
    Falls back to demo data if API key not available.
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or API_FOOTBALL_KEY
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({
                'x-rapidapi-key': self.api_key,
                'x-rapidapi-host': 'v3.football.api-sports.io'
            })
    
    def fetch_upcoming(self, league: str, days: int = 7) -> List[Dict]:
        """
        Fetch upcoming fixtures for a league.
        
        Args:
            league: League code (EPL, L1, etc.)
            days: Fetch matches for next N days
        
        Returns:
            List of fixture dictionaries
        """
        if not self.api_key:
            print("No API key - using demo fixtures")
            return self._get_demo_fixtures(league)
        
        if league not in LEAGUE_IDS:
            print(f"Unknown league: {league}")
            return []
        
        league_id = LEAGUE_IDS[league]
        season = 2024  # Current season
        
        # Date range
        from_date = datetime.now().strftime('%Y-%m-%d')
        to_date = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d')
        
        try:
            url = f"{API_FOOTBALL_URL}/fixtures"
            params = {
                'league': league_id,
                'season': season,
                'from': from_date,
                'to': to_date,
            }
            
            print(f"Fetching {league} fixtures...")
            response = self.session.get(url, params=params, timeout=30)
            data = response.json()
            
            if data.get('errors'):
                print(f"API Error: {data['errors']}")
                return self._get_demo_fixtures(league)
            
            fixtures = []
            for fixture in data.get('response', []):
                match = {
                    'match_id': f"{fixture['fixture']['id']}",
                    'home_team': fixture['teams']['home']['name'],
                    'away_team': fixture['teams']['away']['name'],
                    'league': league,
                    'kickoff': fixture['fixture']['date'][:16].replace('T', ' '),
                    'status': 'scheduled'
                }
                fixtures.append(match)
            
            print(f"  Found {len(fixtures)} upcoming matches")
            return fixtures
            
        except Exception as e:
            print(f"Error fetching fixtures: {e}")
            return self._get_demo_fixtures(league)
    
    def _get_demo_fixtures(self, league: str) -> List[Dict]:
        """Return demo fixtures when API is unavailable."""
        # Demo fixtures for testing (dates near end of 2024-25 season)
        fixtures = {
            'EPL': [
                {'match_id': 'epl_1', 'home_team': 'Man City', 'away_team': 'Liverpool', 'league': 'EPL', 'kickoff': '2025-05-25 15:00', 'status': 'scheduled'},
                {'match_id': 'epl_2', 'home_team': 'Arsenal', 'away_team': 'Chelsea', 'league': 'EPL', 'kickoff': '2025-05-25 17:30', 'status': 'scheduled'},
                {'match_id': 'epl_3', 'home_team': 'Man United', 'away_team': 'Tottenham', 'league': 'EPL', 'kickoff': '2025-05-26 14:00', 'status': 'scheduled'},
            ],
            'L1': [
                {'match_id': 'l1_1', 'home_team': 'PSG', 'away_team': 'Marseille', 'league': 'L1', 'kickoff': '2025-05-25 20:00', 'status': 'scheduled'},
            ],
            'Bundesliga': [
                {'match_id': 'bund_1', 'home_team': 'Bayern Munich', 'away_team': 'Dortmund', 'league': 'Bundesliga', 'kickoff': '2025-05-25 18:30', 'status': 'scheduled'},
            ],
            'SerieA': [
                {'match_id': 'sa_1', 'home_team': 'Inter Milan', 'away_team': 'Juventus', 'league': 'SerieA', 'kickoff': '2025-05-26 20:45', 'status': 'scheduled'},
            ],
            'LaLiga': [
                {'match_id': 'll_1', 'home_team': 'Real Madrid', 'away_team': 'Barcelona', 'league': 'LaLiga', 'kickoff': '2025-05-26 21:00', 'status': 'scheduled'},
            ],
        }
        
        return fixtures.get(league, [])
    
    def save_fixtures(self, fixtures: List[Dict]):
        """Save fixtures to database."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        for fixture in fixtures:
            c.execute('''
                INSERT OR IGNORE INTO matches 
                (match_id, home_team, away_team, league, kickoff, status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                fixture['match_id'],
                fixture['home_team'],
                fixture['away_team'],
                fixture['league'],
                fixture['kickoff'],
                'scheduled'
            ))
        
        conn.commit()
        conn.close()
        print(f"Saved {len(fixtures)} fixtures to database")
    
    def get_all_upcoming(self, leagues: List[str] = None) -> List[Dict]:
        """Fetch and save upcoming fixtures for all leagues."""
        if leagues is None:
            leagues = list(LEAGUE_IDS.keys())
        
        all_fixtures = []
        for league in leagues:
            fixtures = self.fetch_upcoming(league)
            self.save_fixtures(fixtures)
            all_fixtures.extend(fixtures)
        
        return all_fixtures

if __name__ == '__main__':
    fetcher = FixturesFetcher()
    
    # Test fetching fixtures
    fixtures = fetcher.get_all_upcoming()
    print(f"\nTotal upcoming matches: {len(fixtures)}")
    
    for f in fixtures:
        print(f"  {f['home_team']} vs {f['away_team']} ({f['league']}) - {f['kickoff']}")
