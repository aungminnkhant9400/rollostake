"""
Fixtures Fetcher
Gets upcoming matches from API-Football (free tier available).
"""

import requests
import sqlite3
import sys
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from config.settings import load_settings

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
    
    def __init__(
        self,
        api_key: str = None,
        days_ahead: int = 7,
        timezone: str = "Asia/Macau",
        season: Optional[int] = None,
        use_rapidapi: bool = False,
        fallback_to_demo: bool = True,
    ):
        self.api_key = api_key or API_FOOTBALL_KEY
        self.days_ahead = days_ahead
        self.timezone = timezone
        self.season = season
        self.use_rapidapi = use_rapidapi
        self.fallback_to_demo = fallback_to_demo
        self.session = requests.Session()
        if self.api_key:
            if self.use_rapidapi:
                self.session.headers.update({
                    'x-rapidapi-key': self.api_key,
                    'x-rapidapi-host': 'v3.football.api-sports.io'
                })
            else:
                self.session.headers.update({'x-apisports-key': self.api_key})

    def _fallback(self, league: str) -> List[Dict]:
        if self.fallback_to_demo:
            return self._get_demo_fixtures(league)
        return []

    def _season(self) -> int:
        if self.season:
            return int(self.season)
        now = datetime.now()
        return now.year if now.month >= 7 else now.year - 1
    
    def fetch_upcoming(self, league: str, days: int = None) -> List[Dict]:
        """
        Fetch upcoming fixtures for a league.
        
        Args:
            league: League code (EPL, L1, etc.)
            days: Fetch matches for next N days
        
        Returns:
            List of fixture dictionaries
        """
        days = days or self.days_ahead
        if not self.api_key:
            print("No API key - using demo fixtures")
            return self._fallback(league)
        
        if league not in LEAGUE_IDS:
            print(f"Unknown league: {league}")
            return []
        
        league_id = LEAGUE_IDS[league]
        season = self._season()
        
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
            if self.timezone:
                params['timezone'] = self.timezone
            
            print(f"Fetching {league} fixtures from API-Football...")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            remaining = response.headers.get('x-ratelimit-requests-remaining')
            if remaining is not None:
                print(f"  API requests remaining today: {remaining}")
            
            if data.get('errors'):
                print(f"API Error: {data['errors']}")
                return self._fallback(league)
            
            fixtures = []
            for fixture in data.get('response', []):
                status = fixture['fixture']['status']['short']
                match = {
                    'match_id': f"{fixture['fixture']['id']}",
                    'home_team': fixture['teams']['home']['name'],
                    'away_team': fixture['teams']['away']['name'],
                    'league': league,
                    'kickoff': fixture['fixture']['date'][:16].replace('T', ' '),
                    'status': 'scheduled' if status in {'TBD', 'NS'} else status.lower()
                }
                fixtures.append(match)
            
            print(f"  Found {len(fixtures)} upcoming matches")
            return fixtures
            
        except Exception as e:
            print(f"Error fetching fixtures: {e}")
            return self._fallback(league)
    
    def _get_demo_fixtures(self, league: str) -> List[Dict]:
        """Return demo fixtures when API is unavailable."""
        # Demo fixtures for testing, dated from the current run date.
        base = datetime.now() + timedelta(days=2)
        dates = [(base + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(5)]
        fixtures = {
            'EPL': [
                {'match_id': 'epl_1', 'home_team': 'Man City', 'away_team': 'Liverpool', 'league': 'EPL', 'kickoff': f'{dates[0]} 15:00', 'status': 'scheduled'},
                {'match_id': 'epl_2', 'home_team': 'Arsenal', 'away_team': 'Chelsea', 'league': 'EPL', 'kickoff': f'{dates[0]} 17:30', 'status': 'scheduled'},
                {'match_id': 'epl_3', 'home_team': 'Man United', 'away_team': 'Tottenham', 'league': 'EPL', 'kickoff': f'{dates[1]} 14:00', 'status': 'scheduled'},
            ],
            'L1': [
                {'match_id': 'l1_1', 'home_team': 'PSG', 'away_team': 'Marseille', 'league': 'L1', 'kickoff': f'{dates[1]} 20:00', 'status': 'scheduled'},
            ],
            'Bundesliga': [
                {'match_id': 'bund_1', 'home_team': 'Bayern Munich', 'away_team': 'Dortmund', 'league': 'Bundesliga', 'kickoff': f'{dates[2]} 18:30', 'status': 'scheduled'},
            ],
            'SerieA': [
                {'match_id': 'sa_1', 'home_team': 'Inter Milan', 'away_team': 'Juventus', 'league': 'SerieA', 'kickoff': f'{dates[3]} 20:45', 'status': 'scheduled'},
            ],
            'LaLiga': [
                {'match_id': 'll_1', 'home_team': 'Real Madrid', 'away_team': 'Barcelona', 'league': 'LaLiga', 'kickoff': f'{dates[4]} 21:00', 'status': 'scheduled'},
            ],
        }
        
        return fixtures.get(league, [])
    
    def save_fixtures(self, fixtures: List[Dict]):
        """Save fixtures to database."""
        if not fixtures:
            print("Saved 0 fixtures to database")
            return

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        league = fixtures[0]['league']
        current_ids = [fixture['match_id'] for fixture in fixtures]

        placeholders = ','.join('?' for _ in current_ids)
        c.execute(
            f'''
            UPDATE matches
            SET status = 'stale'
            WHERE league = ?
              AND status = 'scheduled'
              AND match_id NOT IN ({placeholders})
            ''',
            [league] + current_ids
        )
        
        saved = 0
        for fixture in fixtures:
            c.execute('''
                INSERT INTO matches
                (match_id, home_team, away_team, league, kickoff, status)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    home_team = excluded.home_team,
                    away_team = excluded.away_team,
                    league = excluded.league,
                    kickoff = excluded.kickoff,
                    status = excluded.status
            ''', (
                fixture['match_id'],
                fixture['home_team'],
                fixture['away_team'],
                fixture['league'],
                fixture['kickoff'],
                fixture.get('status', 'scheduled')
            ))
            saved += 1
        
        conn.commit()
        conn.close()
        print(f"Saved {saved} fixtures to database")
    
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
    settings = load_settings()
    fetcher = FixturesFetcher(
        api_key=settings.get('api_football_key'),
        days_ahead=int(settings.get('fixture_days_ahead', 7)),
        timezone=settings.get('fixture_timezone', 'Asia/Macau'),
        season=settings.get('fixture_season'),
        use_rapidapi=bool(settings.get('api_football_use_rapidapi', False)),
    )
    
    # Test fetching fixtures
    fixtures = fetcher.get_all_upcoming()
    print(f"\nTotal upcoming matches: {len(fixtures)}")
    
    for f in fixtures:
        print(f"  {f['home_team']} vs {f['away_team']} ({f['league']}) - {f['kickoff']}")
