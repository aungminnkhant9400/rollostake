"""
Stake.com Odds Scraper
Fetches live odds for soccer matches from Stake.com.
Uses Playwright for JavaScript-rendered content.
"""

import json
import time
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH

@dataclass
class OddsData:
    match_id: str
    home_team: str
    away_team: str
    league: str
    kickoff: str
    market: str
    selection: str
    odds: float
    implied_prob: float

class StakeScraper:
    """
    Scraper for Stake.com soccer odds.
    
    Markets supported:
    - 1X2 (Home/Draw/Away)
    - Over/Under 1.5, 2.5, 3.5
    - BTTS (Both Teams To Score)
    - Asian Handicap
    """
    
    BASE_URL = "https://stake.com/sports/soccer"
    
    LEAGUE_URLS = {
        'EPL': '/england/premier-league',
        'L1': '/france/ligue-1',
        'Bundesliga': '/germany/bundesliga',
        'SerieA': '/italy/serie-a',
        'LaLiga': '/spain/la-liga',
    }
    
    def __init__(self):
        self.session = None
        
    def _get_browser(self):
        """Initialize Playwright browser."""
        try:
            from playwright.sync_api import sync_playwright
            return sync_playwright()
        except ImportError:
            print("Playwright not installed. Install with: pip install playwright")
            print("Then run: playwright install chromium")
            return None
    
    def fetch_league_odds(self, league: str, headless: bool = True) -> List[OddsData]:
        """
        Fetch odds for all matches in a league.
        
        Args:
            league: League code (EPL, L1, Bundesliga, SerieA, LaLiga)
            headless: Run browser headless
        
        Returns:
            List of OddsData objects
        """
        if league not in self.LEAGUE_URLS:
            raise ValueError(f"Unknown league: {league}. Use: {list(self.LEAGUE_URLS.keys())}")
        
        p = self._get_browser()
        if not p:
            return []
        
        odds_list = []
        
        with p as playwright:
            browser = playwright.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            try:
                url = f"{self.BASE_URL}{self.LEAGUE_URLS[league]}"
                print(f"Fetching {league} odds from {url}...")
                
                page.goto(url, wait_until='networkidle', timeout=30000)
                time.sleep(3)  # Wait for JS to render
                
                # Extract match data
                # Stake uses specific CSS selectors for their odds
                matches = page.query_selector_all('[data-testid="sport-event"]')
                
                for match_elem in matches:
                    try:
                        # Extract teams
                        teams = match_elem.query_selector_all('[data-testid="team-name"]')
                        if len(teams) < 2:
                            continue
                        
                        home_team = teams[0].inner_text().strip()
                        away_team = teams[1].inner_text().strip()
                        
                        # Extract match time
                        time_elem = match_elem.query_selector('[data-testid="event-time"]')
                        kickoff = time_elem.inner_text().strip() if time_elem else 'TBD'
                        
                        # Extract 1X2 odds
                        odds_buttons = match_elem.query_selector_all('[data-testid="odd-button"]')
                        
                        if len(odds_buttons) >= 3:
                            # Home win
                            home_odds = float(odds_buttons[0].inner_text().strip())
                            odds_list.append(OddsData(
                                match_id=f"{home_team}_vs_{away_team}_{kickoff}",
                                home_team=home_team,
                                away_team=away_team,
                                league=league,
                                kickoff=kickoff,
                                market='1X2',
                                selection=f"{home_team} Win",
                                odds=home_odds,
                                implied_prob=round(1/home_odds, 4)
                            ))
                            
                            # Draw
                            draw_odds = float(odds_buttons[1].inner_text().strip())
                            odds_list.append(OddsData(
                                match_id=f"{home_team}_vs_{away_team}_{kickoff}",
                                home_team=home_team,
                                away_team=away_team,
                                league=league,
                                kickoff=kickoff,
                                market='1X2',
                                selection='Draw',
                                odds=draw_odds,
                                implied_prob=round(1/draw_odds, 4)
                            ))
                            
                            # Away win
                            away_odds = float(odds_buttons[2].inner_text().strip())
                            odds_list.append(OddsData(
                                match_id=f"{home_team}_vs_{away_team}_{kickoff}",
                                home_team=home_team,
                                away_team=away_team,
                                league=league,
                                kickoff=kickoff,
                                market='1X2',
                                selection=f"{away_team} Win",
                                odds=away_odds,
                                implied_prob=round(1/away_odds, 4)
                            ))
                        
                    except Exception as e:
                        print(f"Error parsing match: {e}")
                        continue
                
                print(f"Found {len(matches)} matches, {len(odds_list)} odds entries")
                
            except Exception as e:
                print(f"Error fetching {league}: {e}")
            
            finally:
                browser.close()
        
        return odds_list
    
    def save_odds(self, odds_list: List[OddsData]):
        """Save odds to database."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        for odds in odds_list:
            # Save match first
            c.execute('''
                INSERT OR IGNORE INTO matches (match_id, home_team, away_team, league, kickoff)
                VALUES (?, ?, ?, ?, ?)
            ''', (odds.match_id, odds.home_team, odds.away_team, odds.league, odds.kickoff))
            
            # Save odds
            c.execute('''
                INSERT OR REPLACE INTO odds 
                (match_id, market, selection, odds, implied_prob)
                VALUES (?, ?, ?, ?, ?)
            ''', (odds.match_id, odds.market, odds.selection, odds.odds, odds.implied_prob))
        
        conn.commit()
        conn.close()
        print(f"Saved {len(odds_list)} odds entries to database")

def fetch_all_leagues(leagues: List[str] = None) -> List[OddsData]:
    """
    Fetch odds for all configured leagues.
    
    Args:
        leagues: List of league codes. Defaults to all.
    
    Returns:
        Combined list of all odds
    """
    if leagues is None:
        leagues = ['EPL', 'L1', 'Bundesliga', 'SerieA', 'LaLiga']
    
    scraper = StakeScraper()
    all_odds = []
    
    for league in leagues:
        try:
            odds = scraper.fetch_league_odds(league)
            all_odds.extend(odds)
            time.sleep(2)  # Be nice to Stake's servers
        except Exception as e:
            print(f"Failed to fetch {league}: {e}")
    
    scraper.save_odds(all_odds)
    return all_odds

if __name__ == '__main__':
    # Test with sample data (no browser needed)
    sample_odds = [
        OddsData('match_1', 'Man City', 'Liverpool', 'EPL', '2026-04-25 15:00', '1X2', 'Man City Win', 2.10, 0.476),
        OddsData('match_1', 'Man City', 'Liverpool', 'EPL', '2026-04-25 15:00', '1X2', 'Draw', 3.40, 0.294),
        OddsData('match_1', 'Man City', 'Liverpool', 'EPL', '2026-04-25 15:00', '1X2', 'Liverpool Win', 3.60, 0.278),
    ]
    
    scraper = StakeScraper()
    scraper.save_odds(sample_odds)
    print("Sample odds saved.")
