#!/usr/bin/env python3
"""
Free Team News Auto-Scraper
Fetches injury/team news from free sources and applies to model.
"""

import requests
import sqlite3
import re
import sys
from typing import List, Dict, Optional
from datetime import datetime
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH

class TeamNewsScraper:
    """
    Scrapes team news from free sources.
    
    Sources:
    - BBC Sport (injury reports)
    - ESPN FC (team news)
    - Transfermarkt (injury list - basic)
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })
        self.news_items = []
    
    def scrape_bbc_injuries(self) -> List[Dict]:
        """Scrape BBC Sport injury news."""
        try:
            # BBC Sport football injury page
            url = "https://www.bbc.com/sport/football/injuries"
            response = self.session.get(url, timeout=15)
            
            if response.status_code != 200:
                return []
            
            # Parse injury mentions
            # BBC uses structured data but we'll do basic extraction
            content = response.text.lower()
            
            # Look for patterns like "player out", "injured", "suspended"
            injury_patterns = [
                r'(\w+\s+\w+)\s+(?:out|injured|sidelined)\s+(?:for|with)\s+([\w\s]+)',
                r'(\w+\s+\w+)\s+(?:ruled out|missing)\s+(?:of|for)\s+([\w\s]+)',
            ]
            
            items = []
            for pattern in injury_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    player = match[0].title()
                    reason = match[1].strip()
                    items.append({
                        'player': player,
                        'team': self._guess_team_from_context(content, player),
                        'status': 'injured',
                        'reason': reason,
                        'source': 'BBC',
                        'confidence': 'medium'
                    })
            
            return items
            
        except Exception as e:
            print(f"BBC scrape failed: {e}")
            return []
    
    def scrape_espn_injuries(self) -> List[Dict]:
        """Scrape ESPN FC injury news."""
        try:
            # ESPN injury page
            url = "https://www.espn.com/soccer/injuries"
            response = self.session.get(url, timeout=15)
            
            if response.status_code != 200:
                return []
            
            content = response.text.lower()
            
            # ESPN has structured injury tables
            # Extract player names and their status
            items = []
            
            # Look for injury table rows
            # Pattern: player name followed by injury type
            injury_rows = re.findall(
                r'<tr[^>]*>.*?<td[^>]*>([^<]+)</td>.*?<td[^>]*>([^<]+)</td>.*?<td[^>]*>([^<]+)</td>.*?<td[^>]*>([^<]+)</td>.*?<td[^>]*>([^<]+)</td>.*?</tr>',
                response.text,
                re.DOTALL
            )
            
            for row in injury_rows[:20]:  # Limit to first 20
                if len(row) >= 3:
                    player = re.sub(r'<[^>]+>', '', row[0]).strip()
                    team = re.sub(r'<[^>]+>', '', row[1]).strip()
                    injury_type = re.sub(r'<[^>]+>', '', row[3]).strip() if len(row) > 3 else 'unknown'
                    
                    if player and team:
                        items.append({
                            'player': player,
                            'team': team,
                            'status': 'injured',
                            'reason': injury_type,
                            'source': 'ESPN',
                            'confidence': 'high'
                        })
            
            return items
            
        except Exception as e:
            print(f"ESPN scrape failed: {e}")
            return []
    
    def scrape_transfermarkt_injuries(self) -> List[Dict]:
        """Scrape Transfermarkt injury list."""
        try:
            # Transfermarkt has public injury pages
            url = "https://www.transfermarkt.com/verletzungen/aktuelle-verletzungen/"
            response = self.session.get(url, timeout=15)
            
            if response.status_code != 200:
                return []
            
            items = []
            # Basic extraction - Transfermarkt is heavily JS-based
            # This will be limited
            
            return items
            
        except Exception as e:
            print(f"Transfermarkt scrape failed: {e}")
            return []
    
    def _guess_team_from_context(self, content: str, player: str) -> str:
        """Try to guess team from nearby text."""
        # Look for team mentions near player name
        player_lower = player.lower()
        
        # Common team names to check
        teams = [
            'man city', 'liverpool', 'arsenal', 'chelsea', 'man united', 'tottenham',
            'bayern munich', 'dortmund', 'psg', 'marseille', 'inter milan', 'juventus',
            'real madrid', 'barcelona', 'atletico madrid', 'leverkusen'
        ]
        
        # Find position of player in text
        pos = content.find(player_lower)
        if pos >= 0:
            # Check nearby text for team names
            nearby = content[max(0, pos-500):min(len(content), pos+500)]
            for team in teams:
                if team in nearby:
                    return team.title()
        
        return 'Unknown'
    
    def deduplicate_news(self, items: List[Dict]) -> List[Dict]:
        """Remove duplicate player entries."""
        seen = {}
        for item in items:
            key = item['player'].lower()
            if key not in seen or item['confidence'] == 'high':
                seen[key] = item
        
        return list(seen.values())
    
    def fetch_all_news(self) -> List[Dict]:
        """Fetch news from all sources."""
        print("Fetching team news from free sources...")
        
        all_items = []
        
        # BBC
        bbc_items = self.scrape_bbc_injuries()
        all_items.extend(bbc_items)
        print(f"  BBC: {len(bbc_items)} items")
        
        # ESPN
        espn_items = self.scrape_espn_injuries()
        all_items.extend(espn_items)
        print(f"  ESPN: {len(espn_items)} items")
        
        # Transfermarkt
        tm_items = self.scrape_transfermarkt_injuries()
        all_items.extend(tm_items)
        print(f"  Transfermarkt: {len(tm_items)} items")
        
        # Deduplicate
        unique = self.deduplicate_news(all_items)
        print(f"\nTotal unique items: {len(unique)}")
        
        return unique
    
    def save_to_db(self, items: List[Dict]):
        """Save news items to database."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Create news table if not exists
        c.execute('''
            CREATE TABLE IF NOT EXISTS team_news (
                id INTEGER PRIMARY KEY,
                player TEXT,
                team TEXT,
                status TEXT,
                reason TEXT,
                source TEXT,
                confidence TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Clear old news
        c.execute("DELETE FROM team_news")
        
        for item in items:
            c.execute('''
                INSERT INTO team_news (player, team, status, reason, source, confidence)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                item['player'], item['team'], item['status'],
                item['reason'], item['source'], item['confidence']
            ))
        
        conn.commit()
        conn.close()
        print(f"Saved {len(items)} news items to database")
    
    def display_news(self, items: List[Dict]):
        """Display fetched news."""
        if not items:
            print("\nNo team news found.")
            return
        
        print("\nTEAM NEWS:")
        print("="*80)
        
        for item in items:
            conf_icon = "✅" if item['confidence'] == 'high' else "⚠️"
            print(f"{conf_icon} {item['player']} ({item['team']})")
            print(f"   Status: {item['status']} - {item['reason']}")
            print(f"   Source: {item['source']}")
            print()

def run_news_check():
    """Main function to check and display news."""
    scraper = TeamNewsScraper()
    news = scraper.fetch_all_news()
    scraper.save_to_db(news)
    scraper.display_news(news)
    
    return news

if __name__ == '__main__':
    run_news_check()
