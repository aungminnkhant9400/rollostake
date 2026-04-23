"""
Fatigue Analysis Module
Analyzes team fatigue based on fixture congestion, rotation, and travel.
"""

import sqlite3
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = '/home/ubuntu/rollo-stake-model/data/rollo_stake.db'

class FatigueAnalyzer:
    """
    Analyzes team fatigue for upcoming matches.
    
    Factors:
    - Days since last match (recovery time)
    - Matches in last 14/30 days (fixture congestion)
    - Travel distance (home vs away)
    - Rotation indicators (lineup changes)
    """
    
    # Fatigue thresholds
    CRITICAL_DAYS_REST = 3    # Less than 3 days rest = critical
    LOW_DAYS_REST = 5         # Less than 5 days = low
    HIGH_MATCHES_14D = 5      # More than 5 matches in 14 days = high load
    HIGH_MATCHES_30D = 10     # More than 10 matches in 30 days = high load
    
    def __init__(self):
        pass
    
    def get_team_matches(self, team: str, before_date: str, days: int = 90) -> List[Dict]:
        """
        Get recent matches for a team.
        
        Args:
            team: Team name
            before_date: Get matches before this date (YYYY-MM-DD)
            days: Look back this many days
        
        Returns:
            List of match dictionaries
        """
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        cutoff = (datetime.strptime(before_date, '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d')
        
        c.execute('''
            SELECT match_id, home_team, away_team, kickoff as date, 
                   home_goals, away_goals, league,
                   CASE WHEN home_team = ? THEN 'home' ELSE 'away' END as venue
            FROM matches
            WHERE (home_team = ? OR away_team = ?)
              AND kickoff < ?
              AND kickoff >= ?
              AND status = 'completed'
            ORDER BY kickoff DESC
        ''', (team, team, team, before_date, cutoff))
        
        matches = [dict(row) for row in c.fetchall()]
        conn.close()
        
        return matches
    
    def calculate_fatigue(self, team: str, match_date: str) -> Dict:
        """
        Calculate fatigue metrics for a team.
        
        Args:
            team: Team name
            match_date: Date of upcoming match (YYYY-MM-DD)
        
        Returns:
            Fatigue metrics dictionary
        """
        matches = self.get_team_matches(team, match_date, days=60)
        
        if not matches:
            return {
                'team': team,
                'days_since_last_match': None,
                'matches_14d': 0,
                'matches_30d': 0,
                'avg_goals_scored_5': None,
                'avg_goals_conceded_5': None,
                'fatigue_score': 0.0,
                'fatigue_level': 'unknown',
            }
        
        # Days since last match
        last_match_date = datetime.strptime(matches[0]['date'], '%Y-%m-%d')
        upcoming_date = datetime.strptime(match_date, '%Y-%m-%d')
        days_since = (upcoming_date - last_match_date).days
        
        # Matches in last 14 days
        cutoff_14d = upcoming_date - timedelta(days=14)
        matches_14d = sum(1 for m in matches 
                         if datetime.strptime(m['date'], '%Y-%m-%d') >= cutoff_14d)
        
        # Matches in last 30 days
        cutoff_30d = upcoming_date - timedelta(days=30)
        matches_30d = sum(1 for m in matches 
                         if datetime.strptime(m['date'], '%Y-%m-%d') >= cutoff_30d)
        
        # Recent form (last 5 matches)
        last_5 = matches[:5]
        goals_scored = []
        goals_conceded = []
        
        for m in last_5:
            if m['home_team'] == team:
                goals_scored.append(m['home_goals'] or 0)
                goals_conceded.append(m['away_goals'] or 0)
            else:
                goals_scored.append(m['away_goals'] or 0)
                goals_conceded.append(m['home_goals'] or 0)
        
        avg_scored = sum(goals_scored) / len(goals_scored) if goals_scored else None
        avg_conceded = sum(goals_conceded) / len(goals_conceded) if goals_conceded else None
        
        # Calculate fatigue score (0-100, higher = more fatigued)
        fatigue_score = 0.0
        
        # Rest factor (most important)
        if days_since <= self.CRITICAL_DAYS_REST:
            fatigue_score += 40
        elif days_since <= self.LOW_DAYS_REST:
            fatigue_score += 25
        elif days_since >= 7:
            fatigue_score -= 10  # Well rested
        
        # Fixture congestion
        if matches_14d >= self.HIGH_MATCHES_14D:
            fatigue_score += 30
        elif matches_14d >= 4:
            fatigue_score += 15
        
        if matches_30d >= self.HIGH_MATCHES_30D:
            fatigue_score += 20
        elif matches_30d >= 8:
            fatigue_score += 10
        
        # Recent form factor (high goals conceded = fatigue)
        if avg_conceded is not None:
            if avg_conceded >= 2.0:
                fatigue_score += 10
            elif avg_conceded <= 0.5:
                fatigue_score -= 5
        
        # Clamp to 0-100
        fatigue_score = max(0, min(100, fatigue_score))
        
        # Classify
        if fatigue_score >= 60:
            level = 'critical'
        elif fatigue_score >= 40:
            level = 'high'
        elif fatigue_score >= 20:
            level = 'moderate'
        else:
            level = 'low'
        
        return {
            'team': team,
            'days_since_last_match': days_since,
            'matches_14d': matches_14d,
            'matches_30d': matches_30d,
            'avg_goals_scored_5': round(avg_scored, 2) if avg_scored else None,
            'avg_goals_conceded_5': round(avg_conceded, 2) if avg_conceded else None,
            'fatigue_score': round(fatigue_score, 1),
            'fatigue_level': level,
        }
    
    def analyze_matchup(self, home_team: str, away_team: str, match_date: str) -> Dict:
        """
        Analyze fatigue for both teams in a matchup.
        
        Returns:
            Dictionary with both teams' fatigue and advantage assessment
        """
        home_fatigue = self.calculate_fatigue(home_team, match_date)
        away_fatigue = self.calculate_fatigue(away_team, match_date)
        
        # Determine fatigue advantage
        home_score = home_fatigue['fatigue_score']
        away_score = away_fatigue['fatigue_score']
        
        diff = away_score - home_score  # Positive = home team less fatigued
        
        if diff >= 20:
            advantage = 'home_significant'
            advantage_desc = f"{home_team} significantly fresher"
        elif diff >= 10:
            advantage = 'home_moderate'
            advantage_desc = f"{home_team} moderately fresher"
        elif diff <= -20:
            advantage = 'away_significant'
            advantage_desc = f"{away_team} significantly fresher"
        elif diff <= -10:
            advantage = 'away_moderate'
            advantage_desc = f"{away_team} moderately fresher"
        else:
            advantage = 'even'
            advantage_desc = "Similar fatigue levels"
        
        return {
            'home_team': home_fatigue,
            'away_team': away_fatigue,
            'fatigue_advantage': advantage,
            'fatigue_advantage_desc': advantage_desc,
            'fatigue_diff': round(diff, 1),
        }
    
    def get_fatigue_adjustment(self, matchup_analysis: Dict) -> float:
        """
        Get goal expectancy adjustment based on fatigue.
        
        Returns:
            Adjustment factor (multiplier for home team advantage)
        """
        diff = matchup_analysis['fatigue_diff']
        
        # Convert fatigue difference to goal adjustment
        # +20 fatigue diff = +0.3 goals for home team
        adjustment = diff * 0.015
        
        return round(adjustment, 3)

def save_fatigue_analysis(match_id: str, analysis: Dict):
    """Save fatigue analysis to database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Add fatigue columns if not exist
    c.execute("PRAGMA table_info(matches)")
    columns = [col[1] for col in c.fetchall()]
    
    if 'home_fatigue_score' not in columns:
        c.execute('ALTER TABLE matches ADD COLUMN home_fatigue_score REAL')
        c.execute('ALTER TABLE matches ADD COLUMN away_fatigue_score REAL')
        c.execute('ALTER TABLE matches ADD COLUMN fatigue_advantage TEXT')
    
    c.execute('''
        UPDATE matches 
        SET home_fatigue_score = ?,
            away_fatigue_score = ?,
            fatigue_advantage = ?
        WHERE match_id = ?
    ''', (
        analysis['home_team']['fatigue_score'],
        analysis['away_team']['fatigue_score'],
        analysis['fatigue_advantage'],
        match_id
    ))
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    analyzer = FatigueAnalyzer()
    
    # Test with sample teams
    result = analyzer.analyze_matchup('Man City', 'Liverpool', '2026-04-25')
    
    print("Fatigue Analysis: Man City vs Liverpool")
    print(f"  Man City: {result['home_team']['fatigue_level']} ({result['home_team']['fatigue_score']})")
    print(f"  Liverpool: {result['away_team']['fatigue_level']} ({result['away_team']['fatigue_score']})")
    print(f"  Advantage: {result['fatigue_advantage_desc']}")
    print(f"  Adjustment: {analyzer.get_fatigue_adjustment(result):+.3f} goals")
