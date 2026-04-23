"""
Edge Calculator & Pick Classifier
Finds value bets by comparing model probabilities to bookmaker odds.
"""

import sqlite3
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

DB_PATH = '/home/ubuntu/rollo-stake-model/data/rollo_stake.db'

@dataclass
class Pick:
    match_id: str
    home_team: str
    away_team: str
    league: str
    kickoff: str
    selection: str
    market: str
    model_prob: float
    book_prob: float
    edge_pct: float
    odds: float
    stake: float = 200.0
    quality: str = 'KEEP'
    status: str = 'pending'

class EdgeCalculator:
    """
    Calculates edge between model prediction and bookmaker odds.
    
    Edge = Model Prob - Bookmaker Implied Prob
    
    Quality thresholds:
    - STRONG: Edge >= 25%
    - KEEP: Edge >= 10% and < 25%
    - CAUTION: Edge >= 5% and < 10%
    - SKIP: Edge < 5%
    """
    
    # Quality thresholds
    STRONG_THRESHOLD = 0.25
    KEEP_THRESHOLD = 0.10
    CAUTION_THRESHOLD = 0.05
    
    # Flat stake sizing
    BASE_STAKE = 200.0
    STRONG_STAKE = 250.0
    
    # Kelly criterion parameters
    KELLY_FRACTION = 0.15  # 15% Kelly for safety (was 25%)
    MAX_KELLY_PCT = 0.05   # Max 5% of bankroll per bet (was $500 fixed)
    MIN_KELLY_STAKE = 50.0
    
    def __init__(self):
        pass
    
    def calculate_edge(self, model_prob: float, book_odds: float) -> Tuple[float, float]:
        """
        Calculate edge and implied probability.
        
        Returns:
            (edge_pct, implied_prob)
        """
        book_prob = 1.0 / book_odds
        edge = model_prob - book_prob
        edge_pct = edge / book_prob if book_prob > 0 else 0
        
        return round(edge_pct, 4), round(book_prob, 4)
    
    def __init__(self, bankroll: float = 10000.0, use_kelly: bool = True):
        self.bankroll = bankroll
        self.use_kelly = use_kelly
        
    def kelly_stake(self, edge_pct: float, odds: float, model_prob: float) -> float:
        """
        Calculate stake using Kelly Criterion.
        
        Kelly formula: f* = (bp - q) / b
        Where: b = odds - 1, p = model prob, q = 1 - p
        
        We use Quarter Kelly for safety.
        """
        if not self.use_kelly:
            return self.BASE_STAKE
        
        b = odds - 1  # Decimal odds minus 1
        p = model_prob
        q = 1 - p
        
        # Full Kelly fraction
        kelly_fraction = (b * p - q) / b if b > 0 else 0
        
        # 15% Kelly for safety
        stake_fraction = kelly_fraction * self.KELLY_FRACTION
        
        # Calculate stake amount
        stake = self.bankroll * stake_fraction
        
        # Cap at 5% of bankroll max, $50 min
        max_stake = self.bankroll * self.MAX_KELLY_PCT
        stake = min(stake, max_stake)
        stake = max(stake, self.MIN_KELLY_STAKE)
        
        return round(stake, 2)
    
    def classify_pick(self, edge_pct: float) -> str:
        """
        Classify pick quality based on edge.
        """
        if edge_pct >= self.STRONG_THRESHOLD:
            return 'STRONG'
        elif edge_pct >= self.KEEP_THRESHOLD:
            return 'KEEP'
        elif edge_pct >= self.CAUTION_THRESHOLD:
            return 'CAUTION'
        else:
            return 'SKIP'
    
    def determine_stake(self, edge_pct: float, odds: float, model_prob: float, quality: str) -> float:
        """
        Determine stake size using Kelly criterion for ALL picks.
        Caps at 5% of bankroll per bet, $10 minimum.
        """
        b = odds - 1  # Decimal odds minus 1
        p = model_prob
        q = 1 - p
        
        # Full Kelly fraction
        kelly_fraction = (b * p - q) / b if b > 0 else 0
        
        # 15% Kelly for safety
        stake_fraction = kelly_fraction * self.KELLY_FRACTION
        
        # Calculate stake amount
        stake = self.bankroll * stake_fraction
        
        # Cap at 5% of bankroll max, $10 min
        max_stake = self.bankroll * self.MAX_KELLY_PCT
        stake = min(stake, max_stake)
        stake = max(stake, 10)  # $10 minimum
        
        # Quality adjustment
        if quality == 'KEEP':
            stake *= 0.7  # 70% of Kelly for KEEP
        elif quality == 'CAUTION':
            stake *= 0.5  # 50% of Kelly for CAUTION
        
        return round(stake, 2)
    
    def generate_picks(self, league: str = None, min_edge: float = 0.05) -> List[Pick]:
        """
        Generate all value picks for upcoming matches.
        
        Args:
            league: Filter by league (optional)
            min_edge: Minimum edge to include (default 5%)
        
        Returns:
            List of Pick objects sorted by edge (highest first)
        """
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Get predictions with odds
        query = '''
            SELECT 
                m.match_id, m.home_team, m.away_team, m.league, m.kickoff,
                p.prob_home_win, p.prob_draw, p.prob_away_win,
                p.prob_over_1_5, p.prob_over_2_5, p.prob_under_2_5, p.prob_btts_yes,
                o.market, o.selection, o.odds, o.implied_prob
            FROM matches m
            JOIN predictions p ON m.match_id = p.match_id
            JOIN odds o ON m.match_id = o.match_id
            WHERE m.status = 'scheduled'
        '''
        
        if league:
            query += f" AND m.league = '{league}'"
        
        c.execute(query)
        rows = c.fetchall()
        conn.close()
        
        picks = []
        
        for row in rows:
            # Map selection to model probability
            model_prob = self._get_model_prob(row)
            
            if model_prob is None:
                continue
            
            edge_pct, book_prob = self.calculate_edge(model_prob, row['odds'])
            
            if edge_pct >= min_edge:
                quality = self.classify_pick(edge_pct)
                stake = self.determine_stake(edge_pct, row['odds'], model_prob, quality)
                
                pick = Pick(
                    match_id=row['match_id'],
                    home_team=row['home_team'],
                    away_team=row['away_team'],
                    league=row['league'],
                    kickoff=row['kickoff'],
                    selection=row['selection'],
                    market=row['market'],
                    model_prob=round(model_prob, 3),
                    book_prob=book_prob,
                    edge_pct=round(edge_pct * 100, 1),  # Convert to percentage
                    odds=row['odds'],
                    stake=stake,
                    quality=quality
                )
                
                picks.append(pick)
        
        # Sort by edge (descending)
        picks.sort(key=lambda x: x.edge_pct, reverse=True)
        
        return picks
    
    def _get_model_prob(self, row) -> Optional[float]:
        """
        Map bookmaker selection to model probability.
        """
        selection = row['selection']
        home_team = row['home_team']
        away_team = row['away_team']
        
        # 1X2 markets
        if 'Win' in selection:
            if home_team in selection:
                return row['prob_home_win']
            elif away_team in selection:
                return row['prob_away_win']
        
        if selection == 'Draw':
            return row['prob_draw']
        
        # Over/Under markets
        if 'Over 1.5' in selection:
            return row['prob_over_1_5']
        if 'Over 2.5' in selection:
            return row['prob_over_2_5']
        if 'Under 2.5' in selection:
            return row['prob_under_2_5']
        
        # BTTS
        if 'BTTS Yes' in selection or 'Both Teams To Score' in selection:
            return row['prob_btts_yes']
        
        return None
    
    def save_picks(self, picks: List[Pick]) -> List[Pick]:
        """Save picks to database with bankroll-aware staking."""
        # Deduplicate: keep only highest edge per match/selection
        seen = {}
        for pick in picks:
            # Use team names as key to avoid match_id duplication issues
            key = (pick.home_team, pick.away_team, pick.selection)
            if key not in seen or pick.edge_pct > seen[key].edge_pct:
                seen[key] = pick
        
        unique_picks = list(seen.values())
        # Sort by edge and take top 12 (reduce count to stay under bankroll)
        unique_picks.sort(key=lambda x: x.edge_pct, reverse=True)
        top_picks = unique_picks[:12]
        
        # Calculate total stake (ALL qualities)
        total_stake = sum(p.stake for p in top_picks)
        
        # If total exceeds 50% of bankroll, scale down proportionally
        max_total = self.bankroll * 0.5
        if total_stake > max_total:
            scale = max_total / total_stake
            for pick in top_picks:
                pick.stake = round(pick.stake * scale, 2)
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Clear old pending picks first
        c.execute("DELETE FROM picks WHERE status = 'pending'")
        
        for pick in top_picks:
            c.execute('''
                INSERT INTO picks 
                (match_id, selection, market, model_prob, book_prob, edge_pct, 
                 odds, stake, quality, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pick.match_id, pick.selection, pick.market,
                pick.model_prob, pick.book_prob, pick.edge_pct,
                pick.odds, pick.stake, pick.quality, pick.status
            ))
        
        conn.commit()
        conn.close()
        print(f"Saved {len(top_picks)} unique picks (cleared old, top 12 by edge)")
        
        return top_picks

if __name__ == '__main__':
    calc = EdgeCalculator(use_kelly=True)
    
    # Test Kelly calculation
    stake = calc.kelly_stake(0.25, 2.0, 0.55)
    print(f"Kelly stake for 25% edge @ 2.0 odds, 55% prob: ${stake}")
    
    # Test flat calculation
    calc_flat = EdgeCalculator(use_kelly=False)
    stake_flat = calc_flat.determine_stake(0.25, 2.0, 0.55, 'STRONG')
    print(f"Flat stake: ${stake_flat}")
