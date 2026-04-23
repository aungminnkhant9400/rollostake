#!/usr/bin/env python3
"""
Backtesting Framework
Tests the model on historical data to validate performance.
"""

import sqlite3
import sys
sys.path.insert(0, '/home/ubuntu/rollo-stake-model')

from typing import List, Dict, Tuple
from datetime import datetime
from models.dixon_coles import DixonColesModel, MatchResult
from analysis.edge_calculator import EdgeCalculator
from analysis.fatigue import FatigueAnalyzer

DB_PATH = '/home/ubuntu/rollo-stake-model/data/rollo_stake.db'

class Backtester:
    """
    Backtests the betting model on historical data.
    
    Process:
    1. Split data into training and test sets
    2. Train model on training data
    3. Generate predictions for test matches
    4. Calculate edges and simulate picks
    5. Compare to actual results
    6. Calculate ROI, win rate, etc.
    """
    
    def __init__(self, bankroll: float = 10000.0, stake: float = 200.0):
        self.bankroll = bankroll
        self.initial_bankroll = bankroll
        self.stake = stake
        self.results = []
        
    def load_matches(self, league: str = None, min_date: str = None, max_date: str = None) -> List[MatchResult]:
        """Load completed matches from database."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        query = '''
            SELECT home_team, away_team, home_goals, away_goals, kickoff, league
            FROM matches
            WHERE status = 'completed' AND home_goals IS NOT NULL
        '''
        
        if league:
            query += f" AND league = '{league}'"
        if min_date:
            query += f" AND kickoff >= '{min_date}'"
        if max_date:
            query += f" AND kickoff <= '{max_date}'"
        
        query += " ORDER BY kickoff"
        
        c.execute(query)
        rows = c.fetchall()
        conn.close()
        
        matches = []
        for row in rows:
            matches.append(MatchResult(
                home_team=row[0],
                away_team=row[1],
                home_goals=row[2],
                away_goals=row[3],
                date=row[4],
                league=row[5]
            ))
        
        return matches
    
    def run_backtest(self, train_size: int = 50, test_size: int = 20, 
                     min_edge: float = 0.05, use_fatigue: bool = False) -> Dict:
        """
        Run backtest on historical data.
        
        Args:
            train_size: Number of matches to train on
            test_size: Number of matches to test on
            min_edge: Minimum edge to place bet
            use_fatigue: Include fatigue adjustment
        
        Returns:
            Backtest results dictionary
        """
        matches = self.load_matches()
        
        if len(matches) < train_size + test_size:
            print(f"Not enough data. Have {len(matches)}, need {train_size + test_size}")
            return {}
        
        # Split data
        train_matches = matches[:train_size]
        test_matches = matches[train_size:train_size + test_size]
        
        print(f"Training on {len(train_matches)} matches...")
        print(f"Testing on {len(test_matches)} matches...")
        
        # Train model
        model = DixonColesModel()
        model.fit(train_matches)
        
        # Test
        picks_made = 0
        picks_won = 0
        picks_lost = 0
        total_pnl = 0.0
        total_staked = 0.0
        
        results = []
        
        for match in test_matches:
            # Generate prediction
            preds = model.predict(match.home_team, match.away_team)
            
            # Simulate finding odds (in reality would use historical odds)
            # For backtest, use fair odds + margin
            epsilon = 1e-10  # Prevent division by zero
            fair_home = 1.0 / (preds['prob_home_win'] + epsilon)
            fair_draw = 1.0 / (preds['prob_draw'] + epsilon)
            fair_away = 1.0 / (preds['prob_away_win'] + epsilon)
            
            # Add bookmaker margin (assume 5%)
            margin = 0.05
            book_home = fair_home * (1 + margin)
            book_draw = fair_draw * (1 + margin)
            book_away = fair_away * (1 + margin)
            
            # Calculate edges
            edges = [
                ('home', preds['prob_home_win'], book_home),
                ('draw', preds['prob_draw'], book_draw),
                ('away', preds['prob_away_win'], book_away),
            ]
            
            for outcome, prob, odds in edges:
                calc = EdgeCalculator()
                edge_pct, implied = calc.calculate_edge(prob, odds)
                
                if edge_pct >= min_edge:
                    # Simulate bet
                    picks_made += 1
                    total_staked += self.stake
                    
                    # Determine result
                    actual_result = None
                    if match.home_goals > match.away_goals:
                        actual_result = 'home'
                    elif match.home_goals < match.away_goals:
                        actual_result = 'away'
                    else:
                        actual_result = 'draw'
                    
                    if outcome == actual_result:
                        pnl = self.stake * (odds - 1)
                        picks_won += 1
                    else:
                        pnl = -self.stake
                        picks_lost += 1
                    
                    total_pnl += pnl
                    
                    results.append({
                        'match': f"{match.home_team} vs {match.away_team}",
                        'date': match.date,
                        'selection': outcome,
                        'odds': round(odds, 2),
                        'edge': round(edge_pct * 100, 1),
                        'result': 'win' if outcome == actual_result else 'loss',
                        'pnl': round(pnl, 2),
                    })
        
        # Calculate metrics
        win_rate = (picks_won / picks_made * 100) if picks_made > 0 else 0
        roi = (total_pnl / total_staked * 100) if total_staked > 0 else 0
        final_bankroll = self.initial_bankroll + total_pnl
        
        return {
            'train_size': train_size,
            'test_size': test_size,
            'picks_made': picks_made,
            'picks_won': picks_won,
            'picks_lost': picks_lost,
            'win_rate': round(win_rate, 1),
            'total_staked': round(total_staked, 2),
            'total_pnl': round(total_pnl, 2),
            'roi': round(roi, 2),
            'initial_bankroll': self.initial_bankroll,
            'final_bankroll': round(final_bankroll, 2),
            'results': results,
        }
    
    def print_report(self, results: Dict):
        """Print backtest report."""
        if not results:
            print("No backtest results to display.")
            return
        
        print("\n" + "="*60)
        print("BACKTEST RESULTS")
        print("="*60)
        print(f"Training matches: {results['train_size']}")
        print(f"Test matches: {results['test_size']}")
        print(f"Picks made: {results['picks_made']}")
        print(f"Win rate: {results['win_rate']}%")
        print(f"Total staked: ${results['total_staked']:,.2f}")
        print(f"Total P&L: ${results['total_pnl']:+,}")
        print(f"ROI: {results['roi']:+.2f}%")
        print(f"Bankroll: ${results['initial_bankroll']:,.0f} → ${results['final_bankroll']:,.2f}")
        print("="*60)
        
        if results['results']:
            print("\nRecent picks:")
            for r in results['results'][-10:]:
                status = "✅" if r['result'] == 'win' else "❌"
                print(f"  {status} {r['match']} - {r['selection']} @ {r['odds']} ({r['edge']}% edge) → {r['result']} ${r['pnl']:+.0f}")

if __name__ == '__main__':
    # First load sample data if needed
    from scrapers.historical_loader import HistoricalDataLoader
    
    loader = HistoricalDataLoader()
    loader.load_sample_data()
    
    # Run backtest
    backtester = Backtester(bankroll=10000.0, stake=200.0)
    results = backtester.run_backtest(
        train_size=15,
        test_size=10,
        min_edge=0.05
    )
    
    backtester.print_report(results)
