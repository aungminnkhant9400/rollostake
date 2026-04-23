#!/usr/bin/env python3
"""Populate sample data for dashboard demo."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import sqlite3

from models.core import init_db
from models.dixon_coles import save_prediction
from analysis.edge_calculator import EdgeCalculator
from dashboard.generator import DashboardGenerator
from config.paths import DB_PATH, ensure_runtime_dirs

def add_sample_matches():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    matches = [
        ('m1', 'Man City', 'Liverpool', 'EPL', '2026-04-25 15:00', 'scheduled'),
        ('m2', 'Arsenal', 'Chelsea', 'EPL', '2026-04-25 17:30', 'scheduled'),
        ('m3', 'PSG', 'Marseille', 'L1', '2026-04-26 20:00', 'scheduled'),
        ('m4', 'Bayern Munich', 'Dortmund', 'Bundesliga', '2026-04-26 18:30', 'scheduled'),
        ('m5', 'Inter Milan', 'Juventus', 'SerieA', '2026-04-27 20:45', 'scheduled'),
    ]
    
    for match in matches:
        c.execute('''
            INSERT OR IGNORE INTO matches (match_id, home_team, away_team, league, kickoff, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', match)
    
    conn.commit()
    conn.close()
    print(f"Added {len(matches)} sample matches")

def add_sample_odds():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Man City vs Liverpool
    odds = [
        ('m1', '1X2', 'Man City Win', 2.10, 0.476),
        ('m1', '1X2', 'Draw', 3.40, 0.294),
        ('m1', '1X2', 'Liverpool Win', 3.60, 0.278),
        ('m1', 'OU', 'Over 2.5', 1.85, 0.541),
        ('m1', 'OU', 'Under 2.5', 2.00, 0.500),
        ('m1', 'BTTS', 'BTTS Yes', 1.70, 0.588),
        
        # Arsenal vs Chelsea
        ('m2', '1X2', 'Arsenal Win', 1.95, 0.513),
        ('m2', '1X2', 'Draw', 3.50, 0.286),
        ('m2', '1X2', 'Chelsea Win', 4.20, 0.238),
        ('m2', 'OU', 'Over 2.5', 1.90, 0.526),
        
        # PSG vs Marseille
        ('m3', '1X2', 'PSG Win', 1.55, 0.645),
        ('m3', '1X2', 'Draw', 4.20, 0.238),
        ('m3', '1X2', 'Marseille Win', 6.50, 0.154),
        ('m3', 'OU', 'Over 2.5', 1.65, 0.606),
        
        # Bayern vs Dortmund
        ('m4', '1X2', 'Bayern Munich Win', 1.70, 0.588),
        ('m4', '1X2', 'Draw', 4.00, 0.250),
        ('m4', '1X2', 'Dortmund Win', 4.80, 0.208),
        ('m4', 'BTTS', 'BTTS Yes', 1.55, 0.645),
        
        # Inter vs Juventus
        ('m5', '1X2', 'Inter Milan Win', 2.40, 0.417),
        ('m5', '1X2', 'Draw', 3.20, 0.312),
        ('m5', '1X2', 'Juventus Win', 3.10, 0.323),
        ('m5', 'OU', 'Under 2.5', 1.75, 0.571),
    ]
    
    for o in odds:
        c.execute('''
            INSERT OR REPLACE INTO odds (match_id, market, selection, odds, implied_prob)
            VALUES (?, ?, ?, ?, ?)
        ''', o)
    
    conn.commit()
    conn.close()
    print(f"Added {len(odds)} sample odds")

def add_sample_predictions():
    predictions = [
        ('m1', {
            'prob_home_win': 0.52,
            'prob_draw': 0.26,
            'prob_away_win': 0.22,
            'prob_over_2_5': 0.58,
            'prob_under_2_5': 0.42,
            'prob_btts_yes': 0.65,
        }),
        ('m2', {
            'prob_home_win': 0.48,
            'prob_draw': 0.28,
            'prob_away_win': 0.24,
            'prob_over_2_5': 0.55,
            'prob_under_2_5': 0.45,
        }),
        ('m3', {
            'prob_home_win': 0.68,
            'prob_draw': 0.20,
            'prob_away_win': 0.12,
            'prob_over_2_5': 0.62,
        }),
        ('m4', {
            'prob_home_win': 0.55,
            'prob_draw': 0.24,
            'prob_away_win': 0.21,
            'prob_btts_yes': 0.70,
        }),
        ('m5', {
            'prob_home_win': 0.42,
            'prob_draw': 0.30,
            'prob_away_win': 0.28,
            'prob_under_2_5': 0.58,
        }),
    ]
    
    for match_id, preds in predictions:
        save_prediction(match_id, preds)
    
    print(f"Added {len(predictions)} sample predictions")

def generate_and_show():
    calc = EdgeCalculator()
    picks = calc.generate_picks(min_edge=0.05)
    calc.save_picks(picks)
    
    gen = DashboardGenerator()
    path = gen.generate()
    
    print(f"\nDashboard with {len(picks)} picks generated!")
    print(f"Open: file://{path}")

if __name__ == '__main__':
    ensure_runtime_dirs()
    init_db()
    add_sample_matches()
    add_sample_odds()
    add_sample_predictions()
    generate_and_show()
