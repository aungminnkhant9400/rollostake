# Rollo Stake Model v1.0
# Systematic value-betting engine for soccer
# Separation of concerns: each module does ONE thing

import json
import os
import sqlite3
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Tuple
import requests
from bs4 import BeautifulSoup
import numpy as np
from scipy.optimize import minimize
from scipy.stats import poisson

# Config
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH = os.path.join(DATA_DIR, 'rollo_stake.db')

def init_db():
    """Initialize SQLite database for persistence."""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Matches table
    c.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY,
            match_id TEXT UNIQUE,
            home_team TEXT,
            away_team TEXT,
            league TEXT,
            kickoff TIMESTAMP,
            home_goals INTEGER,
            away_goals INTEGER,
            status TEXT DEFAULT 'scheduled',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Odds table
    c.execute('''
        CREATE TABLE IF NOT EXISTS odds (
            id INTEGER PRIMARY KEY,
            match_id TEXT,
            bookmaker TEXT DEFAULT 'stake',
            market TEXT,
            selection TEXT,
            odds REAL,
            implied_prob REAL,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
    ''')
    
    # Model predictions
    c.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY,
            match_id TEXT,
            lambda_h REAL,
            lambda_a REAL,
            prob_home_win REAL,
            prob_draw REAL,
            prob_away_win REAL,
            prob_over_1_5 REAL,
            prob_over_2_5 REAL,
            prob_under_2_5 REAL,
            prob_btts_yes REAL,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
    ''')
    
    # Picks / bets
    c.execute('''
        CREATE TABLE IF NOT EXISTS picks (
            id INTEGER PRIMARY KEY,
            match_id TEXT,
            selection TEXT,
            market TEXT,
            model_prob REAL,
            book_prob REAL,
            edge_pct REAL,
            odds REAL,
            stake REAL DEFAULT 200,
            quality TEXT,
            status TEXT DEFAULT 'pending',
            result TEXT,
            pnl REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
    ''')
    
    # Bankroll tracking
    c.execute('''
        CREATE TABLE IF NOT EXISTS bankroll (
            id INTEGER PRIMARY KEY,
            week INTEGER,
            starting_bank REAL,
            total_staked REAL,
            total_pnl REAL,
            ending_bank REAL,
            roi_pct REAL,
            record TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("Database initialized.")
