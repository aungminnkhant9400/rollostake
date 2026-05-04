# Rollo Stake Model v1.0
# Systematic value-betting engine for soccer
# Separation of concerns: each module does ONE thing

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH, ensure_runtime_dirs

def _table_columns(cursor, table: str) -> set:
    cursor.execute(f"PRAGMA table_info({table})")
    return {col[1] for col in cursor.fetchall()}


def _add_column_if_missing(cursor, table: str, column: str, definition: str):
    columns = _table_columns(cursor, table)
    if column not in columns:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

def init_db():
    """Initialize SQLite database for persistence."""
    ensure_runtime_dirs()
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
            home_fatigue_score REAL,
            away_fatigue_score REAL,
            fatigue_advantage TEXT,
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
            adj_prob_home REAL,
            adj_prob_draw REAL,
            adj_prob_away REAL,
            adjustment_note TEXT,
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
            range_code TEXT DEFAULT 'D',
            quality TEXT,
            reasoning TEXT,
            risk_note TEXT,
            payout REAL DEFAULT 0,
            status TEXT DEFAULT 'pending',
            result TEXT,
            pnl REAL,
            settled_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
    ''')
    
    # Bankroll tracking
    c.execute('''
        CREATE TABLE IF NOT EXISTS bankroll (
            id INTEGER PRIMARY KEY,
            range_code TEXT DEFAULT 'D',
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

    # Settled match/pick results. Picks also keep denormalized result fields for
    # dashboard speed, while this table provides an audit trail.
    c.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY,
            pick_id INTEGER,
            match_id TEXT,
            range_code TEXT,
            quality TEXT,
            result TEXT,
            home_goals INTEGER,
            away_goals INTEGER,
            stake REAL,
            odds REAL,
            payout REAL,
            pnl REAL,
            settled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (pick_id) REFERENCES picks(id),
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
    ''')

    # Lightweight migrations for databases created before the Range C/D work.
    _add_column_if_missing(c, 'matches', 'home_fatigue_score', 'REAL')
    _add_column_if_missing(c, 'matches', 'away_fatigue_score', 'REAL')
    _add_column_if_missing(c, 'matches', 'fatigue_advantage', 'TEXT')
    _add_column_if_missing(c, 'predictions', 'adj_prob_home', 'REAL')
    _add_column_if_missing(c, 'predictions', 'adj_prob_draw', 'REAL')
    _add_column_if_missing(c, 'predictions', 'adj_prob_away', 'REAL')
    _add_column_if_missing(c, 'predictions', 'adjustment_note', 'TEXT')
    _add_column_if_missing(c, 'picks', 'range_code', "TEXT DEFAULT 'D'")
    _add_column_if_missing(c, 'picks', 'reasoning', 'TEXT')
    _add_column_if_missing(c, 'picks', 'risk_note', 'TEXT')
    _add_column_if_missing(c, 'picks', 'payout', 'REAL DEFAULT 0')
    _add_column_if_missing(c, 'picks', 'settled_at', 'TIMESTAMP')
    _add_column_if_missing(c, 'bankroll', 'range_code', "TEXT DEFAULT 'D'")

    # Repeated pipeline runs should refresh the prediction for a fixture, not
    # accumulate duplicate rows that later multiply market candidates.
    c.execute('''
        DELETE FROM predictions
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM predictions
            GROUP BY match_id
        )
    ''')
    c.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_match_id
        ON predictions(match_id)
    ''')
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("Database initialized.")
