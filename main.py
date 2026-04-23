#!/usr/bin/env python3
"""
Rollo Stake Model v1.0 - Main Orchestrator
Runs the full pipeline: load history â†’ fit model â†’ scrape odds â†’ fatigue â†’ edge â†’ dashboard
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from models.core import init_db
from models.dixon_coles import DixonColesModel, MatchResult, save_prediction
from scrapers.historical_loader import HistoricalDataLoader, get_historical_matches
from scrapers.stake_scraper import fetch_all_leagues
from analysis.edge_calculator import EdgeCalculator
from analysis.fatigue import FatigueAnalyzer, save_fatigue_analysis
from dashboard.generator import DashboardGenerator
from tests.backtest import Backtester
from config.paths import DB_PATH
from config.settings import load_settings

def run_pipeline(leagues=None, skip_scrape=False, use_fatigue=True):
    """
    Run the full betting model pipeline.
    
    Pipeline:
    1. Initialize database
    2. Load historical match data
    3. Fit Dixon-Coles model
    4. Scrape odds from Stake.com (if not skipped)
    5. Fatigue analysis for upcoming matches
    6. Generate predictions
    7. Calculate edges and generate picks
    8. Generate HTML dashboard
    """
    print("=" * 60)
    print("ROLLO STAKE MODEL v1.0")
    print("=" * 60)
    settings = load_settings()
    requested_leagues = leagues
    configured_leagues = requested_leagues if requested_leagues else settings.get('leagues')
    
    # Step 1: Initialize database
    print("\n[1/8] Initializing database...")
    init_db()
    
    # Step 2: Load historical data from database (real data)
    print("\n[2/8] Loading historical data...")
    
    # Try loading from database first (has real data from football-data.co.uk)
    import sqlite3
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    c.execute('SELECT home_team, away_team, home_goals, away_goals, kickoff, league FROM matches WHERE status="completed" AND home_goals IS NOT NULL')
    rows = c.fetchall()
    db.close()
    
    historical = []
    for row in rows:
        historical.append({
            'home_team': row[0], 'away_team': row[1],
            'home_goals': row[2], 'away_goals': row[3],
            'date': row[4], 'league': row[5]
        })
    
    if len(historical) < 50:
        print(f"Only {len(historical)} matches in DB, loading sample data...")
        loader = HistoricalDataLoader()
        historical = loader.load_sample_data()
    
    print(f"Total historical matches: {len(historical)}")
    
    # Step 2b: Load upcoming fixtures
    print("\n[2b/8] Loading upcoming fixtures...")
    fixture_source = settings.get('fixture_source', 'manual')
    if fixture_source == 'manual':
        upcoming = get_upcoming_matches()
        if configured_leagues:
            upcoming = [m for m in upcoming if m['league'] in configured_leagues]
        if not upcoming:
            print("No manual fixtures found. Add fixtures with:")
            print("  python3 scrapers/manual_fixtures.py --interactive")
            return []
    else:
        from scrapers.fixtures import FixturesFetcher
        fetcher = FixturesFetcher(
            api_key=settings.get('api_football_key'),
            days_ahead=int(settings.get('fixture_days_ahead', 7)),
            timezone=settings.get('fixture_timezone', 'Asia/Macau'),
            season=settings.get('fixture_season'),
            use_rapidapi=bool(settings.get('api_football_use_rapidapi', False)),
        )
        upcoming = fetcher.get_all_upcoming(configured_leagues)
    
    if not upcoming:
        print("No upcoming matches found - using demo data")
        upcoming = [
            {'match_id': 'demo1', 'home_team': 'Man City', 'away_team': 'Liverpool', 
             'league': 'EPL', 'kickoff': '2026-04-26 15:00'},
            {'match_id': 'demo2', 'home_team': 'Arsenal', 'away_team': 'Chelsea',
             'league': 'EPL', 'kickoff': '2026-04-26 17:30'},
        ]
    
    print(f"Upcoming matches: {len(upcoming)}")
    
    # Step 3: Fit model (only on teams in upcoming fixtures)
    print("\n[3/8] Fitting Dixon-Coles model...")
    
    # Get teams in upcoming fixtures
    fixture_teams = set()
    for match in upcoming:
        fixture_teams.add(match['home_team'])
        fixture_teams.add(match['away_team'])
    
    # Filter historical matches to relevant teams only
    relevant_matches = []
    for m in historical:
        if m.get('home_team') in fixture_teams and m.get('away_team') in fixture_teams:
            if m.get('home_goals') is not None and m.get('away_goals') is not None:
                relevant_matches.append(MatchResult(
                    home_team=m['home_team'],
                    away_team=m['away_team'],
                    home_goals=m['home_goals'],
                    away_goals=m['away_goals'],
                    date=m.get('date', '2025-01-01'),
                    league=m.get('league', 'EPL')
                ))
    
    model = DixonColesModel()
    if len(relevant_matches) >= 10:
        model.fit(relevant_matches)
        print(f"Model fitted with {len(relevant_matches)} relevant matches")
    else:
        print("Not enough data - using default parameters")
    
    # Step 4: Scrape odds
    if not skip_scrape:
        print("\n[4/8] Scraping odds from Stake.com...")
        try:
            odds = fetch_all_leagues(configured_leagues)
            print(f"Scraped {len(odds)} odds entries")
        except Exception as e:
            print(f"Scraping failed: {e}")
            print("Using existing/demo data")
    else:
        print("\n[4/8] Skipping scrape (using existing data)")
    
    # Step 5: Generate predictions before fatigue/manual adjustments
    print("\n[5/8] Generating predictions...")
    for match in upcoming:
        preds = model.predict(match['home_team'], match['away_team'])
        save_prediction(match['match_id'], preds)

    print(f"Generated predictions for {len(upcoming)} matches")

    # Step 6: Analyze upcoming matches (already loaded in step 2b)
    print("\n[6/8] Analyzing upcoming matches...")
    print(f"  {len(upcoming)} matches scheduled")
    
    # Step 6: Fatigue analysis
    if use_fatigue:
        print("\n[6/8] Running fatigue analysis...")
        fatigue = FatigueAnalyzer()
        
        # Track if we applied any adjustments
        adjustments_applied = 0
        
        for match in upcoming:
            analysis = fatigue.analyze_matchup(
                match['home_team'], 
                match['away_team'],
                match['kickoff'][:10]
            )
            save_fatigue_analysis(match['match_id'], analysis)
            
            # Apply fatigue adjustment to predictions if significant
            if abs(analysis['fatigue_diff']) >= 10:
                # Get current prediction
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('SELECT prob_home_win, prob_away_win FROM predictions WHERE match_id = ?', (match['match_id'],))
                row = c.fetchone()
                conn.close()
                
                if row:
                    # Adjust home win probability based on fatigue advantage
                    adjustment = analysis['fatigue_diff'] * 0.001  # Small adjustment
                    new_home = max(0.05, min(0.95, row[0] + adjustment))
                    new_away = max(0.05, min(0.95, row[1] - adjustment))
                    
                    # Update prediction
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    c.execute('''
                        UPDATE predictions 
                        SET prob_home_win = ?, prob_away_win = ?
                        WHERE match_id = ?
                    ''', (new_home, new_away, match['match_id']))
                    conn.commit()
                    conn.close()
                    adjustments_applied += 1
            
            if analysis['fatigue_advantage'] != 'even':
                print(f"  {match['home_team']} vs {match['away_team']}: {analysis['fatigue_advantage_desc']} (diff: {analysis['fatigue_diff']:+.1f})")
        
        if adjustments_applied > 0:
            print(f"  Applied fatigue adjustments to {adjustments_applied} matches")
    else:
        print("\n[6/8] Skipping fatigue analysis")
    
    # Step 7b: Apply team news adjustments (if any exist)
    print("\n[7b/8] Checking team news adjustments...")
    from analysis.team_news import TeamNewsAdjuster
    adjuster = TeamNewsAdjuster()
    
    # Load any saved adjustments from previous runs
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('PRAGMA table_info(predictions)')
    columns = [col[1] for col in c.fetchall()]
    conn.close()
    
    if 'adj_prob_home' in columns:
        print("  Adjusted predictions available (run scripts/team_news_cli.py to add)")
    else:
        print("  No adjustments applied yet")
    
    # Step 8: Calculate edges and picks
    staking_mode = settings.get('staking_mode', 'kelly')
    use_ranges = bool(settings.get('use_ranges', False))
    print(f"\n[8/8] Calculating edges and generating picks ({staking_mode}, ranges={use_ranges})...")
    range_configs = EdgeCalculator.range_configs_from_settings(settings)
    calc = EdgeCalculator(
        bankroll=float(settings.get('bankroll', 1000.0)),
        staking_mode=staking_mode,
        flat_stake=float(settings.get('flat_stake', 200.0)),
        use_ranges=use_ranges,
        range_configs=range_configs,
    )
    if use_ranges:
        picks = calc.generate_range_picks(requested_leagues[0] if requested_leagues else None)
        saved_picks = calc.save_range_picks(picks)
    else:
        picks = calc.generate_picks(requested_leagues[0] if requested_leagues else None, min_edge=float(settings.get('min_edge', 0.05)))
        saved_picks = calc.save_picks(
            picks,
            max_picks=int(settings.get('max_picks', 12)),
            scale_to_bankroll=staking_mode != 'flat',
        )
    
    # Count by quality
    strong = sum(1 for p in saved_picks if p.quality == 'STRONG')
    keep = sum(1 for p in saved_picks if p.quality == 'KEEP')
    caution = sum(1 for p in saved_picks if p.quality == 'CAUTION')
    total_stake = sum(p.stake for p in saved_picks)
    range_c = sum(1 for p in saved_picks if p.range_code == 'C')
    range_d = sum(1 for p in saved_picks if p.range_code == 'D')
    
    print(f"Generated {len(saved_picks)} picks:")
    print(f"  STRONG: {strong}")
    print(f"  KEEP: {keep}")
    print(f"  CAUTION: {caution}")
    print(f"  Range C: {range_c}")
    print(f"  Range D: {range_d}")
    print(f"  Total stake: ${total_stake:.0f}")
    
    # Generate dashboard
    print("\n" + "=" * 60)
    gen = DashboardGenerator()
    dashboard_path = gen.generate()
    
    print("\nPipeline complete!")
    print(f"Dashboard: file://{dashboard_path}")
    print(f"\nRun backtest: python3 tests/backtest.py")
    print(f"View dashboard: python3 -m http.server 8080 --directory {os.path.dirname(dashboard_path)}")
    
    return picks

def get_upcoming_matches():
    """Get upcoming matches from database."""
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''
        SELECT match_id, home_team, away_team, league, kickoff
        FROM matches
        WHERE status = 'scheduled'
        ORDER BY kickoff
    ''')
    
    matches = [
        {
            'match_id': row[0],
            'home_team': row[1],
            'away_team': row[2],
            'league': row[3],
            'kickoff': row[4]
        }
        for row in c.fetchall()
    ]
    
    conn.close()
    return matches

def update_results(match_id, result, home_goals=None, away_goals=None):
    """Update match results and calculate P&L."""
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Update match status
    c.execute('''
        UPDATE matches SET status = ?, home_goals = ?, away_goals = ?
        WHERE match_id = ?
    ''', ('completed', home_goals, away_goals, match_id))
    
    # Update picks P&L
    c.execute('''
        UPDATE picks SET status = 'settled', result = ?
        WHERE match_id = ?
    ''', (result, match_id))
    
    # Calculate P&L for each pick
    c.execute('''
        SELECT id, odds, stake, range_code, quality FROM picks WHERE match_id = ?
    ''', (match_id,))
    
    for pick_id, odds, stake, range_code, quality in c.fetchall():
        if result == 'win':
            pnl = stake * (odds - 1)
            payout = stake + pnl
        elif result == 'loss':
            pnl = -stake
            payout = 0
        else:
            pnl = 0
            payout = stake
        
        c.execute(
            'UPDATE picks SET pnl = ?, payout = ?, settled_at = CURRENT_TIMESTAMP WHERE id = ?',
            (pnl, payout, pick_id)
        )
        c.execute('DELETE FROM results WHERE pick_id = ?', (pick_id,))
        c.execute(
            '''
            INSERT INTO results
            (pick_id, match_id, range_code, quality, result, home_goals, away_goals,
             stake, odds, payout, pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (pick_id, match_id, range_code, quality, result, home_goals, away_goals,
             stake, odds, payout, pnl)
        )
    
    conn.commit()
    conn.close()
    
    print(f"Updated results for {match_id}: {result}")


def settle_pick(pick_id, result):
    """Settle one pick and calculate pick-level P&L."""
    import sqlite3

    result = result.lower()
    if result not in {'win', 'loss', 'push', 'pending'}:
        raise ValueError("Result must be one of: win, loss, push, pending")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT match_id, odds, stake, range_code, quality FROM picks WHERE id = ?', (pick_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        raise ValueError(f"No pick found with id {pick_id}")

    match_id, odds, stake, range_code, quality = row
    if result == 'win':
        pnl = stake * (odds - 1)
        payout = stake + pnl
        status = 'settled'
    elif result == 'loss':
        pnl = -stake
        payout = 0
        status = 'settled'
    elif result == 'push':
        pnl = 0
        payout = stake
        status = 'settled'
    else:
        pnl = None
        payout = 0
        status = 'pending'
        result = None

    c.execute(
        '''
        UPDATE picks
        SET status = ?, result = ?, pnl = ?, payout = ?,
            settled_at = CASE WHEN ? = 'settled' THEN CURRENT_TIMESTAMP ELSE NULL END
        WHERE id = ?
        ''',
        (status, result, pnl, payout, status, pick_id)
    )
    c.execute('DELETE FROM results WHERE pick_id = ?', (pick_id,))
    if status == 'settled':
        c.execute(
            '''
            INSERT INTO results
            (pick_id, match_id, range_code, quality, result, stake, odds, payout, pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (pick_id, match_id, range_code, quality, result, stake, odds, payout, pnl)
        )
    conn.commit()
    conn.close()

    print(f"Updated pick {pick_id}: {result or 'pending'}")

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Rollo Stake Model')
    parser.add_argument('--skip-scrape', action='store_true', help='Skip odds scraping')
    parser.add_argument('--leagues', nargs='+', help='Leagues to process')
    parser.add_argument('--no-fatigue', action='store_true', help='Skip fatigue analysis')
    parser.add_argument('--backtest', action='store_true', help='Run backtest only')
    parser.add_argument('--update-result', nargs=4, metavar=('MATCH_ID', 'RESULT', 'HG', 'AG'), help='Update match result')
    parser.add_argument('--settle-pick', nargs=2, metavar=('PICK_ID', 'RESULT'), help='Settle one pick: win, loss, push, or pending')
    
    args = parser.parse_args()
    
    if args.backtest:
        backtester = Backtester(bankroll=10000.0, stake=200.0)
        results = backtester.run_backtest(
            train_size=20,
            test_size=15,
            min_edge=0.05
        )
        backtester.print_report(results)
    elif args.update_result:
        match_id, result, hg, ag = args.update_result
        update_results(match_id, result, int(hg), int(ag))
    elif args.settle_pick:
        pick_id, result = args.settle_pick
        settle_pick(int(pick_id), result)
    else:
        run_pipeline(
            leagues=args.leagues,
            skip_scrape=args.skip_scrape,
            use_fatigue=not args.no_fatigue
        )
