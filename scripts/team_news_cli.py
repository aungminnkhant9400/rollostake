#!/usr/bin/env python3
"""
Quick Team News Input
One-liner interface for adjusting predictions with team news.
"""

import sys
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.team_news import TeamNewsAdjuster
from models.dixon_coles import save_prediction
from config.paths import DB_PATH

def quick_adjust():
    """Quick interactive adjustment for upcoming matches."""
    
    # Show upcoming matches
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT match_id, home_team, away_team, league, kickoff
        FROM matches
        WHERE status = 'scheduled'
        ORDER BY kickoff
    ''')
    matches = c.fetchall()
    conn.close()
    
    if not matches:
        print("No upcoming matches found")
        return
    
    print("\nUPCOMING MATCHES:")
    for i, (mid, home, away, league, kickoff) in enumerate(matches, 1):
        print(f"  {i}. {home} vs {away} ({league}) - {kickoff}")
    
    print("\nEnter adjustments (or 'done' to finish):")
    print("Format: <team> <factor> <details>")
    print("Examples:")
    print("  'Liverpool injury Salander striker star'")
    print("  'Man City transfer Haaland striker good'")
    print("  'Arsenal manager Arteta positive'")
    print("  'Chelsea motivation derby'")
    print("  'done'")
    
    adjuster = TeamNewsAdjuster()
    
    while True:
        user_input = input("\n> ").strip()
        
        if user_input.lower() == 'done':
            break
        
        parts = user_input.split()
        if len(parts) < 3:
            print("Format: <team> <factor> <details>")
            continue
        
        team = parts[0]
        factor = parts[1].lower()
        
        try:
            if factor == 'injury':
                # Format: team injury player position importance
                player = parts[2]
                position = parts[3] if len(parts) > 3 else 'midfield'
                importance = parts[4] if len(parts) > 4 else 'key'
                adjuster.add_injury(team, player, position, importance)
            
            elif factor == 'transfer':
                # Format: team transfer player position quality
                player = parts[2]
                position = parts[3] if len(parts) > 3 else 'midfield'
                quality = parts[4] if len(parts) > 4 else 'good'
                adjuster.add_transfer(team, player, position, quality)
            
            elif factor == 'manager':
                # Format: team manager name impact
                manager = parts[2]
                impact = parts[3] if len(parts) > 3 else 'neutral'
                adjuster.add_manager_change(team, manager, impact)
            
            elif factor == 'motivation':
                # Format: team motivation situation
                situation = parts[2]
                adjuster.add_motivation(team, situation)
            
            else:
                print(f"Unknown factor: {factor}. Use: injury, transfer, manager, motivation")
        
        except Exception as e:
            print(f"Error: {e}")
    
    # Apply adjustments to all upcoming matches
    if adjuster.adjustments:
        print(f"\nApplying {len(adjuster.adjustments)} adjustments...")
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        for mid, home, away, league, kickoff in matches:
            # Get base predictions
            c.execute('''
                SELECT prob_home_win, prob_draw, prob_away_win
                FROM predictions
                WHERE match_id = ?
            ''', (mid,))
            
            row = c.fetchone()
            if row:
                base = {
                    'prob_home_win': row[0],
                    'prob_draw': row[1],
                    'prob_away_win': row[2]
                }
                
                adjusted = adjuster.apply_to_predictions(mid, home, away, base)
                
                # Save adjusted predictions
                adjuster.save_to_db(mid, adjusted)
                
                if abs(adjusted['prob_home_win'] - base['prob_home_win']) > 0.02:
                    print(f"  {home} vs {away}: H {base['prob_home_win']:.1%} → {adjusted['prob_home_win']:.1%}")
        
        conn.close()
        print("\nAdjustments applied! Regenerate picks to see updated edges.")
    else:
        print("No adjustments made.")

if __name__ == '__main__':
    quick_adjust()
