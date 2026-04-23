#!/usr/bin/env python3
"""
Team News & Adjustments Module
Manually adjust model predictions based on real-world factors the model can't see.

Factors supported:
- Injuries/suspensions (key players missing)
- New signings (transfers in)
- Manager change (tactical shift)
- Recent form (last 5 games trend)
- Motivation (title race, relegation, derby)
- Weather/conditions (if extreme)
"""

import sqlite3
import sys
from typing import List, Dict, Optional
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH

@dataclass
class TeamAdjustment:
    team: str
    factor: str          # 'injury', 'transfer_in', 'manager', 'form', 'motivation'
    description: str     # e.g., "Erling Haaland injured - out 3 weeks"
    impact: float        # -0.3 to +0.3 (negative = hurts team, positive = helps)
    affected_area: str   # 'attack', 'defense', 'overall'

class TeamNewsAdjuster:
    """
    Adjusts model predictions based on manual team news input.
    
    Impact guidelines:
    - Star player injured: -0.15 to -0.25 (attack or defense)
    - Multiple injuries: -0.20 to -0.30 (overall)
    - Key signing: +0.10 to +0.20 (attack or defense)
    - New manager (positive): +0.05 to +0.15 (overall)
    - New manager (negative/transition): -0.05 to -0.15 (overall)
    - Title race motivation: +0.05 to +0.10 (overall)
    - Relegation scrap: +0.05 to +0.15 (overall)
    - Derby/motivated: +0.05 to +0.10 (overall)
    """
    
    def __init__(self):
        self.adjustments = []
    
    def add_injury(self, team: str, player: str, position: str, importance: str = 'key'):
        """
        Add injury adjustment.
        
        Args:
            team: Team name
            player: Player name
            position: 'striker', 'midfield', 'defense', 'goalkeeper'
            importance: 'key' (-0.20), 'squad' (-0.10), 'star' (-0.25)
        """
        impact_map = {'squad': -0.10, 'key': -0.20, 'star': -0.25}
        area_map = {'striker': 'attack', 'midfield': 'overall', 'defense': 'defense', 'goalkeeper': 'defense'}
        
        impact = impact_map.get(importance, -0.15)
        area = area_map.get(position, 'overall')
        
        adj = TeamAdjustment(
            team=team,
            factor='injury',
            description=f"{player} ({position}) injured",
            impact=impact,
            affected_area=area
        )
        self.adjustments.append(adj)
        print(f"Added: {team} - {player} injury ({impact:+.0%} {area})")
    
    def add_transfer(self, team: str, player: str, position: str, quality: str = 'good'):
        """
        Add new signing adjustment.
        
        Args:
            quality: 'squad' (+0.05), 'good' (+0.15), 'star' (+0.25)
        """
        impact_map = {'squad': 0.05, 'good': 0.15, 'star': 0.25}
        area_map = {'striker': 'attack', 'midfield': 'overall', 'defense': 'defense', 'goalkeeper': 'defense'}
        
        impact = impact_map.get(quality, 0.10)
        area = area_map.get(position, 'overall')
        
        adj = TeamAdjustment(
            team=team,
            factor='transfer_in',
            description=f"Signed {player} ({position})",
            impact=impact,
            affected_area=area
        )
        self.adjustments.append(adj)
        print(f"Added: {team} - Signed {player} ({impact:+.0%} {area})")
    
    def add_manager_change(self, team: str, new_manager: str, expected_impact: str = 'neutral'):
        """
        Add manager change adjustment.
        
        Args:
            expected_impact: 'positive' (+0.10), 'negative' (-0.10), 'neutral' (0)
        """
        impact_map = {'negative': -0.10, 'neutral': 0.0, 'positive': 0.10}
        impact = impact_map.get(expected_impact, 0.0)
        
        adj = TeamAdjustment(
            team=team,
            factor='manager',
            description=f"New manager: {new_manager}",
            impact=impact,
            affected_area='overall'
        )
        self.adjustments.append(adj)
        if impact != 0:
            print(f"Added: {team} - Manager {new_manager} ({impact:+.0%} overall)")
    
    def add_motivation(self, team: str, situation: str):
        """
        Add motivation factor.
        
        Args:
            situation: 'title_race', 'relegation', 'derby', 'european_spot', 'cup_final'
        """
        impact_map = {
            'title_race': 0.08,
            'relegation': 0.12,
            'derby': 0.08,
            'european_spot': 0.06,
            'cup_final': 0.10
        }
        impact = impact_map.get(situation, 0.05)
        
        adj = TeamAdjustment(
            team=team,
            factor='motivation',
            description=situation.replace('_', ' ').title(),
            impact=impact,
            affected_area='overall'
        )
        self.adjustments.append(adj)
        print(f"Added: {team} - {situation.replace('_', ' ').title()} motivation ({impact:+.0%})")
    
    def get_team_adjustment(self, team: str) -> Dict:
        """
        Get total adjustment for a team.
        
        Returns:
            {'attack': float, 'defense': float, 'overall': float}
        """
        team_adjs = [a for a in self.adjustments if a.team == team]
        
        result = {'attack': 0.0, 'defense': 0.0, 'overall': 0.0}
        
        for adj in team_adjs:
            result[adj.affected_area] += adj.impact
        
        # Clamp to reasonable bounds
        for key in result:
            result[key] = max(-0.50, min(0.50, result[key]))
        
        return result
    
    def apply_to_predictions(self, match_id: str, home_team: str, away_team: str, 
                            base_probs: Dict) -> Dict:
        """
        Apply adjustments to base model predictions.
        
        Args:
            match_id: Match identifier
            base_probs: Dict with 'prob_home_win', 'prob_draw', 'prob_away_win'
        
        Returns:
            Adjusted probabilities
        """
        home_adj = self.get_team_adjustment(home_team)
        away_adj = self.get_team_adjustment(away_team)
        
        # Start with base probabilities
        home_win = base_probs['prob_home_win']
        draw = base_probs['prob_draw']
        away_win = base_probs['prob_away_win']
        
        # Calculate net adjustment
        # Home attack vs Away defense
        home_attack_boost = home_adj['attack'] - away_adj['defense']
        away_attack_boost = away_adj['attack'] - home_adj['defense']
        
        # Overall factors (motivation, manager, etc.)
        home_overall = home_adj['overall']
        away_overall = away_adj['overall']
        
        # Apply adjustments
        # Boost home win probability if home attack improved or away defense weakened
        home_boost = home_attack_boost + home_overall
        away_boost = away_attack_boost + away_overall
        
        # Convert to probability shifts (dampened to avoid extreme shifts)
        home_shift = home_boost * 0.5  # Dampen to avoid overcorrection
        away_shift = away_boost * 0.5
        
        # Adjust probabilities
        new_home = max(0.05, min(0.95, home_win + home_shift))
        new_away = max(0.05, min(0.95, away_win + away_shift))
        
        # Draw probability adjusts to maintain sum ≈ 1
        remaining = 1.0 - new_home - new_away
        new_draw = max(0.05, min(0.90, remaining))
        
        # Normalize to ensure they sum to 1
        total = new_home + new_draw + new_away
        new_home /= total
        new_draw /= total
        new_away /= total
        
        return {
            'prob_home_win': round(new_home, 3),
            'prob_draw': round(new_draw, 3),
            'prob_away_win': round(new_away, 3),
            'home_adjustments': home_adj,
            'away_adjustments': away_adj,
            'adjustment_note': f"Home: {home_overall:+.0%} overall, Away: {away_overall:+.0%} overall"
        }
    
    def save_to_db(self, match_id: str, adjusted_probs: Dict):
        """Save adjusted predictions to database."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Check if predictions table has adjustment columns
        c.execute("PRAGMA table_info(predictions)")
        columns = [col[1] for col in c.fetchall()]
        
        if 'adj_prob_home' not in columns:
            c.execute('ALTER TABLE predictions ADD COLUMN adj_prob_home REAL')
            c.execute('ALTER TABLE predictions ADD COLUMN adj_prob_draw REAL')
            c.execute('ALTER TABLE predictions ADD COLUMN adj_prob_away REAL')
            c.execute('ALTER TABLE predictions ADD COLUMN adjustment_note TEXT')
        
        c.execute('''
            UPDATE predictions 
            SET adj_prob_home = ?,
                adj_prob_draw = ?,
                adj_prob_away = ?,
                adjustment_note = ?
            WHERE match_id = ?
        ''', (
            adjusted_probs['prob_home_win'],
            adjusted_probs['prob_draw'],
            adjusted_probs['prob_away_win'],
            adjusted_probs.get('adjustment_note', ''),
            match_id
        ))
        
        conn.commit()
        conn.close()

def interactive_adjustments():
    """Interactive interface for adding team news."""
    adjuster = TeamNewsAdjuster()
    
    print("\n" + "="*60)
    print("TEAM NEWS & ADJUSTMENTS")
    print("="*60)
    print("\nAdd factors the model can't see:")
    print("1. Injury - key player missing")
    print("2. Transfer - new signing")
    print("3. Manager - new coach")
    print("4. Motivation - title race, relegation, derby")
    print("5. Done - finish and apply")
    
    while True:
        choice = input("\nChoice (1-5): ").strip()
        
        if choice == '5':
            break
        elif choice == '1':
            team = input("Team: ").strip()
            player = input("Player: ").strip()
            pos = input("Position (striker/midfield/defense/goalkeeper): ").strip()
            imp = input("Importance (star/key/squad): ").strip()
            adjuster.add_injury(team, player, pos, imp)
        elif choice == '2':
            team = input("Team: ").strip()
            player = input("Player signed: ").strip()
            pos = input("Position: ").strip()
            qual = input("Quality (star/good/squad): ").strip()
            adjuster.add_transfer(team, player, pos, qual)
        elif choice == '3':
            team = input("Team: ").strip()
            manager = input("New manager: ").strip()
            impact = input("Expected impact (positive/negative/neutral): ").strip()
            adjuster.add_manager_change(team, manager, impact)
        elif choice == '4':
            team = input("Team: ").strip()
            sit = input("Situation (title_race/relegation/derby/european_spot/cup_final): ").strip()
            adjuster.add_motivation(team, sit)
        else:
            print("Invalid choice")
    
    return adjuster

if __name__ == '__main__':
    # Demo adjustments
    adj = TeamNewsAdjuster()
    
    # Example: Liverpool vs Man City with news
    adj.add_injury("Man City", "Erling Haaland", "striker", "star")
    adj.add_transfer("Liverpool", "Antonio", "striker", "good")
    adj.add_motivation("Liverpool", "title_race")
    
    # Get adjustments
    city_adj = adj.get_team_adjustment("Man City")
    liverpool_adj = adj.get_team_adjustment("Liverpool")
    
    print(f"\nMan City adjustments: {city_adj}")
    print(f"Liverpool adjustments: {liverpool_adj}")
    
    # Apply to sample prediction
    base = {'prob_home_win': 0.40, 'prob_draw': 0.25, 'prob_away_win': 0.35}
    adjusted = adj.apply_to_predictions("test", "Liverpool", "Man City", base)
    
    print(f"\nBase: H={base['prob_home_win']:.1%} D={base['prob_draw']:.1%} A={base['prob_away_win']:.1%}")
    print(f"Adjusted: H={adjusted['prob_home_win']:.1%} D={adjusted['prob_draw']:.1%} A={adjusted['prob_away_win']:.1%}")
    print(f"Note: {adjusted['adjustment_note']}")
