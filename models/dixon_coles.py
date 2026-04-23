"""
Dixon-Coles Model Implementation
Poisson-based goal prediction with time decay and home advantage.
"""

import numpy as np
from scipy.optimize import minimize
from scipy.stats import poisson
from typing import List, Tuple, Dict
from dataclasses import dataclass
import sqlite3

DB_PATH = '/home/ubuntu/rollo-stake-model/data/rollo_stake.db'

@dataclass
class MatchResult:
    home_team: str
    away_team: str
    home_goals: int
    away_goals: int
    date: str
    league: str

class DixonColesModel:
    """
    Dixon-Coles model for predicting soccer match outcomes.
    
    Parameters:
    - alpha: home team attack strength
    - beta: home team defense strength  
    - gamma: away team attack strength
    - delta: away team defense strength
    - rho: home advantage factor
    """
    
    def __init__(self):
        self.teams = set()
        self.params = {}
        self.home_advantage = 1.35  # Home teams score ~35% more
        self.rho = -0.08  # Dixon-Coles correlation parameter (low scores correlated)
        self.avg_goals_home = 1.55
        self.avg_goals_away = 1.15
        
    def _dc_correction(self, home_goals, away_goals, lambda_h, lambda_a, rho):
        """
        Dixon-Coles correction factor for low-score matches.
        Accounts for correlation between low scores (0-0, 1-0, 0-1, 1-1).
        """
        if home_goals == 0 and away_goals == 0:
            return 1 - lambda_h * lambda_a * rho
        elif home_goals == 0 and away_goals == 1:
            return 1 + lambda_h * rho
        elif home_goals == 1 and away_goals == 0:
            return 1 + lambda_a * rho
        elif home_goals == 1 and away_goals == 1:
            return 1 - rho
        else:
            return 1.0
    
    def _log_likelihood(self, params, matches: List[MatchResult], team_index: Dict):
        """
        Calculate log-likelihood for parameter optimization.
        """
        n_teams = len(team_index)
        # params: [attack_home(n), defense_home(n), attack_away(n), defense_away(n)]
        # Simplified: use same attack/defense for home/away but with home advantage
        
        attack = params[:n_teams]
        defense = params[n_teams:2*n_teams]
        home_adv = params[2*n_teams] if len(params) > 2*n_teams else 1.35
        rho = params[2*n_teams + 1] if len(params) > 2*n_teams + 1 else -0.08
        
        log_lik = 0
        for match in matches:
            i = team_index[match.home_team]
            j = team_index[match.away_team]
            
            lambda_h = attack[i] * defense[j] * home_adv
            lambda_a = attack[j] * defense[i]
            
            # Poisson probability with DC correction
            base_prob = (poisson.pmf(match.home_goals, lambda_h) * 
                        poisson.pmf(match.away_goals, lambda_a))
            
            correction = self._dc_correction(
                match.home_goals, match.away_goals, 
                lambda_h, lambda_a, rho
            )
            
            prob = base_prob * correction
            log_lik += np.log(max(prob, 1e-10))
        
        return -log_lik  # Minimize negative log-likelihood
    
    def fit(self, matches: List[MatchResult]):
        """
        Fit model parameters to historical match data.
        Uses most recent 200 matches for speed if dataset is large.
        """
        # Limit to most recent 200 matches for speed
        if len(matches) > 200:
            matches = sorted(matches, key=lambda x: x.date, reverse=True)[:200]
            print(f"Using most recent 200 matches (from {len(matches)} total)")
        
        # Build team index
        for match in matches:
            self.teams.add(match.home_team)
            self.teams.add(match.away_team)
        
        team_index = {team: i for i, team in enumerate(sorted(self.teams))}
        n_teams = len(team_index)
        
        # Initial parameters: attack=1.0, defense=1.0, home_adv=1.35, rho=-0.08
        x0 = np.ones(2 * n_teams + 2)
        x0[2*n_teams] = 1.35  # home advantage
        x0[2*n_teams + 1] = -0.08  # rho
        
        # Constraints: attack and defense should be positive
        bounds = [(0.1, 3.0)] * (2 * n_teams) + [(1.0, 2.0), (-0.3, 0.0)]
        
        # Optimize
        result = minimize(
            self._log_likelihood,
            x0,
            args=(matches, team_index),
            method='L-BFGS-B',
            bounds=bounds,
            options={'maxiter': 100}
        )
        
        # Store parameters
        self.params = {
            'attack': result.x[:n_teams],
            'defense': result.x[n_teams:2*n_teams],
            'home_advantage': result.x[2*n_teams],
            'rho': result.x[2*n_teams + 1],
            'team_index': team_index
        }
        
        print(f"Model fitted. Final log-likelihood: {-result.fun:.2f}")
        return self
    
    def predict(self, home_team: str, away_team: str) -> Dict:
        """
        Predict match outcome probabilities.
        Returns dict with lambda values and outcome probabilities.
        """
        if not self.params:
            raise ValueError("Model not fitted yet. Call fit() first.")
        
        team_index = self.params['team_index']
        
        if home_team not in team_index or away_team not in team_index:
            # Teams not in training data - use league averages
            lambda_h = self.avg_goals_home
            lambda_a = self.avg_goals_away
        else:
            i = team_index[home_team]
            j = team_index[away_team]
            
            attack = self.params['attack']
            defense = self.params['defense']
            home_adv = self.params['home_advantage']
            
            lambda_h = attack[i] * defense[j] * home_adv
            lambda_a = attack[j] * defense[i]
        
        # Calculate outcome probabilities by summing over score matrices
        max_goals = 10
        
        # Score matrix
        probs = np.zeros((max_goals + 1, max_goals + 1))
        for h in range(max_goals + 1):
            for a in range(max_goals + 1):
                base = (poisson.pmf(h, lambda_h) * poisson.pmf(a, lambda_a))
                correction = self._dc_correction(h, a, lambda_h, lambda_a, self.params['rho'])
                probs[h, a] = base * correction
        
        # Normalize
        probs = probs / probs.sum()
        
        # Outcome probabilities
        prob_home = np.sum(np.tril(probs, -1))  # Home wins
        prob_draw = np.sum(np.diag(probs))       # Draws
        prob_away = np.sum(np.triu(probs, 1))    # Away wins
        
        # Over/Under probabilities
        prob_over_1_5 = 1 - probs[0, 0] - probs[1, 0] - probs[0, 1]
        prob_over_2_5 = 1 - np.sum(probs[:3, :3])
        prob_under_2_5 = np.sum(probs[:3, :3])
        
        # BTTS
        btts_matrix = probs.copy()
        btts_matrix[0, :] = 0  # No home goals
        btts_matrix[:, 0] = 0  # No away goals
        prob_btts = btts_matrix.sum() / (1 - probs[0, 0])  # Conditional on not 0-0
        
        return {
            'lambda_h': round(lambda_h, 3),
            'lambda_a': round(lambda_a, 3),
            'prob_home_win': round(prob_home, 3),
            'prob_draw': round(prob_draw, 3),
            'prob_away_win': round(prob_away, 3),
            'prob_over_1_5': round(prob_over_1_5, 3),
            'prob_over_2_5': round(prob_over_2_5, 3),
            'prob_under_2_5': round(prob_under_2_5, 3),
            'prob_btts_yes': round(prob_btts, 3),
        }

def save_prediction(match_id: str, preds: Dict):
    """Save prediction to database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''
        INSERT OR REPLACE INTO predictions 
        (match_id, lambda_h, lambda_a, prob_home_win, prob_draw, prob_away_win,
         prob_over_1_5, prob_over_2_5, prob_under_2_5, prob_btts_yes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        match_id, 
        preds.get('lambda_h', 0),
        preds.get('lambda_a', 0),
        preds.get('prob_home_win', 0),
        preds.get('prob_draw', 0),
        preds.get('prob_away_win', 0),
        preds.get('prob_over_1_5', 0),
        preds.get('prob_over_2_5', 0), 
        preds.get('prob_under_2_5', 0),
        preds.get('prob_btts_yes', 0)
    ))
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    # Test with sample data
    sample_matches = [
        MatchResult('Team A', 'Team B', 2, 1, '2026-04-01', 'EPL'),
        MatchResult('Team B', 'Team A', 0, 2, '2026-04-02', 'EPL'),
        MatchResult('Team A', 'Team C', 3, 0, '2026-04-03', 'EPL'),
        MatchResult('Team C', 'Team A', 1, 1, '2026-04-04', 'EPL'),
    ]
    
    model = DixonColesModel()
    model.fit(sample_matches)
    
    preds = model.predict('Team A', 'Team B')
    print("\nPrediction for Team A vs Team B:")
    for k, v in preds.items():
        print(f"  {k}: {v}")
