"""
PyTorch Dixon-Coles style model.

This keeps the same football logic as the existing SciPy Dixon-Coles model, but
optimizes the likelihood with torch tensors so training can run on CUDA/A100.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class TorchMatch:
    home_team: str
    away_team: str
    home_goals: int
    away_goals: int
    date: str
    league: str


@dataclass
class TorchDixonColesConfig:
    epochs: int = 600
    lr: float = 0.035
    weight_decay: float = 0.001
    half_life_days: float = 365.0
    max_goals: int = 10
    min_lambda: float = 0.05
    max_lambda: float = 6.0
    verbose: bool = False


class TorchDixonColesModel:
    """GPU-trainable Dixon-Coles style goal model with league effects."""

    def __init__(self, config: Optional[TorchDixonColesConfig] = None, device: str = "auto"):
        try:
            import torch
        except Exception as exc:  # pragma: no cover - exercised on missing dependency
            raise RuntimeError("PyTorch is required for TorchDixonColesModel") from exc

        self.torch = torch
        self.config = config or TorchDixonColesConfig()
        if device == "auto":
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        else:
            self.device = torch.device(device)

        self.team_index: Dict[str, int] = {}
        self.league_index: Dict[str, int] = {}
        self.params: Dict[str, "torch.Tensor"] = {}
        self.fitted = False

    def _date_value(self, value: str) -> datetime:
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%d/%m/%Y", "%d/%m/%y"):
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                continue
        return datetime(2000, 1, 1)

    def _build_indices(self, matches: Iterable[TorchMatch]) -> None:
        teams = set()
        leagues = set()
        for match in matches:
            teams.add(match.home_team)
            teams.add(match.away_team)
            leagues.add(match.league)
        self.team_index = {team: idx for idx, team in enumerate(sorted(teams))}
        self.league_index = {league: idx for idx, league in enumerate(sorted(leagues))}

    def _prepare_tensors(self, matches: List[TorchMatch]):
        torch = self.torch
        latest = max(self._date_value(match.date) for match in matches)
        home_idx = []
        away_idx = []
        league_idx = []
        home_goals = []
        away_goals = []
        weights = []

        for match in matches:
            home_idx.append(self.team_index[match.home_team])
            away_idx.append(self.team_index[match.away_team])
            league_idx.append(self.league_index[match.league])
            home_goals.append(match.home_goals)
            away_goals.append(match.away_goals)

            age_days = max((latest - self._date_value(match.date)).days, 0)
            if self.config.half_life_days > 0:
                weights.append(0.5 ** (age_days / self.config.half_life_days))
            else:
                weights.append(1.0)

        return {
            "home_idx": torch.tensor(home_idx, dtype=torch.long, device=self.device),
            "away_idx": torch.tensor(away_idx, dtype=torch.long, device=self.device),
            "league_idx": torch.tensor(league_idx, dtype=torch.long, device=self.device),
            "home_goals": torch.tensor(home_goals, dtype=torch.float32, device=self.device),
            "away_goals": torch.tensor(away_goals, dtype=torch.float32, device=self.device),
            "weights": torch.tensor(weights, dtype=torch.float32, device=self.device),
        }

    def _lambdas(self, tensors, params):
        torch = self.torch
        attack = params["attack"]
        defense = params["defense"]
        home_adv = params["home_adv"]
        league_home_base = params["league_home_base"]
        league_away_base = params["league_away_base"]

        home_log = (
            league_home_base[tensors["league_idx"]]
            + home_adv[tensors["league_idx"]]
            + attack[tensors["home_idx"]]
            - defense[tensors["away_idx"]]
        )
        away_log = (
            league_away_base[tensors["league_idx"]]
            + attack[tensors["away_idx"]]
            - defense[tensors["home_idx"]]
        )
        lambda_h = torch.exp(home_log).clamp(self.config.min_lambda, self.config.max_lambda)
        lambda_a = torch.exp(away_log).clamp(self.config.min_lambda, self.config.max_lambda)
        return lambda_h, lambda_a

    def _rho(self, params):
        return -0.30 * self.torch.sigmoid(params["rho_raw"])

    def _dc_correction_tensor(self, home_goals, away_goals, lambda_h, lambda_a, rho):
        torch = self.torch
        correction = torch.ones_like(lambda_h)

        mask_00 = (home_goals == 0) & (away_goals == 0)
        mask_01 = (home_goals == 0) & (away_goals == 1)
        mask_10 = (home_goals == 1) & (away_goals == 0)
        mask_11 = (home_goals == 1) & (away_goals == 1)

        correction = torch.where(mask_00, 1 - lambda_h * lambda_a * rho, correction)
        correction = torch.where(mask_01, 1 + lambda_h * rho, correction)
        correction = torch.where(mask_10, 1 + lambda_a * rho, correction)
        correction = torch.where(mask_11, 1 - rho, correction)
        return correction.clamp_min(1e-6)

    def _loss(self, tensors, params):
        torch = self.torch
        lambda_h, lambda_a = self._lambdas(tensors, params)
        home_goals = tensors["home_goals"]
        away_goals = tensors["away_goals"]
        weights = tensors["weights"]
        rho = self._rho(params)

        log_home = home_goals * torch.log(lambda_h) - lambda_h - torch.lgamma(home_goals + 1)
        log_away = away_goals * torch.log(lambda_a) - lambda_a - torch.lgamma(away_goals + 1)
        correction = self._dc_correction_tensor(home_goals, away_goals, lambda_h, lambda_a, rho)
        nll = -(log_home + log_away + torch.log(correction))
        weighted = (nll * weights).sum() / weights.sum().clamp_min(1e-6)

        reg = (
            params["attack"].pow(2).mean()
            + params["defense"].pow(2).mean()
            + params["home_adv"].pow(2).mean()
        )
        return weighted + self.config.weight_decay * reg

    def fit(self, matches: List[TorchMatch]):
        if len(matches) < 10:
            raise ValueError("TorchDixonColesModel needs at least 10 matches")

        torch = self.torch
        self._build_indices(matches)
        tensors = self._prepare_tensors(matches)
        n_teams = len(self.team_index)
        n_leagues = len(self.league_index)

        params = {
            "attack": torch.zeros(n_teams, device=self.device, requires_grad=True),
            "defense": torch.zeros(n_teams, device=self.device, requires_grad=True),
            "home_adv": torch.full((n_leagues,), 0.15, device=self.device, requires_grad=True),
            "league_home_base": torch.full((n_leagues,), math.log(1.45), device=self.device, requires_grad=True),
            "league_away_base": torch.full((n_leagues,), math.log(1.15), device=self.device, requires_grad=True),
            "rho_raw": torch.tensor(-1.25, device=self.device, requires_grad=True),
        }

        optimizer = torch.optim.Adam(params.values(), lr=self.config.lr)
        last_loss = None
        for epoch in range(1, self.config.epochs + 1):
            optimizer.zero_grad(set_to_none=True)
            loss = self._loss(tensors, params)
            loss.backward()
            optimizer.step()

            with torch.no_grad():
                params["attack"].sub_(params["attack"].mean())
                params["defense"].sub_(params["defense"].mean())

            last_loss = float(loss.detach().cpu())
            if self.config.verbose and (epoch == 1 or epoch % 100 == 0 or epoch == self.config.epochs):
                print(f"epoch={epoch} loss={last_loss:.4f} device={self.device}", flush=True)

        self.params = {key: value.detach().clone() for key, value in params.items()}
        self.fitted = True
        return {"loss": last_loss, "device": str(self.device), "teams": n_teams, "leagues": n_leagues}

    def _lambda_for_match(self, home_team: str, away_team: str, league: str) -> Tuple[float, float, float]:
        torch = self.torch
        if not self.fitted or home_team not in self.team_index or away_team not in self.team_index:
            return 1.45, 1.15, -0.08

        league_idx = self.league_index.get(league)
        if league_idx is None:
            league_idx = 0

        tensors = {
            "home_idx": torch.tensor([self.team_index[home_team]], dtype=torch.long, device=self.device),
            "away_idx": torch.tensor([self.team_index[away_team]], dtype=torch.long, device=self.device),
            "league_idx": torch.tensor([league_idx], dtype=torch.long, device=self.device),
        }
        with torch.no_grad():
            lambda_h, lambda_a = self._lambdas(tensors, self.params)
            rho = self._rho(self.params)
        return float(lambda_h[0].cpu()), float(lambda_a[0].cpu()), float(rho.cpu())

    def predict_score_distribution(
        self,
        home_team: str,
        away_team: str,
        league: str,
        max_goals: Optional[int] = None,
    ) -> Dict[Tuple[int, int], float]:
        max_goals = self.config.max_goals if max_goals is None else max_goals
        lambda_h, lambda_a, rho = self._lambda_for_match(home_team, away_team, league)
        dist = {}
        for home_goals in range(max_goals + 1):
            home_prob = math.exp(-lambda_h) * (lambda_h**home_goals) / math.factorial(home_goals)
            for away_goals in range(max_goals + 1):
                away_prob = math.exp(-lambda_a) * (lambda_a**away_goals) / math.factorial(away_goals)
                correction = self._dc_correction_scalar(home_goals, away_goals, lambda_h, lambda_a, rho)
                dist[(home_goals, away_goals)] = max(home_prob * away_prob * correction, 0.0)
        total = sum(dist.values())
        if total > 0:
            dist = {score: prob / total for score, prob in dist.items()}
        return dist

    def _dc_correction_scalar(self, home_goals: int, away_goals: int, lambda_h: float, lambda_a: float, rho: float) -> float:
        if home_goals == 0 and away_goals == 0:
            return max(1 - lambda_h * lambda_a * rho, 1e-6)
        if home_goals == 0 and away_goals == 1:
            return max(1 + lambda_h * rho, 1e-6)
        if home_goals == 1 and away_goals == 0:
            return max(1 + lambda_a * rho, 1e-6)
        if home_goals == 1 and away_goals == 1:
            return max(1 - rho, 1e-6)
        return 1.0

    def predict(self, home_team: str, away_team: str, league: str) -> Dict[str, float]:
        lambda_h, lambda_a, _ = self._lambda_for_match(home_team, away_team, league)
        dist = self.predict_score_distribution(home_team, away_team, league)

        prob_home = sum(prob for (h, a), prob in dist.items() if h > a)
        prob_draw = sum(prob for (h, a), prob in dist.items() if h == a)
        prob_away = sum(prob for (h, a), prob in dist.items() if h < a)
        prob_under_2_5 = sum(prob for (h, a), prob in dist.items() if h + a <= 2)
        prob_over_2_5 = 1 - prob_under_2_5
        prob_over_1_5 = sum(prob for (h, a), prob in dist.items() if h + a > 1.5)
        prob_btts = sum(prob for (h, a), prob in dist.items() if h > 0 and a > 0)

        return {
            "lambda_h": round(lambda_h, 4),
            "lambda_a": round(lambda_a, 4),
            "prob_home_win": round(prob_home, 5),
            "prob_draw": round(prob_draw, 5),
            "prob_away_win": round(prob_away, 5),
            "prob_over_1_5": round(prob_over_1_5, 5),
            "prob_over_2_5": round(prob_over_2_5, 5),
            "prob_under_2_5": round(prob_under_2_5, 5),
            "prob_btts_yes": round(prob_btts, 5),
        }
