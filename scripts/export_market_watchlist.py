#!/usr/bin/env python3
"""Export model-ranked market candidates for manual odds shopping.

This does not invent bookmaker odds. It scans scheduled fixtures with saved
model predictions and lists the sportsbook price needed to create value.
"""

import argparse
import csv
import math
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from models.core import init_db


MATCH_TOTAL_LINES = (0.5, 1.5, 2.5, 3.0, 3.5)
TEAM_TOTAL_LINES = (0.5, 1.5, 2.5)


def _poisson_pmf(goals: int, expected: float) -> float:
    return math.exp(-expected) * (expected**goals) / math.factorial(goals)


def _score_distribution(lambda_h: float, lambda_a: float, max_goals: int = 10) -> dict:
    dist = {}
    for home_goals in range(max_goals + 1):
        for away_goals in range(max_goals + 1):
            dist[(home_goals, away_goals)] = _poisson_pmf(home_goals, lambda_h) * _poisson_pmf(away_goals, lambda_a)

    total = sum(dist.values())
    if total <= 0:
        return {}
    return {score: prob / total for score, prob in dist.items()}


def _decision_prob(win_prob: float, loss_prob: float):
    total = win_prob + loss_prob
    if total <= 0:
        return None
    return win_prob / total


def _match_total_prob(dist: dict, line: float, over: bool):
    win_prob = 0.0
    loss_prob = 0.0
    for (home_goals, away_goals), prob in dist.items():
        total_goals = home_goals + away_goals
        if over:
            if total_goals > line:
                win_prob += prob
            elif total_goals < line:
                loss_prob += prob
        else:
            if total_goals < line:
                win_prob += prob
            elif total_goals > line:
                loss_prob += prob
    return _decision_prob(win_prob, loss_prob)


def _team_total_prob(dist: dict, home_side: bool, line: float, over: bool):
    win_prob = 0.0
    loss_prob = 0.0
    for (home_goals, away_goals), prob in dist.items():
        goals = home_goals if home_side else away_goals
        if over:
            if goals > line:
                win_prob += prob
            elif goals < line:
                loss_prob += prob
        else:
            if goals < line:
                win_prob += prob
            elif goals > line:
                loss_prob += prob
    return _decision_prob(win_prob, loss_prob)


def _range_for_required_odds(required_odds: float) -> str:
    if 2.50 <= required_odds <= 5.00:
        return "C"
    if 1.70 <= required_odds <= 2.70:
        return "D"
    return "WATCH"


def _add_candidate(candidates: list, fixture: dict, market: str, selection: str, model_prob, min_edge: float):
    if model_prob is None or model_prob <= 0 or model_prob >= 1:
        return

    fair_odds = 1.0 / model_prob
    required_odds = (1.0 + min_edge) / model_prob
    candidates.append(
        {
            "range_hint": _range_for_required_odds(required_odds),
            "match_id": fixture["match_id"],
            "league": fixture["league"],
            "kickoff": fixture["kickoff"],
            "home_team": fixture["home_team"],
            "away_team": fixture["away_team"],
            "market": market,
            "selection": selection,
            "model_prob": round(model_prob, 4),
            "fair_odds": round(fair_odds, 3),
            "min_odds_for_edge": round(required_odds, 3),
            "lambda_h": round(float(fixture["lambda_h"]), 3),
            "lambda_a": round(float(fixture["lambda_a"]), 3),
        }
    )


def build_watchlist(min_edge: float = 0.05, max_rows: int = 160) -> list:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        """
        SELECT m.match_id, m.home_team, m.away_team, m.league, m.kickoff,
               p.lambda_h, p.lambda_a,
               COALESCE(p.adj_prob_home, p.prob_home_win) AS prob_home_win,
               COALESCE(p.adj_prob_draw, p.prob_draw) AS prob_draw,
               COALESCE(p.adj_prob_away, p.prob_away_win) AS prob_away_win,
               p.prob_btts_yes
        FROM matches m
        JOIN predictions p ON m.match_id = p.match_id
        WHERE m.status = 'scheduled'
        ORDER BY m.kickoff, m.league, m.home_team
        """
    )
    fixtures = [dict(row) for row in c.fetchall()]
    conn.close()

    candidates = []
    for fixture in fixtures:
        lambda_h = max(float(fixture["lambda_h"] or 0), 0.01)
        lambda_a = max(float(fixture["lambda_a"] or 0), 0.01)
        dist = _score_distribution(lambda_h, lambda_a)

        _add_candidate(candidates, fixture, "1X2", f"{fixture['home_team']} Win", fixture["prob_home_win"], min_edge)
        _add_candidate(candidates, fixture, "1X2", "Draw", fixture["prob_draw"], min_edge)
        _add_candidate(candidates, fixture, "1X2", f"{fixture['away_team']} Win", fixture["prob_away_win"], min_edge)

        for line in MATCH_TOTAL_LINES:
            label = f"{line:g}"
            _add_candidate(candidates, fixture, "OU", f"Over {label}", _match_total_prob(dist, line, True), min_edge)
            _add_candidate(candidates, fixture, "OU", f"Under {label}", _match_total_prob(dist, line, False), min_edge)

        for line in TEAM_TOTAL_LINES:
            label = f"{line:g}"
            _add_candidate(candidates, fixture, "TT", f"{fixture['home_team']} O{label}", _team_total_prob(dist, True, line, True), min_edge)
            _add_candidate(candidates, fixture, "TT", f"{fixture['home_team']} U{label}", _team_total_prob(dist, True, line, False), min_edge)
            _add_candidate(candidates, fixture, "TT", f"{fixture['away_team']} O{label}", _team_total_prob(dist, False, line, True), min_edge)
            _add_candidate(candidates, fixture, "TT", f"{fixture['away_team']} U{label}", _team_total_prob(dist, False, line, False), min_edge)

        btts_yes = float(fixture["prob_btts_yes"] or 0)
        _add_candidate(candidates, fixture, "BTTS", "BTTS Yes", btts_yes, min_edge)
        _add_candidate(candidates, fixture, "BTTS", "BTTS No", 1.0 - btts_yes, min_edge)

    candidates.sort(
        key=lambda item: (
            {"D": 0, "C": 1, "WATCH": 2}.get(item["range_hint"], 3),
            item["min_odds_for_edge"],
            item["kickoff"],
        )
    )
    return candidates[:max_rows]


def export_watchlist(path: str, min_edge: float, max_rows: int) -> list:
    rows = build_watchlist(min_edge=min_edge, max_rows=max_rows)
    fieldnames = [
        "range_hint",
        "match_id",
        "league",
        "kickoff",
        "home_team",
        "away_team",
        "market",
        "selection",
        "model_prob",
        "fair_odds",
        "min_odds_for_edge",
        "lambda_h",
        "lambda_a",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Export model-ranked markets that need bookmaker odds")
    parser.add_argument("--output", default="market_watchlist.csv", help="CSV output path")
    parser.add_argument("--min-edge", type=float, default=0.05, help="Required edge, e.g. 0.05 for 5%%")
    parser.add_argument("--max-rows", type=int, default=160, help="Maximum rows to export")
    args = parser.parse_args()

    rows = export_watchlist(args.output, args.min_edge, args.max_rows)
    print(f"Exported {len(rows)} market candidates to {args.output}")
    for row in rows[:20]:
        print(
            f"{row['range_hint']} {row['league']} {row['home_team']} vs {row['away_team']} "
            f"{row['selection']} model={row['model_prob']:.1%} "
            f"fair={row['fair_odds']:.2f} need>={row['min_odds_for_edge']:.2f}"
        )


if __name__ == "__main__":
    main()
