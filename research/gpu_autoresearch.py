#!/usr/bin/env python3
"""
GPU AutoResearch for a PyTorch Dixon-Coles model.

This runner uses the same historical odds/candidate evaluation layer as
autoresearch.py, but replaces the SciPy model with TorchDixonColesModel. On the
A100 server it should run with --device cuda or --device auto.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import load_settings
from models.torch_dixon_coles import TorchDixonColesConfig, TorchDixonColesModel, TorchMatch
from research.autoresearch import (
    CACHE_DIR,
    RESULTS_DIR,
    Candidate,
    HistoricalMatch,
    build_candidates_for_match,
    build_config_grid,
    candidate_cache_path,
    evaluate_config,
    load_candidate_batches,
    load_historical_matches,
    parse_date,
    parse_markets,
    print_summary,
    save_candidate_batches,
    write_outputs,
)


def torch_matches(matches: Sequence[HistoricalMatch]) -> List[TorchMatch]:
    return [
        TorchMatch(
            home_team=match.home_team,
            away_team=match.away_team,
            home_goals=match.home_goals,
            away_goals=match.away_goals,
            date=match.match_date.isoformat(),
            league=match.league,
        )
        for match in matches
    ]


def gpu_candidate_cache_path(
    leagues: Sequence[str],
    seasons: Sequence[str],
    markets: Sequence[str],
    train_size_per_league: int,
    min_train_size_per_league: int,
    batch_days: int,
    start_date: Optional[date],
    end_date: Optional[date],
    model_config: TorchDixonColesConfig,
) -> Path:
    payload = {
        "version": 1,
        "model": "torch_dixon_coles",
        "leagues": list(leagues),
        "seasons": list(seasons),
        "candidate_markets": list(markets),
        "train_size_per_league": train_size_per_league,
        "min_train_size_per_league": min_train_size_per_league,
        "batch_days": batch_days,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "model_config": asdict(model_config),
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"gpu_candidates_{digest}.json"


def train_window(
    by_league: Dict[str, List[HistoricalMatch]],
    current: date,
    train_size_per_league: int,
) -> List[HistoricalMatch]:
    train = []
    for league_matches in by_league.values():
        train.extend([m for m in league_matches if m.match_date < current][-train_size_per_league:])
    train.sort(key=lambda m: (m.match_date, m.league, m.home_team, m.away_team))
    return train


def test_window(
    by_league: Dict[str, List[HistoricalMatch]],
    current: date,
    window_end: date,
) -> List[HistoricalMatch]:
    test = []
    for league_matches in by_league.values():
        test.extend([m for m in league_matches if current <= m.match_date < window_end])
    test.sort(key=lambda m: (m.match_date, m.league, m.home_team, m.away_team))
    return test


def has_enough_training(
    by_league: Dict[str, List[HistoricalMatch]],
    current: date,
    min_train_size_per_league: int,
) -> bool:
    for league_matches in by_league.values():
        if len([m for m in league_matches if m.match_date < current]) < min_train_size_per_league:
            return False
    return True


def build_gpu_candidate_batches(
    matches: Sequence[HistoricalMatch],
    leagues: Sequence[str],
    markets: Sequence[str],
    train_size_per_league: int,
    min_train_size_per_league: int,
    batch_days: int,
    start_date: Optional[date],
    end_date: Optional[date],
    model_config: TorchDixonColesConfig,
    device: str,
) -> List[List[Candidate]]:
    by_league = {
        league: sorted([m for m in matches if m.league == league], key=lambda m: m.match_date)
        for league in leagues
    }
    if not matches:
        return []

    first_date = start_date or min(m.match_date for m in matches)
    last_date = end_date or max(m.match_date for m in matches)
    batches = []
    current = first_date
    fit_count = 0

    while current <= last_date:
        window_end = current + timedelta(days=batch_days)
        batch_id = f"{current.isoformat()}_{(window_end - timedelta(days=1)).isoformat()}"
        test = test_window(by_league, current, window_end)
        if test and has_enough_training(by_league, current, min_train_size_per_league):
            train = train_window(by_league, current, train_size_per_league)
            model = TorchDixonColesModel(model_config, device=device)
            info = model.fit(torch_matches(train))
            fit_count += 1
            if fit_count == 1 or fit_count % 10 == 0:
                print(
                    f"  GPU fit {fit_count}: {batch_id} train={len(train)} "
                    f"test={len(test)} loss={info['loss']:.4f} device={info['device']}",
                    flush=True,
                )

            batch_candidates = []
            for match in test:
                preds = model.predict(match.home_team, match.away_team, match.league)
                batch_candidates.extend(build_candidates_for_match(batch_id, match, preds, markets))
            if batch_candidates:
                batches.append(batch_candidates)
        current = window_end

    print(f"Built {len(batches)} GPU candidate batches from {fit_count} model fits", flush=True)
    return batches


def main():
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Run GPU Torch Dixon-Coles AutoResearch")
    parser.add_argument("--leagues", nargs="+", default=settings.get("leagues", ["EPL", "L1", "Bundesliga", "SerieA", "LaLiga"]))
    parser.add_argument("--seasons", nargs="+", default=["2122", "2223", "2324", "2425", "2526"])
    parser.add_argument("--markets", default="1X2,OU")
    parser.add_argument("--candidate-markets", default="1X2,OU")
    parser.add_argument("--train-size-per-league", type=int, default=300)
    parser.add_argument("--min-train-size-per-league", type=int, default=120)
    parser.add_argument("--batch-days", type=int, default=14)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--epochs", type=int, default=500)
    parser.add_argument("--lr", type=float, default=0.035)
    parser.add_argument("--weight-decay", type=float, default=0.001)
    parser.add_argument("--half-life-days", type=float, default=365.0)
    parser.add_argument("--device", default="auto", help="auto, cuda, cuda:0, or cpu")
    parser.add_argument("--stake", type=float, default=float(settings.get("flat_stake", 10)))
    parser.add_argument("--bankroll", type=float, default=float(settings.get("bankroll", 100)))
    parser.add_argument("--min-picks", type=int, default=40)
    parser.add_argument("--c-max-picks", default="0,1,2")
    parser.add_argument("--d-max-picks", default="1,2,3")
    parser.add_argument("--min-edges", default="0.15,0.25,0.35")
    parser.add_argument("--max-picks-per-match", default="1")
    parser.add_argument("--max-family-per-match", default="1")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--no-candidate-cache", action="store_true")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.quick:
        args.seasons = args.seasons[-1:]
        args.leagues = args.leagues[:2]
        args.train_size_per_league = 80
        args.min_train_size_per_league = 30
        args.epochs = min(args.epochs, 80)
        args.c_max_picks = "0,1"
        args.d_max_picks = "1"
        args.min_edges = "0.15"

    start_date = parse_date(args.start_date) if args.start_date else None
    end_date = parse_date(args.end_date) if args.end_date else None
    eval_markets = parse_markets(args.markets)
    candidate_markets = parse_markets(args.candidate_markets)

    model_config = TorchDixonColesConfig(
        epochs=args.epochs,
        lr=args.lr,
        weight_decay=args.weight_decay,
        half_life_days=args.half_life_days,
        verbose=args.verbose,
    )
    cache_path = gpu_candidate_cache_path(
        leagues=args.leagues,
        seasons=args.seasons,
        markets=candidate_markets,
        train_size_per_league=args.train_size_per_league,
        min_train_size_per_league=args.min_train_size_per_league,
        batch_days=args.batch_days,
        start_date=start_date,
        end_date=end_date,
        model_config=model_config,
    )

    candidate_batches = None
    if not args.no_candidate_cache and not args.refresh_cache:
        candidate_batches = load_candidate_batches(cache_path)

    if candidate_batches is None:
        matches = load_historical_matches(args.leagues, args.seasons, refresh_cache=args.refresh_cache)
        print(f"Training Torch Dixon-Coles candidates on device={args.device}", flush=True)
        candidate_batches = build_gpu_candidate_batches(
            matches=matches,
            leagues=args.leagues,
            markets=candidate_markets,
            train_size_per_league=args.train_size_per_league,
            min_train_size_per_league=args.min_train_size_per_league,
            batch_days=args.batch_days,
            start_date=start_date,
            end_date=end_date,
            model_config=model_config,
            device=args.device,
        )
        if not args.no_candidate_cache:
            save_candidate_batches(cache_path, candidate_batches)

    configs = build_config_grid(args)
    print(f"\nPrepared {len(candidate_batches)} GPU weekly batches and {len(configs)} configs")
    results = [
        evaluate_config(config, candidate_batches, args.stake, args.bankroll, args.min_picks)
        for config in configs
    ]
    results.sort(key=lambda r: (r["score"], r["picks"], r["roi_pct"]), reverse=True)

    leaderboard_path, best_path, breakdown_path, picks_path = write_outputs(
        results,
        RESULTS_DIR,
        args.top,
        candidate_batches,
        args.stake,
    )
    print_summary(results, candidate_batches, args.top, args.stake)
    print(f"\nLeaderboard: {leaderboard_path}")
    print(f"Best config JSON: {best_path}")
    if breakdown_path:
        print(f"Breakdown CSV: {breakdown_path}")
    if picks_path:
        print(f"Best picks CSV: {picks_path}")


if __name__ == "__main__":
    main()
