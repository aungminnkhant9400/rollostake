#!/usr/bin/env python3
"""
AutoResearch runner for Rollo Stake.

This script runs rolling historical experiments over football-data.co.uk odds,
then ranks Range C/D configuration variants by ROI, drawdown, and sample size.
It is intentionally separate from main.py so experiments do not mutate the
production dashboard, picks, or SQLite database.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import statistics
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import nullcontext, redirect_stdout
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import load_settings
from models.dixon_coles import DixonColesModel, MatchResult
from scrapers.football_data import BASE_URL, LEAGUE_CODES, SEASONS


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "research"
CACHE_DIR = RESEARCH_DIR / "cache"
RESULTS_DIR = RESEARCH_DIR / "results"
SUPPORTED_MARKETS = ("1X2", "OU", "BTTS", "AH")


@dataclass(frozen=True)
class HistoricalMatch:
    match_id: str
    home_team: str
    away_team: str
    league: str
    match_date: date
    home_goals: int
    away_goals: int
    odds: Dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class Candidate:
    batch_id: str
    match_id: str
    home_team: str
    away_team: str
    league: str
    match_date: str
    market: str
    selection: str
    selection_key: str
    odds: float
    model_prob: float
    book_prob: float
    edge_pct: float
    result: str


@dataclass(frozen=True)
class ResearchConfig:
    config_id: str
    c_max_picks: int
    d_max_picks: int
    min_edge: float
    max_picks_per_match: int
    max_family_per_match: int
    markets: Tuple[str, ...]
    c_min_odds: float = 2.50
    c_max_odds: float = 5.00
    d_min_odds: float = 1.70
    d_max_odds: float = 2.70


def parse_csv_list(raw: str, cast):
    return [cast(item.strip()) for item in raw.split(",") if item.strip()]


def parse_markets(raw: str) -> Tuple[str, ...]:
    markets = tuple(item.strip().upper() for item in raw.split(",") if item.strip())
    unknown = [market for market in markets if market not in SUPPORTED_MARKETS]
    if unknown:
        raise ValueError(f"Unsupported market(s): {', '.join(unknown)}")
    return markets


def parse_date(value: str) -> Optional[date]:
    value = (value or "").strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def safe_float(row: Dict[str, str], *keys: str) -> Optional[float]:
    for key in keys:
        raw = row.get(key)
        if raw in (None, ""):
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        if value > 1.0:
            return value
    return None


def safe_number(row: Dict[str, str], *keys: str) -> Optional[float]:
    for key in keys:
        raw = row.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except ValueError:
            continue
    return None


def fetch_football_data_csv(league: str, season: str, refresh: bool = False) -> str:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    league_code = LEAGUE_CODES[league]
    cache_path = CACHE_DIR / f"{season}_{league_code}.csv"
    if cache_path.exists() and not refresh:
        return cache_path.read_text(encoding="utf-8", errors="ignore")

    url = BASE_URL.format(season=season, league=league_code)
    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 RolloStakeAutoResearch/1.0"},
    )
    response.raise_for_status()
    text = response.content.decode("utf-8", errors="ignore")
    cache_path.write_text(text, encoding="utf-8")
    return text


def load_historical_matches(
    leagues: Sequence[str],
    seasons: Sequence[str],
    refresh_cache: bool = False,
) -> List[HistoricalMatch]:
    matches: List[HistoricalMatch] = []
    for league in leagues:
        if league not in LEAGUE_CODES:
            raise ValueError(f"Unknown league: {league}")
        for season in seasons:
            label = SEASONS.get(season, season)
            print(f"Fetching research data: {label} {league}")
            text = fetch_football_data_csv(league, season, refresh=refresh_cache)
            reader = csv.DictReader(io.StringIO(text))
            loaded = 0
            for row in reader:
                home = (row.get("HomeTeam") or "").strip()
                away = (row.get("AwayTeam") or "").strip()
                if not home or not away or not row.get("FTHG") or not row.get("FTAG"):
                    continue
                match_date = parse_date(row.get("Date", ""))
                if not match_date:
                    continue
                try:
                    home_goals = int(row["FTHG"])
                    away_goals = int(row["FTAG"])
                except ValueError:
                    continue

                odds = {}
                home_odds = safe_float(row, "B365H", "AvgH", "MaxH")
                draw_odds = safe_float(row, "B365D", "AvgD", "MaxD")
                away_odds = safe_float(row, "B365A", "AvgA", "MaxA")
                over_25 = safe_float(row, "B365>2.5", "Avg>2.5", "Max>2.5")
                under_25 = safe_float(row, "B365<2.5", "Avg<2.5", "Max<2.5")
                btts_yes = safe_float(row, "B365BTTSY", "AvgBTTSY", "MaxBTTSY")
                btts_no = safe_float(row, "B365BTTSN", "AvgBTTSN", "MaxBTTSN")
                ah_line = safe_number(row, "AHh")
                ah_home = safe_float(row, "B365AHH", "AvgAHH", "MaxAHH")
                ah_away = safe_float(row, "B365AHA", "AvgAHA", "MaxAHA")

                if home_odds:
                    odds["1X2_HOME"] = home_odds
                if draw_odds:
                    odds["1X2_DRAW"] = draw_odds
                if away_odds:
                    odds["1X2_AWAY"] = away_odds
                if over_25:
                    odds["OU_OVER_2.5"] = over_25
                if under_25:
                    odds["OU_UNDER_2.5"] = under_25
                if btts_yes:
                    odds["BTTS_YES"] = btts_yes
                if btts_no:
                    odds["BTTS_NO"] = btts_no
                if ah_line is not None and ah_home:
                    odds[f"AH_HOME_{ah_line:+.2f}"] = ah_home
                if ah_line is not None and ah_away:
                    odds[f"AH_AWAY_{-ah_line:+.2f}"] = ah_away

                match_id = f"{league}_{season}_{home}_vs_{away}_{match_date.isoformat()}"
                matches.append(
                    HistoricalMatch(
                        match_id=match_id,
                        home_team=home,
                        away_team=away,
                        league=league,
                        match_date=match_date,
                        home_goals=home_goals,
                        away_goals=away_goals,
                        odds=odds,
                    )
                )
                loaded += 1
            print(f"  Loaded {loaded} completed matches")
    matches.sort(key=lambda m: (m.match_date, m.league, m.home_team, m.away_team))
    return matches


def poisson_pmf(goals: int, expected: float) -> float:
    return math.exp(-expected) * (expected**goals) / math.factorial(goals)


def score_distribution(lambda_h: float, lambda_a: float, max_goals: int = 10) -> Dict[Tuple[int, int], float]:
    dist = {}
    for home_goals in range(max_goals + 1):
        home_prob = poisson_pmf(home_goals, max(lambda_h, 0.01))
        for away_goals in range(max_goals + 1):
            dist[(home_goals, away_goals)] = home_prob * poisson_pmf(away_goals, max(lambda_a, 0.01))
    total = sum(dist.values())
    return {score: prob / total for score, prob in dist.items()} if total else dist


def decision_prob(win_prob: float, loss_prob: float) -> Optional[float]:
    total = win_prob + loss_prob
    if total <= 0:
        return None
    return win_prob / total


def total_goals_prob(dist: Dict[Tuple[int, int], float], line: float, direction: str) -> Optional[float]:
    win_prob = 0.0
    loss_prob = 0.0
    for (home_goals, away_goals), prob in dist.items():
        total = home_goals + away_goals
        if direction == "over":
            if total > line:
                win_prob += prob
            elif total < line:
                loss_prob += prob
        else:
            if total < line:
                win_prob += prob
            elif total > line:
                loss_prob += prob
    return decision_prob(win_prob, loss_prob)


def handicap_prob(
    dist: Dict[Tuple[int, int], float],
    line: float,
    side: str,
) -> Optional[float]:
    win_prob = 0.0
    loss_prob = 0.0
    for (home_goals, away_goals), prob in dist.items():
        if side == "home":
            margin = home_goals + line - away_goals
        else:
            margin = away_goals + line - home_goals
        if margin > 0:
            win_prob += prob
        elif margin < 0:
            loss_prob += prob
    return decision_prob(win_prob, loss_prob)


def calculate_edge(model_prob: float, odds: float) -> Tuple[float, float]:
    book_prob = 1.0 / odds
    edge = (model_prob - book_prob) / book_prob
    return edge, book_prob


def settle_1x2(match: HistoricalMatch, side: str) -> str:
    if match.home_goals > match.away_goals:
        actual = "home"
    elif match.away_goals > match.home_goals:
        actual = "away"
    else:
        actual = "draw"
    return "win" if side == actual else "loss"


def settle_total(match: HistoricalMatch, line: float, direction: str) -> str:
    total = match.home_goals + match.away_goals
    if total == line:
        return "push"
    if direction == "over":
        return "win" if total > line else "loss"
    return "win" if total < line else "loss"


def settle_btts(match: HistoricalMatch, want_yes: bool) -> str:
    actual_yes = match.home_goals > 0 and match.away_goals > 0
    return "win" if actual_yes == want_yes else "loss"


def settle_handicap(match: HistoricalMatch, line: float, side: str) -> str:
    if side == "home":
        margin = match.home_goals + line - match.away_goals
    else:
        margin = match.away_goals + line - match.home_goals
    if margin > 0:
        return "win"
    if margin < 0:
        return "loss"
    return "push"


def candidate_from_prob(
    batch_id: str,
    match: HistoricalMatch,
    market: str,
    selection: str,
    selection_key: str,
    odds: Optional[float],
    model_prob: Optional[float],
    result: str,
) -> Optional[Candidate]:
    if odds is None or model_prob is None or model_prob <= 0:
        return None
    edge, book_prob = calculate_edge(model_prob, odds)
    return Candidate(
        batch_id=batch_id,
        match_id=match.match_id,
        home_team=match.home_team,
        away_team=match.away_team,
        league=match.league,
        match_date=match.match_date.isoformat(),
        market=market,
        selection=selection,
        selection_key=selection_key,
        odds=round(odds, 4),
        model_prob=round(model_prob, 5),
        book_prob=round(book_prob, 5),
        edge_pct=round(edge * 100, 4),
        result=result,
    )


def build_candidates_for_match(
    batch_id: str,
    match: HistoricalMatch,
    preds: Dict[str, float],
    markets: Sequence[str],
) -> List[Candidate]:
    candidates: List[Candidate] = []
    dist = score_distribution(float(preds["lambda_h"]), float(preds["lambda_a"]))
    market_set = {m.upper() for m in markets}

    if "1X2" in market_set:
        raw = [
            (
                "1X2",
                f"{match.home_team} Win",
                "home",
                match.odds.get("1X2_HOME"),
                float(preds["prob_home_win"]),
                settle_1x2(match, "home"),
            ),
            (
                "1X2",
                "Draw",
                "draw",
                match.odds.get("1X2_DRAW"),
                float(preds["prob_draw"]),
                settle_1x2(match, "draw"),
            ),
            (
                "1X2",
                f"{match.away_team} Win",
                "away",
                match.odds.get("1X2_AWAY"),
                float(preds["prob_away_win"]),
                settle_1x2(match, "away"),
            ),
        ]
        for market, selection, key, odds, prob, result in raw:
            candidate = candidate_from_prob(batch_id, match, market, selection, key, odds, prob, result)
            if candidate:
                candidates.append(candidate)

    if "OU" in market_set:
        for direction in ("over", "under"):
            prob = total_goals_prob(dist, 2.5, direction)
            odds = match.odds.get("OU_OVER_2.5" if direction == "over" else "OU_UNDER_2.5")
            selection = f"{direction.title()} 2.5"
            result = settle_total(match, 2.5, direction)
            candidate = candidate_from_prob(batch_id, match, "OU", selection, direction, odds, prob, result)
            if candidate:
                candidates.append(candidate)

    if "BTTS" in market_set:
        btts_yes = float(preds["prob_btts_yes"])
        raw = [
            ("BTTS Yes", "yes", match.odds.get("BTTS_YES"), btts_yes, settle_btts(match, True)),
            ("BTTS No", "no", match.odds.get("BTTS_NO"), 1 - btts_yes, settle_btts(match, False)),
        ]
        for selection, key, odds, prob, result in raw:
            candidate = candidate_from_prob(batch_id, match, "BTTS", selection, key, odds, prob, result)
            if candidate:
                candidates.append(candidate)

    if "AH" in market_set:
        for key, odds in match.odds.items():
            if not key.startswith("AH_"):
                continue
            _, side, line_raw = key.split("_", 2)
            side = side.lower()
            line = float(line_raw)
            prob = handicap_prob(dist, line, side)
            team = match.home_team if side == "home" else match.away_team
            selection = f"{team} AH {line:+g}"
            result = settle_handicap(match, line, side)
            candidate = candidate_from_prob(batch_id, match, "AH", selection, f"{side}_{line:+g}", odds, prob, result)
            if candidate:
                candidates.append(candidate)

    return candidates


def train_match_results(matches: Iterable[HistoricalMatch]) -> List[MatchResult]:
    return [
        MatchResult(
            home_team=m.home_team,
            away_team=m.away_team,
            home_goals=m.home_goals,
            away_goals=m.away_goals,
            date=m.match_date.isoformat(),
            league=m.league,
        )
        for m in matches
    ]


def build_candidate_task(task) -> Tuple[str, List[Candidate]]:
    batch_id, league, train, test, markets, verbose = task
    model = DixonColesModel()
    fit_context = nullcontext() if verbose else redirect_stdout(io.StringIO())
    with fit_context:
        model.fit(train_match_results(train))

    if verbose:
        print(f"{batch_id} {league}: train={len(train)} test={len(test)}", flush=True)

    candidates: List[Candidate] = []
    for match in test:
        preds = model.predict(match.home_team, match.away_team)
        candidates.extend(build_candidates_for_match(batch_id, match, preds, markets))
    return batch_id, candidates


def build_weekly_candidate_batches(
    matches: Sequence[HistoricalMatch],
    leagues: Sequence[str],
    markets: Sequence[str],
    train_size: int,
    min_train_size: int,
    batch_days: int,
    start_date: Optional[date],
    end_date: Optional[date],
    fit_workers: int = 1,
    verbose: bool = False,
) -> List[List[Candidate]]:
    by_league: Dict[str, List[HistoricalMatch]] = {
        league: sorted([m for m in matches if m.league == league], key=lambda m: m.match_date)
        for league in leagues
    }
    if not matches:
        return []

    first_date = start_date or min(m.match_date for m in matches)
    last_date = end_date or max(m.match_date for m in matches)
    tasks = []
    current = first_date
    while current <= last_date:
        window_end = current + timedelta(days=batch_days)
        batch_id = f"{current.isoformat()}_{(window_end - timedelta(days=1)).isoformat()}"
        for league, league_matches in by_league.items():
            train = [m for m in league_matches if m.match_date < current][-train_size:]
            test = [m for m in league_matches if current <= m.match_date < window_end]
            if len(train) < min_train_size or not test:
                continue
            tasks.append((batch_id, league, train, test, tuple(markets), verbose))
        current = window_end

    print(
        f"Building historical candidate batches: {len(tasks)} rolling model fits "
        f"({fit_workers} worker{'s' if fit_workers != 1 else ''})",
        flush=True,
    )

    batches_by_id: Dict[str, List[Candidate]] = {}
    completed = 0
    if fit_workers > 1 and len(tasks) > 1:
        with ProcessPoolExecutor(max_workers=fit_workers) as pool:
            futures = [pool.submit(build_candidate_task, task) for task in tasks]
            for future in as_completed(futures):
                batch_id, candidates = future.result()
                if candidates:
                    batches_by_id.setdefault(batch_id, []).extend(candidates)
                completed += 1
                if completed == 1 or completed % 25 == 0 or completed == len(tasks):
                    print(f"  Completed {completed}/{len(tasks)} model fits", flush=True)
    else:
        for task in tasks:
            batch_id, candidates = build_candidate_task(task)
            if candidates:
                batches_by_id.setdefault(batch_id, []).extend(candidates)
            completed += 1
            if completed == 1 or completed % 25 == 0 or completed == len(tasks):
                print(f"  Completed {completed}/{len(tasks)} model fits", flush=True)

    batches = [batches_by_id[key] for key in sorted(batches_by_id)]
    return batches


def candidate_cache_path(
    leagues: Sequence[str],
    seasons: Sequence[str],
    markets: Sequence[str],
    train_size: int,
    min_train_size: int,
    batch_days: int,
    start_date: Optional[date],
    end_date: Optional[date],
) -> Path:
    payload = {
        "version": 2,
        "leagues": list(leagues),
        "seasons": list(seasons),
        "candidate_markets": list(markets),
        "train_size": train_size,
        "min_train_size": min_train_size,
        "batch_days": batch_days,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"candidates_{digest}.json"


def load_candidate_batches(path: Path) -> Optional[List[List[Candidate]]]:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    batches = []
    for raw_batch in payload.get("batches", []):
        batches.append([Candidate(**raw_candidate) for raw_candidate in raw_batch])
    print(f"Loaded candidate cache: {path}", flush=True)
    return batches


def save_candidate_batches(path: Path, batches: Sequence[Sequence[Candidate]]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_count": len(batches),
        "candidate_count": sum(len(batch) for batch in batches),
        "batches": [[asdict(candidate) for candidate in batch] for batch in batches],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    print(f"Saved candidate cache: {path}", flush=True)


def quality(edge_pct: float) -> str:
    if edge_pct >= 25:
        return "STRONG"
    if edge_pct >= 10:
        return "KEEP"
    if edge_pct >= 5:
        return "CAUTION"
    return "SKIP"


def exposure_family(candidate: Candidate) -> str:
    if candidate.market == "1X2":
        if candidate.selection_key == "draw":
            return "draw"
        return f"{candidate.selection_key}-positive"
    if candidate.market in ("OU", "BTTS"):
        if candidate.selection_key in ("under", "no"):
            return "low-goals"
        return "high-goals"
    if candidate.market == "AH":
        return f"{candidate.selection_key.split('_', 1)[0]}-positive"
    return candidate.market.lower()


def select_batch_picks(candidates: Sequence[Candidate], config: ResearchConfig) -> List[Tuple[str, Candidate]]:
    selected: List[Tuple[str, Candidate]] = []
    exposure = set()
    match_counts: Dict[str, int] = {}
    family_counts: Dict[Tuple[str, str], int] = {}

    range_defs = [
        ("C", config.c_min_odds, config.c_max_odds, config.c_max_picks),
        ("D", config.d_min_odds, config.d_max_odds, config.d_max_picks),
    ]

    def add_pick(range_code: str, candidate: Candidate) -> bool:
        exposure_key = (candidate.match_id, candidate.market, candidate.selection)
        if exposure_key in exposure:
            return False
        if match_counts.get(candidate.match_id, 0) >= config.max_picks_per_match:
            return False
        family_key = (candidate.match_id, exposure_family(candidate))
        if family_counts.get(family_key, 0) >= config.max_family_per_match:
            return False

        exposure.add(exposure_key)
        match_counts[candidate.match_id] = match_counts.get(candidate.match_id, 0) + 1
        family_counts[family_key] = family_counts.get(family_key, 0) + 1
        selected.append((range_code, candidate))
        return True

    allowed_markets = set(config.markets)
    for range_code, min_odds, max_odds, max_picks in range_defs:
        range_candidates = [
            candidate
            for candidate in candidates
            if candidate.market in allowed_markets
            and min_odds <= candidate.odds <= max_odds
            and candidate.edge_pct >= config.min_edge * 100
        ]
        range_candidates.sort(key=lambda c: (c.edge_pct, c.model_prob), reverse=True)
        count = 0
        for candidate in range_candidates:
            if count >= max_picks:
                break
            if add_pick(range_code, candidate):
                count += 1

    return selected


def max_drawdown(equity_curve: Sequence[float]) -> float:
    peak = equity_curve[0] if equity_curve else 0.0
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        max_dd = max(max_dd, peak - value)
    return max_dd


def evaluate_config(
    config: ResearchConfig,
    batches: Sequence[Sequence[Candidate]],
    stake: float,
    starting_bank_per_range: float,
    min_picks: int,
) -> Dict:
    picks = []
    for batch in batches:
        picks.extend(select_batch_picks(batch, config))
    picks.sort(key=lambda item: (item[1].match_date, item[0], item[1].match_id, item[1].market))

    pnl = 0.0
    total_staked = 0.0
    wins = losses = pushes = 0
    equity = [starting_bank_per_range * 2]
    by_range: Dict[str, Dict[str, float]] = {}
    by_market: Dict[str, Dict[str, float]] = {}
    by_quality: Dict[str, Dict[str, float]] = {}

    for range_code, candidate in picks:
        result = candidate.result
        if result == "win":
            pick_pnl = stake * (candidate.odds - 1)
            wins += 1
        elif result == "loss":
            pick_pnl = -stake
            losses += 1
        else:
            pick_pnl = 0.0
            pushes += 1

        pnl += pick_pnl
        total_staked += stake
        equity.append(equity[-1] + pick_pnl)

        q = quality(candidate.edge_pct)
        for bucket, key in ((by_range, range_code), (by_market, candidate.market), (by_quality, q)):
            stats = bucket.setdefault(key, {"picks": 0, "pnl": 0.0, "wins": 0, "losses": 0, "pushes": 0})
            stats["picks"] += 1
            stats["pnl"] += pick_pnl
            if result == "win":
                stats["wins"] += 1
            elif result == "loss":
                stats["losses"] += 1
            else:
                stats["pushes"] += 1

    pick_count = len(picks)
    roi = (pnl / total_staked * 100) if total_staked else 0.0
    win_rate = (wins / (wins + losses) * 100) if wins + losses else 0.0
    drawdown = max_drawdown(equity)
    drawdown_pct = drawdown / (starting_bank_per_range * 2) * 100 if starting_bank_per_range else 0.0
    low_sample_penalty = max(0, min_picks - pick_count) * 2.0
    score = roi - (0.5 * drawdown_pct) - low_sample_penalty

    return {
        "config_id": config.config_id,
        "score": round(score, 4),
        "roi_pct": round(roi, 4),
        "total_pnl": round(pnl, 4),
        "total_staked": round(total_staked, 4),
        "picks": pick_count,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate_pct": round(win_rate, 4),
        "max_drawdown": round(drawdown, 4),
        "max_drawdown_pct": round(drawdown_pct, 4),
        "avg_picks_per_batch": round(pick_count / len(batches), 4) if batches else 0.0,
        "config": asdict(config),
        "by_range": by_range,
        "by_market": by_market,
        "by_quality": by_quality,
    }


def build_config_grid(args) -> List[ResearchConfig]:
    configs = []
    markets = tuple(m.strip().upper() for m in args.markets.split(",") if m.strip())
    for c_max in parse_csv_list(args.c_max_picks, int):
        for d_max in parse_csv_list(args.d_max_picks, int):
            for min_edge in parse_csv_list(args.min_edges, float):
                for max_match in parse_csv_list(args.max_picks_per_match, int):
                    for max_family in parse_csv_list(args.max_family_per_match, int):
                        config_id = (
                            f"c{c_max}_d{d_max}_edge{min_edge:.3f}_"
                            f"match{max_match}_family{max_family}_markets{'-'.join(markets)}"
                        )
                        configs.append(
                            ResearchConfig(
                                config_id=config_id,
                                c_max_picks=c_max,
                                d_max_picks=d_max,
                                min_edge=min_edge,
                                max_picks_per_match=max_match,
                                max_family_per_match=max_family,
                                markets=markets,
                            )
                        )
    return configs


def write_outputs(results: List[Dict], output_dir: Path, top: int) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    leaderboard_path = output_dir / f"leaderboard_{timestamp}.csv"
    best_path = output_dir / f"best_config_{timestamp}.json"

    fieldnames = [
        "rank",
        "config_id",
        "score",
        "roi_pct",
        "total_pnl",
        "total_staked",
        "picks",
        "wins",
        "losses",
        "pushes",
        "win_rate_pct",
        "max_drawdown",
        "max_drawdown_pct",
        "avg_picks_per_batch",
    ]
    with leaderboard_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for rank, result in enumerate(results, start=1):
            row = {key: result[key] for key in fieldnames if key != "rank"}
            row["rank"] = rank
            writer.writerow(row)

    best_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "note": (
            "Research uses historical 1X2/OU/BTTS/AH odds available from football-data.co.uk. "
            "Team-total markets are not historically covered by this source."
        ),
        "best": results[0] if results else None,
        "top": results[:top],
    }
    best_path.write_text(json.dumps(best_payload, indent=2), encoding="utf-8")
    return leaderboard_path, best_path


def print_summary(
    results: Sequence[Dict],
    candidate_batches: Sequence[Sequence[Candidate]],
    top: int,
    stake: float,
):
    candidates = sum(len(batch) for batch in candidate_batches)
    print("\n" + "=" * 72)
    print("AUTO RESEARCH SUMMARY")
    print("=" * 72)
    print(f"Weekly batches: {len(candidate_batches)}")
    print(f"Market candidates: {candidates}")
    print(f"Configs tested: {len(results)}")
    print("\nTop configs:")
    for rank, result in enumerate(results[:top], start=1):
        print(
            f"{rank:>2}. score={result['score']:+.2f} "
            f"roi={result['roi_pct']:+.2f}% "
            f"picks={result['picks']} "
            f"record={result['wins']}-{result['losses']}-{result['pushes']} "
            f"dd={result['max_drawdown_pct']:.1f}% "
            f"{result['config_id']}"
        )

    if results:
        best = results[0]
        print("\nBest by market:")
        for market, stats in sorted(best["by_market"].items()):
            staked = stats["picks"] * stake
            roi = (stats["pnl"] / staked * 100) if staked else 0.0
            print(
                f"  {market}: picks={stats['picks']} "
                f"record={stats['wins']}-{stats['losses']}-{stats['pushes']} "
                f"pnl={stats['pnl']:+.2f} roi={roi:+.2f}%"
            )


def main():
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Run Rollo Stake AutoResearch experiments")
    parser.add_argument("--leagues", nargs="+", default=settings.get("leagues", list(LEAGUE_CODES.keys())))
    parser.add_argument("--seasons", nargs="+", default=["2324", "2425", "2526"])
    parser.add_argument("--markets", default="1X2,OU,BTTS,AH", help="Markets to evaluate in the config grid")
    parser.add_argument(
        "--candidate-markets",
        default="1X2,OU,BTTS,AH",
        help="Markets to build/cache during rolling model fits. Keep broad for reusable cache.",
    )
    parser.add_argument("--train-size", type=int, default=200)
    parser.add_argument("--min-train-size", type=int, default=80)
    parser.add_argument("--batch-days", type=int, default=7)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--stake", type=float, default=float(settings.get("flat_stake", 10)))
    parser.add_argument("--bankroll", type=float, default=float(settings.get("bankroll", 100)))
    parser.add_argument("--min-picks", type=int, default=50)
    parser.add_argument("--c-max-picks", default="8,10,12")
    parser.add_argument("--d-max-picks", default="8,10,12")
    parser.add_argument("--min-edges", default="0.05,0.08,0.10")
    parser.add_argument("--max-picks-per-match", default="1,2")
    parser.add_argument("--max-family-per-match", default="1")
    parser.add_argument("--workers", type=int, default=1, help="Workers for model fitting and config scoring")
    parser.add_argument("--fit-workers", type=int, default=0, help="Override workers for rolling model fits")
    parser.add_argument("--no-candidate-cache", action="store_true", help="Disable cached rolling candidates")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--quick", action="store_true", help="Run a small smoke-test grid")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.quick:
        args.c_max_picks = "8"
        args.d_max_picks = "8"
        args.min_edges = "0.05"
        args.max_picks_per_match = "1"
        args.max_family_per_match = "1"
        args.batch_days = max(args.batch_days, 14)

    start_date = parse_date(args.start_date) if args.start_date else None
    end_date = parse_date(args.end_date) if args.end_date else None
    eval_markets = parse_markets(args.markets)
    candidate_markets = parse_markets(args.candidate_markets)

    cache_path = candidate_cache_path(
        leagues=args.leagues,
        seasons=args.seasons,
        markets=candidate_markets,
        train_size=args.train_size,
        min_train_size=args.min_train_size,
        batch_days=args.batch_days,
        start_date=start_date,
        end_date=end_date,
    )
    candidate_batches = None
    if not args.no_candidate_cache and not args.refresh_cache:
        candidate_batches = load_candidate_batches(cache_path)

    if candidate_batches is None:
        matches = load_historical_matches(args.leagues, args.seasons, refresh_cache=args.refresh_cache)
        candidate_batches = build_weekly_candidate_batches(
            matches=matches,
            leagues=args.leagues,
            markets=candidate_markets,
            train_size=args.train_size,
            min_train_size=args.min_train_size,
            batch_days=args.batch_days,
            start_date=start_date,
            end_date=end_date,
            fit_workers=args.fit_workers or args.workers,
            verbose=args.verbose,
        )
        if not args.no_candidate_cache:
            save_candidate_batches(cache_path, candidate_batches)

    configs = build_config_grid(args)
    print(f"\nPrepared {len(candidate_batches)} weekly batches and {len(configs)} configs")

    if args.workers > 1 and len(configs) > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            results = list(
                pool.map(
                    evaluate_config,
                    configs,
                    [candidate_batches] * len(configs),
                    [args.stake] * len(configs),
                    [args.bankroll] * len(configs),
                    [args.min_picks] * len(configs),
                )
            )
    else:
        results = [
            evaluate_config(config, candidate_batches, args.stake, args.bankroll, args.min_picks)
            for config in configs
        ]

    results.sort(key=lambda r: (r["score"], r["picks"], r["roi_pct"]), reverse=True)
    leaderboard_path, best_path = write_outputs(results, RESULTS_DIR, args.top)
    print_summary(results, candidate_batches, args.top, args.stake)
    print(f"\nLeaderboard: {leaderboard_path}")
    print(f"Best config JSON: {best_path}")


if __name__ == "__main__":
    main()
