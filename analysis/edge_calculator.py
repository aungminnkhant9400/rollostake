"""
Edge Calculator & Pick Classifier
Finds value bets by comparing model probabilities to bookmaker odds.
"""

import json
import math
import re
import sqlite3
import sys
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH, DATA_DIR
from utils.match_resolver import parse_kickoff_utc

@dataclass
class Pick:
    match_id: str
    home_team: str
    away_team: str
    league: str
    kickoff: str
    selection: str
    market: str
    model_prob: float
    book_prob: float
    edge_pct: float
    odds: float
    stake: float = 200.0
    range_code: str = 'D'
    quality: str = 'KEEP'
    reasoning: str = ''
    risk_note: str = ''
    status: str = 'pending'


@dataclass(frozen=True)
class RangeConfig:
    code: str
    name: str
    bankroll: float
    flat_stake: float
    min_odds: float
    max_odds: float
    max_picks: int
    min_edge: float
    market_min_picks: Dict[str, int] = field(default_factory=dict)
    market_max_picks: Dict[str, int] = field(default_factory=dict)
    max_picks_per_match: int = 2
    max_family_per_match: int = 1
    allowed_markets: Tuple[str, ...] = field(default_factory=tuple)
    allowed_selection_types: Tuple[str, ...] = field(default_factory=tuple)

class EdgeCalculator:
    """
    Calculates edge between model prediction and bookmaker odds.
    
    Edge = Model Prob - Bookmaker Implied Prob
    
    Quality thresholds:
    - STRONG: Edge >= 25%
    - KEEP: Edge >= 10% and < 25%
    - CAUTION: Edge >= 5% and < 10%
    - SKIP: Edge < 5%
    """
    
    # Quality thresholds
    STRONG_THRESHOLD = 0.25
    KEEP_THRESHOLD = 0.10
    CAUTION_THRESHOLD = 0.05
    
    # Flat stake sizing
    BASE_STAKE = 200.0
    STRONG_STAKE = 250.0
    
    # Kelly criterion parameters
    KELLY_FRACTION = 0.15  # 15% Kelly for safety (was 25%)
    MAX_KELLY_PCT = 0.05   # Max 5% of bankroll per bet (was $500 fixed)
    MIN_KELLY_STAKE = 50.0

    DEFAULT_RANGES = {
        'C': RangeConfig('C', 'High Risk', 10000.0, 200.0, 2.50, 5.00, 12, 0.05),
        'D': RangeConfig('D', 'Low Risk', 10000.0, 200.0, 1.70, 2.70, 12, 0.05),
    }

    @staticmethod
    def range_configs_from_settings(settings: Dict) -> Dict[str, RangeConfig]:
        """Build RangeConfig objects from settings.json."""
        configs = {}
        active_ranges = {
            str(code).upper()
            for code in settings.get('active_ranges', [])
        }
        for code, raw in settings.get('ranges', {}).items():
            code = code.upper()
            if active_ranges and code not in active_ranges:
                continue
            configs[code.upper()] = RangeConfig(
                code=code,
                name=raw.get('name', {'C': 'High Risk', 'D': 'Low Risk'}.get(code, f'Risk {code}')),
                bankroll=float(raw.get('bankroll', 10000.0)),
                flat_stake=float(raw.get('flat_stake', settings.get('flat_stake', 200.0))),
                min_odds=float(raw.get('min_odds', 1.0)),
                max_odds=float(raw.get('max_odds', 999.0)),
                max_picks=int(raw.get('max_picks', settings.get('max_picks', 12))),
                min_edge=float(raw.get('min_edge', settings.get('min_edge', 0.05))),
                market_min_picks={
                    str(market).upper(): int(count)
                    for market, count in raw.get('market_min_picks', {}).items()
                },
                market_max_picks={
                    str(market).upper(): int(count)
                    for market, count in raw.get('market_max_picks', {}).items()
                },
                max_picks_per_match=int(raw.get('max_picks_per_match', settings.get('max_picks_per_match', 2))),
                max_family_per_match=int(raw.get('max_family_per_match', settings.get('max_family_per_match', 1))),
                allowed_markets=tuple(
                    str(market).upper()
                    for market in raw.get('allowed_markets', settings.get('allowed_markets', []))
                ),
                allowed_selection_types=tuple(
                    str(selection_type).lower()
                    for selection_type in raw.get('allowed_selection_types', settings.get('allowed_selection_types', []))
                ),
            )
        return configs or EdgeCalculator.DEFAULT_RANGES
    
    def calculate_edge(self, model_prob: float, book_odds: float) -> Tuple[float, float]:
        """
        Calculate edge and implied probability.
        
        Returns:
            (edge_pct, implied_prob)
        """
        book_prob = 1.0 / book_odds
        edge = model_prob - book_prob
        edge_pct = edge / book_prob if book_prob > 0 else 0
        
        return round(edge_pct, 4), round(book_prob, 4)
    
    def __init__(
        self,
        bankroll: float = 10000.0,
        use_kelly: bool = True,
        use_ranges: bool = False,
        staking_mode: str = None,
        flat_stake: float = 200.0,
        range_configs: Dict[str, RangeConfig] = None,
        bookmaker: str = None,
    ):
        self.bankroll = bankroll
        self.staking_mode = (staking_mode or ('kelly' if use_kelly else 'flat')).lower()
        self.use_kelly = self.staking_mode == 'kelly'
        self.use_ranges = use_ranges
        self.flat_stake = flat_stake
        self.range_configs = range_configs or self.DEFAULT_RANGES
        self.bookmaker = bookmaker
        self._context_cache = {}
        
    def kelly_stake(self, edge_pct: float, odds: float, model_prob: float) -> float:
        """
        Calculate stake using Kelly Criterion.
        
        Kelly formula: f* = (bp - q) / b
        Where: b = odds - 1, p = model prob, q = 1 - p
        
        We use Quarter Kelly for safety.
        """
        if not self.use_kelly:
            return self.BASE_STAKE
        
        b = odds - 1  # Decimal odds minus 1
        p = model_prob
        q = 1 - p
        
        # Full Kelly fraction
        kelly_fraction = (b * p - q) / b if b > 0 else 0
        
        # 15% Kelly for safety
        stake_fraction = kelly_fraction * self.KELLY_FRACTION
        
        # Calculate stake amount
        stake = self.bankroll * stake_fraction
        
        # Cap at 5% of bankroll max, $50 min
        max_stake = self.bankroll * self.MAX_KELLY_PCT
        stake = min(stake, max_stake)
        stake = max(stake, self.MIN_KELLY_STAKE)
        
        return round(stake, 2)
    
    def classify_pick(self, edge_pct: float) -> str:
        """
        Classify pick quality based on edge.
        """
        if edge_pct >= self.STRONG_THRESHOLD:
            return 'STRONG'
        elif edge_pct >= self.KEEP_THRESHOLD:
            return 'KEEP'
        elif edge_pct >= self.CAUTION_THRESHOLD:
            return 'CAUTION'
        else:
            return 'SKIP'
    
    def determine_stake(self, edge_pct: float, odds: float, model_prob: float, quality: str) -> float:
        """
        Determine stake size from configured staking mode.
        """
        if self.staking_mode == 'flat':
            return round(self.flat_stake, 2)

        b = odds - 1  # Decimal odds minus 1
        p = model_prob
        q = 1 - p
        
        # Full Kelly fraction
        kelly_fraction = (b * p - q) / b if b > 0 else 0
        
        # 15% Kelly for safety
        stake_fraction = kelly_fraction * self.KELLY_FRACTION
        
        # Calculate stake amount
        stake = self.bankroll * stake_fraction
        
        # Cap at 5% of bankroll max, $10 min
        max_stake = self.bankroll * self.MAX_KELLY_PCT
        stake = min(stake, max_stake)
        stake = max(stake, 10)  # $10 minimum
        
        # Quality adjustment
        if quality == 'KEEP':
            stake *= 0.7  # 70% of Kelly for KEEP
        elif quality == 'CAUTION':
            stake *= 0.5  # 50% of Kelly for CAUTION
        
        return round(stake, 2)

    def flat_range_stake(self, range_config: RangeConfig) -> float:
        """Return the flat stake used by the C/D range model."""
        return range_config.flat_stake
    
    def generate_picks(self, league: str = None, min_edge: float = 0.05) -> List[Pick]:
        """
        Generate all value picks for upcoming matches.
        
        Args:
            league: Filter by league (optional)
            min_edge: Minimum edge to include (default 5%)
        
        Returns:
            List of Pick objects sorted by edge (highest first)
        """
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Get predictions with odds
        query = '''
            SELECT 
                m.match_id, m.home_team, m.away_team, m.league, m.kickoff,
                COALESCE(p.adj_prob_home, p.prob_home_win) AS prob_home_win,
                COALESCE(p.adj_prob_draw, p.prob_draw) AS prob_draw,
                COALESCE(p.adj_prob_away, p.prob_away_win) AS prob_away_win,
                p.lambda_h, p.lambda_a,
                p.prob_over_1_5, p.prob_over_2_5, p.prob_under_2_5, p.prob_btts_yes,
                p.adjustment_note,
                m.home_fatigue_score, m.away_fatigue_score, m.fatigue_advantage,
                o.market, o.selection, o.odds, o.implied_prob
            FROM matches m
            JOIN predictions p ON m.match_id = p.match_id
            JOIN odds o ON m.match_id = o.match_id
            WHERE m.status = 'scheduled'
        '''
        
        params = []
        if self.bookmaker:
            query += " AND LOWER(o.bookmaker) = LOWER(?)"
            params.append(self.bookmaker)
        if league:
            query += " AND m.league = ?"
            params.append(league)
        
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        
        picks = []
        now_utc = datetime.now(timezone.utc)
        
        for row in rows:
            kickoff_utc = parse_kickoff_utc(row['kickoff'])
            if kickoff_utc is not None and kickoff_utc <= now_utc:
                continue

            # Map selection to model probability
            model_prob = self._get_model_prob(row)
            
            if model_prob is None:
                continue

            context_delta, context_notes = self._external_factor_adjustment(row, row['selection'], row['market'])
            model_prob = max(0.01, min(0.99, model_prob + context_delta))
            
            edge_pct, book_prob = self.calculate_edge(model_prob, row['odds'])
            
            if edge_pct >= min_edge:
                quality = self.classify_pick(edge_pct)
                stake = self.determine_stake(edge_pct, row['odds'], model_prob, quality)
                
                pick = Pick(
                    match_id=row['match_id'],
                    home_team=row['home_team'],
                    away_team=row['away_team'],
                    league=row['league'],
                    kickoff=row['kickoff'],
                    selection=row['selection'],
                    market=row['market'],
                    model_prob=round(model_prob, 3),
                    book_prob=book_prob,
                    edge_pct=round(edge_pct * 100, 1),  # Convert to percentage
                    odds=row['odds'],
                    stake=stake,
                    quality=quality,
                    reasoning=self._build_reasoning(row, model_prob, book_prob, edge_pct, context_notes),
                    risk_note=self._build_risk_note(row)
                )
                
                picks.append(pick)
        
        # Sort by edge (descending)
        picks.sort(key=lambda x: x.edge_pct, reverse=True)
        
        return picks

    def generate_range_picks(self, league: str = None) -> List[Pick]:
        """Generate risk-band picks using flat staking and odds bands."""
        all_candidates = self.generate_picks(league=league, min_edge=0.0)
        learned_adjustments = self._learned_performance_adjustments()
        loss_traps = self._loss_trap_segments()
        selected = []
        exposure = set()

        for code, config in self.range_configs.items():
            match_counts = {}
            family_counts = {}
            range_candidates = [
                p for p in all_candidates
                if config.min_odds <= p.odds <= config.max_odds
                and p.edge_pct / 100 >= config.min_edge
                and self._range_filter_match(p, config)
            ]
            range_candidates.sort(
                key=lambda p: (
                    self._historical_pick_score(p, code, learned_adjustments),
                    p.model_prob,
                    p.edge_pct,
                ),
                reverse=True,
            )
            clean_candidates = [
                p for p in range_candidates
                if not self._is_hard_loss_trap(p, code, loss_traps)
                and not self._matches_loss_trap(p, code, loss_traps)
            ]
            fallback_candidates = [
                p for p in range_candidates
                if not self._is_hard_loss_trap(p, code, loss_traps)
            ]

            count = 0
            market_counts = {}

            def add_pick(pick: Pick) -> bool:
                nonlocal count
                exposure_key = (pick.home_team, pick.away_team, pick.market, pick.selection)
                if exposure_key in exposure:
                    return False
                market_limit = config.market_max_picks.get(pick.market.upper())
                if market_limit is not None and market_counts.get(pick.market, 0) >= market_limit:
                    return False
                if match_counts.get(pick.match_id, 0) >= config.max_picks_per_match:
                    return False
                family_key = (pick.match_id, self._exposure_family(pick))
                if family_counts.get(family_key, 0) >= config.max_family_per_match:
                    return False

                pick.range_code = code
                pick.stake = self.flat_range_stake(config)
                pick.reasoning = self._with_risk_band_reasoning(
                    pick.reasoning or self._build_reasoning_from_pick(pick),
                    config.name,
                )
                selected.append(pick)
                exposure.add(exposure_key)
                match_counts[pick.match_id] = match_counts.get(pick.match_id, 0) + 1
                market_counts[pick.market] = market_counts.get(pick.market, 0) + 1
                family_counts[family_key] = family_counts.get(family_key, 0) + 1
                count += 1
                return True

            for market, minimum in config.market_min_picks.items():
                market_count = 0
                for pick in clean_candidates:
                    if count >= config.max_picks or market_count >= minimum:
                        break
                    if pick.market != market:
                        continue
                    if add_pick(pick):
                        market_count += 1

            for pick in clean_candidates:
                if count >= config.max_picks:
                    break
                if add_pick(pick):
                    continue

            for pick in fallback_candidates:
                if count >= config.max_picks:
                    break
                if add_pick(pick):
                    continue

        self._annotate_correlated_exposure(selected)
        selected.sort(key=lambda p: (p.range_code, p.kickoff, -p.edge_pct))
        return selected

    def _historical_pick_score(self, pick: Pick, code: str, adjustments: Dict[str, Dict[Tuple[str, str], float]]) -> float:
        """Rank by model probability, calibrated by settled performance segments."""
        code_adjustments = adjustments.get(code, {})
        return (
            pick.model_prob
            + code_adjustments.get(("quality", pick.quality), 0.0)
            + code_adjustments.get(("market", pick.market.upper()), 0.0)
            + code_adjustments.get(("family", self._exposure_family(pick)), 0.0)
            + code_adjustments.get(("selection_type", self._selection_type(pick)), 0.0)
            + code_adjustments.get(("line", self._line_segment_from_values(pick.market, pick.selection)), 0.0)
            + code_adjustments.get(("odds_bucket", self._odds_bucket(pick.odds)), 0.0)
            + self._market_structure_prior(pick, code)
            + self._external_card_prior(pick, code)
        )

    def _learned_performance_adjustments(self) -> Dict[str, Dict[Tuple[str, str], float]]:
        """Learn ranking adjustments from settled weekly performance segments."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT COALESCE(r.range_code, p.range_code) AS range_code,
                   COALESCE(r.quality, p.quality) AS quality,
                   p.market, p.selection, p.odds, m.home_team, m.away_team,
                   r.result, COALESCE(r.stake, 0) AS stake, COALESCE(r.pnl, 0) AS pnl
            FROM results r
            LEFT JOIN picks p ON r.pick_id = p.id
            LEFT JOIN matches m ON r.match_id = m.match_id
            WHERE COALESCE(r.range_code, p.range_code) IS NOT NULL
              AND r.result IN ('win', 'loss', 'push')
            """
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()

        by_range: Dict[str, List[Dict]] = {}
        for row in rows:
            by_range.setdefault(str(row["range_code"]).upper(), []).append(row)

        adjustments: Dict[str, Dict[Tuple[str, str], float]] = {}
        for code, items in by_range.items():
            range_wins = sum(1 for item in items if item.get("result") == "win")
            range_losses = sum(1 for item in items if item.get("result") == "loss")
            range_staked = sum(float(item.get("stake") or 0) for item in items)
            range_pnl = sum(float(item.get("pnl") or 0) for item in items)
            range_win_rate = (range_wins + 1) / (range_wins + range_losses + 2)
            range_roi = (range_pnl / range_staked) if range_staked else 0.0
            adjustments[code] = {}

            segments: Dict[Tuple[str, str], List[Dict]] = {}
            for item in items:
                market = str(item.get("market") or "").upper()
                quality = str(item.get("quality") or "").upper()
                selection = str(item.get("selection") or "")
                home_team = str(item.get("home_team") or "")
                away_team = str(item.get("away_team") or "")
                odds = float(item.get("odds") or 0)
                for key in self._segment_keys_from_values(quality, market, selection, home_team, away_team, odds):
                    if key[1]:
                        segments.setdefault(key, []).append(item)

            caps = {
                "quality": 0.20,
                "market": 0.10,
                "family": 0.12,
                "selection_type": 0.08,
                "line": 0.12,
                "odds_bucket": 0.04,
            }
            min_decisions = {
                "quality": 3,
                "market": 3,
                "family": 3,
                "selection_type": 3,
                "line": 2,
                "odds_bucket": 4,
            }
            for key, segment_rows in segments.items():
                wins = sum(1 for item in segment_rows if item.get("result") == "win")
                losses = sum(1 for item in segment_rows if item.get("result") == "loss")
                decisions = wins + losses
                if decisions < min_decisions[key[0]]:
                    continue
                staked = sum(float(item.get("stake") or 0) for item in segment_rows)
                pnl = sum(float(item.get("pnl") or 0) for item in segment_rows)
                segment_win_rate = (wins + 1) / (decisions + 2)
                segment_roi = (pnl / staked) if staked else 0.0
                win_rate_delta = max(-0.20, min(0.20, segment_win_rate - range_win_rate))
                roi_delta = max(-1.0, min(1.0, segment_roi - range_roi))
                raw_adjustment = (1.35 * win_rate_delta) + (0.015 * roi_delta)
                actual_win_rate = (wins / decisions) if decisions else 0.0
                if decisions >= 5 and actual_win_rate < 0.45:
                    raw_adjustment -= min(0.14, (0.45 - actual_win_rate) * 0.70)
                if code == "C" and decisions >= 4 and losses >= wins:
                    raw_adjustment -= 0.04
                cap = caps[key[0]]
                adjustments[code][key] = max(-cap, min(cap, raw_adjustment))

        return adjustments

    def _loss_trap_segments(self) -> Dict[str, set]:
        """Segments that repeatedly lost inside the same risk band.

        These are skipped first, but can still be used as a last resort if the
        card cannot otherwise fill. That keeps the pick count stable while
        stopping repeated bad patterns from leading the slate.
        """
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT COALESCE(r.range_code, p.range_code) AS range_code,
                   COALESCE(r.quality, p.quality) AS quality,
                   p.market, p.selection, p.odds, m.home_team, m.away_team,
                   r.result, COALESCE(r.stake, 0) AS stake, COALESCE(r.pnl, 0) AS pnl
            FROM results r
            LEFT JOIN picks p ON r.pick_id = p.id
            LEFT JOIN matches m ON r.match_id = m.match_id
            WHERE COALESCE(r.range_code, p.range_code) IS NOT NULL
              AND r.result IN ('win', 'loss', 'push')
            """
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()

        by_range: Dict[str, List[Dict]] = {}
        for row in rows:
            by_range.setdefault(str(row["range_code"]).upper(), []).append(row)

        traps: Dict[str, set] = {}
        min_decisions = {
            "quality": 3,
            "market": 3,
            "family": 2,
            "selection_type": 3,
            "line": 2,
            "odds_bucket": 4,
        }
        for code, items in by_range.items():
            wins = sum(1 for item in items if item.get("result") == "win")
            losses = sum(1 for item in items if item.get("result") == "loss")
            staked = sum(float(item.get("stake") or 0) for item in items)
            pnl = sum(float(item.get("pnl") or 0) for item in items)
            range_win_rate = (wins + 1) / (wins + losses + 2)
            range_roi = (pnl / staked) if staked else 0.0

            segments: Dict[Tuple[str, str], List[Dict]] = {}
            for item in items:
                keys = self._segment_keys_from_values(
                    str(item.get("quality") or ""),
                    str(item.get("market") or ""),
                    str(item.get("selection") or ""),
                    str(item.get("home_team") or ""),
                    str(item.get("away_team") or ""),
                    float(item.get("odds") or 0),
                )
                for key in keys:
                    if key[1]:
                        segments.setdefault(key, []).append(item)

            traps[code] = set()
            for key, segment_rows in segments.items():
                segment_wins = sum(1 for item in segment_rows if item.get("result") == "win")
                segment_losses = sum(1 for item in segment_rows if item.get("result") == "loss")
                decisions = segment_wins + segment_losses
                if decisions < min_decisions[key[0]]:
                    continue
                segment_staked = sum(float(item.get("stake") or 0) for item in segment_rows)
                segment_pnl = sum(float(item.get("pnl") or 0) for item in segment_rows)
                segment_win_rate = (segment_wins + 1) / (decisions + 2)
                segment_roi = (segment_pnl / segment_staked) if segment_staked else 0.0
                winless_repeat = segment_wins == 0 and segment_losses >= min_decisions[key[0]]
                below_band = segment_win_rate <= range_win_rate - 0.08 and segment_roi <= min(range_roi, 0.0) - 0.15
                if winless_repeat or below_band:
                    traps[code].add(key)

        return traps

    def _segment_keys_from_values(
        self,
        quality: str,
        market: str,
        selection: str,
        home_team: str,
        away_team: str,
        odds: float,
    ) -> Tuple[Tuple[str, str], ...]:
        market = str(market or "").upper()
        quality = str(quality or "").upper()
        selection = str(selection or "")
        home_team = str(home_team or "")
        away_team = str(away_team or "")
        return (
            ("quality", quality),
            ("market", market),
            ("family", self._exposure_family_from_values(market, selection, home_team, away_team)),
            ("selection_type", self._selection_type_from_values(market, selection, home_team, away_team)),
            ("line", self._line_segment_from_values(market, selection)),
            ("odds_bucket", self._odds_bucket(odds)),
        )

    def _matches_loss_trap(self, pick: Pick, code: str, traps: Dict[str, set]) -> bool:
        code_traps = traps.get(str(code).upper(), set())
        if not code_traps:
            return False
        keys = self._segment_keys_from_values(
            pick.quality,
            pick.market,
            pick.selection,
            pick.home_team,
            pick.away_team,
            pick.odds,
        )
        return any(key in code_traps for key in keys)

    def _is_hard_loss_trap(self, pick: Pick, code: str, traps: Dict[str, set]) -> bool:
        """Block repeated loss shapes from fallback selection."""
        code = str(code or "").upper()
        market = (pick.market or "").upper()
        side = self._selection_type(pick)
        line_segment = self._line_segment_from_values(pick.market, pick.selection)
        trap_keys = traps.get(code, set())

        if code == "C":
            if market == "OU" and (
                ("market", "OU") in trap_keys
                or ("selection_type", "under") in trap_keys
                or ("family", "low-goals") in trap_keys
            ):
                return True
            if side == "under" and line_segment == "goal-line-1.5":
                return True
            if market == "AH":
                line = self._handicap_line(pick.selection)
                if line is not None and line <= -1.5:
                    return True
            if market == "1X2":
                supports, downgrades = self._context_signal_counts(pick.reasoning)
                if side == "away" and downgrades >= supports:
                    return True

        return self._matches_loss_trap(pick, code, traps) and (
            (market == "AH" and self._handicap_line(pick.selection) is not None and self._handicap_line(pick.selection) <= -1.5)
            or (market == "OU" and side == "under")
            or (market == "1X2" and side == "away")
        )

    def _selection_type_from_values(self, market: str, selection: str, home_team: str, away_team: str) -> str:
        if market == '1X2':
            if selection == 'Draw':
                return 'draw'
            if home_team and home_team in selection:
                return 'home'
            if away_team and away_team in selection:
                return 'away'
        if market == 'AH':
            if home_team and home_team in selection:
                return 'home'
            if away_team and away_team in selection:
                return 'away'
        if market in ('OU', 'TT', 'BTTS'):
            family = self._exposure_family_from_values(market, selection, home_team, away_team)
            if family == 'low-goals':
                return 'under'
            if family == 'high-goals':
                return 'over'
        return 'other'

    def _market_structure_prior(self, pick: Pick, code: str) -> float:
        """Bias ranking toward robust market structures without copying any external card.

        This is deliberately small and generic. The weekly self-learning layer
        still comes from settled results; this prior only helps the selector
        prefer markets whose risk shape is clearer when context agrees.
        """
        market = (pick.market or "").upper()
        selection = pick.selection or ""
        side = self._selection_type(pick)
        family = self._exposure_family(pick)
        prior = 0.0

        if market == "TT" and side == "under":
            prior += 0.12 if code == "D" else 0.10
        elif market == "BTTS" and "BTTS No" in selection:
            prior += 0.07 if code == "D" else 0.05
        elif market == "OU" and side == "under":
            prior += 0.05 if code == "D" else 0.04

        if market == "AH":
            line = self._handicap_line(selection)
            if line is not None:
                if line in (0.0, 0.5):
                    prior += 0.06 if code == "D" else 0.04
                elif line == -1.0:
                    prior += 0.03
                elif line <= -1.5:
                    prior -= 0.02

        if market == "1X2":
            prior -= 0.08 if code == "D" else 0.04
            if side == "away":
                prior -= 0.02

        reasoning = pick.reasoning or ""
        supports, downgrades = self._context_signal_counts(reasoning)
        prior += min(0.04, supports * 0.015)
        prior -= min(0.08, downgrades * 0.025)

        if family == "low-goals" and "H2H supports lower scoring" in reasoning:
            prior += 0.03

        if code == "C":
            if market == "AH":
                line = self._handicap_line(selection)
                if line is not None and line <= -1.5:
                    prior -= 0.08
                    if line <= -2.0:
                        prior -= 0.04
                    if downgrades:
                        prior -= 0.04
            if market == "OU" and side == "under":
                goal_line = self._goal_line(selection)
                if goal_line is not None and goal_line <= 2.5:
                    prior -= 0.08
                    if goal_line <= 1.5:
                        prior -= 0.08
            if market == "BTTS" and side == "under":
                prior -= 0.02
                if not supports or downgrades > supports:
                    prior -= 0.04
            if market == "1X2":
                if side == "away":
                    prior -= 0.05
                if not supports:
                    prior -= 0.03

        return max(-0.12, min(0.14, prior))

    def _external_card_prior(self, pick: Pick, code: str) -> float:
        """Use aggregate external-card lessons without copying external picks."""
        profile = self._external_card_profile()
        if not profile:
            return 0.0

        market = (pick.market or "").upper()
        side = self._selection_type(pick)
        line = self._handicap_line(pick.selection) if market == "AH" else None
        prior = 0.0

        profile_range = profile.get("ranges", {}).get(str(code or "").lower(), {})
        market_share = float(profile_range.get("market_share", {}).get(market, 0.0) or 0.0)
        if market_share >= 0.20:
            prior += 0.015

        if market == "AH" and line in (0.0, 0.5):
            prior += 0.05 if code == "D" else 0.035
        elif market == "AH" and line is not None and line <= -1.5:
            prior -= 0.06

        if market == "TT":
            prior += 0.035 if code == "D" else 0.02
            if side == "under":
                prior += 0.015

        if market == "OU" and side == "under":
            goal_line = self._goal_line(pick.selection)
            if code == "D" and goal_line is not None and goal_line >= 3.0:
                prior += 0.03
            elif code == "C" and goal_line is not None and goal_line <= 2.5:
                prior -= 0.03

        if market == "1X2" and side == "away":
            prior -= 0.03

        return max(-0.08, min(0.08, prior))

    def _external_card_profile(self) -> Dict:
        cache_key = ("external_card_profile",)
        if cache_key in self._context_cache:
            return self._context_cache[cache_key]

        path = DATA_DIR / "external_card_profile.json"
        if not path.exists():
            self._context_cache[cache_key] = {}
            return {}

        try:
            with open(path, "r", encoding="utf-8") as f:
                profile = json.load(f)
        except (OSError, json.JSONDecodeError):
            profile = {}

        self._context_cache[cache_key] = profile
        return profile

    def _context_signal_counts(self, reasoning: str) -> Tuple[int, int]:
        reasoning = reasoning or ""
        supports = (
            reasoning.count("supports pick")
            + reasoning.count("supports lower scoring")
            + reasoning.count("supports goals")
        )
        downgrades = (
            reasoning.count("downgrades pick")
            + reasoning.count("downgrades lower scoring")
            + reasoning.count("downgrades goals")
        )
        return supports, downgrades

    def _line_segment_from_values(self, market: str, selection: str) -> str:
        market = str(market or "").upper()
        selection = str(selection or "")
        if market in ('OU', 'TT'):
            match = re.search(r'\b(?:Over|Under|O|U)\s*(\d+(?:\.\d+)?)\b', selection, re.IGNORECASE)
            return f"goal-line-{match.group(1)}" if match else ""
        if market == 'BTTS':
            return selection
        if market == 'AH':
            if re.search(r'\bDNB\b', selection, re.IGNORECASE):
                return "ah-0"
            match = re.search(r'(?:\bAH\s*)?([+-]?\d+(?:\.\d+)?)\s*$', selection, re.IGNORECASE)
            return f"ah-{match.group(1)}" if match else ""
        return market

    def _handicap_line(self, selection: str) -> Optional[float]:
        if re.search(r'\bDNB\b', selection or "", re.IGNORECASE):
            return 0.0
        match = re.search(r'(?:\bAH\s*)?([+-]?\d+(?:\.\d+)?)\s*$', selection or "", re.IGNORECASE)
        return float(match.group(1)) if match else None

    def _goal_line(self, selection: str) -> Optional[float]:
        match = re.search(r'\b(?:Over|Under|O|U)\s*(\d+(?:\.\d+)?)\b', selection or "", re.IGNORECASE)
        return float(match.group(1)) if match else None

    def _odds_bucket(self, odds: float) -> str:
        if odds <= 0:
            return ""
        if odds < 2.0:
            return "odds-1.70-1.99"
        if odds < 2.5:
            return "odds-2.00-2.49"
        if odds < 3.0:
            return "odds-2.50-2.99"
        if odds < 4.0:
            return "odds-3.00-3.99"
        return "odds-4.00-plus"

    def _selection_type(self, pick: Pick) -> str:
        return self._selection_type_from_values(
            pick.market,
            pick.selection,
            pick.home_team,
            pick.away_team,
        )

    def _range_filter_match(self, pick: Pick, config: RangeConfig) -> bool:
        if config.allowed_markets and pick.market.upper() not in config.allowed_markets:
            return False
        if config.allowed_selection_types and self._selection_type(pick) not in config.allowed_selection_types:
            return False
        return True
    
    def _get_model_prob(self, row) -> Optional[float]:
        """
        Map bookmaker selection to model probability.
        """
        selection = row['selection']
        home_team = row['home_team']
        away_team = row['away_team']
        
        # 1X2 markets
        if 'Win' in selection:
            if home_team in selection:
                return row['prob_home_win']
            elif away_team in selection:
                return row['prob_away_win']
        
        if selection == 'Draw':
            return row['prob_draw']

        dnb_prob = self._dnb_prob(row, selection)
        if dnb_prob is not None:
            return dnb_prob
        
        # Team total goals markets. Supports "Team O1.5", "Team U0.5",
        # "Team Over 1.5", and "Team Under 1.5".
        team_total_prob = self._team_total_prob(row, selection)
        if team_total_prob is not None:
            return team_total_prob

        # Match total goals markets. Supports Over/Under 0.5, 1.5, 2.5, 3.0, 3.5, etc.
        total_prob = self._match_total_prob(row, selection)
        if total_prob is not None:
            return total_prob

        # Asian Handicap markets. The line is interpreted as the handicap attached
        # to the named team, e.g. "Chelsea AH -0.5" or "Forest AH +0.5".
        handicap_prob = self._asian_handicap_prob(row, selection)
        if handicap_prob is not None:
            return handicap_prob
        
        # BTTS
        if 'BTTS Yes' in selection or 'Both Teams To Score' in selection:
            return row['prob_btts_yes']
        if 'BTTS No' in selection:
            return 1 - row['prob_btts_yes'] if row['prob_btts_yes'] else None
        
        return None

    def _lambda_pair(self, row) -> Optional[Tuple[float, float]]:
        lambda_h = row['lambda_h'] if 'lambda_h' in row.keys() else None
        lambda_a = row['lambda_a'] if 'lambda_a' in row.keys() else None
        if lambda_h is None or lambda_a is None:
            return None
        return max(float(lambda_h), 0.01), max(float(lambda_a), 0.01)

    def _poisson_pmf(self, goals: int, expected: float) -> float:
        return math.exp(-expected) * (expected ** goals) / math.factorial(goals)

    def _score_distribution(self, row, max_goals: int = 10) -> Optional[Dict[Tuple[int, int], float]]:
        lambdas = self._lambda_pair(row)
        if not lambdas:
            return None
        lambda_h, lambda_a = lambdas
        dist = {}
        for home_goals in range(max_goals + 1):
            home_prob = self._poisson_pmf(home_goals, lambda_h)
            for away_goals in range(max_goals + 1):
                dist[(home_goals, away_goals)] = home_prob * self._poisson_pmf(away_goals, lambda_a)
        total = sum(dist.values())
        if total <= 0:
            return None
        return {score: prob / total for score, prob in dist.items()}

    def _decision_prob(self, win_prob: float, loss_prob: float) -> Optional[float]:
        decision_total = win_prob + loss_prob
        if decision_total <= 0:
            return None
        return win_prob / decision_total

    def _match_total_prob(self, row, selection: str) -> Optional[float]:
        match = re.search(r'\b(Over|Under)\s+(\d+(?:\.\d+)?)\b', selection, re.IGNORECASE)
        if not match:
            return None
        direction = match.group(1).lower()
        line = float(match.group(2))
        dist = self._score_distribution(row)
        if not dist:
            return None

        win_prob = 0.0
        loss_prob = 0.0
        for (home_goals, away_goals), prob in dist.items():
            total_goals = home_goals + away_goals
            if direction == 'over':
                if total_goals > line:
                    win_prob += prob
                elif total_goals < line:
                    loss_prob += prob
            else:
                if total_goals < line:
                    win_prob += prob
                elif total_goals > line:
                    loss_prob += prob
        return self._decision_prob(win_prob, loss_prob)

    def _team_total_prob(self, row, selection: str) -> Optional[float]:
        match = re.search(r'\b(O|U|Over|Under)\s*(\d+(?:\.\d+)?)\b', selection, re.IGNORECASE)
        if not match:
            return None

        home_team = row['home_team']
        away_team = row['away_team']
        team = None
        if home_team in selection:
            team = 'home'
        elif away_team in selection:
            team = 'away'
        if team is None:
            return None

        direction = match.group(1).lower()
        line = float(match.group(2))
        dist = self._score_distribution(row)
        if not dist:
            return None

        win_prob = 0.0
        loss_prob = 0.0
        for (home_goals, away_goals), prob in dist.items():
            team_goals = home_goals if team == 'home' else away_goals
            if direction in ('o', 'over'):
                if team_goals > line:
                    win_prob += prob
                elif team_goals < line:
                    loss_prob += prob
            else:
                if team_goals < line:
                    win_prob += prob
                elif team_goals > line:
                    loss_prob += prob
        return self._decision_prob(win_prob, loss_prob)

    def _asian_handicap_prob(self, row, selection: str) -> Optional[float]:
        match = re.search(r'(?:\bAH\s*)?([+-]?\d+(?:\.\d+)?)\s*$', selection, re.IGNORECASE)
        if not match:
            return None

        home_team = row['home_team']
        away_team = row['away_team']
        team = None
        if home_team in selection:
            team = 'home'
        elif away_team in selection:
            team = 'away'
        if team is None:
            return None

        handicap = float(match.group(1))
        dist = self._score_distribution(row)
        if not dist:
            return None

        win_prob = 0.0
        loss_prob = 0.0
        for (home_goals, away_goals), prob in dist.items():
            if team == 'home':
                margin = home_goals + handicap - away_goals
            else:
                margin = away_goals + handicap - home_goals
            if margin > 0:
                win_prob += prob
            elif margin < 0:
                loss_prob += prob
        return self._decision_prob(win_prob, loss_prob)

    def _dnb_prob(self, row, selection: str) -> Optional[float]:
        if not re.search(r'\bDNB\b', selection or "", re.IGNORECASE):
            return None
        home_team = row['home_team']
        away_team = row['away_team']
        if home_team in selection:
            return self._decision_prob(float(row['prob_home_win'] or 0), float(row['prob_away_win'] or 0))
        if away_team in selection:
            return self._decision_prob(float(row['prob_away_win'] or 0), float(row['prob_home_win'] or 0))
        return None

    def _exposure_family_from_values(self, market: str, selection: str, home_team: str, away_team: str) -> str:
        if market == '1X2':
            if selection == 'Draw':
                return 'draw'
            if home_team and home_team in selection:
                return 'home-positive'
            if away_team and away_team in selection:
                return 'away-positive'
        if market in ('OU', 'TT', 'BTTS'):
            if any(token in selection for token in ('Under', ' U', 'BTTS No')):
                return 'low-goals'
            if any(token in selection for token in ('Over', ' O', 'BTTS Yes')):
                return 'high-goals'
        if market == 'AH':
            if home_team and home_team in selection:
                return 'home-positive'
            if away_team and away_team in selection:
                return 'away-positive'
        return market.lower()

    def _exposure_family(self, pick: Pick) -> str:
        return self._exposure_family_from_values(
            pick.market,
            pick.selection,
            pick.home_team,
            pick.away_team,
        )

    def _annotate_correlated_exposure(self, picks: List[Pick]) -> None:
        by_match = {}
        for pick in picks:
            by_match.setdefault(pick.match_id, []).append(pick)

        for match_picks in by_match.values():
            if len(match_picks) <= 1:
                continue
            labels = [
                f"{self.range_configs.get(p.range_code, self.DEFAULT_RANGES.get(p.range_code)).name}:{p.selection}"
                for p in match_picks
            ]
            families = sorted({self._exposure_family(p) for p in match_picks})
            note = (
                "Correlated exposure: same match also has "
                + ", ".join(labels)
                + f". Exposure families: {', '.join(families)}."
            )
            for pick in match_picks:
                pick.risk_note = f"{pick.risk_note} {note}".strip()

    def _external_factor_adjustment(self, row, selection: str, market: str) -> Tuple[float, List[str]]:
        """Adjust a candidate with non-price football context before edge is calculated."""
        notes = []
        adjustment = 0.0
        side = self._selection_type_from_values(
            str(market or "").upper(),
            str(selection or ""),
            str(row['home_team'] or ""),
            str(row['away_team'] or ""),
        )

        fatigue_delta, fatigue_note = self._fatigue_context_adjustment(row, side, market)
        adjustment += fatigue_delta
        if fatigue_note:
            notes.append(fatigue_note)

        table_delta, table_note = self._table_context_adjustment(row, side)
        adjustment += table_delta
        if table_note:
            notes.append(table_note)

        h2h_delta, h2h_note = self._h2h_context_adjustment(row, selection, market, side)
        adjustment += h2h_delta
        if h2h_note:
            notes.append(h2h_note)

        schedule_delta, schedule_note = self._schedule_context_adjustment(row, side, market)
        adjustment += schedule_delta
        if schedule_note:
            notes.append(schedule_note)

        news_delta, news_note = self._team_news_context_adjustment(row, side)
        adjustment += news_delta
        if news_note:
            notes.append(news_note)

        existing_note = str(row['adjustment_note'] or "").strip() if 'adjustment_note' in row.keys() else ""
        if existing_note and existing_note != "Home: +0% overall, Away: +0% overall":
            notes.append(f"manual team-news probabilities loaded ({existing_note})")

        return max(-0.12, min(0.12, adjustment)), notes[:4]

    def _fatigue_context_adjustment(self, row, side: str, market: str) -> Tuple[float, str]:
        home_score = row['home_fatigue_score'] if 'home_fatigue_score' in row.keys() else None
        away_score = row['away_fatigue_score'] if 'away_fatigue_score' in row.keys() else None
        if home_score is None or away_score is None:
            return 0.0, ""

        home_score = float(home_score)
        away_score = float(away_score)
        diff = away_score - home_score
        delta = 0.0
        if side == "home" and abs(diff) >= 10:
            delta = max(-0.04, min(0.04, diff * 0.0015))
        elif side == "away" and abs(diff) >= 10:
            delta = max(-0.04, min(0.04, -diff * 0.0015))
        elif side in ("over", "under") and max(home_score, away_score) >= 55:
            delta = 0.02 if side == "over" else -0.02
        elif side in ("over", "under") and max(home_score, away_score) <= 20:
            delta = -0.015 if side == "over" else 0.015

        if abs(delta) < 0.01:
            return 0.0, ""
        direction = "supports" if delta > 0 else "downgrades"
        return delta, f"fatigue {direction} pick (home {home_score:.0f}, away {away_score:.0f})"

    def _table_context_adjustment(self, row, side: str) -> Tuple[float, str]:
        table = self._league_table(str(row['league'] or ""), str(row['kickoff'] or ""))
        home = table.get(str(row['home_team'] or ""))
        away = table.get(str(row['away_team'] or ""))
        if not home or not away or home["played"] < 6 or away["played"] < 6:
            return 0.0, ""

        if side in ("over", "under"):
            home_gf = home["gf"] / home["played"]
            home_ga = home["ga"] / home["played"]
            away_gf = away["gf"] / away["played"]
            away_ga = away["ga"] / away["played"]
            attack_avg = (home_gf + away_gf) / 2
            concession_avg = (home_ga + away_ga) / 2
            goal_pressure = attack_avg + concession_avg
            if side == "under":
                if goal_pressure <= 2.35:
                    return 0.025, f"table goal profile supports lower scoring ({goal_pressure:.1f})"
                if goal_pressure >= 3.05:
                    return -0.03, f"table goal profile downgrades lower scoring ({goal_pressure:.1f})"
            if side == "over":
                if goal_pressure >= 3.05:
                    return 0.025, f"table goal profile supports goals ({goal_pressure:.1f})"
                if goal_pressure <= 2.35:
                    return -0.025, f"table goal profile downgrades goals ({goal_pressure:.1f})"
            return 0.0, ""

        if side not in ("home", "away"):
            return 0.0, ""

        backed = home if side == "home" else away
        opponent = away if side == "home" else home
        ppg_gap = backed["ppg"] - opponent["ppg"]
        rank_gap = opponent["rank"] - backed["rank"]
        delta = 0.0
        if ppg_gap >= 0.35 or rank_gap >= 6:
            delta = 0.03
        elif ppg_gap <= -0.35 or rank_gap <= -6:
            delta = -0.04

        motivation = ""
        teams = max(len(table), 1)
        if backed["rank"] <= 5:
            motivation = "top-table motivation"
            delta += 0.01
        elif backed["rank"] >= teams - 4:
            motivation = "relegation-pressure motivation"
            delta += 0.01

        delta = max(-0.05, min(0.05, delta))
        if abs(delta) < 0.01:
            return 0.0, ""
        direction = "supports" if delta > 0 else "downgrades"
        detail = motivation or f"table rank {backed['rank']} vs {opponent['rank']}"
        return delta, f"standings {direction} pick ({detail})"

    def _h2h_context_adjustment(self, row, selection: str, market: str, side: str) -> Tuple[float, str]:
        rows = self._h2h_rows(str(row['home_team'] or ""), str(row['away_team'] or ""))
        if len(rows) < 4:
            return 0.0, ""

        market = str(market or "").upper()
        if side in ("home", "away") and market in ("1X2", "AH"):
            covers = 0
            losses = 0
            handicap = 0.0
            match = re.search(r'\bAH\s*([+-]?\d+(?:\.\d+)?)\b', str(selection or ""), re.IGNORECASE)
            if match:
                handicap = float(match.group(1))
            for item in rows:
                home_goals = int(item["home_goals"])
                away_goals = int(item["away_goals"])
                selected_goals = home_goals if side == "home" and item["home_team"] == row["home_team"] else None
                opponent_goals = away_goals if side == "home" and item["home_team"] == row["home_team"] else None
                if side == "home" and item["away_team"] == row["home_team"]:
                    selected_goals = away_goals
                    opponent_goals = home_goals
                if side == "away" and item["home_team"] == row["away_team"]:
                    selected_goals = home_goals
                    opponent_goals = away_goals
                if side == "away" and item["away_team"] == row["away_team"]:
                    selected_goals = away_goals
                    opponent_goals = home_goals
                if selected_goals is None or opponent_goals is None:
                    continue
                margin = selected_goals + handicap - opponent_goals
                if margin > 0:
                    covers += 1
                elif margin < 0:
                    losses += 1
            if covers >= 3:
                return 0.025, f"H2H supports pick ({covers}/{len(rows)} recent covers)"
            if losses >= 3:
                return -0.025, f"H2H downgrades pick ({losses}/{len(rows)} recent misses)"

        totals = [int(item["home_goals"]) + int(item["away_goals"]) for item in rows]
        btts_count = sum(1 for item in rows if int(item["home_goals"]) > 0 and int(item["away_goals"]) > 0)
        avg_total = sum(totals) / len(totals)
        if side == "under":
            if avg_total <= 2.2 or btts_count <= 2:
                return 0.02, f"H2H supports lower scoring (avg {avg_total:.1f})"
            if avg_total >= 3.0 and btts_count >= 3:
                return -0.025, f"H2H downgrades lower scoring (avg {avg_total:.1f})"
        if side == "over":
            if avg_total >= 3.0 or btts_count >= 3:
                return 0.02, f"H2H supports goals (avg {avg_total:.1f})"
            if avg_total <= 2.2:
                return -0.02, f"H2H downgrades goals (avg {avg_total:.1f})"
        return 0.0, ""

    def _schedule_context_adjustment(self, row, side: str, market: str) -> Tuple[float, str]:
        home_load = self._team_fixture_load(str(row['home_team'] or ""), str(row['kickoff'] or ""), str(row['match_id'] or ""))
        away_load = self._team_fixture_load(str(row['away_team'] or ""), str(row['kickoff'] or ""), str(row['match_id'] or ""))
        home_busy = home_load["recent"] >= 2 or home_load["soon"] >= 1
        away_busy = away_load["recent"] >= 2 or away_load["soon"] >= 1
        if not home_busy and not away_busy:
            return 0.0, ""

        delta = 0.0
        if side == "home":
            delta = -0.025 if home_busy and not away_busy else 0.02 if away_busy and not home_busy else -0.01
        elif side == "away":
            delta = -0.025 if away_busy and not home_busy else 0.02 if home_busy and not away_busy else -0.01
        elif side == "under" and (home_busy or away_busy):
            delta = 0.015
        elif side == "over" and (home_busy or away_busy):
            delta = -0.015

        if abs(delta) < 0.01:
            return 0.0, ""
        direction = "supports" if delta > 0 else "downgrades"
        return delta, f"fixture congestion {direction} pick"

    def _team_news_context_adjustment(self, row, side: str) -> Tuple[float, str]:
        home_news = self._team_news_items(str(row['home_team'] or ""))
        away_news = self._team_news_items(str(row['away_team'] or ""))
        if not home_news and not away_news:
            return 0.0, ""

        if side in ("over", "under"):
            total_hits = len(home_news) + len(away_news)
            attacking_hits = sum(
                1
                for item in home_news + away_news
                if self._news_attack_hit(item)
            )
            defensive_hits = max(0, total_hits - attacking_hits)
            delta = 0.0
            if side == "under":
                delta = attacking_hits * 0.015 - defensive_hits * 0.01
            elif side == "over":
                delta = defensive_hits * 0.012 - attacking_hits * 0.012
            delta = max(-0.05, min(0.05, delta))
            if abs(delta) < 0.01:
                return 0.0, ""
            direction = "supports" if delta > 0 else "downgrades"
            if side == "under":
                return delta, f"attack/defence news {direction} lower scoring ({attacking_hits} attack, {defensive_hits} defence items)"
            return delta, f"attack/defence news {direction} goals ({attacking_hits} attack, {defensive_hits} defence items)"

        if side not in ("home", "away"):
            return 0.0, ""

        backed_news = home_news if side == "home" else away_news
        opponent_news = away_news if side == "home" else home_news
        backed_hits = len(backed_news)
        opponent_hits = len(opponent_news)
        delta = (opponent_hits - backed_hits) * 0.02
        delta = max(-0.06, min(0.06, delta))
        if abs(delta) < 0.01:
            return 0.0, ""
        direction = "supports" if delta > 0 else "downgrades"
        return delta, f"injury/suspension news {direction} pick ({backed_hits} backed-team, {opponent_hits} opponent items)"

    def _news_attack_hit(self, item: Dict) -> bool:
        text = " ".join(
            str(item.get(field) or "")
            for field in ("player", "status", "reason")
        ).lower()
        attack_words = (
            "striker", "forward", "winger", "attacker", "playmaker",
            "top scorer", "scorer", "goals", "hamstring", "calf"
        )
        return any(word in text for word in attack_words)

    def _league_table(self, league: str, kickoff: str) -> Dict[str, Dict]:
        cache_key = ("table", league, kickoff[:10])
        if cache_key in self._context_cache:
            return self._context_cache[cache_key]

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT home_team, away_team, home_goals, away_goals
            FROM matches
            WHERE league = ?
              AND status = 'completed'
              AND home_goals IS NOT NULL
              AND away_goals IS NOT NULL
              AND kickoff < ?
              AND kickoff >= date(?, '-365 day')
            """,
            (league, kickoff, kickoff[:10] or kickoff),
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()

        table = {}
        for item in rows:
            for team in (item["home_team"], item["away_team"]):
                table.setdefault(team, {"played": 0, "points": 0, "gf": 0, "ga": 0, "gd": 0, "ppg": 0.0, "rank": 99})
            home = table[item["home_team"]]
            away = table[item["away_team"]]
            hg = int(item["home_goals"])
            ag = int(item["away_goals"])
            home["played"] += 1
            away["played"] += 1
            home["gf"] += hg
            home["ga"] += ag
            away["gf"] += ag
            away["ga"] += hg
            if hg > ag:
                home["points"] += 3
            elif ag > hg:
                away["points"] += 3
            else:
                home["points"] += 1
                away["points"] += 1

        ranked = sorted(
            table.items(),
            key=lambda item: (
                item[1]["points"],
                item[1]["gf"] - item[1]["ga"],
                item[1]["gf"],
            ),
            reverse=True,
        )
        for rank, (_, item) in enumerate(ranked, 1):
            item["gd"] = item["gf"] - item["ga"]
            item["ppg"] = item["points"] / item["played"] if item["played"] else 0.0
            item["rank"] = rank

        self._context_cache[cache_key] = table
        return table

    def _h2h_rows(self, home_team: str, away_team: str) -> List[Dict]:
        cache_key = ("h2h", home_team, away_team)
        if cache_key in self._context_cache:
            return self._context_cache[cache_key]

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT home_team, away_team, home_goals, away_goals, kickoff
            FROM matches
            WHERE status = 'completed'
              AND home_goals IS NOT NULL
              AND away_goals IS NOT NULL
              AND ((home_team = ? AND away_team = ?) OR (home_team = ? AND away_team = ?))
            ORDER BY kickoff DESC
            LIMIT 5
            """,
            (home_team, away_team, away_team, home_team),
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()
        self._context_cache[cache_key] = rows
        return rows

    def _team_fixture_load(self, team: str, kickoff: str, match_id: str) -> Dict[str, int]:
        cache_key = ("load", team, kickoff[:16], match_id)
        if cache_key in self._context_cache:
            return self._context_cache[cache_key]

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT
                SUM(CASE WHEN kickoff < ? AND kickoff >= datetime(?, '-10 day') THEN 1 ELSE 0 END) AS recent,
                SUM(CASE WHEN kickoff > ? AND kickoff <= datetime(?, '+5 day') THEN 1 ELSE 0 END) AS soon
            FROM matches
            WHERE match_id != ?
              AND (home_team = ? OR away_team = ?)
            """,
            (kickoff, kickoff, kickoff, kickoff, match_id, team, team),
        )
        row = c.fetchone()
        conn.close()
        result = {
            "recent": int(row["recent"] or 0) if row else 0,
            "soon": int(row["soon"] or 0) if row else 0,
        }
        self._context_cache[cache_key] = result
        return result

    def _team_news_items(self, team: str) -> List[Dict]:
        cache_key = ("news", team)
        if cache_key in self._context_cache:
            return self._context_cache[cache_key]

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='team_news'")
        if not c.fetchone():
            conn.close()
            self._context_cache[cache_key] = []
            return []

        c.execute(
            """
            SELECT player, team, status, reason, source, confidence
            FROM team_news
            WHERE team = ?
              AND LOWER(status) IN ('injured', 'injury', 'suspended', 'out', 'doubtful')
            """,
            (team,),
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()
        self._context_cache[cache_key] = rows
        return rows

    def _build_reasoning(self, row, model_prob: float, book_prob: float, edge_pct: float, context_notes: List[str] = None) -> str:
        """Create a short model explanation suitable for manual review."""
        edge_display = edge_pct * 100
        notes = [
            f"Model prices this at {model_prob:.1%} versus bookmaker implied {book_prob:.1%}, creating +{edge_display:.1f}% edge."
        ]

        market = row['market']
        selection = row['selection']
        if market == '1X2':
            notes.append("1X2 value should be manually checked against injuries, motivation, and likely rotation.")
        elif market in ('OU', 'TT'):
            notes.append("Goals market should be manually checked against fatigue, tactical setup, and recent scoring profiles.")
        elif market == 'BTTS':
            notes.append("BTTS value should be manually checked against both teams' scoring and clean-sheet trends.")
        elif market == 'AH':
            notes.append("Asian handicap value should be checked for push rules, lineup strength, and game-state risk.")

        if context_notes:
            notes.append("External context: " + "; ".join(context_notes) + ".")

        return " ".join(notes)

    def _with_risk_band_reasoning(self, reasoning: str, risk_name: str) -> str:
        reasoning = re.sub(r"\s*(High Risk|Low Risk) candidate by odds profile\.", "", reasoning)
        return f"{reasoning} {risk_name} candidate by odds profile.".strip()

    def _build_reasoning_from_pick(self, pick: Pick) -> str:
        return (
            f"Model prices {pick.selection} at {pick.model_prob:.1%} versus "
            f"{pick.book_prob:.1%} book implied, giving +{pick.edge_pct:.1f}% edge."
        )

    def _build_risk_note(self, row) -> str:
        if row['odds'] >= 3.0:
            return "High-odds pick: require manual verification before staking."
        if row['market'] in ('OU', 'TT', 'BTTS'):
            return "Check lineup and fatigue news before kickoff."
        if row['market'] == 'AH':
            return "Check exact handicap line, push rules, and team news before kickoff."
        return "Check team news and odds movement before kickoff."
    
    def save_picks(self, picks: List[Pick], max_picks: int = 12, scale_to_bankroll: bool = True) -> List[Pick]:
        """Save picks to database with bankroll-aware staking."""
        # Deduplicate: keep only highest edge per match/selection
        seen = {}
        for pick in picks:
            # Use team names as key to avoid match_id duplication issues
            key = (pick.home_team, pick.away_team, pick.selection)
            if key not in seen or pick.edge_pct > seen[key].edge_pct:
                seen[key] = pick
        
        unique_picks = list(seen.values())
        # Sort by edge and take top picks (reduce count to stay under bankroll)
        unique_picks.sort(key=lambda x: x.edge_pct, reverse=True)
        top_picks = unique_picks[:max_picks]
        
        # Calculate total stake (ALL qualities)
        total_stake = sum(p.stake for p in top_picks)
        
        # If total exceeds 50% of bankroll, scale down proportionally
        max_total = self.bankroll * 0.5
        if scale_to_bankroll and total_stake > max_total:
            scale = max_total / total_stake
            for pick in top_picks:
                pick.stake = round(pick.stake * scale, 2)
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Clear old pending picks first
        c.execute("DELETE FROM picks WHERE status = 'pending'")
        
        for pick in top_picks:
            c.execute('''
                INSERT INTO picks 
                (match_id, selection, market, model_prob, book_prob, edge_pct, 
                 odds, stake, range_code, quality, reasoning, risk_note, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pick.match_id, pick.selection, pick.market,
                pick.model_prob, pick.book_prob, pick.edge_pct,
                pick.odds, pick.stake, pick.range_code, pick.quality,
                pick.reasoning, pick.risk_note, pick.status
            ))
        
        conn.commit()
        conn.close()
        print(f"Saved {len(top_picks)} unique picks (cleared old, top 12 by edge)")
        
        return top_picks

    def save_range_picks(self, picks: List[Pick]) -> List[Pick]:
        """Save risk-band picks without bankroll scaling."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM picks WHERE status = 'pending'")

        for pick in picks:
            c.execute('''
                INSERT INTO picks
                (match_id, selection, market, model_prob, book_prob, edge_pct,
                 odds, stake, range_code, quality, reasoning, risk_note, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pick.match_id, pick.selection, pick.market,
                pick.model_prob, pick.book_prob, pick.edge_pct,
                pick.odds, pick.stake, pick.range_code, pick.quality,
                pick.reasoning, pick.risk_note, pick.status
            ))

        conn.commit()
        conn.close()
        print(f"Saved {len(picks)} risk-band picks")
        return picks

if __name__ == '__main__':
    calc = EdgeCalculator(use_kelly=True)
    
    # Test Kelly calculation
    stake = calc.kelly_stake(0.25, 2.0, 0.55)
    print(f"Kelly stake for 25% edge @ 2.0 odds, 55% prob: ${stake}")
    
    # Test flat calculation
    calc_flat = EdgeCalculator(use_kelly=False)
    stake_flat = calc_flat.determine_stake(0.25, 2.0, 0.55, 'STRONG')
    print(f"Flat stake: ${stake_flat}")
