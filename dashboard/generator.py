"""Static risk-band dashboard generator."""

import html
import hashlib
import json
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.edge_calculator import EdgeCalculator
from config.paths import DASHBOARD_DIR, DB_PATH, ensure_runtime_dirs
from config.settings import load_settings
from models.core import init_db
from utils.match_resolver import parse_kickoff_utc


class DashboardGenerator:
    """Generates a static HTML dashboard for the risk-band workflow."""

    def __init__(self):
        ensure_runtime_dirs()
        init_db()
        self.output_file = DASHBOARD_DIR / "index.html"
        self.settings = load_settings()
        try:
            self.display_tz = ZoneInfo(self.settings.get("fixture_timezone", "Asia/Macau"))
        except (Exception, ZoneInfoNotFoundError):
            self.display_tz = timezone(timedelta(hours=8), "Asia/Macau")
        self.range_configs = EdgeCalculator.range_configs_from_settings(self.settings)
        self.active_range_codes = list(self.range_configs.keys())
        if set(self.active_range_codes) == {"C", "D"}:
            self.active_range_codes = ["D", "C"]

    def _risk_name(self, code: str) -> str:
        config = self.range_configs.get((code or "").upper())
        if config:
            return config.name
        return {"C": "High Risk", "D": "Low Risk"}.get((code or "").upper(), code or "")

    def get_picks(self) -> List[Dict]:
        """Fetch the current pending card with match details."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute(
            """
            SELECT p.*, m.home_team, m.away_team, m.league, m.kickoff,
                   m.home_fatigue_score, m.away_fatigue_score, m.fatigue_advantage
            FROM picks p
            JOIN matches m ON p.match_id = m.match_id
            WHERE p.status = 'pending'
              AND m.status = 'scheduled'
            ORDER BY COALESCE(p.range_code, 'D'), m.kickoff, p.edge_pct DESC
            """
        )

        picks = [dict(row) for row in c.fetchall()]
        conn.close()
        picks.sort(key=lambda row: (self._kickoff_sort_key(row.get("kickoff")), -float(row.get("edge_pct") or 0)))
        return picks

    def get_results_history(self) -> List[Dict]:
        """Fetch settled results for the cumulative history table."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT r.id, r.pick_id, r.match_id,
                   COALESCE(r.range_code, p.range_code) AS range_code,
                   COALESCE(r.quality, p.quality) AS quality,
                   r.result, r.home_goals, r.away_goals, r.stake, r.odds,
                   r.payout, r.pnl, r.settled_at, m.kickoff AS played_at,
                   m.home_team, m.away_team, p.selection
            FROM results r
            LEFT JOIN matches m ON r.match_id = m.match_id
            LEFT JOIN picks p ON r.pick_id = p.id
            ORDER BY COALESCE(m.kickoff, r.settled_at), r.id
            """
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()
        rows.sort(key=lambda row: (self._kickoff_sort_key(row.get("played_at")), int(row.get("id") or 0)))
        return rows

    def _quality_summary(self, picks: List[Dict]) -> str:
        rows = []
        for quality in ("STRONG", "KEEP", "CAUTION"):
            settled = [p for p in picks if p.get("result") in ("win", "loss", "push") and p.get("quality") == quality]
            wins = sum(1 for p in settled if p.get("result") == "win")
            losses = sum(1 for p in settled if p.get("result") == "loss")
            pushes = sum(1 for p in settled if p.get("result") == "push")
            decisions = wins + losses
            win_rate = (wins / decisions * 100) if decisions else 0
            pnl = sum(float(p.get("pnl") or 0) for p in settled)
            rows.append(
                f"<div><span>{quality}</span><strong>{wins}W-{losses}L"
                f"{('-' + str(pushes) + 'P') if pushes else ''}</strong>"
                f"<small>{win_rate:.1f}% · ${pnl:+,.0f}</small></div>"
            )
        return f"<div class=\"quality-summary\">{''.join(rows)}</div>"

    def _results_for_code(self, results: List[Dict], code: str) -> List[Dict]:
        return [
            r for r in results
            if (r.get("range_code") or "").upper() == code
            and r.get("result") in ("win", "loss", "push")
        ]

    def _quality_summary_from_results(self, results: List[Dict], code: str = None) -> str:
        if code:
            results = self._results_for_code(results, code)

        rows = []
        for quality in ("STRONG", "KEEP", "CAUTION"):
            settled = [r for r in results if r.get("result") in ("win", "loss", "push") and r.get("quality") == quality]
            wins = sum(1 for r in settled if r.get("result") == "win")
            losses = sum(1 for r in settled if r.get("result") == "loss")
            pushes = sum(1 for r in settled if r.get("result") == "push")
            decisions = wins + losses
            win_rate = (wins / decisions * 100) if decisions else 0
            pnl = sum(float(r.get("pnl") or 0) for r in settled)
            rows.append(
                f"<div><span>{quality}</span><strong>{wins}W-{losses}L"
                f"{('-' + str(pushes) + 'P') if pushes else ''}</strong>"
                f"<small>{win_rate:.1f}% / ${pnl:+,.0f}</small></div>"
            )
        return f"<div class=\"quality-summary\">{''.join(rows)}</div>"

    def _stats(self, rows: List[Dict]) -> Dict:
        wins = sum(1 for r in rows if r.get("result") == "win")
        losses = sum(1 for r in rows if r.get("result") == "loss")
        pushes = sum(1 for r in rows if r.get("result") == "push")
        staked = sum(float(r.get("stake") or 0) for r in rows)
        pnl = sum(float(r.get("pnl") or 0) for r in rows)
        roi = (pnl / staked * 100) if staked else 0.0
        return {
            "settled": len(rows),
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "staked": staked,
            "pnl": pnl,
            "roi": roi,
        }

    def _record(self, item: Dict) -> str:
        push = f"-{item['pushes']}P" if item["pushes"] else ""
        return f"{item['wins']}W-{item['losses']}L{push}"

    def _money(self, value: float) -> str:
        return f"${value:+,.2f}"

    def _risk_performance_summary(self, code: str, results: List[Dict]) -> str:
        range_results = self._results_for_code(results, code)
        total = self._stats(range_results)
        label = html.escape(self._risk_name(code))
        cards = []
        for card_label, rows, is_leader in [
            (label, range_results, True),
            ("STRONG", [r for r in range_results if r.get("quality") == "STRONG"], False),
            ("KEEP", [r for r in range_results if r.get("quality") == "KEEP"], False),
            ("CAUTION", [r for r in range_results if r.get("quality") == "CAUTION"], False),
        ]:
            item = self._stats(rows)
            tone = "good" if item["roi"] >= 0 else "bad"
            card_class = " performance-leader" if is_leader else ""
            cards.append(
                f'<div class="performance-card{card_class}">'
                f"<span>{card_label}</span>"
                f'<strong class="{tone}">{item["roi"]:+.1f}%</strong>'
                f"<small>{item['settled']} settled - {self._record(item)}</small>"
                f"<div>Staked ${item['staked']:,.0f} / P&amp;L {self._money(item['pnl'])}</div>"
                "</div>"
            )
        return (
            '<section class="range-performance">'
            '<div class="performance-heading">'
            f'<div><span>Settled performance</span><h2>{label} results</h2></div>'
            f"<p>{total['settled']} settled picks in this risk band, split by quality.</p>"
            "</div>"
            f'<div class="performance-grid">{"".join(cards)}</div>'
            "</section>"
        )

    def _range_performance_summary(self, results: List[Dict]) -> str:
        settled = [r for r in results if r.get("result") in ("win", "loss", "push")]

        def stats(rows: List[Dict]) -> Dict:
            wins = sum(1 for r in rows if r.get("result") == "win")
            losses = sum(1 for r in rows if r.get("result") == "loss")
            pushes = sum(1 for r in rows if r.get("result") == "push")
            staked = sum(float(r.get("stake") or 0) for r in rows)
            pnl = sum(float(r.get("pnl") or 0) for r in rows)
            roi = (pnl / staked * 100) if staked else 0.0
            return {
                "settled": len(rows),
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "staked": staked,
                "pnl": pnl,
                "roi": roi,
            }

        def record(item: Dict) -> str:
            push = f"-{item['pushes']}P" if item["pushes"] else ""
            return f"{item['wins']}W-{item['losses']}L{push}"

        def money(value: float) -> str:
            return f"${value:+,.2f}"

        overall = stats(settled)
        codes = ["C", "D"]
        by_code = {
            code: stats([r for r in settled if (r.get("range_code") or "").upper() == code])
            for code in codes
        }

        usable = {code: item for code, item in by_code.items() if item["staked"] > 0}
        leader_code = max(usable, key=lambda code: usable[code]["roi"]) if usable else ""
        leader_text = (
            f"Best so far: {self._risk_name(leader_code)} at {usable[leader_code]['roi']:+.1f}% ROI."
            if leader_code
            else "No settled range results yet."
        )

        cards = []
        for label, item, code in (
            ("Overall", overall, ""),
            (self._risk_name("C"), by_code["C"], "C"),
            (self._risk_name("D"), by_code["D"], "D"),
        ):
            card_class = " performance-leader" if code and code == leader_code else ""
            tone = "good" if item["roi"] >= 0 else "bad"
            cards.append(
                f'<div class="performance-card{card_class}">'
                f"<span>{label}</span>"
                f'<strong class="{tone}">{item["roi"]:+.1f}%</strong>'
                f"<small>{item['settled']} settled - {record(item)}</small>"
                f"<div>Staked ${item['staked']:,.0f} / P&amp;L {money(item['pnl'])}</div>"
                "</div>"
            )

        return (
            '<section class="range-performance">'
            '<div class="performance-heading">'
            '<div><span>Settled performance</span><h2>Total ROI and risk-band comparison</h2></div>'
            f"<p>{leader_text}</p>"
            "</div>"
            f'<div class="performance-grid">{"".join(cards)}</div>'
            "</section>"
        )

    def _history_table(self, results: List[Dict], code: str = None) -> str:
        if code:
            results = self._results_for_code(results, code)

        if not results:
            return '<div class="history empty-history">No settled picks yet for this risk band.</div>'

        running = 0.0
        rows = []
        for result in results:
            running += float(result.get("pnl") or 0)
            match = html.escape(f"{result.get('home_team') or ''} vs {result.get('away_team') or ''}".strip())
            selection = html.escape(str(result.get("selection") or ""))
            risk_name = html.escape(self._risk_name(str(result.get("range_code") or "")))
            played_at = html.escape(self._display_kickoff(result.get("played_at")))
            rows.append(
                "<tr>"
                f"<td>{played_at}</td>"
                f"<td>{risk_name}</td>"
                f"<td>{html.escape(str(result.get('quality') or ''))}</td>"
                f"<td>{match}</td>"
                f"<td>{selection}</td>"
                f"<td>{html.escape(str(result.get('result') or ''))}</td>"
                f"<td>${float(result.get('pnl') or 0):+,.0f}</td>"
                f"<td>${running:+,.0f}</td>"
                "</tr>"
            )

        return (
            f'<div class="history"><h2>{html.escape(self._risk_name(code)) if code else "Settled"} History</h2><table><thead><tr>'
            '<th>Played</th><th>Risk Band</th><th>Quality</th><th>Match</th>'
            '<th>Pick</th><th>Result</th><th>P&L</th><th>Cumulative</th>'
            '</tr></thead><tbody>'
            + ''.join(rows)
            + '</tbody></table></div>'
        )

    def _quality_label(self, quality: str) -> str:
        labels = {
            "STRONG": "STRONG",
            "KEEP": "KEEP",
            "CAUTION": "CAUTION",
            "SKIP": "SKIP",
        }
        return labels.get(quality or "KEEP", quality or "KEEP")

    def _local_kickoff(self, kickoff: str):
        kickoff_utc = parse_kickoff_utc(kickoff)
        if kickoff_utc is None:
            return None
        return kickoff_utc.astimezone(self.display_tz)

    def _kickoff_sort_key(self, kickoff: str) -> str:
        kickoff_utc = parse_kickoff_utc(kickoff)
        if kickoff_utc is None:
            return f"9999-12-31T23:59:59+00:00|{kickoff or ''}"
        return kickoff_utc.astimezone(timezone.utc).isoformat()

    def _display_kickoff(self, kickoff: str) -> str:
        local = self._local_kickoff(kickoff)
        if local is None:
            return str(kickoff or "TBD")
        return local.strftime("%Y-%m-%d %H:%M")

    def _date_key(self, kickoff: str) -> str:
        local = self._local_kickoff(kickoff)
        if local is None:
            return (kickoff or "TBD")[:10]
        return local.strftime("%Y-%m-%d")

    def _date_label(self, kickoff: str) -> str:
        local = self._local_kickoff(kickoff)
        if local is None:
            if not kickoff:
                return "TBD"
            raw = kickoff[:10]
            try:
                return datetime.strptime(raw, "%Y-%m-%d").strftime("%a %b %d")
            except ValueError:
                return raw
        return local.strftime("%a %b %d")

    def _get_h2h(self, home_team: str, away_team: str, limit: int = 5) -> str:
        """Get head-to-head history between two teams."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''
            SELECT home_team, away_team, home_goals, away_goals, kickoff
            FROM matches
            WHERE status = 'completed'
            AND (
                (home_team = ? AND away_team = ?)
                OR (home_team = ? AND away_team = ?)
            )
            ORDER BY kickoff DESC
            LIMIT ?
        ''', (home_team, away_team, away_team, home_team, limit))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return ""
        
        h2h_rows = []
        for h, a, hg, ag, date in rows:
            winner = "H" if hg > ag else "A" if ag > hg else "D"
            h2h_rows.append(
                f"<tr><td>{date[:10]}</td>"
                f"<td>{html.escape(h)} {hg}-{ag} {html.escape(a)}</td>"
                f"<td>{winner}</td></tr>"
            )
        
        return (
            '<div class="h2h"><h3>Head-to-Head</h3>'
            '<table><thead><tr><th>Date</th><th>Match</th><th>W</th></tr></thead><tbody>'
            + ''.join(h2h_rows)
            + '</tbody></table></div>'
        )

    def _get_recent_form(self, team: str, limit: int = 5) -> str:
        """Get recent form for a team."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''
            SELECT home_team, away_team, home_goals, away_goals, kickoff
            FROM matches
            WHERE status = 'completed'
            AND home_goals IS NOT NULL
            AND away_goals IS NOT NULL
            AND (home_team = ? OR away_team = ?)
            ORDER BY kickoff DESC
            LIMIT ?
        ''', (team, team, limit))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return ""
        
        form_rows = []
        for h, a, hg, ag, date in rows:
            if h == team:
                result = "W" if hg > ag else "D" if hg == ag else "L"
                score = f"{hg}-{ag}"
                opponent = a
            else:
                result = "W" if ag > hg else "D" if ag == hg else "L"
                score = f"{ag}-{hg}"
                opponent = h
            
            form_rows.append(
                f'<span class="form-{result.lower()}">{result}</span>'
                f'<small>{html.escape(opponent)} {score}</small>'
            )
        
        return (
            '<div class="recent-form"><h3>Recent Form</h3>'
            '<div class="form-row">'
            + ''.join(form_rows)
            + '</div></div>'
        )

    def _pick_reasoning(self, pick: Dict) -> str:
        parts = []
        if pick.get("reasoning"):
            parts.append(html.escape(pick["reasoning"]))
        else:
            parts.append(
                html.escape(
                    f"Model {pick['model_prob']:.1%} vs book {pick['book_prob']:.1%}; "
                    f"edge +{pick['edge_pct']:.1f}%."
                )
            )

        if pick.get("risk_note"):
            parts.append(f"<strong>Risk:</strong> {html.escape(pick['risk_note'])}")

        fatigue = pick.get("fatigue_advantage")
        if fatigue and fatigue != "even":
            home_score = pick.get("home_fatigue_score")
            away_score = pick.get("away_fatigue_score")
            parts.append(
                "<strong>Fatigue:</strong> "
                f"{html.escape(str(fatigue))} "
                f"(home {home_score}, away {away_score})."
            )

        return "<br><br>".join(parts)

    def _group_by_date(self, picks: List[Dict]) -> Dict[str, List[Dict]]:
        grouped = defaultdict(list)
        for pick in sorted(picks, key=lambda item: self._kickoff_sort_key(item.get("kickoff"))):
            grouped[self._date_key(pick.get("kickoff"))].append(pick)
        return dict(sorted(grouped.items(), key=lambda item: item[0]))

    def _render_filter(self, code: str, picks: List[Dict]) -> str:
        dates = []
        seen = set()
        for pick in picks:
            key = self._date_key(pick.get("kickoff"))
            if key not in seen:
                seen.add(key)
                dates.append((key, self._date_label(pick.get("kickoff"))))

        options = ['<option value="all">All dates</option>']
        options.extend(
            f'<option value="{html.escape(key)}">{html.escape(label)}</option>'
            for key, label in dates
        )

        return f"""
<div class="filter-bar">
  <label for="{code}-filter">Filter by kickoff</label>
  <select id="{code}-filter" class="filter-select" onchange="filterRange('{code}', this.value)">
    {''.join(options)}
  </select>
  <div class="filter-count">Showing <span id="{code}-visible">{len(picks)}</span> of {len(picks)} picks</div>
</div>
"""

    def _get_match_news(self, home_team: str, away_team: str) -> Dict[str, List[Dict]]:
        """Fetch team news for both teams in a match."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        teams = [home_team, away_team]
        placeholders = ",".join("?" for _ in teams)
        c.execute(f"""
            SELECT player, team, status, reason
            FROM team_news
            WHERE team IN ({placeholders})
            GROUP BY LOWER(player), team
            ORDER BY team, status
        """, tuple(teams))
        rows = [dict(row) for row in c.fetchall()]
        conn.close()
        result = {"home": [], "away": []}
        for row in rows:
            if row["team"] == home_team:
                result["home"].append(row)
            elif row["team"] == away_team:
                result["away"].append(row)
        return result

    def _render_pick(self, code: str, pick: Dict, index: int) -> str:
        quality = pick.get("quality") or "KEEP"
        quality_class = quality.lower()
        date_key = self._date_key(pick.get("kickoff"))
        kickoff = html.escape(self._display_kickoff(pick.get("kickoff")))
        selection = html.escape(str(pick.get("selection") or ""))
        market = html.escape(str(pick.get("market") or ""))
        matchup = html.escape(f"{pick.get('home_team')} vs {pick.get('away_team')}")
        league = html.escape(str(pick.get("league") or ""))
        pick_id = int(pick["id"])
        win_profit = round(float(pick["stake"]) * (float(pick["odds"]) - 1))
        reasoning = self._pick_reasoning(pick)
        
        # Get H2H and form
        h2h_html = self._get_h2h(pick.get('home_team', ''), pick.get('away_team', ''))
        home_form = self._get_recent_form(pick.get('home_team', ''))
        away_form = self._get_recent_form(pick.get('away_team', ''))

        # News indicator
        news = self._get_match_news(pick.get('home_team', ''), pick.get('away_team', ''))
        total_news = len(news["home"]) + len(news["away"])
        news_badge = ""
        news_details = ""
        if total_news > 0:
            # Determine direction from reasoning snippet
            delta_sign = ""
            reasoning_lower = (pick.get("reasoning") or "").lower()
            if "news supports" in reasoning_lower or "injury/suspension news supports" in reasoning_lower:
                delta_sign = "+"
            elif "news downgrades" in reasoning_lower or "injury/suspension news downgrades" in reasoning_lower:
                delta_sign = "−"
            news_class = "news-good" if delta_sign == "+" else "news-bad" if delta_sign == "−" else "news-neut"
            news_badge = f'<span class="news-badge {news_class}">{delta_sign}News</span>'
            # Compact news list for details
            news_rows = []
            for side, label in (("home", pick.get('home_team')), ("away", pick.get('away_team'))):
                items = news[side]
                if items:
                    line = "; ".join(f"{html.escape(i['player'] or '—')} ({html.escape(i['status'] or '?')})" for i in items[:3])
                    if len(items) > 3:
                        line += f" +{len(items)-3} more"
                    news_rows.append(f"<small><strong>{html.escape(label)}:</strong> {line}</small>")
            if news_rows:
                news_details = f'<div class="news-block">{"<br>".join(news_rows)}</div>'

        return f"""
<div class="pick-card {quality_class}" id="pick-{pick_id}" data-date="{html.escape(date_key)}">
  <div class="pick-summary" onclick="toggleDetails({pick_id})">
    <div class="rank">#{index}</div>
    <div class="pick-main">
      <div class="pick-title"><span class="market-pill">{market}</span>{selection} <span class="badge {quality_class}">{self._quality_label(quality)}</span></div>
      <div class="pick-meta">{matchup} · <span>{kickoff}</span> · {league}</div>
    </div>
    <div class="numbers">
      <div><strong>@{float(pick['odds']):.2f}</strong><span>Odds</span></div>
      <div><strong class="good">+{float(pick['edge_pct']):.1f}%</strong><span>Edge</span></div>
      <div><strong>${float(pick['stake']):.0f}</strong><span>Stake</span></div>
    </div>
    <div class="result" id="result-{pick_id}">pending</div>
    <div class="actions">
      <button class="win" onclick="event.stopPropagation(); markPick('{code}', {pick_id}, 'win')">WIN</button>
      <button class="loss" onclick="event.stopPropagation(); markPick('{code}', {pick_id}, 'loss')">LOSS</button>
      <button onclick="event.stopPropagation(); markPick('{code}', {pick_id}, 'push')">PUSH</button>
      <button onclick="event.stopPropagation(); markPick('{code}', {pick_id}, 'pending')">RST</button>
    </div>
  </div>
  <div class="pick-details" id="details-{pick_id}">
    <div class="reasoning">{reasoning}</div>
    <div class="metrics">
      <span>Model <strong>{float(pick['model_prob']):.1%}</strong></span>
      <span>Book <strong>{float(pick['book_prob']):.1%}</strong></span>
      <span>Win <strong class="good">+${win_profit}</strong></span>
      <span>Lose <strong class="bad">-${float(pick['stake']):.0f}</strong></span>
    </div>
    {h2h_html}
    {home_form}
    {away_form}
    {news_details}
  </div>
</div>
"""

    def _render_range(self, code: str, picks: List[Dict], results_history: List[Dict], active: bool) -> str:
        config = self.range_configs[code]
        grouped = self._group_by_date(picks)
        strong = sum(1 for p in picks if p.get("quality") == "STRONG")
        keep = sum(1 for p in picks if p.get("quality") == "KEEP")
        caution = sum(1 for p in picks if p.get("quality") == "CAUTION")
        total_stake = sum(float(p["stake"]) for p in picks)
        settled = self._stats(self._results_for_code(results_history, code))
        settled_record = self._record(settled) if settled["settled"] else "-"
        settled_roi = (f'{settled["roi"]:+.1f}%' if settled["staked"] else "-")
        settled_pnl_class = "good" if settled["pnl"] >= 0 else "bad"
        settled_bank = config.bankroll + settled["pnl"]
        cards = []
        index = 1

        for date_key, date_picks in grouped.items():
            label = self._date_label(date_picks[0].get("kickoff"))
            cards.append(
                f'<div class="day-header" data-date="{html.escape(date_key)}">'
                f'<span>{html.escape(label)}</span><small>{len(date_picks)} picks</small></div>'
            )
            for pick in date_picks:
                cards.append(self._render_pick(code, pick, index))
                index += 1

        if not cards:
            cards.append('<div class="empty">No picks generated for this risk band yet.</div>')

        return f"""
<section class="range {'on' if active else ''}" id="range-{code}">
  {self._risk_performance_summary(code, results_history)}
  <div class="pnl-bar">
    <div><span>Base Bank</span><strong>${config.bankroll:,.0f}</strong></div>
    <div><span>Flat Stake</span><strong>${config.flat_stake:,.0f}</strong></div>
    <div><span>Odds Band</span><strong>{config.min_odds:.2f}-{config.max_odds:.2f}</strong></div>
    <div><span>Strong</span><strong class="good">{strong}</strong></div>
    <div><span>Keep</span><strong>{keep}</strong></div>
    <div><span>Caution</span><strong class="warn">{caution}</strong></div>
    <div><span>Record</span><strong id="{code}-record">{settled_record}</strong></div>
    <div><span>Staked</span><strong id="{code}-staked">${settled["staked"]:,.0f}</strong></div>
    <div><span>P&amp;L</span><strong id="{code}-pnl" class="{settled_pnl_class}">{self._money(settled["pnl"])}</strong></div>
    <div><span>ROI</span><strong id="{code}-roi" class="{settled_pnl_class if settled["staked"] else ""}">{settled_roi}</strong></div>
    <div><span>Bank</span><strong id="{code}-bank" class="{settled_pnl_class}">${settled_bank:,.2f}</strong></div>
  </div>
  <div class="range-note">{len(picks)} picks · planned stake ${total_stake:,.0f} · correlated same-match exposure is flagged in each pick's risk note.</div>
  {self._render_filter(code, picks)}
  {''.join(cards)}
  {self._history_table(results_history, code)}
</section>
"""

    def _pick_to_dict(self, pick) -> Dict:
        return {
            "match_id": pick.match_id,
            "home_team": pick.home_team,
            "away_team": pick.away_team,
            "league": pick.league,
            "kickoff": pick.kickoff,
            "selection": pick.selection,
            "market": pick.market,
            "model_prob": pick.model_prob,
            "book_prob": pick.book_prob,
            "edge_pct": pick.edge_pct,
            "odds": pick.odds,
            "stake": pick.stake,
            "quality": pick.quality,
            "reasoning": pick.reasoning,
            "risk_note": pick.risk_note,
        }

    def _parley_source_band(self, odds: float) -> str:
        if odds < 1.70:
            return "LOW ODDS"
        if odds <= 2.15:
            return "LOW RISK"
        return "BOOSTER"

    def _parley_candidates(self) -> List[Dict]:
        calc = EdgeCalculator(
            bankroll=float(self.settings.get("bankroll", 10000)),
            use_ranges=bool(self.settings.get("use_ranges", False)),
            staking_mode=self.settings.get("staking_mode"),
            flat_stake=float(self.settings.get("flat_stake", 10)),
            range_configs=self.range_configs,
            bookmaker=self.settings.get("default_bookmaker"),
        )
        learned = calc._learned_performance_adjustments()
        loss_traps = calc._loss_trap_segments()
        raw_candidates = calc.generate_picks(min_edge=0.02)
        candidates = []
        for pick in raw_candidates:
            odds = float(pick.odds or 0)
            model_prob = float(pick.model_prob or 0)
            if odds < 1.25 or odds > 2.70:
                continue
            if model_prob < 0.54:
                continue
            if pick.edge_pct < 3.0:
                continue
            if calc._is_hard_loss_trap(pick, "D", loss_traps) or calc._is_hard_loss_trap(pick, "C", loss_traps):
                continue

            band = self._parley_source_band(odds)
            learned_score = (
                0.65 * calc._historical_pick_score(pick, "D", learned)
                + 0.35 * calc._historical_pick_score(pick, "C", learned)
            )
            odds_penalty = max(0.0, odds - 1.85) * 0.10
            low_odds_bonus = 0.05 if odds <= 1.90 else 0.0
            booster_penalty = 0.08 if band == "BOOSTER" else 0.0
            parley_score = learned_score + low_odds_bonus - odds_penalty - booster_penalty

            item = self._pick_to_dict(pick)
            item["source_band"] = band
            item["parley_score"] = parley_score
            candidates.append(item)

        candidates.sort(
            key=lambda p: (
                float(p.get("parley_score") or 0),
                float(p.get("model_prob") or 0),
                float(p.get("edge_pct") or 0),
                -float(p.get("odds") or 0),
            ),
            reverse=True,
        )
        return candidates

    def _build_parley_slip(self, label: str, candidates: List[Dict], max_legs: int, max_boosters: int = 0) -> Dict:
        legs = []
        used_matches = set()
        booster_count = 0
        for pick in candidates:
            match_id = pick.get("match_id")
            if match_id in used_matches:
                continue
            is_booster = pick.get("source_band") == "BOOSTER"
            if is_booster and booster_count >= max_boosters:
                continue
            legs.append(pick)
            used_matches.add(match_id)
            if is_booster:
                booster_count += 1
            if len(legs) == max_legs:
                break

        odds = 1.0
        model_prob = 1.0
        for leg in legs:
            odds *= float(leg.get("odds") or 1)
            model_prob *= float(leg.get("model_prob") or 0)
        legs.sort(key=lambda leg: self._kickoff_sort_key(leg.get("kickoff")))

        return {
            "label": label,
            "legs": legs,
            "odds": odds,
            "model_prob": model_prob,
            "boosters": booster_count,
            "status": "pending",
            "quality": "KEEP",
        }

    def _parley_slip_key(self, slip: Dict) -> str:
        raw = "|".join(
            [str(slip.get("label") or "Parley")]
            + [
                f"{leg.get('match_id')}:{leg.get('market')}:{leg.get('selection')}"
                for leg in slip.get("legs", [])
            ]
        )
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _clear_replaceable_pending_parleys(self, c) -> None:
        now_utc = datetime.now(timezone.utc)
        c.execute(
            """
            SELECT s.id, MIN(m.kickoff) AS first_kickoff
            FROM parley_slips s
            JOIN parley_legs l ON s.id = l.slip_id
            JOIN matches m ON l.match_id = m.match_id
            WHERE s.status = 'pending'
            GROUP BY s.id
            """
        )
        replaceable_ids = []
        for slip_id, first_kickoff in c.fetchall():
            kickoff_utc = parse_kickoff_utc(first_kickoff)
            if kickoff_utc is None or kickoff_utc >= now_utc:
                replaceable_ids.append(slip_id)
        if not replaceable_ids:
            return
        placeholders = ",".join("?" for _ in replaceable_ids)
        c.execute(f"DELETE FROM parley_legs WHERE slip_id IN ({placeholders})", replaceable_ids)
        c.execute(f"DELETE FROM parley_slips WHERE id IN ({placeholders})", replaceable_ids)

    def save_parley_slips(self) -> List[Dict]:
        candidates = self._parley_candidates()
        stake = max(float(self.settings.get("flat_stake", 10)) / 2, 1)
        slips = [
            self._build_parley_slip("Conservative 2-leg", candidates, 2, max_boosters=0),
            self._build_parley_slip("Balanced 3-leg", candidates, 3, max_boosters=1),
        ]
        slips = [slip for slip in slips if len(slip.get("legs", [])) >= 2]

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        self._clear_replaceable_pending_parleys(c)
        saved = []
        for slip in slips:
            slip_key = self._parley_slip_key(slip)
            c.execute(
                """
                INSERT OR IGNORE INTO parley_slips
                (slip_key, label, odds, model_prob, stake, quality, status)
                VALUES (?, ?, ?, ?, ?, 'KEEP', 'pending')
                """,
                (slip_key, slip["label"], slip["odds"], slip["model_prob"], stake),
            )
            c.execute("SELECT id FROM parley_slips WHERE slip_key = ?", (slip_key,))
            row = c.fetchone()
            if not row:
                continue
            slip_id = row[0]
            c.execute("SELECT COUNT(*) FROM parley_legs WHERE slip_id = ?", (slip_id,))
            if c.fetchone()[0]:
                continue
            for leg_order, leg in enumerate(slip["legs"], 1):
                c.execute(
                    """
                    INSERT INTO parley_legs
                    (slip_id, leg_order, match_id, selection, market, odds, model_prob,
                     edge_pct, source_band)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        slip_id,
                        leg_order,
                        leg.get("match_id"),
                        leg.get("selection"),
                        leg.get("market"),
                        leg.get("odds"),
                        leg.get("model_prob"),
                        leg.get("edge_pct"),
                        leg.get("source_band"),
                    ),
                )
            slip["id"] = slip_id
            slip["slip_key"] = slip_key
            saved.append(slip)
        conn.commit()
        conn.close()
        return saved

    def _get_parley_slips(self) -> List[Dict]:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT *
            FROM parley_slips
            ORDER BY COALESCE(settled_at, created_at), id
            """
        )
        slips = [dict(row) for row in c.fetchall()]
        for slip in slips:
            c.execute(
                """
                SELECT l.*, m.home_team, m.away_team, m.league, m.kickoff
                FROM parley_legs l
                JOIN matches m ON l.match_id = m.match_id
                WHERE l.slip_id = ?
                ORDER BY l.leg_order
                """,
                (slip["id"],),
            )
            slip["legs"] = [dict(row) for row in c.fetchall()]
        conn.close()
        return slips

    def _render_parley_card(self, slip: Dict, index: int, stake: float) -> str:
        legs = slip["legs"]
        if not legs:
            return ""

        stake = float(slip.get("stake") or stake)
        status = str(slip.get("status") or "pending")
        result = str(slip.get("result") or status)
        potential_profit = stake * (float(slip["odds"]) - 1)
        pnl = float(slip.get("pnl") or 0)
        result_class = "good" if result == "win" else "bad" if result == "loss" else ""
        details = []
        for leg in legs:
            match = html.escape(f"{leg.get('home_team') or ''} vs {leg.get('away_team') or ''}")
            pick = html.escape(str(leg.get("selection") or ""))
            market = html.escape(str(leg.get("market") or ""))
            kickoff = html.escape(self._display_kickoff(leg.get("kickoff")))
            band = html.escape(str(leg.get("source_band") or "MODEL"))
            leg_result = html.escape(str(leg.get("result") or "pending"))
            details.append(
                '<div class="parley-leg">'
                f'<div><span class="market-pill">{market}</span><strong>{pick}</strong></div>'
                f"<small>{match} &middot; {kickoff}</small>"
                '<div class="metrics">'
                f"<span>Odds <strong>@{float(leg.get('odds') or 0):.2f}</strong></span>"
                f"<span>Model <strong>{float(leg.get('model_prob') or 0):.1%}</strong></span>"
                f"<span>Edge <strong class=\"good\">+{float(leg.get('edge_pct') or 0):.1f}%</strong></span>"
                f"<span>Band <strong>{band}</strong></span>"
                f"<span>Result <strong>{leg_result}</strong></span>"
                "</div>"
                "</div>"
            )

        slip_id = f"parley-{slip.get('id') or index}"
        leg_count = len(legs)
        title = html.escape(str(slip["label"]))
        result_html = (
            f'<div class="result {result_class}">{html.escape(result)}</div>'
            if status == "settled"
            else '<div class="result">pending</div>'
        )
        return f"""
<div class="pick-card keep parley-pick" id="pick-{slip_id}">
  <div class="pick-summary" onclick="toggleDetails('{slip_id}')">
    <div class="rank">#{index}</div>
    <div class="pick-main">
      <div class="pick-title"><span class="market-pill">PARLEY</span>{title} <span class="badge keep">KEEP</span></div>
      <div class="pick-meta">{leg_count} legs &middot; low odds model pool &middot; no same-match legs</div>
    </div>
    <div class="numbers">
      <div><strong>@{float(slip["odds"]):.2f}</strong><span>Total Odds</span></div>
      <div><strong class="good">{float(slip["model_prob"]):.1%}</strong><span>Model Hit</span></div>
      <div><strong>${stake:,.0f}</strong><span>Stake</span></div>
      <div><strong class="{'good' if pnl >= 0 else 'bad'}">{('$' + format(pnl, '+,.0f')) if status == 'settled' else ('+$' + format(potential_profit, ',.0f'))}</strong><span>{'P&L' if status == 'settled' else 'Profit'}</span></div>
    </div>
    {result_html}
  </div>
  <div class="pick-details" id="details-{slip_id}">
    <div class="reasoning">This parley is built from the model's separate parley pool, not copied from Low Risk. It prefers lower odds, higher model probability, and segments that have learned better from settled results. It allows only limited booster odds because every leg must win.</div>
    <div class="metrics">
      <span>Combined Odds <strong>@{float(slip["odds"]):.2f}</strong></span>
      <span>Combined Model <strong>{float(slip["model_prob"]):.1%}</strong></span>
      <span>Win <strong class="good">+${potential_profit:,.0f}</strong></span>
      <span>Lose <strong class="bad">-${stake:,.0f}</strong></span>
    </div>
    <div class="parley-legs">{"".join(details)}</div>
  </div>
</div>
"""

    def _parley_stats(self, slips: List[Dict]) -> Dict:
        settled = [slip for slip in slips if slip.get("status") == "settled" and slip.get("result") in ("win", "loss", "push")]
        wins = sum(1 for slip in settled if slip.get("result") == "win")
        losses = sum(1 for slip in settled if slip.get("result") == "loss")
        pushes = sum(1 for slip in settled if slip.get("result") == "push")
        staked = sum(float(slip.get("stake") or 0) for slip in settled)
        pnl = sum(float(slip.get("pnl") or 0) for slip in settled)
        roi = (pnl / staked * 100) if staked else 0.0
        return {
            "settled": len(settled),
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "staked": staked,
            "pnl": pnl,
            "roi": roi,
        }

    def _render_parley_history(self, slips: List[Dict]) -> str:
        settled = [
            slip for slip in slips
            if slip.get("status") == "settled" and slip.get("result") in ("win", "loss", "push")
        ]
        if not settled:
            return '<div class="history empty-history">No settled parley history yet.</div>'

        running = 0.0
        rows = []
        for slip in settled:
            running += float(slip.get("pnl") or 0)
            legs = slip.get("legs", [])
            played_at = max((self._kickoff_sort_key(leg.get("kickoff")) for leg in legs), default=datetime.min)
            played_label = played_at.strftime("%Y-%m-%d %H:%M") if played_at != datetime.min else ""
            leg_text = " / ".join(
                f"{leg.get('home_team') or ''} vs {leg.get('away_team') or ''}: {leg.get('selection') or ''}"
                for leg in legs
            )
            rows.append(
                "<tr>"
                f"<td>{html.escape(played_label)}</td>"
                "<td>Parley</td>"
                f"<td>{html.escape(str(slip.get('quality') or 'KEEP'))}</td>"
                f"<td>{html.escape(str(slip.get('label') or 'Parley'))}</td>"
                f"<td>{html.escape(leg_text)}</td>"
                f"<td>{html.escape(str(slip.get('result') or ''))}</td>"
                f"<td>${float(slip.get('pnl') or 0):+,.0f}</td>"
                f"<td>${running:+,.0f}</td>"
                "</tr>"
            )
        return (
            '<div class="history"><h2>Parley History</h2><table><thead><tr>'
            '<th>Played</th><th>Risk Band</th><th>Quality</th><th>Slip</th>'
            '<th>Legs</th><th>Result</th><th>P&L</th><th>Cumulative</th>'
            '</tr></thead><tbody>'
            + ''.join(rows)
            + '</tbody></table></div>'
        )

    def _render_parley(self) -> str:
        candidates = self._parley_candidates()
        saved_slips = self._get_parley_slips()
        pending_slips = [slip for slip in saved_slips if slip.get("status") == "pending"]
        stats = self._parley_stats(saved_slips)
        stake = max(float(self.settings.get("flat_stake", 10)) / 2, 1)
        bank = float(self.settings.get("bankroll", 100))
        cards = [
            self._render_parley_card(slip, index, stake)
            for index, slip in enumerate(pending_slips, 1)
        ]
        if not cards:
            cards.append('<div class="empty">No pending parley slips saved yet.</div>')
        record = f"{stats['wins']}W-{stats['losses']}L" + (f"-{stats['pushes']}P" if stats["pushes"] else "")
        pnl_class = "good" if stats["pnl"] >= 0 else "bad"
        roi_class = "good" if stats["roi"] >= 0 else "bad"
        settled_bank = bank + stats["pnl"]

        return f"""
<section class="range" id="range-PARLEY">
  <section class="range-performance">
    <div class="performance-heading">
      <div><span>Parley</span><h2>Low-odds slips from the model pool</h2></div>
      <p>Recommended: 2 legs first. The 3-leg slip may include one controlled odds booster.</p>
    </div>
    <div class="performance-grid">
      <div class="performance-card performance-leader"><span>Parley</span><strong class="{roi_class}">{stats["roi"]:+.1f}%</strong><small>{stats["settled"]} settled - {record}</small><div>Staked ${stats["staked"]:,.0f} / P&amp;L ${stats["pnl"]:+,.2f}</div></div>
      <div class="performance-card"><span>Source</span><strong>{len(candidates)}</strong><small>Parley candidates</small><div>Selected from full model odds pool.</div></div>
      <div class="performance-card"><span>Stake</span><strong>${stake:,.0f}</strong><small>Suggested per slip</small><div>Half of flat stake.</div></div>
      <div class="performance-card"><span>Rule</span><strong>1</strong><small>Booster max</small><div>Low odds first, no same-match correlation.</div></div>
    </div>
  </section>
  <div class="pnl-bar">
    <div><span>Base Bank</span><strong>${bank:,.0f}</strong></div>
    <div><span>Slip Stake</span><strong>${stake:,.0f}</strong></div>
    <div><span>Record</span><strong>{record}</strong></div>
    <div><span>P&amp;L</span><strong class="{pnl_class}">${stats["pnl"]:+,.2f}</strong></div>
    <div><span>Bank</span><strong class="{pnl_class}">${settled_bank:,.2f}</strong></div>
  </div>
  <div class="range-note">{len(pending_slips)} pending parley slips &middot; every leg must win &middot; Parley is now saved and settled separately from High Risk and Low Risk.</div>
  <div class="day-header"><span>Recommended Parley Slips</span><small>{len(pending_slips)} slips</small></div>
  {"".join(cards)}
  {self._render_parley_history(saved_slips)}
</section>
"""

    def generate(self):
        """Generate the dashboard and return the output path."""
        picks = self.get_picks()
        results_history = self.get_results_history()
        by_range = {
            code: [p for p in picks if (p.get("range_code") or "D").upper() == code]
            for code in self.active_range_codes
        }

        js_picks = {
            code: [
                {
                    "id": int(p["id"]),
                    "odds": float(p["odds"]),
                    "stake": float(p["stake"]),
                    "status": p.get("result") or p.get("status") or "pending",
                }
                for p in range_picks
            ]
            for code, range_picks in by_range.items()
        }
        js_bank = {code: self.range_configs[code].bankroll for code in self.active_range_codes}
        js_settled = {}
        for code in self.active_range_codes:
            range_results = [
                r for r in results_history
                if (r.get("range_code") or "").upper() == code
                and r.get("result") in ("win", "loss", "push")
            ]
            js_settled[code] = {
                "wins": sum(1 for r in range_results if r.get("result") == "win"),
                "losses": sum(1 for r in range_results if r.get("result") == "loss"),
                "pushes": sum(1 for r in range_results if r.get("result") == "push"),
                "staked": sum(float(r.get("stake") or 0) for r in range_results),
                "pnl": sum(float(r.get("pnl") or 0) for r in range_results),
            }

        generated = datetime.now(timezone.utc).astimezone(self.display_tz).strftime("%A, %B %d, %Y at %H:%M")
        total_picks = len(picks)
        flat_stake = float(self.settings.get("flat_stake", 0))
        source_label = html.escape(str(self.settings.get("default_bookmaker", "polymarket")).title())
        tab_buttons = "".join(
            f'<button class="tab {"on" if index == 0 else ""}" onclick="switchRange(\'{code}\')">'
            f"{html.escape(self.range_configs[code].name)}</button>"
            for index, code in enumerate(self.active_range_codes)
        ) + '<button class="tab" onclick="switchRange(\'PARLEY\')">Parley</button>'
        active_codes_json = json.dumps(self.active_range_codes)
        range_sections = "".join(
            self._render_range(code, by_range[code], results_history, index == 0)
            for index, code in enumerate(self.active_range_codes)
        ) + self._render_parley()

        html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RolloStake - Polymarket Risk Bands</title>
<style>
:root {{
  --background:#1a1a18;
  --panel:#2a2a26;
  --panel-strong:#3a3a34;
  --ink:#f5efe4;
  --muted:#a8a090;
  --accent:#d77b46;
  --accent-soft:#e89a6a;
  --line:#f5efe41a;
  --shadow:#0000004d;
  --good:#86efac;
  --bad:#fca5a5;
  --warn:#fdba74;
  --blue:#93c5fd;
}}
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ background:var(--background); color:var(--ink); min-height:100vh; font-family:Georgia,"Times New Roman",serif; font-size:14px; line-height:1.6; transition:background .25s,color .25s; }}
.shell {{ max-width:1200px; margin:0 auto; padding:24px 16px 64px; }}
.site-header {{ display:flex; justify-content:space-between; align-items:flex-start; gap:16px; margin:0 auto 28px; max-width:1200px; padding:24px 16px 0; }}
.brand h1 {{ color:var(--ink); font-size:2rem; line-height:1.1; font-weight:700; margin-bottom:8px; }}
.brand p {{ color:var(--muted); }}
.tabs {{ border-bottom:1px solid var(--line); display:flex; gap:4px; margin-bottom:24px; }}
.tab {{ background:transparent; border:0; color:var(--muted); border-radius:8px 8px 0 0; padding:12px 16px; font-family:inherit; font-size:1rem; cursor:pointer; transition:all .2s; }}
.tab:hover {{ color:var(--ink); background:var(--panel); }}
.tab.on {{ background:var(--panel); color:var(--accent); border-bottom:2px solid var(--accent); font-weight:600; }}
.intro {{ background:var(--panel); border:1px solid var(--line); border-radius:16px; box-shadow:0 1px 3px var(--shadow); padding:20px; line-height:1.65; margin-bottom:16px; color:var(--muted); }}
.intro strong {{ color:var(--ink); }}
.quality-summary {{ display:grid; grid-template-columns:repeat(3,minmax(140px,1fr)); gap:16px; margin:0 0 24px; }}
.quality-summary div {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:16px; box-shadow:0 1px 3px var(--shadow); }}
.quality-summary span {{ display:block; color:var(--muted); font-size:.75rem; font-weight:700; text-transform:uppercase; letter-spacing:.05em; margin-bottom:4px; }}
.quality-summary strong {{ display:block; color:var(--ink); font-size:1.5rem; line-height:1.2; }}
.quality-summary small {{ color:var(--muted); font-size:.875rem; }}
.range-performance {{ background:var(--panel); border:1px solid var(--line); border-radius:16px; box-shadow:0 1px 3px var(--shadow); padding:20px; margin:0 0 16px; }}
.performance-heading {{ display:flex; justify-content:space-between; align-items:flex-start; gap:16px; margin-bottom:16px; }}
.performance-heading span {{ display:block; color:var(--muted); font-size:.75rem; font-weight:700; text-transform:uppercase; letter-spacing:.05em; margin-bottom:4px; }}
.performance-heading h2 {{ color:var(--ink); font-size:1.25rem; line-height:1.2; font-weight:600; }}
.performance-heading p {{ color:var(--accent); font-size:.95rem; text-align:right; max-width:320px; }}
.performance-grid {{ display:grid; grid-template-columns:repeat(3,minmax(150px,1fr)); gap:12px; }}
.performance-card {{ background:var(--panel-strong); border:1px solid var(--line); border-radius:12px; padding:16px; }}
.performance-card.performance-leader {{ border-color:#86efac66; box-shadow:0 0 0 1px #86efac1f inset; }}
.performance-card span {{ display:block; color:var(--muted); font-size:.75rem; font-weight:700; text-transform:uppercase; letter-spacing:.05em; margin-bottom:4px; }}
.performance-card strong {{ display:block; font-size:1.7rem; line-height:1.15; margin-bottom:2px; }}
.performance-card small {{ display:block; color:var(--ink); font-size:.9rem; margin-bottom:8px; }}
.performance-card div {{ color:var(--muted); font-size:.875rem; }}
.range {{ display:none; }}
.range.on {{ display:block; }}
.pnl-bar {{ display:grid; grid-template-columns:repeat(5,minmax(110px,1fr)); gap:16px; margin-bottom:16px; }}
.pnl-bar div {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; box-shadow:0 1px 3px var(--shadow); padding:16px; min-height:86px; }}
.pnl-bar span {{ display:block; color:var(--muted); font-size:.75rem; font-weight:700; text-transform:uppercase; letter-spacing:.05em; margin-bottom:4px; }}
.pnl-bar strong {{ font-size:1.5rem; color:var(--ink); line-height:1.2; }}
.good {{ color:var(--good)!important; }}
.bad {{ color:var(--bad)!important; }}
.warn {{ color:var(--warn)!important; }}
.range-note {{ color:var(--muted); margin:8px 0 16px; font-size:.875rem; }}
.filter-bar {{ display:flex; gap:12px; align-items:center; background:var(--panel); border:1px solid var(--line); border-radius:12px; box-shadow:0 1px 3px var(--shadow); padding:12px 16px; margin-bottom:16px; }}
.filter-bar label {{ color:var(--muted); font-size:.75rem; font-weight:700; text-transform:uppercase; letter-spacing:.05em; }}
.filter-select {{ background:var(--panel-strong); border:1px solid var(--line); color:var(--ink); border-radius:9999px; padding:8px 14px; min-width:160px; font-family:inherit; }}
.filter-select:focus {{ border-color:var(--accent); outline:none; box-shadow:0 0 0 3px #a44b1a1a; }}
.filter-count {{ margin-left:auto; color:var(--muted); font-size:.875rem; }}
.filter-count span {{ color:var(--accent); font-weight:700; }}
.day-header {{ display:flex; justify-content:space-between; align-items:center; color:var(--ink); padding:14px 4px 8px; margin-top:18px; border-bottom:1px solid var(--line); font-size:1.05rem; font-weight:600; }}
.day-header small {{ color:var(--muted); font-size:.75rem; text-transform:uppercase; font-weight:700; letter-spacing:.05em; }}
.pick-card {{ background:var(--panel); border:1px solid var(--line); border-radius:16px; box-shadow:0 1px 3px var(--shadow); margin:16px 0; overflow:hidden; transition:box-shadow .2s,border-color .2s,background .2s; }}
.pick-card:hover {{ box-shadow:0 4px 12px var(--shadow); }}
.pick-card.strong {{ border-color:#22c55e33; }}
.pick-card.win {{ border-color:#22c55e55; background:#22c55e0f; }}
.pick-card.loss {{ border-color:#ef444455; background:#ef44440f; }}
.pick-card.push {{ border-color:#9ca3af55; }}
.pick-summary {{ display:flex; gap:16px; align-items:center; padding:18px 20px; cursor:pointer; }}
.pick-summary:hover {{ background:var(--panel-strong); }}
.rank {{ color:var(--accent); font-size:1.6rem; font-weight:700; min-width:42px; }}
.pick-main {{ flex:1; min-width:190px; }}
.pick-title {{ color:var(--ink); font-weight:600; font-size:1.125rem; }}
.market-pill {{ display:inline-block; margin-right:8px; border:1px solid var(--line); border-radius:9999px; padding:2px 8px; color:var(--accent); background:var(--panel-strong); font-size:.72rem; font-weight:700; vertical-align:middle; }}
.pick-meta {{ color:var(--muted); font-size:.875rem; margin-top:4px; }}
.badge {{ display:inline-block; margin-left:8px; border-radius:9999px; padding:4px 10px; font-size:.75rem; font-weight:600; vertical-align:middle; }}
.badge.strong {{ color:#86efac; background:#22c55e33; }}
.badge.keep {{ color:#93c5fd; background:#3b82f633; }}
.badge.caution {{ color:#fdba74; background:#f9731633; }}
.news-badge {{ display:inline-block; margin-left:6px; border-radius:9999px; padding:2px 8px; font-size:.65rem; font-weight:700; vertical-align:middle; }}
.news-badge.news-good {{ color:#86efac; background:#22c55e22; }}
.news-badge.news-bad {{ color:#fca5a5; background:#ef444422; }}
.news-badge.news-neut {{ color:#a8a090; background:#f5efe41a; }}
.news-block {{ margin-top:10px; padding:10px 12px; background:var(--panel-strong); border:1px solid var(--line); border-radius:10px; }}
.news-block small {{ display:block; color:var(--muted); line-height:1.6; }}
.news-block strong {{ color:var(--ink); }}
.numbers {{ display:flex; gap:10px; text-align:center; flex-wrap:wrap; justify-content:flex-end; }}
.numbers div {{ min-width:62px; background:var(--panel-strong); border-radius:12px; padding:8px 10px; }}
.numbers strong {{ display:block; font-size:1rem; color:var(--ink); }}
.numbers span {{ display:block; color:var(--muted); font-size:.72rem; text-transform:uppercase; letter-spacing:.05em; margin-top:2px; }}
.result {{ min-width:86px; text-align:center; color:var(--muted); font-size:.875rem; font-weight:700; }}
.actions {{ display:flex; gap:6px; flex-wrap:wrap; justify-content:flex-end; max-width:148px; }}
button {{ background:var(--panel-strong); border:1px solid var(--line); color:var(--ink); border-radius:9999px; padding:6px 10px; font-family:inherit; font-size:.75rem; font-weight:700; cursor:pointer; transition:all .2s; }}
button:hover {{ border-color:var(--accent); color:var(--accent); }}
button.win {{ color:var(--good); border-color:#22c55e33; }}
button.loss {{ color:var(--bad); border-color:#ef444433; }}
.pick-details {{ display:none; border-top:1px solid var(--line); padding:18px 20px 20px 78px; }}
.pick-details.open {{ display:block; }}
.reasoning {{ color:var(--muted); line-height:1.7; margin-bottom:12px; }}
.reasoning strong {{ color:var(--ink); }}
.metrics {{ display:flex; gap:18px; flex-wrap:wrap; color:var(--muted); }}
.metrics strong {{ color:var(--ink); }}
.hidden {{ display:none!important; }}
.empty {{ background:var(--panel); border:1px solid var(--line); border-radius:16px; padding:48px; color:var(--muted); text-align:center; }}
.history {{ background:var(--panel); border:1px solid var(--line); border-radius:16px; box-shadow:0 1px 3px var(--shadow); margin-top:24px; overflow:auto; }}
.history h2 {{ font-size:1.25rem; font-weight:600; padding:16px 20px; border-bottom:1px solid var(--line); }}
.history table {{ width:100%; border-collapse:collapse; min-width:780px; }}
.history th,.history td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; font-size:.875rem; color:var(--muted); }}
.history th {{ color:var(--muted); text-transform:uppercase; letter-spacing:.05em; font-size:.75rem; }}
.empty-history {{ padding:18px; color:var(--muted); }}
.parley-pick {{ border-color:#3b82f633; }}
.parley-legs {{ display:grid; gap:12px; margin-top:14px; }}
.parley-leg {{ background:var(--panel-strong); border:1px solid var(--line); border-radius:12px; padding:12px; }}
.parley-leg strong {{ color:var(--ink); }}
.parley-leg small {{ display:block; color:var(--muted); margin:4px 0 8px; }}
.h2h {{ margin-top:12px; margin-bottom:12px; }}
.h2h h3 {{ font-size:.75rem; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; margin-bottom:8px; }}
.h2h table {{ width:100%; border-collapse:collapse; font-size:.875rem; }}
.h2h th,.h2h td {{ padding:6px 8px; border-bottom:1px solid var(--line); color:var(--muted); text-align:left; }}
.h2h th {{ color:var(--muted); font-size:.75rem; text-transform:uppercase; }}
.recent-form {{ margin-top:12px; margin-bottom:12px; }}
.recent-form h3 {{ font-size:.75rem; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; margin-bottom:8px; }}
.form-row {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; }}
.form-row span {{ display:inline-flex; align-items:center; justify-content:center; width:24px; height:24px; border-radius:9999px; font-size:.75rem; font-weight:700; }}
.form-row .form-w {{ color:#86efac; background:#22c55e33; }}
.form-row .form-d {{ color:#fdba74; background:#f9731633; }}
.form-row .form-l {{ color:#fca5a5; background:#ef444433; }}
.form-row small {{ color:var(--muted); font-size:.75rem; margin-left:2px; }}
::-webkit-scrollbar {{ width:8px; height:8px; }}
::-webkit-scrollbar-track {{ background:var(--panel); }}
::-webkit-scrollbar-thumb {{ background:var(--muted); border-radius:4px; }}
::-webkit-scrollbar-thumb:hover {{ background:var(--accent); }}
@media (max-width:900px) {{
  .site-header {{ gap:12px; align-items:flex-start; flex-direction:column; }}
  .pnl-bar {{ grid-template-columns:repeat(2,minmax(120px,1fr)); gap:10px; }}
  .quality-summary {{ grid-template-columns:1fr; }}
  .performance-heading {{ flex-direction:column; }}
  .performance-heading p {{ max-width:none; text-align:left; }}
  .performance-grid {{ grid-template-columns:1fr; }}
  .pick-summary {{ align-items:flex-start; flex-direction:column; }}
  .numbers {{ width:100%; justify-content:space-between; }}
  .actions {{ width:100%; justify-content:flex-start; }}
  .pick-details {{ padding:14px 16px; }}
}}
@media (max-width:520px) {{
  .shell {{ padding:20px 12px 48px; }}
  .site-header {{ padding:20px 16px 0; }}
  .brand h1 {{ font-size:2rem; }}
  .tabs {{ margin-left:-12px; margin-right:-12px; padding-left:12px; }}
  .tab {{ flex:1; justify-content:center; }}
  .intro {{ padding:18px; }}
  .pnl-bar {{ grid-template-columns:1fr; }}
  .filter-bar {{ align-items:stretch; flex-direction:column; }}
  .filter-select {{ width:100%; }}
  .filter-count {{ margin-left:0; }}
  .numbers {{ justify-content:flex-start; }}
  .numbers div {{ flex:1 1 88px; }}
  .result {{ text-align:left; }}
}}
@media (min-width:640px) {{
  .shell {{ padding:24px; }}
  .site-header {{ padding:24px 24px 0; }}
  .brand h1 {{ font-size:2.5rem; }}
}}
@media (min-width:1024px) {{
  .shell {{ padding:24px 32px 64px; }}
  .site-header {{ padding:24px 32px 0; }}
}}
</style>
</head>
<body>
<header class="site-header">
  <div class="brand">
    <h1>RolloStake</h1>
    <p>Football edge intelligence &middot; {source_label} odds &middot; {total_picks} picks &middot; generated {html.escape(generated)} Macau time</p>
  </div>
</header>
<main class="shell">
  <div class="tabs">
    {tab_buttons}
  </div>
  <div class="intro">
    <strong>Polymarket risk-band workflow.</strong> All kickoff and played dates are shown in Macau time. High Risk keeps the bigger price band for carefully vetted upside; Low Risk keeps the tighter price band. Both use flat ${flat_stake:,.0f} staking, quality flags, correlated-exposure notes, and browser-side result settlement.
  </div>
  {range_sections}
</main>
<script>
const KEY = 'rollo-range-results-v1';
const PICKS = {json.dumps(js_picks)};
const BANK = {json.dumps(js_bank)};
const SETTLED = {json.dumps(js_settled)};
const ACTIVE_RANGES = {active_codes_json};
let state = {{}};
try {{
  state = JSON.parse(localStorage.getItem(KEY) || '{{}}');
}} catch (error) {{
  localStorage.removeItem(KEY);
}}

function switchRange(code) {{
  document.querySelectorAll('.tab').forEach(tab => {{
    tab.classList.toggle('on', tab.getAttribute('onclick').includes("'" + code + "'"));
  }});
  document.querySelectorAll('.range').forEach(range => range.classList.remove('on'));
  document.getElementById('range-' + code).classList.add('on');
}}

function toggleDetails(id) {{
  document.getElementById('details-' + id).classList.toggle('open');
}}

function currentStatus(pick) {{
  return (state[pick.id] || pick.status || 'pending').toLowerCase();
}}

function markPick(range, id, status) {{
  state[id] = status;
  localStorage.setItem(KEY, JSON.stringify(state));
  updateRange(range);
}}

function filterRange(range, date) {{
  const root = document.getElementById('range-' + range);
  let visible = 0;
  root.querySelectorAll('.pick-card').forEach(card => {{
    const show = date === 'all' || card.dataset.date === date;
    card.classList.toggle('hidden', !show);
    if (show) visible++;
  }});
  root.querySelectorAll('.day-header').forEach(header => {{
    header.classList.toggle('hidden', !(date === 'all' || header.dataset.date === date));
  }});
  document.getElementById(range + '-visible').textContent = visible;
}}

function updateRange(range) {{
  const settled = SETTLED[range] || {{ wins: 0, losses: 0, pushes: 0, pnl: 0, staked: 0 }};
  let wins = settled.wins || 0;
  let losses = settled.losses || 0;
  let pushes = settled.pushes || 0;
  let pnl = settled.pnl || 0;
  let staked = settled.staked || 0;
  PICKS[range].forEach(pick => {{
    const status = currentStatus(pick);
    const card = document.getElementById('pick-' + pick.id);
    card.classList.remove('win', 'loss', 'push');
    if (['win', 'loss', 'push'].includes(status)) card.classList.add(status);

    const result = document.getElementById('result-' + pick.id);
    if (status === 'win') {{
      wins++;
      staked += pick.stake;
      const profit = Math.round(pick.stake * (pick.odds - 1));
      pnl += profit;
      result.innerHTML = '<span class="good">+$' + profit.toLocaleString() + '</span>';
    }} else if (status === 'loss') {{
      losses++;
      staked += pick.stake;
      pnl -= pick.stake;
      result.innerHTML = '<span class="bad">-$' + pick.stake.toLocaleString() + '</span>';
    }} else if (status === 'push') {{
      pushes++;
      result.innerHTML = '<span>PUSH</span>';
    }} else {{
      result.textContent = 'pending';
    }}
  }});

  const record = wins + losses + pushes === 0 ? '-' : wins + 'W-' + losses + 'L' + (pushes ? '-' + pushes + 'P' : '');
  document.getElementById(range + '-record').textContent = record;
  document.getElementById(range + '-staked').textContent = '$' + staked.toLocaleString();

  const pnlEl = document.getElementById(range + '-pnl');
  pnlEl.textContent = (pnl >= 0 ? '+$' : '-$') + Math.abs(pnl).toLocaleString();
  pnlEl.className = pnl >= 0 ? 'good' : 'bad';

  const roiEl = document.getElementById(range + '-roi');
  if (staked > 0) {{
    const roi = Math.round((pnl / staked) * 1000) / 10;
    roiEl.textContent = (roi >= 0 ? '+' : '') + roi + '%';
    roiEl.className = roi >= 0 ? 'good' : 'bad';
  }} else {{
    roiEl.textContent = '-';
    roiEl.className = '';
  }}

  const bankEl = document.getElementById(range + '-bank');
  const bank = BANK[range] + pnl;
  bankEl.textContent = '$' + bank.toLocaleString();
  bankEl.className = pnl >= 0 ? 'good' : 'bad';
}}

ACTIVE_RANGES.forEach(updateRange);
</script>
</body>
</html>
"""

        html_doc = "\n".join(line.rstrip() for line in html_doc.splitlines()) + "\n"

        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write(html_doc)

        print(f"Dashboard generated: {self.output_file}")
        return str(self.output_file)


if __name__ == "__main__":
    gen = DashboardGenerator()
    gen.generate()
