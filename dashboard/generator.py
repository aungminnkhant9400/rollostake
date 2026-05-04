"""Static Range C/D dashboard generator."""

import html
import json
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.edge_calculator import EdgeCalculator
from config.paths import DASHBOARD_DIR, DB_PATH, ensure_runtime_dirs
from config.settings import load_settings


class DashboardGenerator:
    """Generates a static HTML dashboard for the Range C/D workflow."""

    def __init__(self):
        ensure_runtime_dirs()
        self.output_file = DASHBOARD_DIR / "index.html"
        self.settings = load_settings()
        self.range_configs = EdgeCalculator.range_configs_from_settings(self.settings)

    def get_picks(self) -> List[Dict]:
        """Fetch picks with match details."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute(
            """
            SELECT p.*, m.home_team, m.away_team, m.league, m.kickoff,
                   m.home_fatigue_score, m.away_fatigue_score, m.fatigue_advantage
            FROM picks p
            JOIN matches m ON p.match_id = m.match_id
            WHERE p.status IN ('pending', 'settled')
            ORDER BY COALESCE(p.range_code, 'D'), m.kickoff, p.edge_pct DESC
            """
        )

        picks = [dict(row) for row in c.fetchall()]
        conn.close()
        return picks

    def get_results_history(self) -> List[Dict]:
        """Fetch settled results for the cumulative history table."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT r.*, m.home_team, m.away_team, p.selection
            FROM results r
            LEFT JOIN matches m ON r.match_id = m.match_id
            LEFT JOIN picks p ON r.pick_id = p.id
            ORDER BY r.settled_at, r.id
            """
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()
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

    def _history_table(self, results: List[Dict]) -> str:
        if not results:
            return '<div class="history empty-history">No settled picks yet.</div>'

        running = 0.0
        rows = []
        for result in results:
            running += float(result.get("pnl") or 0)
            match = html.escape(f"{result.get('home_team') or ''} vs {result.get('away_team') or ''}".strip())
            selection = html.escape(str(result.get("selection") or ""))
            rows.append(
                "<tr>"
                f"<td>{html.escape(str(result.get('settled_at') or ''))}</td>"
                f"<td>{html.escape(str(result.get('range_code') or ''))}</td>"
                f"<td>{html.escape(str(result.get('quality') or ''))}</td>"
                f"<td>{match}</td>"
                f"<td>{selection}</td>"
                f"<td>{html.escape(str(result.get('result') or ''))}</td>"
                f"<td>${float(result.get('pnl') or 0):+,.0f}</td>"
                f"<td>${running:+,.0f}</td>"
                "</tr>"
            )

        return (
            '<div class="history"><h2>Settled History</h2><table><thead><tr>'
            '<th>Settled</th><th>Range</th><th>Quality</th><th>Match</th>'
            '<th>Pick</th><th>Result</th><th>P&L</th><th>Cumulative</th>'
            '</tr></thead><tbody>'
            + ''.join(rows[-20:])
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

    def _date_key(self, kickoff: str) -> str:
        return (kickoff or "TBD")[:10]

    def _date_label(self, kickoff: str) -> str:
        if not kickoff:
            return "TBD"
        raw = kickoff[:10]
        try:
            return datetime.strptime(raw, "%Y-%m-%d").strftime("%a %b %d")
        except ValueError:
            return raw

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
        for pick in picks:
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

    def _render_pick(self, code: str, pick: Dict, index: int) -> str:
        quality = pick.get("quality") or "KEEP"
        quality_class = quality.lower()
        date_key = self._date_key(pick.get("kickoff"))
        kickoff = html.escape(str(pick.get("kickoff") or "TBD"))
        selection = html.escape(str(pick.get("selection") or ""))
        matchup = html.escape(f"{pick.get('home_team')} vs {pick.get('away_team')}")
        league = html.escape(str(pick.get("league") or ""))
        pick_id = int(pick["id"])
        win_profit = round(float(pick["stake"]) * (float(pick["odds"]) - 1))
        reasoning = self._pick_reasoning(pick)
        
        # Get H2H and form
        h2h_html = self._get_h2h(pick.get('home_team', ''), pick.get('away_team', ''))
        home_form = self._get_recent_form(pick.get('home_team', ''))
        away_form = self._get_recent_form(pick.get('away_team', ''))

        return f"""
<div class="pick-card {quality_class}" id="pick-{pick_id}" data-date="{html.escape(date_key)}">
  <div class="pick-summary" onclick="toggleDetails({pick_id})">
    <div class="rank">#{index}</div>
    <div class="pick-main">
      <div class="pick-title">{selection} <span class="badge {quality_class}">{self._quality_label(quality)}</span></div>
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
  </div>
</div>
"""

    def _render_range(self, code: str, picks: List[Dict], active: bool) -> str:
        config = self.range_configs[code]
        grouped = self._group_by_date(picks)
        strong = sum(1 for p in picks if p.get("quality") == "STRONG")
        caution = sum(1 for p in picks if p.get("quality") == "CAUTION")
        total_stake = sum(float(p["stake"]) for p in picks)
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
            cards.append('<div class="empty">No picks generated for this range yet.</div>')

        return f"""
<section class="range {'on' if active else ''}" id="range-{code}">
  <div class="pnl-bar">
    <div><span>Base Bank</span><strong>${config.bankroll:,.0f}</strong></div>
    <div><span>Flat Stake</span><strong>${config.flat_stake:,.0f}</strong></div>
    <div><span>Odds Band</span><strong>{config.min_odds:.2f}-{config.max_odds:.2f}</strong></div>
    <div><span>Strong</span><strong class="good">{strong}</strong></div>
    <div><span>Caution</span><strong class="warn">{caution}</strong></div>
    <div><span>Record</span><strong id="{code}-record">-</strong></div>
    <div><span>Staked</span><strong id="{code}-staked">$0</strong></div>
    <div><span>P&amp;L</span><strong id="{code}-pnl">$0</strong></div>
    <div><span>ROI</span><strong id="{code}-roi">-</strong></div>
    <div><span>Bank</span><strong id="{code}-bank">${config.bankroll:,.0f}</strong></div>
  </div>
  <div class="range-note">{len(picks)} picks · planned stake ${total_stake:,.0f} · correlated same-match exposure is flagged in each pick's risk note.</div>
  {self._render_filter(code, picks)}
  {''.join(cards)}
</section>
"""

    def generate(self):
        """Generate the dashboard and return the output path."""
        picks = self.get_picks()
        results_history = self.get_results_history()
        by_range = {
            "C": [p for p in picks if (p.get("range_code") or "D").upper() == "C"],
            "D": [p for p in picks if (p.get("range_code") or "D").upper() == "D"],
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
        js_bank = {code: self.range_configs[code].bankroll for code in ("C", "D")}

        generated = datetime.now().strftime("%A, %B %d, %Y at %H:%M")
        total_picks = len(picks)

        html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Rollo Stake Model - Range C/D</title>
<style>
:root {{
  --background:#f5efe4;
  --panel:#fffaf2;
  --panel-strong:#f1e4d0;
  --ink:#20201c;
  --muted:#68624e;
  --accent:#a44b1a;
  --accent-soft:#d77b46;
  --line:#20201c1a;
  --shadow:#0000000d;
  --good:#15803d;
  --bad:#b91c1c;
  --warn:#b45309;
  --blue:#2563eb;
}}
[data-theme=dark] {{
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
.theme-toggle {{ border:1px solid var(--line); background:var(--panel); color:var(--ink); cursor:pointer; border-radius:9999px; align-items:center; gap:8px; padding:8px 16px; font-family:inherit; font-size:.875rem; transition:all .2s; display:flex; }}
.theme-toggle:hover {{ background:var(--panel-strong); border-color:var(--accent); }}
.theme-icon {{ width:16px; height:16px; display:inline-block; text-align:center; }}
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
.pick-meta {{ color:var(--muted); font-size:.875rem; margin-top:4px; }}
.badge {{ display:inline-block; margin-left:8px; border-radius:9999px; padding:4px 10px; font-size:.75rem; font-weight:600; vertical-align:middle; }}
.badge.strong {{ color:#166534; background:#dcfce7; }}
.badge.keep {{ color:#1d4ed8; background:#dbeafe; }}
.badge.caution {{ color:#9a3412; background:#ffedd5; }}
[data-theme=dark] .badge.strong {{ color:#86efac; background:#22c55e33; }}
[data-theme=dark] .badge.keep {{ color:#93c5fd; background:#3b82f633; }}
[data-theme=dark] .badge.caution {{ color:#fdba74; background:#f9731633; }}
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
.h2h {{ margin-top:12px; margin-bottom:12px; }}
.h2h h3 {{ font-size:.75rem; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; margin-bottom:8px; }}
.h2h table {{ width:100%; border-collapse:collapse; font-size:.875rem; }}
.h2h th,.h2h td {{ padding:6px 8px; border-bottom:1px solid var(--line); color:var(--muted); text-align:left; }}
.h2h th {{ color:var(--muted); font-size:.75rem; text-transform:uppercase; }}
.recent-form {{ margin-top:12px; margin-bottom:12px; }}
.recent-form h3 {{ font-size:.75rem; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; margin-bottom:8px; }}
.form-row {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; }}
.form-row span {{ display:inline-flex; align-items:center; justify-content:center; width:24px; height:24px; border-radius:9999px; font-size:.75rem; font-weight:700; }}
.form-row .form-w {{ color:#166534; background:#dcfce7; }}
.form-row .form-d {{ color:#9a3412; background:#ffedd5; }}
.form-row .form-l {{ color:#991b1b; background:#fee2e2; }}
[data-theme=dark] .form-row .form-w {{ color:#86efac; background:#22c55e33; }}
[data-theme=dark] .form-row .form-d {{ color:#fdba74; background:#f9731633; }}
[data-theme=dark] .form-row .form-l {{ color:#fca5a5; background:#ef444433; }}
.form-row small {{ color:var(--muted); font-size:.75rem; margin-left:2px; }}
::-webkit-scrollbar {{ width:8px; height:8px; }}
::-webkit-scrollbar-track {{ background:var(--panel); }}
::-webkit-scrollbar-thumb {{ background:var(--muted); border-radius:4px; }}
::-webkit-scrollbar-thumb:hover {{ background:var(--accent); }}
@media (max-width:900px) {{
  .site-header {{ gap:12px; align-items:flex-start; flex-direction:column; }}
  .pnl-bar {{ grid-template-columns:repeat(2,minmax(120px,1fr)); gap:10px; }}
  .quality-summary {{ grid-template-columns:1fr; }}
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
    <p>Football edge intelligence &middot; {total_picks} picks &middot; generated {html.escape(generated)}</p>
  </div>
  <button class="theme-toggle" type="button" onclick="toggleTheme()" aria-label="Toggle theme">
    <svg class="theme-icon" aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="12" cy="12" r="4"></circle>
      <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"></path>
    </svg>
    <span id="theme-label">Dark</span>
  </button>
</header>
<main class="shell">
  <div class="tabs">
    <button class="tab on" onclick="switchRange('C')">Range C</button>
    <button class="tab" onclick="switchRange('D')">Range D</button>
  </div>
  <div class="intro">
    <strong>Range C/D workflow.</strong> Same betting concept: odds bands, flat $200 staking, quality flags, correlated-exposure notes, and browser-side result settlement. The presentation now follows the RolloForge visual system instead of your friend's dashboard skin.
  </div>
  {self._quality_summary(picks)}
  {self._render_range('C', by_range['C'], True)}
  {self._render_range('D', by_range['D'], False)}
  {self._history_table(results_history)}
</main>
<script>
const KEY = 'rollo-range-results-v1';
const THEME_KEY = 'rollo-dashboard-theme';
const PICKS = {json.dumps(js_picks)};
const BANK = {json.dumps(js_bank)};
let state = JSON.parse(localStorage.getItem(KEY) || '{{}}');

function applyTheme(theme) {{
  document.documentElement.dataset.theme = theme;
  document.getElementById('theme-label').textContent = theme === 'dark' ? 'Dark' : 'Light';
}}

function toggleTheme() {{
  const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  localStorage.setItem(THEME_KEY, next);
  applyTheme(next);
}}

function switchRange(code) {{
  document.querySelectorAll('.tab').forEach((tab, index) => {{
    tab.classList.toggle('on', ['C', 'D'][index] === code);
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
  let wins = 0, losses = 0, pushes = 0, pnl = 0, staked = 0;
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

applyTheme(localStorage.getItem(THEME_KEY) || 'dark');
['C', 'D'].forEach(updateRange);
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
