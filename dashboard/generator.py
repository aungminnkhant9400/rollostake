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
  --bg:#0a0a0a; --panel:#141414; --panel2:#1f1f1f; --border:#2d2d2d;
  --text:#e5e5e5; --muted:#8a8a8a; --dim:#585858; --accent:#e07a3a;
  --good:#22c55e; --bad:#ef4444; --warn:#fbbf24; --blue:#60a5fa;
}}
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ background:var(--bg); color:var(--text); font-family:Inter,Segoe UI,Arial,sans-serif; font-size:13px; }}
header {{ position:sticky; top:0; z-index:10; background:rgba(10,10,10,.96); border-bottom:1px solid var(--border); padding:0 24px; height:58px; display:flex; align-items:center; justify-content:space-between; }}
.logo {{ font-weight:900; letter-spacing:.02em; }}
.logo em {{ color:var(--accent); font-style:normal; }}
.header-meta {{ color:var(--muted); font-size:11px; }}
.shell {{ max-width:1120px; margin:0 auto; padding:18px 20px 64px; }}
.tabs {{ display:flex; gap:6px; margin-bottom:14px; }}
.tab {{ background:var(--panel); border:1px solid var(--border); color:var(--muted); border-radius:8px; padding:10px 24px; font-size:11px; font-weight:800; letter-spacing:.08em; cursor:pointer; }}
.tab.on {{ color:var(--accent); border-color:rgba(224,122,58,.45); background:rgba(224,122,58,.08); }}
.intro {{ background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:15px 18px; line-height:1.65; margin-bottom:14px; color:#cfcfcf; }}
.intro strong {{ color:#fff; }}
.quality-summary {{ display:grid; grid-template-columns:repeat(3,minmax(140px,1fr)); gap:8px; margin:0 0 14px; }}
.quality-summary div {{ background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:12px 14px; }}
.quality-summary span {{ display:block; color:var(--dim); font-size:9px; font-weight:900; text-transform:uppercase; letter-spacing:.08em; margin-bottom:4px; }}
.quality-summary strong {{ display:block; color:#fff; font-size:16px; }}
.quality-summary small {{ color:var(--muted); font-size:11px; }}
.range {{ display:none; }}
.range.on {{ display:block; }}
.pnl-bar {{ display:grid; grid-template-columns:repeat(10,minmax(82px,1fr)); border:1px solid var(--border); border-radius:8px; overflow:hidden; margin-bottom:10px; }}
.pnl-bar div {{ background:var(--panel); border-right:1px solid var(--border); padding:11px 10px; text-align:center; min-height:58px; }}
.pnl-bar div:last-child {{ border-right:0; }}
.pnl-bar span {{ display:block; color:var(--dim); font-size:9px; font-weight:800; text-transform:uppercase; letter-spacing:.08em; margin-bottom:5px; }}
.pnl-bar strong {{ font-size:15px; color:#fff; }}
.good {{ color:var(--good)!important; }}
.bad {{ color:var(--bad)!important; }}
.warn {{ color:var(--warn)!important; }}
.range-note {{ color:var(--muted); margin:10px 0 14px; font-size:12px; }}
.filter-bar {{ display:flex; gap:12px; align-items:center; background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:10px 14px; margin-bottom:12px; }}
.filter-bar label {{ color:var(--dim); font-size:10px; font-weight:900; text-transform:uppercase; letter-spacing:.08em; }}
.filter-select {{ background:var(--panel2); border:1px solid var(--border); color:var(--text); border-radius:6px; padding:7px 12px; min-width:160px; }}
.filter-count {{ margin-left:auto; color:var(--muted); font-size:11px; }}
.filter-count span {{ color:var(--accent); font-weight:900; }}
.day-header {{ display:flex; justify-content:space-between; align-items:center; border-left:3px solid var(--accent); background:linear-gradient(180deg,rgba(224,122,58,.07),transparent); padding:10px 14px; margin:18px 0 7px; border-radius:4px; }}
.day-header small {{ color:var(--dim); text-transform:uppercase; font-weight:800; letter-spacing:.08em; }}
.pick-card {{ background:var(--panel); border:1px solid var(--border); border-radius:8px; margin-bottom:8px; overflow:hidden; }}
.pick-card.strong {{ border-color:rgba(34,197,94,.45); box-shadow:0 0 16px rgba(34,197,94,.06); }}
.pick-card.win {{ border-color:rgba(34,197,94,.55); background:rgba(34,197,94,.03); }}
.pick-card.loss {{ border-color:rgba(239,68,68,.55); background:rgba(239,68,68,.03); }}
.pick-card.push {{ border-color:rgba(148,163,184,.45); }}
.pick-summary {{ display:flex; gap:14px; align-items:center; padding:13px 16px; cursor:pointer; }}
.pick-summary:hover {{ background:var(--panel2); }}
.rank {{ color:var(--accent); font-size:22px; font-weight:900; min-width:40px; }}
.pick-main {{ flex:1; min-width:190px; }}
.pick-title {{ color:#fff; font-weight:800; font-size:15px; }}
.pick-meta {{ color:var(--muted); font-size:11px; margin-top:3px; }}
.badge {{ display:inline-block; margin-left:6px; border-radius:4px; padding:2px 6px; font-size:9px; font-weight:900; letter-spacing:.05em; }}
.badge.strong {{ color:var(--good); background:rgba(34,197,94,.12); }}
.badge.keep {{ color:var(--blue); background:rgba(96,165,250,.12); }}
.badge.caution {{ color:var(--warn); background:rgba(251,191,36,.12); }}
.numbers {{ display:flex; gap:14px; text-align:center; }}
.numbers div {{ min-width:54px; }}
.numbers strong {{ display:block; font-size:14px; color:#fff; }}
.numbers span {{ display:block; color:var(--dim); font-size:8px; text-transform:uppercase; letter-spacing:.08em; margin-top:2px; }}
.result {{ min-width:78px; text-align:center; color:var(--muted); font-size:11px; font-weight:800; }}
.actions {{ display:flex; gap:4px; flex-wrap:wrap; justify-content:flex-end; }}
button {{ background:var(--panel2); border:1px solid var(--border); color:var(--muted); border-radius:6px; padding:6px 10px; font-size:9px; font-weight:900; cursor:pointer; }}
button.win {{ color:var(--good); border-color:rgba(34,197,94,.35); }}
button.loss {{ color:var(--bad); border-color:rgba(239,68,68,.35); }}
.pick-details {{ display:none; border-top:1px solid var(--border); padding:15px 20px 16px 70px; }}
.pick-details.open {{ display:block; }}
.reasoning {{ color:#cfcfcf; line-height:1.7; margin-bottom:12px; }}
.reasoning strong {{ color:#fff; }}
.metrics {{ display:flex; gap:18px; flex-wrap:wrap; color:var(--muted); }}
.metrics strong {{ color:#fff; }}
.hidden {{ display:none!important; }}
.empty {{ background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:28px; color:var(--muted); text-align:center; }}
.history {{ background:var(--panel); border:1px solid var(--border); border-radius:8px; margin-top:18px; overflow:auto; }}
.history h2 {{ font-size:13px; padding:14px 16px; border-bottom:1px solid var(--border); }}
.history table {{ width:100%; border-collapse:collapse; min-width:780px; }}
.history th,.history td {{ padding:10px 12px; border-bottom:1px solid var(--border); text-align:left; font-size:11px; color:var(--muted); }}
.history th {{ color:var(--dim); text-transform:uppercase; letter-spacing:.08em; font-size:9px; }}
.empty-history {{ padding:18px; color:var(--muted); }}
.h2h {{ margin-top:12px; margin-bottom:12px; }}
.h2h h3 {{ font-size:11px; color:var(--dim); text-transform:uppercase; letter-spacing:.08em; margin-bottom:8px; }}
.h2h table {{ width:100%; border-collapse:collapse; font-size:11px; }}
.h2h th,.h2h td {{ padding:6px 8px; border-bottom:1px solid var(--border); color:var(--muted); text-align:left; }}
.h2h th {{ color:var(--dim); font-size:9px; text-transform:uppercase; }}
.recent-form {{ margin-top:12px; margin-bottom:12px; }}
.recent-form h3 {{ font-size:11px; color:var(--dim); text-transform:uppercase; letter-spacing:.08em; margin-bottom:8px; }}
.form-row {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; }}
.form-row span {{ display:inline-flex; align-items:center; justify-content:center; width:24px; height:24px; border-radius:4px; font-size:11px; font-weight:800; }}
.form-row .form-w {{ color:var(--good); background:rgba(34,197,94,.15); }}
.form-row .form-d {{ color:var(--warn); background:rgba(251,191,36,.15); }}
.form-row .form-l {{ color:var(--bad); background:rgba(239,68,68,.15); }}
.form-row small {{ color:var(--muted); font-size:10px; margin-left:2px; }}
@media (max-width:900px) {{
  header {{ height:auto; gap:8px; align-items:flex-start; flex-direction:column; padding:12px 16px; }}
  .pnl-bar {{ grid-template-columns:repeat(2,minmax(120px,1fr)); }}
  .quality-summary {{ grid-template-columns:1fr; }}
  .pick-summary {{ align-items:flex-start; flex-direction:column; }}
  .numbers {{ width:100%; justify-content:space-between; }}
  .actions {{ width:100%; justify-content:flex-start; }}
  .pick-details {{ padding:14px 16px; }}
}}
</style>
</head>
<body>
<header>
  <div class="logo">ROLLO STAKE MODEL <em>Range C/D</em></div>
  <div class="header-meta">{total_picks} picks · generated {html.escape(generated)}</div>
</header>
<main class="shell">
  <div class="tabs">
    <button class="tab on" onclick="switchRange('C')">RANGE C</button>
    <button class="tab" onclick="switchRange('D')">RANGE D</button>
  </div>
  <div class="intro">
    <strong>Range C/D mode.</strong> Picks are split by odds band, use flat $200 staking, and flag same-match correlated exposure. Result buttons update this static dashboard in your browser local storage; database settlement can still be handled from the Python CLI.
  </div>
  {self._quality_summary(picks)}
  {self._render_range('C', by_range['C'], True)}
  {self._render_range('D', by_range['D'], False)}
  {self._history_table(results_history)}
</main>
<script>
const KEY = 'rollo-range-results-v1';
const PICKS = {json.dumps(js_picks)};
const BANK = {json.dumps(js_bank)};
let state = JSON.parse(localStorage.getItem(KEY) || '{{}}');

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
