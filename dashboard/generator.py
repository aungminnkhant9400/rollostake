"""
RolloForge Theme Dashboard Generator
Dark theme with detailed expandable cards showing WHY each pick was chosen.
"""

import sqlite3
from typing import List, Dict
from datetime import datetime
import os

DB_PATH = '/home/ubuntu/rollo-stake-model/data/rollo_stake.db'
OUTPUT_DIR = '/home/ubuntu/rollo-stake-model/dashboard'

class DashboardGenerator:
    """Generates HTML dashboard with detailed reasoning on card click."""
    
    def __init__(self):
        self.output_file = os.path.join(OUTPUT_DIR, 'index.html')
    
    def get_picks(self, status: str = 'pending') -> List[Dict]:
        """Fetch picks with match details."""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute('''
            SELECT p.*, m.home_team, m.away_team, m.league, m.kickoff,
                   m.home_fatigue_score, m.away_fatigue_score, m.fatigue_advantage
            FROM picks p
            JOIN matches m ON p.match_id = m.match_id
            WHERE p.status = ?
            ORDER BY p.edge_pct DESC
        ''', (status,))
        
        picks = [dict(row) for row in c.fetchall()]
        conn.close()
        
        return picks
    
    def generate_reasoning(self, pick: Dict) -> str:
        """Generate detailed reasoning for why this pick was chosen."""
        reasons = []
        
        # Edge explanation
        edge = pick['edge_pct']
        if edge >= 50:
            reasons.append(f"<strong>Massive value:</strong> Model sees {pick['model_prob']:.1%} chance but bookmaker only prices it at {pick['book_prob']:.1%} — that's a <strong>+{edge:.1f}% edge</strong>, which is exceptional.")
        elif edge >= 25:
            reasons.append(f"<strong>Strong value:</strong> Model gives {pick['model_prob']:.1%} vs bookmaker's {pick['book_prob']:.1%}, creating a <strong>+{edge:.1f}% edge</strong>.")
        elif edge >= 10:
            reasons.append(f"<strong>Solid value:</strong> Model probability ({pick['model_prob']:.1%}) exceeds bookmaker implied ({pick['book_prob']:.1%}) by <strong>+{edge:.1f}%</strong>.")
        else:
            reasons.append(f"<strong>Moderate value:</strong> Edge of +{edge:.1f}% exists between model ({pick['model_prob']:.1%}) and bookmaker ({pick['book_prob']:.1%}).")
        
        # Market context
        market = pick['market']
        if market == '1X2':
            if 'Win' in pick['selection'] and pick['home_team'] in pick['selection']:
                reasons.append("<strong>Home advantage:</strong> Home teams historically win ~45% of matches, and this team benefits from familiar conditions.")
            elif 'Win' in pick['selection'] and pick['away_team'] in pick['selection']:
                reasons.append("<strong>Away value:</strong> Bookmakers often overprice home teams. Model identifies this away team as undervalued.")
            elif pick['selection'] == 'Draw':
                reasons.append("<strong>Draw value:</strong> Draws are typically overpriced by bookmakers. Model sees this as a likely outcome based on evenly matched teams.")
        elif market == 'OU':
            if 'Under' in pick['selection']:
                reasons.append("<strong>Low scoring expected:</strong> Model predicts fewer goals than the bookmaker threshold. Both teams show defensive tendencies or attacking struggles in recent data.")
            else:
                reasons.append("<strong>High scoring expected:</strong> Model predicts more goals than the bookmaker line. Both teams have shown attacking prowess historically.")
        elif market == 'BTTS':
            reasons.append("<strong>Both teams attacking:</strong> Model expects both sides to find the net based on their offensive output vs defensive records.")
        
        # Fatigue factor
        home_fatigue = pick.get('home_fatigue_score')
        away_fatigue = pick.get('away_fatigue_score')
        fatigue_adv = pick.get('fatigue_advantage')
        
        if fatigue_adv and fatigue_adv != 'even':
            if 'home' in fatigue_adv and pick['home_team'] in pick['selection']:
                reasons.append(f"<strong>Fatigue advantage:</strong> {pick['home_team']} is fresher (fatigue score: {home_fatigue}) compared to {pick['away_team']} ({away_fatigue}), supporting this pick.")
            elif 'away' in fatigue_adv and pick['away_team'] in pick['selection']:
                reasons.append(f"<strong>Fatigue advantage:</strong> {pick['away_team']} is fresher (fatigue score: {away_fatigue}) compared to {pick['home_team']} ({home_fatigue}), supporting this pick.")
            elif 'home' in fatigue_adv:
                reasons.append(f"<strong>Note:</strong> {pick['home_team']} is fresher, but model still finds value in this pick despite that.")
            elif 'away' in fatigue_adv:
                reasons.append(f"<strong>Note:</strong> {pick['away_team']} is fresher, but model still finds value in this pick despite that.")
        
        # Kelly staking reason
        stake = pick['stake']
        if stake >= 100:
            reasons.append(f"<strong>Kelly stake ${stake:.0f}:</strong> High confidence warrants larger position (15% Kelly criterion applied).")
        elif stake >= 50:
            reasons.append(f"<strong>Kelly stake ${stake:.0f}:</strong> Moderate position size based on edge size and bankroll.")
        else:
            reasons.append(f"<strong>Kelly stake ${stake:.0f}:</strong> Conservative position due to lower edge or higher odds variance.")
        
        return "<br><br>".join(reasons)
    
    def generate(self):
        """Generate HTML dashboard with detailed reasoning."""
        picks = self.get_picks()
        bankroll = 1000.0
        
        strong_count = sum(1 for p in picks if p['quality'] == 'STRONG')
        keep_count = sum(1 for p in picks if p['quality'] == 'KEEP')
        caution_count = sum(1 for p in picks if p['quality'] == 'CAUTION')
        total_stake = sum(p['stake'] for p in picks)
        
        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rollo Stake Model</title>
<style>
:root {{
    --bg: #1a1a18;
    --panel: #2a2a26;
    --panel-hover: #353530;
    --panel-deep: #1e1e1c;
    --ink: #f5efe4;
    --muted: #a8a090;
    --accent: #d77b46;
    --line: rgba(245, 239, 228, 0.1);
    --green: #22c55e;
    --green-dim: rgba(34, 197, 94, 0.15);
    --orange: #f97316;
    --orange-dim: rgba(249, 115, 22, 0.15);
    --yellow: #eab308;
    --yellow-dim: rgba(234, 179, 8, 0.15);
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{
    font-family: Georgia, 'Times New Roman', serif;
    background: var(--bg);
    color: var(--ink);
    line-height: 1.6;
    min-height: 100vh;
}}
.container{{
    max-width: 1100px;
    margin: 0 auto;
    padding: 24px 16px;
}}

/* Header */
header{{
    margin-bottom: 24px;
    padding-bottom: 16px;
    border-bottom: 1px solid var(--line);
}}
h1{{
    font-size: 1.75rem;
    font-weight: 700;
    color: var(--ink);
}}
h1 em{{
    color: var(--accent);
    font-style: normal;
}}
.sub{{
    color: var(--muted);
    font-size: 0.875rem;
    margin-top: 4px;
}}
.date{{
    color: var(--muted);
    font-size: 0.75rem;
    margin-top: 8px;
}}

/* Stats Bar */
.stats-bar{{
    display: flex;
    gap: 12px;
    margin-bottom: 24px;
    flex-wrap: wrap;
}}
.stat-pill{{
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 12px 16px;
    min-width: 90px;
    text-align: center;
}}
.stat-pill.strong{{border-color: var(--green); background: var(--green-dim);}}
.stat-pill.keep{{border-color: var(--orange); background: var(--orange-dim);}}
.stat-pill.caution{{border-color: var(--yellow); background: var(--yellow-dim);}}
.stat-label{{
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--muted);
    margin-bottom: 4px;
}}
.stat-val{{
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--ink);
}}
.stat-val.accent{{color: var(--accent);}}
.stat-val.green{{color: var(--green);}}

/* Date Section */
.date-section{{
    margin-bottom: 20px;
    padding: 12px 16px;
    background: var(--panel);
    border-radius: 10px;
    border-left: 3px solid var(--accent);
}}
.date-title{{
    font-size: 1rem;
    font-weight: 600;
    color: var(--accent);
}}
.date-sub{{
    font-size: 0.8rem;
    color: var(--muted);
    margin-top: 2px;
}}

/* Pick Cards */
.pick-card{{
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 12px;
    margin-bottom: 10px;
    overflow: hidden;
    cursor: pointer;
    transition: all 0.2s;
}}
.pick-card:hover{{
    background: var(--panel-hover);
    border-color: var(--accent);
}}
.pick-card.strong{{border-left: 3px solid var(--green);}}
.pick-card.keep{{border-left: 3px solid var(--orange);}}
.pick-card.caution{{border-left: 3px solid var(--yellow);}}

.pick-summary{{
    padding: 14px 18px;
    display: flex;
    align-items: center;
    gap: 14px;
    flex-wrap: wrap;
}}
.pick-rank{{
    font-size: 1.25rem;
    font-weight: 800;
    color: var(--accent);
    min-width: 30px;
}}
.pick-main{{
    flex: 1;
    min-width: 200px;
}}
.pick-title{{
    font-size: 1rem;
    font-weight: 600;
    color: var(--ink);
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
}}
.quality-badge{{
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 0.65rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}
.quality-badge.strong{{background: var(--green-dim); color: var(--green);}}
.quality-badge.keep{{background: var(--orange-dim); color: var(--orange);}}
.quality-badge.caution{{background: var(--yellow-dim); color: var(--yellow);}}
.pick-meta{{
    font-size: 0.8rem;
    color: var(--muted);
    margin-top: 3px;
}}
.pick-meta .kickoff{{color: var(--accent); font-weight: 600;}}

.pick-nums{{
    display: flex;
    gap: 16px;
}}
.num{{
    text-align: center;
}}
.num-val{{
    font-size: 0.95rem;
    font-weight: 700;
    color: var(--ink);
}}
.num-val.accent{{color: var(--accent);}}
.num-val.green{{color: var(--green);}}
.num-label{{
    font-size: 0.6rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
}}

/* Expanded Details */
.pick-details{{
    display: none;
    padding: 0 18px 16px 62px;
    border-top: 1px solid var(--line);
}}
.pick-details.open{{
    display: block;
}}

/* Reasoning Section */
.reasoning-box{{
    background: var(--panel-deep);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 14px 16px;
    margin-top: 10px;
}}
.reasoning-title{{
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--accent);
    margin-bottom: 10px;
}}
.reasoning-text{{
    font-size: 0.85rem;
    color: var(--muted);
    line-height: 1.7;
}}
.reasoning-text strong{{
    color: var(--ink);
    font-weight: 600;
}}

/* Metrics Grid */
.metrics-grid{{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
    gap: 10px;
    margin-bottom: 12px;
}}
.metric-box{{
    background: rgba(0,0,0,0.2);
    padding: 10px 12px;
    border-radius: 8px;
}}
.metric-label{{
    font-size: 0.6rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}
.metric-value{{
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--ink);
    margin-top: 2px;
}}
</style>
</head>
<body>
<div class="container">
<header>
<h1>Rollo Stake Model <em>v1.0</em></h1>
<div class="sub">Value betting with Dixon-Coles modeling</div>
<div class="date">Week 1 · Generated: {datetime.now().strftime('%A, %B %d, %Y at %H:%M')}</div>
</header>

<div class="stats-bar">
<div class="stat-pill">
<div class="stat-label">Bankroll</div>
<div class="stat-val accent">${bankroll:,.0f}</div>
</div>
<div class="stat-pill">
<div class="stat-label">Staked</div>
<div class="stat-val">${total_stake:.0f}</div>
</div>
<div class="stat-pill strong">
<div class="stat-label">Strong</div>
<div class="stat-val green">{strong_count}</div>
</div>
<div class="stat-pill keep">
<div class="stat-label">Keep</div>
<div class="stat-val">{keep_count}</div>
</div>
<div class="stat-pill caution">
<div class="stat-label">Caution</div>
<div class="stat-val">{caution_count}</div>
</div>
</div>

<div class="date-section">
<div class="date-title">Matchday {datetime.now().strftime('%A, %B %d')}</div>
<div class="date-sub">{len(picks)} predictions generated · Kelly criterion active · $1,000 bankroll</div>
</div>
'''
        
        for i, pick in enumerate(picks[:20], 1):
            quality_class = pick['quality'].lower()
            reasoning = self.generate_reasoning(pick)
            
            html += f'''
<div class="pick-card {quality_class}" onclick="toggleDetails(this)">
<div class="pick-summary">
<div class="pick-rank">#{i}</div>
<div class="pick-main">
<div class="pick-title">
{pick['selection']}
<span class="quality-badge {quality_class}">{pick['quality']}</span>
</div>
<div class="pick-meta">{pick['home_team']} vs {pick['away_team']} · <span class="kickoff">{pick['kickoff']}</span></div>
</div>
<div class="pick-nums">
<div class="num"><div class="num-val accent">@{pick['odds']}</div><div class="num-label">Odds</div></div>
<div class="num"><div class="num-val green">+{pick['edge_pct']:.1f}%</div><div class="num-label">Edge</div></div>
<div class="num"><div class="num-val">${pick['stake']:.0f}</div><div class="num-label">Stake</div></div>
</div>
</div>
<div class="pick-details">
<div class="metrics-grid">
<div class="metric-box">
<div class="metric-label">Model Probability</div>
<div class="metric-value">{pick['model_prob']:.1%}</div>
</div>
<div class="metric-box">
<div class="metric-label">Book Implied</div>
<div class="metric-value">{pick['book_prob']:.1%}</div>
</div>
<div class="metric-box">
<div class="metric-label">Value Edge</div>
<div class="metric-value">+{pick['edge_pct']:.1f}%</div>
</div>
<div class="metric-box">
<div class="metric-label">Kelly Stake</div>
<div class="metric-value">${pick['stake']:.2f}</div>
</div>
<div class="metric-box">
<div class="metric-label">Market</div>
<div class="metric-value">{pick['market']}</div>
</div>
<div class="metric-box">
<div class="metric-label">League</div>
<div class="metric-value">{pick['league']}</div>
</div>
</div>
<div class="reasoning-box">
<div class="reasoning-title">Why This Pick?</div>
<div class="reasoning-text">{reasoning}</div>
</div>
</div>
</div>
'''
        
        html += '''
</div>

<script>
function toggleDetails(card) {
    const details = card.querySelector('.pick-details');
    details.classList.toggle('open');
}
</script>
</body>
</html>
'''
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(self.output_file, 'w') as f:
            f.write(html)
        
        print(f"Dashboard generated: {self.output_file}")
        return self.output_file

if __name__ == '__main__':
    gen = DashboardGenerator()
    gen.generate()
