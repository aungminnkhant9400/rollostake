# Rollo Stake Model v1.0

Systematic value-betting engine for soccer. Finds +EV bets by comparing Dixon-Coles model probabilities against bookmaker odds.

## Current Status: ✅ WORKING

**What's live:**
- 7,989 real historical matches loaded (5 leagues, 2021-2025)
- Dixon-Coles model fitted per league
- 12 picks generated with realistic edges
- Dark HTML dashboard with STRONG/KEEP/CAUTION classification
- Kelly criterion staking (optimal bet sizing)
- Team news adjustment CLI

## Quick Start

```bash
cd /home/ubuntu/rollo-stake-model
./run_daily.sh
```

Then open `dashboard/index.html` in your browser.

## Weekly Fixture + Odds Workflow

Fetch real upcoming fixtures from football-data.org, export a blank odds slate, then fill the odds from your bookmaker.

```bash
export FOOTBALL_DATA_TOKEN="your-football-data-token"
python3 scripts/fetch_weekly_fixtures.py --days 7 --export week_slate.csv
python3 scripts/import_weekly_slate.py week_slate.csv
python3 main.py --skip-scrape --no-fatigue
```

On Windows PowerShell:

```powershell
$env:FOOTBALL_DATA_TOKEN="your-football-data-token"
python scripts\fetch_weekly_fixtures.py --days 7 --export week_slate.csv
python scripts\import_weekly_slate.py week_slate.csv
python main.py --skip-scrape --no-fatigue
```

### Team Total Odds

Team over/under is supported, but it needs real bookmaker odds before those
markets become official picks. Export the model-ranked team-total shortlist,
fill the `odds` column from your bookmaker, import it, then rerun the model.

```bash
python3 scripts/team_total_odds_cli.py --export-template team_total_odds.csv
python3 scripts/team_total_odds_cli.py --import-file team_total_odds.csv
python3 main.py --skip-scrape --no-fatigue
```

On Windows PowerShell:

```powershell
python scripts\team_total_odds_cli.py --export-template team_total_odds.csv
python scripts\team_total_odds_cli.py --import-file team_total_odds.csv
python main.py --skip-scrape --no-fatigue
```

For direct prompting instead of editing CSV:

```powershell
python scripts\team_total_odds_cli.py --interactive --max-rows 40
```

## Architecture

```
rollo-stake-model/
├── main.py                 # Orchestrator
├── run_daily.sh            # Daily automation script
├── config/
│   └── settings.json       # API keys & config
├── models/
│   ├── core.py             # SQLite DB
│   └── dixon_coles.py      # Dixon-Coles model
├── scrapers/
│   ├── stake_scraper.py    # Stake.com odds (placeholder)
│   ├── football_data.py    # Historical data (football-data.co.uk)
│   ├── fixtures.py         # Upcoming fixtures (demo data)
│   ├── manual_odds.py      # CLI to input real bookmaker odds
│   ├── news_scraper.py     # Free team news (blocked by sites)
│   └── historical_loader.py # Multi-season data loader
├── analysis/
│   ├── edge_calculator.py  # Value bet finder + Kelly staking
│   ├── fatigue.py          # Fixture congestion analysis
│   └── team_news.py        # Injury/transfer adjustments
├── dashboard/
│   └── generator.py        # HTML dashboard
├── scripts/
│   └── team_news_cli.py    # Interactive team news input
├── data/
│   └── rollo_stake.db      # SQLite database (not in repo)
└── tests/
    ├── populate_demo.py    # Demo data
    ├── add_odds.py         # Realistic odds
    └── backtest.py         # Model validation
```

## Collaboration: Garfis + Codex + Rollo

This repo is set up for collaboration between:
- **Garfis** (me) — Initial build, architecture, model
- **Codex (GPT-5.4)** — Your local coding assistant
- **Rollo** — You, the user

### Workflow
1. Clone this repo to your laptop
2. Use Codex to modify/improve
3. Push to GitHub
4. I (Garfis) read commits and suggest improvements

### What Codex Should Know

| File | Purpose | Modify? |
|------|---------|---------|
| `main.py` | Entry point | ✅ Add features |
| `models/dixon_coles.py` | Core math | ⚠️ Be careful |
| `analysis/edge_calculator.py` | Kelly + edge | ✅ Adjust staking |
| `dashboard/generator.py` | HTML output | ✅ Add UI features |
| `config/settings.json` | Settings | ✅ Adjust thresholds |

### Priority Tasks for Codex
1. **Results tracking** — Input match outcomes, calculate P&L
2. **Flat staking option** — $X/pick instead of Kelly %
3. **Real fixtures API** — API-Football integration
4. **Two-range architecture** — Separate bankrolls for C/D ranges

## Model Details

### Dixon-Coles Model
- Poisson distribution for goal scoring
- Home advantage factor (~35%)
- Dixon-Coles correction for low-score correlation
- Team-specific attack/defense ratings
- Trained per league on all historical data

### Edge Calculation
- **STRONG**: Edge ≥ 25% → Kelly stake
- **KEEP**: Edge ≥ 10%, < 25% → Kelly stake
- **CAUTION**: Edge ≥ 5%, < 10% → Kelly stake (capped)
- **SKIP**: Edge < 5%

### Kelly Criterion
```
Stake = Bankroll × Edge / (Odds - 1)
```
Where `Edge = Model Prob - (1 / Odds)`

### Markets Supported
- 1X2 (Home/Draw/Away)
- Over/Under 2.5 goals

## Data Sources

| Source | Status | Notes |
|--------|--------|-------|
| Football-Data.co.uk | ✅ Working | Free, 7,989 matches, 5 leagues |
| Demo fixtures | ✅ Working | Sample upcoming matches |
| Realistic odds | ✅ Working | Manually populated |
| API-Football | ⚠️ Needs key | Free tier: 100 calls/day |
| Stake.com scraper | ❌ Blocked | Anti-bot protection |
| Team news scraper | ❌ Blocked | All sites block bots |

## To Activate Real-Time Mode

### 1. Get API-Football Key (Optional - for real fixtures)
- Sign up: https://www.api-football.com/
- Free tier: 100 requests/day
- Add to `config/settings.json`:
```json
{
  "api_football_key": "your-key-here"
}
```

### 2. Run Daily
```bash
# Add to crontab (runs daily at 9 AM)
0 9 * * * /home/ubuntu/rollo-stake-model/run_daily.sh
```

## Team News Adjustments

The model can't see real-world factors. Use the CLI to adjust manually.

```bash
python3 scripts/team_news_cli.py
```

**Commands:**
```
Liverpool injury Salah striker star
Man City transfer Haaland striker good
Arsenal motivation title_race
done
```

## Backtesting

```bash
python3 tests/backtest.py
```

## What's Missing (Priority Order)

1. **Results tracking** — Can't track P&L yet
2. **Flat staking** — Only Kelly % now
3. **Real fixtures** — Need API-Football
4. **Auto odds** — Need residential proxy or paid API
5. **Two-range system** — Separate C/D bankrolls

## License

Private — for Rollo's use only.
