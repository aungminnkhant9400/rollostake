# Rollo Stake Model v1.0

Systematic value-betting engine for soccer. Finds +EV bets by comparing Dixon-Coles model probabilities against bookmaker odds.

## Current Status: ✅ WORKING

**What's live:**
- 3,293 real historical matches loaded (5 leagues)
- Dixon-Coles model fitted on 87 relevant matches
- 91 picks generated with realistic edges
- Dark HTML dashboard with STRONG/KEEP/CAUTION classification

## Quick Start

```bash
cd /home/ubuntu/rollo-stake-model
./run_daily.sh
```

Then open `dashboard/index.html` in your browser.

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
│   ├── stake_scraper.py    # Stake.com odds
│   ├── football_data.py    # Historical data (football-data.co.uk)
│   └── fixtures.py         # Upcoming fixtures (API-Football)
├── analysis/
│   ├── edge_calculator.py  # Value bet finder
│   └── fatigue.py          # Fixture congestion analysis
├── dashboard/
│   └── generator.py        # HTML dashboard
├── data/
│   └── rollo_stake.db      # SQLite database
└── tests/
    ├── populate_demo.py    # Demo data
    ├── add_odds.py         # Realistic odds
    └── backtest.py         # Model validation
```

## Model Details

### Dixon-Coles Model
- Poisson distribution for goal scoring
- Home advantage factor (~35%)
- Dixon-Coles correction for low-score correlation
- Team-specific attack/defense ratings
- Trained on 200 most recent relevant matches

### Edge Calculation
- **STRONG**: Edge ≥ 25% → $250 stake
- **KEEP**: Edge ≥ 10%, < 25% → $200 stake
- **CAUTION**: Edge ≥ 5%, < 10% → $200 stake
- **SKIP**: Edge < 5%

### Markets Supported
- 1X2 (Home/Draw/Away)
- Over/Under 2.5 goals
- Both Teams To Score (BTTS)

## Data Sources

| Source | Status | Notes |
|--------|--------|-------|
| Football-Data.co.uk | ✅ Working | Free historical data, 1,752 matches loaded |
| Demo fixtures | ✅ Working | Sample upcoming matches |
| Realistic odds | ✅ Working | Manually populated market odds |
| API-Football | ⚠️ Needs key | Free tier: 100 calls/day |
| Stake.com scraper | ⚠️ Blocked | Anti-bot protection, needs workaround |

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

### 2. Stake.com Scraper (Optional - for live odds)
Current status: Blocked by anti-bot protection
Workarounds to try:
- Use residential proxy
- Add delays between requests
- Try mobile version of site
- Use alternative bookmaker API

### 3. Run Daily
```bash
# Add to crontab (runs daily at 9 AM)
0 9 * * * /home/ubuntu/rollo-stake-model/run_daily.sh
```

## Backtesting

```bash
cd /home/ubuntu/rollo-stake-model
python3 tests/backtest.py
```

Validates model performance on historical data.

## Manual Usage

```bash
# Full pipeline with all features
python3 main.py

# Skip scraping (use existing data)
python3 main.py --skip-scrape

# Skip fatigue analysis
python3 main.py --no-fatigue

# Specific leagues only
python3 main.py --leagues EPL L1

# Update match result
python3 main.py --update-result "match_id" win 2 1
```

## Dashboard

Generated at: `dashboard/index.html`

Features:
- Bankroll tracking
- Pick quality flags (STRONG/KEEP/CAUTION)
- Edge percentages
- Model probabilities
- Stake sizing
- P&L tracking (after results updated)

## Roadmap

- [x] Dixon-Coles model
- [x] Edge calculator
- [x] HTML dashboard
- [x] Historical data (Football-Data)
- [x] Fatigue analysis
- [x] Backtesting
- [ ] Live odds scraping (Stake.com blocked)
- [ ] API-Football integration (needs key)
- [ ] Kelly criterion stake sizing
- [ ] Telegram alerts
- [ ] Injury/rotation tracking

## License

Private - for Rollo's use only.

## Team News Adjustments

The model can't see real-world factors like injuries or transfers. Use the team news CLI to adjust predictions manually.

### Usage

```bash
cd /home/ubuntu/rollo-stake-model
source .venv/bin/activate
python3 scripts/team_news_cli.py
```

### Supported Commands

```
<team> injury <player> <position> <importance>
<team> transfer <player> <position> <quality>
<team> manager <name> <impact>
<team> motivation <situation>
```

### Examples

```
Liverpool injury Salah striker star
Man City transfer Haaland striker good
Arsenal manager Arteta positive
Chelsea motivation derby
done
```

### Impact Levels

| Factor | Values | Impact |
|--------|--------|--------|
| injury importance | star/key/squad | -25% / -20% / -10% |
| transfer quality | star/good/squad | +25% / +15% / +5% |
| manager impact | positive/negative/neutral | +10% / -10% / 0% |
| motivation | title_race/relegation/derby | +8% / +12% / +8% |

### How It Works

Adjustments modify base model predictions:
- Attack adjustments: Affects goal scoring probability
- Defense adjustments: Affects goal conceding probability  
- Overall adjustments: Affects win probability directly
- All adjustments are dampened to avoid overcorrection
