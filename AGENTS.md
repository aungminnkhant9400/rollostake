# RolloStake Agent Handoff

This file is the working context for another model or agent taking over this repo.

## What This Project Does

RolloStake is a football prediction and staking dashboard. It compares Dixon-Coles model probabilities against available odds, learns from settled picks, separates risk bands, and generates weekly betting cards.

Active dashboard:

```powershell
C:\Users\aungm\Desktop\rollostake\dashboard\index.html
```

Live database:

```powershell
C:\Users\aungm\Desktop\rollostake\data\rollo_stake.db
```

## Main User Goal

The user wants weekly football predictions that improve over time. The system must update previous results, settle picks, learn separately from High Risk, Low Risk, and Parley outcomes, study friend cards for useful structure, fetch upcoming fixtures and odds, apply football context, generate next-week picks, and rebuild the dashboard.

Context factors to include when available:

- Table position
- Head-to-head
- Fatigue and fixture congestion
- European competition schedule
- Injuries and suspensions
- Motivation
- Team news

## Core Concepts

- High Risk: bigger odds, higher upside, more volatile. Current range code is `C`.
- Low Risk: tighter odds, steadier single-pick card. Current range code is `D`.
- Parley: separate lower-odds multi-leg slips. It must not simply copy Low Risk.
- STRONG / KEEP / CAUTION: quality labels from edge and learned historical performance. Do not assume STRONG is always best.
- Learning: own settled RolloStake results matter most. Friend cards are secondary structure signals.
- Official markets: `1X2`, `OU`, `BTTS`, `TT`, `AH`.
- Quarter AH lines: avoid official picks until settlement supports half-win/half-loss accounting.

## Current Dashboard Behavior

Tabs are ordered:

1. Low Risk
2. High Risk
3. Parley

History tables show match played/kickoff date, not settled/import date.

All dashboard dates and times must be displayed in Macau time (`Asia/Macau`, UTC+8). The database may contain a mix of local kickoff strings and UTC ISO strings, so use `utils.match_resolver.parse_kickoff_utc()` / `format_kickoff_local()` instead of slicing or printing raw kickoff values.

Result settlement must only touch picks whose final-result window has passed in Macau time. `scripts/import_match_results.py` enforces this guard and skips future or not-yet-final rows. Do not manually settle a future match or a match still in progress.

Parley currently:

- Uses the full upcoming model candidate pool.
- Prefers odds from `1.25` to `1.70`.
- Allows normal low-risk style odds from `1.70` to `2.15`.
- Allows limited booster odds from `2.15` to `2.70`.
- Blocks learned loss-trap shapes from both High Risk and Low Risk.
- Uses learned segment performance, weighted more toward Low Risk.
- Allows only one leg per match.
- Builds a conservative 2-leg and balanced 3-leg slip.
- Is saved in `parley_slips` and `parley_legs`, shown like High Risk and Low Risk, and settled as its own record once every leg has a result.

## Weekly `/stake` Workflow

Use this flow when the user types `/stake` or asks for weekly predictions.

1. Check git status.

```powershell
git status -sb
```

2. Import previous results.

```powershell
python scripts\import_match_results.py match_results.csv
```

3. Study friend cards if available.

```powershell
python scripts\study_external_card.py friend_cards
```

4. Dry-run Polymarket odds before writing.

```powershell
python scripts\scrape_polymarket_full.py --days 7 --dry-run
```

Only continue if unresolved matches are acceptable and no fake fixtures are created.

5. Import Polymarket odds.

```powershell
python scripts\scrape_polymarket_full.py --days 7
```

Never use `--create-missing` unless the user explicitly asks.

6. Rebuild predictions and dashboard.

```powershell
python scripts\rebuild_card.py
```

7. Validate touched Python files.

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'; python -m py_compile analysis\edge_calculator.py dashboard\generator.py scripts\rebuild_card.py scripts\import_match_results.py scripts\study_external_card.py scripts\scrape_polymarket_full.py scripts\import_historical_odds.py utils\match_resolver.py
```

## `update` Workflow

When the user says `update`, do this sequence:

1. Check git status.

```powershell
git status -sb
```

2. Import/settle completed results only.

```powershell
python scripts\import_match_results.py match_results.csv
```

The importer must skip any match whose final-result window has not passed in Macau time. Settled history must show match played/kickoff date, never settled/import date.

3. Study friend cards from the repo folder.

```powershell
python scripts\study_external_card.py friend_cards
```

4. Count remaining pending picks.

```powershell
@'
import sqlite3
from config.paths import DB_PATH
conn = sqlite3.connect(DB_PATH)
count = conn.execute("SELECT COUNT(*) FROM picks WHERE status='pending'").fetchone()[0]
conn.close()
print(count)
'@ | python -
```

5. If pending picks are `2` or more, rebuild the dashboard only.

```powershell
python scripts\rebuild_card.py
```

6. If pending picks are fewer than `2`, fetch/find next-week fixtures and odds, then rebuild predictions.

```powershell
python scripts\scrape_polymarket_full.py --days 7 --dry-run
python scripts\scrape_polymarket_full.py --days 7
python scripts\rebuild_card.py
```

Never use `--create-missing` unless the user explicitly asks. Do not clear past pending picks before settlement; `analysis/edge_calculator.py` should only replace pending picks for matches that have not kicked off yet.

7. Report High Risk, Low Risk, and Parley separately with record, P&L, ROI, bank, and remaining pending count.

## Important Files

- `analysis/edge_calculator.py`: Candidate generation, edge scoring, learned adjustments, loss traps, risk-band selection.
- `dashboard/generator.py`: Static dashboard rendering, risk tabs, history, Parley tab.
- `scripts/rebuild_card.py`: Regenerates risk-band picks and dashboard.
- `scripts/import_match_results.py`: Imports final scores and settles picks.
- `scripts/scrape_polymarket_full.py`: Discovers upcoming Polymarket football markets and imports supported odds.
- `scripts/study_external_card.py`: Parses friend prediction cards and stores aggregate structure in `data/external_card_profile.json`.
- `friend_cards/`: Raw weekly prediction HTML cards from the user's friend. Add new friend cards here before studying them.
- `scrapers/browser_news_scraper.py`: Optional browser/Kimi WebBridge team-news scraper plus manual JSON fallback.
- `scripts/news_impact_report.py`: Reports which pending picks have visible news/context adjustments.
- `utils/match_resolver.py`: Normalizes and resolves Polymarket matches to existing fixtures.
- `utils/team_normalizer.py`: Team name alias map used by odds, fixture, and news matching.
- `config/settings.json`: Active ranges, bankroll, stake size, leagues, bookmaker, and fixture settings.
- `match_results.csv`: Local result import source.

## Model Rules To Preserve

- Do not mix High Risk and Low Risk history together in the dashboard.
- Do not show settled/import date in history; show match played date.
- Do not blindly chase more picks. The user wants better win rate, not filler.
- Low Risk `KEEP` has recently outperformed Low Risk `STRONG`; respect learned performance.
- High Risk has performed poorly; avoid repeated losing shapes such as aggressive AH `-1.5/-2.5`, unsupported away 1X2 shots, and bad low-total fillers unless context strongly supports them.
- If a risk band's live bank is below its flat stake, that band must be paused. Do not generate official picks that cannot be staked from the live band bank.
- Parley should focus on low odds and high probability, with only small controlled exposure to booster legs.
- Always report record, P&L, ROI, and bank when running the weekly workflow.

## Known Limitations

- Live injury/news ingestion is partly automated through `scrapers/browser_news_scraper.py`, but it depends on Kimi WebBridge or a manual JSON fallback and still needs careful verification.
- Some table, H2H, fatigue, and manual team-news adjustment logic exists.
- Parley slips are saved and settled separately from High Risk and Low Risk. They use `parley_slips` and `parley_legs`, not the single-pick `results` table.
- Friend cards are studied as aggregate market-shape lessons, not as exact picks.
- Browser automation may be blocked from reloading `file://` pages. Rebuild the HTML and ask the user to refresh if needed.

## Git Safety

The repo may contain user or prior-agent changes. Never reset or revert unrelated work. Before edits, inspect status. After changes, report what changed and whether it was pushed.
