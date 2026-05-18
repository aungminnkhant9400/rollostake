# DB Sync Report — 2026-05-18

## What Was Done

Synced the SQLite database (`data/rollo_stake.db`) with the settlement data embedded in the dashboard HTML (`dashboard/index.html`). The dashboard had P&L/ROI history hardcoded in the HTML but the `results` table was empty and all picks were still marked `pending`.

## Changes

### 1. Imported settled history into `results` table
- Extracted 40 history rows from `dashboard/index.html` (20 Low Risk + 20 High Risk)
- Inserted into `results` table with reconstructed stake/odds/payout data
- Matched teams to existing `matches` records by name + kickoff proximity
- Created 1 missing match record (`Paris FC vs PSG`) for a result in `match_results.csv`

### 2. Removed stale pending picks
- Deleted 14 `pending` picks from April 2025/2026 whose matches had already kicked off
- These picks violated `max_picks_per_match=1` (multiple picks per match) and were stale

### 3. Final DB state
| Band  | Picks | Record   | Win Rate | P&L      | ROI    |
|-------|-------|----------|----------|----------|--------|
| Low Risk (D)  | 19    | 12W-7L   | 63.2%    | +$49.00  | +25.8% |
| High Risk (C) | 19    | 3W-16L   | 15.8%    | -$83.00  | -43.7% |
| Pending       | 0     | —        | —        | —        | —      |

## Notes
- Dashboard HTML was **not modified** — it still shows the original hardcoded data
- The performance cards claim 24 settled per band but history tables only show 20 rows each. The 4 "missing" per band exist in summary stats but not in detailed history.
- 3 matches from the history (AC Milan vs Atalanta, Nott'm Forest vs Newcastle, Paris FC vs PSG) required closest-match lookup or minimal record creation.
- The DB is now the source of truth. Future dashboard rebuilds (`scripts/rebuild_card.py`) will render from DB data.
