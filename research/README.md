# Rollo Stake AutoResearch

AutoResearch runs rolling historical experiments over the same Range C/D concept used by the dashboard. It is for research only: it does not update `data/rollo_stake.db`, current picks, results, or `dashboard/index.html`.

## What It Tests

- Range C and Range D max pick counts
- Minimum edge thresholds
- Same-match exposure caps
- Same exposure-family caps
- Market mix for historical markets available from football-data.co.uk

The historical source has usable 1X2 odds and often has over/under, BTTS, and Asian handicap odds depending on league/season. It does not provide team-total odds, so TT selection still needs live weekly odds coverage.

## Server Command

From the repo root inside the GPU container:

```bash
python research/autoresearch.py \
  --seasons 2324 2425 2526 \
  --leagues EPL L1 Bundesliga SerieA LaLiga \
  --workers 8 \
  --top 20
```

If you want a faster first full pass, use 14-day batches:

```bash
python research/autoresearch.py \
  --seasons 2324 2425 2526 \
  --leagues EPL L1 Bundesliga SerieA LaLiga \
  --workers 8 \
  --batch-days 14 \
  --top 20
```

For a quick smoke test:

```bash
python research/autoresearch.py --quick --seasons 2526 --leagues EPL --top 5
```

Outputs are written to:

```text
research/results/leaderboard_*.csv
research/results/best_config_*.json
```

These generated outputs are ignored by Git. Commit code changes, not every experiment output.

## Speed

The expensive step is rolling model fitting. AutoResearch caches the generated historical candidates in:

```text
research/cache/candidates_*.json
```

After one broad run, later market/filter tests reuse that cache and skip the model fits.

Recommended broad cache build:

```bash
python research/autoresearch.py \
  --seasons 2324 2425 2526 \
  --leagues EPL L1 Bundesliga SerieA LaLiga \
  --workers 8 \
  --batch-days 14 \
  --candidate-markets 1X2,OU,BTTS,AH \
  --top 20
```

Then run quick filtered tests using the same cache:

```bash
python research/autoresearch.py \
  --seasons 2324 2425 2526 \
  --leagues EPL L1 Bundesliga SerieA LaLiga \
  --workers 8 \
  --batch-days 14 \
  --markets 1X2 \
  --top 20
```

Use `--refresh-cache` when you intentionally want to rebuild from source CSVs and refit the rolling models.

## Important Constraint

The current Dixon-Coles model uses SciPy/NumPy CPU optimization. The A100 GPU is not used yet. The server is still useful because it can run long, parallel research jobs, but true GPU acceleration would require a future PyTorch/JAX model.
