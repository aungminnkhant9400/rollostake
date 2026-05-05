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
research/results/breakdown_*.csv
research/results/best_picks_*.csv
```

These generated outputs are ignored by Git. Commit code changes, not every experiment output.

The breakdown CSV is the main diagnostic file. It reports the best config by:

```text
league
season
range
market
quality
odds_bucket
edge_bucket
selection_type
league_range
league_selection
season_league
range_odds_bucket
range_edge_bucket
```

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

The production pipeline still uses the original SciPy/NumPy Dixon-Coles model. GPU research is available through `research/gpu_autoresearch.py`, but production probabilities should only be switched after the GPU model beats the current model on holdout seasons.

## GPU Dixon-Coles

`research/gpu_autoresearch.py` uses `models/torch_dixon_coles.py`, a PyTorch Dixon-Coles style model with team attack/defense parameters, league effects, home advantage, time decay, and score-distribution outputs for 1X2 and over/under.

Smoke test on the server:

```bash
python research/gpu_autoresearch.py \
  --quick \
  --device cuda \
  --markets 1X2,OU \
  --candidate-markets 1X2,OU \
  --top 5
```

Full A100 research run for all five leagues and totals:

```bash
python research/gpu_autoresearch.py \
  --seasons 2122 2223 2324 2425 2526 \
  --leagues EPL L1 Bundesliga SerieA LaLiga \
  --device cuda \
  --markets 1X2,OU \
  --candidate-markets 1X2,OU \
  --train-size-per-league 300 \
  --min-train-size-per-league 120 \
  --batch-days 14 \
  --epochs 500 \
  --c-max-picks 0,1,2 \
  --d-max-picks 1,2,3 \
  --min-edges 0.15,0.25,0.35 \
  --max-picks-per-match 1 \
  --top 20
```

Confirm GPU use while it is running:

```bash
nvidia-smi
```

## Overnight AutoLoop

`research/autoloop.py` is the overnight researcher. It runs broad GPU experiments, reads the resulting breakdown CSVs, chooses adaptive follow-up experiments from positive slices, and writes a morning report.

Smoke test:

```bash
python research/autoloop.py \
  --quick \
  --device cuda \
  --initial-experiments 1 \
  --adaptive-experiments 1 \
  --top 5
```

Overnight run:

```bash
nohup python research/autoloop.py \
  --seasons 2122 2223 2324 2425 2526 \
  --leagues EPL L1 Bundesliga SerieA LaLiga \
  --device cuda \
  --candidate-markets 1X2,OU \
  --train-size-per-league 300 \
  --min-train-size-per-league 120 \
  --batch-days 14 \
  --epochs 500 \
  --c-max-picks 0,1,2 \
  --d-max-picks 1,2,3 \
  --min-edges 0.15,0.25,0.35 \
  --initial-experiments 3 \
  --adaptive-experiments 4 \
  --timeout-minutes 240 \
  --top 20 \
  > research/results/autoloop_nohup.log 2>&1 &
```

Check progress:

```bash
tail -f research/results/autoloop_nohup.log
nvidia-smi
```

Morning outputs:

```text
research/results/autoloop_report_*.md
research/results/autoloop_results_*.json
```

## LLM Code-Editing Agent Loop

`research/agent_loop.py` is the true code-editing research loop. It uses DeepSeek to propose small patches, applies each patch in an isolated git worktree, runs GPU AutoResearch, and keeps a report of patches that beat the baseline. It does not push or modify production by default.

Set the API key on the server:

```bash
export DEEPSEEK_API_KEY="your_key_here"
export DEEPSEEK_MODEL="deepseek-v4-pro"
```

Smoke test with a short evaluation command:

```bash
python research/agent_loop.py \
  --iterations 1 \
  --model deepseek-v4-pro \
  --eval-timeout-minutes 30 \
  --eval-command "python research/gpu_autoresearch.py --quick --device cuda --markets 1X2,OU --candidate-markets 1X2,OU --top 5"
```

Overnight code-editing run:

```bash
nohup python research/agent_loop.py \
  --iterations 5 \
  --model deepseek-v4-pro \
  --eval-timeout-minutes 240 \
  --min-roi 1.0 \
  --min-picks 40 \
  --remove-rejected-worktrees \
  > research/results/agent_loop_nohup.log 2>&1 &
```

Check progress:

```bash
tail -f research/results/agent_loop_nohup.log
nvidia-smi
```

Morning outputs:

```text
research/results/agent_loop_report_*.md
research/results/agent_loop_results_*.json
research/agent_runs/*/iter_*.patch
```

Accepted patches remain in their worktree so they can be reviewed before manually applying them to `main`.
