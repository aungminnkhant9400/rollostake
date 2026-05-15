#!/bin/bash
# Daily run script for Rollo Stake Model
# Run this daily to generate fresh predictions

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

echo "=========================================="
echo "Rollo Stake Model - Daily Run"
echo "=========================================="
echo ""

# Step 1: Load latest historical data (if football-data has new matches)
echo "[1/5] Updating historical data..."
python3 -c "
from scrapers.football_data import FootballDataLoader
from config.settings import load_settings
loader = FootballDataLoader()
settings = load_settings()
for season in settings.get('historical_seasons', ['2526']):
    for league in settings.get('leagues', ['EPL', 'L1', 'Bundesliga', 'SerieA', 'LaLiga']):
        matches = loader.fetch_season(league, season)
        loader.save_to_db(matches)
"

# Step 2: Fetch upcoming fixtures
echo ""
echo "[2/5] Fetching upcoming fixtures..."
python3 -c "
from scrapers.fixtures import FixturesFetcher
import json

# Try to load API key from config
import os
config_path = 'config/settings.json'
api_key = None
if os.path.exists(config_path):
    with open(config_path) as f:
        config = json.load(f)
        api_key = config.get('api_football_key')

fetcher = FixturesFetcher(api_key=api_key)
fixtures = fetcher.get_all_upcoming()
print(f'Loaded {len(fixtures)} upcoming matches')
"

# Step 3: Scrape team news (EPL via browser + manual JSON fallback)
echo ""
echo "[3/5] Fetching team news..."
python3 scrapers/browser_news_scraper.py --sources premier_injuries,manual

# Step 4: Add realistic odds (replace with scraper when working)
echo ""
echo "[4/5] Updating odds..."
python3 tests/add_odds.py

# Step 5: Run full pipeline
echo ""
echo "[5/5] Generating predictions..."
python3 main.py --skip-scrape --no-fatigue

echo ""
echo "=========================================="
echo "Done! Open dashboard/index.html"
echo "=========================================="
