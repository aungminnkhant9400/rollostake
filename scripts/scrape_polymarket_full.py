#!/usr/bin/env python3
"""
Full Polymarket Soccer Odds Scraper

Discovers all upcoming soccer matches on Polymarket, fetches every available market,
and imports odds into the Rollo Stake database.

Markets scraped:
- moneyline              -> 1X2
- totals                 -> OU (Over/Under goals)
- both_teams_to_score    -> BTTS
- spreads                -> AH (Asian Handicap)
- soccer_halftime_result -> HT_1X2
- total_corners          -> CORNERS_OU
- soccer_exact_score     -> EXACT_SCORE
- soccer_anytime_goalscorer -> GOALSCORER

Usage:
    python scripts/scrape_polymarket_full.py --days 7
    python scripts/scrape_polymarket_full.py --days 7 --leagues EPL LaLiga SerieA
    python scripts/scrape_polymarket_full.py --days 7 --save-csv data/polymarket_odds.csv
"""

import argparse
import csv
import json
import re
import sqlite3
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from models.core import init_db


TEAM_ALIASES = {
    "Aston Villa FC": "Aston Villa",
    "Liverpool FC": "Liverpool",
    "Paris Saint-Germain FC": "PSG",
    "Racing Club de Lens": "Lens",
}

SUPPORTED_LEAGUES = {"EPL", "L1", "Bundesliga", "SerieA", "LaLiga"}

URL_LEAGUE_MAP = {
    "epl-": "EPL",
    "lal-": "LaLiga",
    "sea-": "SerieA",
    "bun-": "Bundesliga",
    "fl1-": "L1",
    "tur-": "Turkey",
    "spl-": "Saudi",
    "fif-": "International",
    "chi-": "China",
    "arg-": "Argentina",
    "bol-": "Bolivia",
    "ukr-": "Ukraine",
    "isp-": "India",
    "efa-": "Europe",
    "brco-": "Brazil",
    "mex-": "Mexico",
    "jpn-": "Japan",
    "kor-": "Korea",
    "aus-": "Australia",
    "cl-": "Chile",
    "uru-": "Uruguay",
    "per-": "Peru",
    "ecu-": "Ecuador",
    "col-": "Colombia",
}


def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8", "ignore")


def market_objects(html: str) -> list[dict]:
    objects = []
    seen = set()
    for match in re.finditer(r'"slug":"([^"]+)"', html):
        start = html.rfind('{"id":"', 0, match.start())
        end = html.find(',"events":[', match.end())
        if start < 0 or end < 0:
            continue
        raw = html[start:end] + "}"
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        slug = obj.get("slug")
        if slug in seen:
            continue
        seen.add(slug)
        objects.append(obj)
    return objects


def discover_matches(days_ahead: int = 7) -> list[dict]:
    """Discover upcoming soccer matches from Polymarket."""
    url = "https://polymarket.com/sports/soccer/games"
    html = fetch_html(url)

    all_slugs = set()

    # Pattern 1: /event/slug
    event_slugs = re.findall(r'/event/([a-z0-9-]+\d{4}-\d{2}-\d{2}[a-z0-9-]*)', html)
    all_slugs.update(event_slugs)

    # Pattern 2: /sports/league/slug
    sports_slugs = re.findall(r'/sports/[a-z-]+/([a-z0-9-]+\d{4}-\d{2}-\d{2}[a-z0-9-]*)', html)
    all_slugs.update(sports_slugs)

    skip_suffixes = ("more-markets", "exact-score", "halftime-result", "total-corners", "player-props", "corners")
    clean_slugs = []
    for slug in all_slugs:
        if any(suffix in slug for suffix in skip_suffixes):
            continue
        clean_slugs.append(slug)

    today = datetime.now(timezone.utc).date()
    cutoff = today + timedelta(days=days_ahead)

    matches = []
    seen_slugs = set()
    for slug in clean_slugs:
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', slug)
        if not date_match:
            continue

        match_date = datetime.strptime(date_match.group(1), "%Y-%m-%d").date()
        if match_date < today - timedelta(days=1) or match_date > cutoff:
            continue

        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        # Build URL - /event/ usually works for all matches
        event_url = f"https://polymarket.com/event/{slug}"
        matches.append({
            "slug": slug,
            "url": event_url,
            "display_text": slug.replace("-", " ").title(),
            "match_date": match_date.isoformat(),
        })

    return matches


def extract_match_metadata(html: str) -> dict:
    title_match = re.search(r'"title":"([^"]+)"', html)
    title = title_match.group(1) if title_match else ""

    start_match = re.search(r'"startDate"\s*:\s*"([^"]+)"', html)
    kickoff = start_match.group(1) if start_match else None

    teams = title.split(" vs ") if " vs " in title else title.split(" vs. ")
    home_team = teams[0].strip() if len(teams) >= 1 else ""
    away_team = teams[1].strip() if len(teams) >= 2 else ""

    return {
        "title": title,
        "home_team": home_team,
        "away_team": away_team,
        "kickoff": kickoff,
    }


def local_team(name: str) -> str:
    return TEAM_ALIASES.get(name, name.replace(" FC", "").strip())


def line_from_text(value: str):
    match = re.search(r"\(([+-]?\d+(?:\.\d+)?)\)", value or "")
    if match:
        return float(match.group(1))
    match = re.search(r"([+-]?\d+(?:\.\d+)?)", value or "")
    return float(match.group(1)) if match else None


def decimal_odds(price) -> float | None:
    try:
        price = float(price)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return round(1.0 / price, 4)


def add_price(rows: list[dict], match_id: str, market: str, selection: str, price) -> None:
    odds = decimal_odds(price)
    if odds is None:
        return
    rows.append({
        "match_id": match_id,
        "market": market,
        "selection": selection,
        "odds": odds,
    })


def convert_all_markets(match_id: str, objects: list[dict]) -> list[dict]:
    rows = []

    for obj in objects:
        market_type = obj.get("sportsMarketType")
        title = obj.get("groupItemTitle") or obj.get("question") or ""
        outcomes = obj.get("outcomes") or []
        prices = obj.get("outcomePrices") or []

        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices, str):
            prices = json.loads(prices)

        if not prices:
            continue

        if market_type == "moneyline":
            if title.startswith("Draw"):
                add_price(rows, match_id, "1X2", "Draw", prices[0])
            else:
                add_price(rows, match_id, "1X2", f"{local_team(title)} Win", prices[0])

        elif market_type == "totals":
            line = line_from_text(title)
            if line is not None and len(prices) >= 2:
                add_price(rows, match_id, "OU", f"Over {line:g}", prices[0])
                add_price(rows, match_id, "OU", f"Under {line:g}", prices[1])

        elif market_type == "both_teams_to_score":
            if len(prices) >= 2:
                add_price(rows, match_id, "BTTS", "BTTS Yes", prices[0])
                add_price(rows, match_id, "BTTS", "BTTS No", prices[1])

        elif market_type == "spreads":
            line = line_from_text(title)
            if line is not None and len(outcomes) >= 2 and len(prices) >= 2:
                first_team = local_team(outcomes[0])
                second_team = local_team(outcomes[1])
                add_price(rows, match_id, "AH", f"{first_team} AH {line:+g}", prices[0])
                add_price(rows, match_id, "AH", f"{second_team} AH {-line:+g}", prices[1])

        elif market_type == "soccer_halftime_result":
            if title.startswith("Draw"):
                add_price(rows, match_id, "HT_1X2", "HT Draw", prices[0])
            else:
                add_price(rows, match_id, "HT_1X2", f"HT {local_team(title)} Win", prices[0])

        elif market_type == "total_corners":
            line = line_from_text(title)
            if line is not None and len(prices) >= 2:
                add_price(rows, match_id, "CORNERS_OU", f"Over {line:g}", prices[0])
                add_price(rows, match_id, "CORNERS_OU", f"Under {line:g}", prices[1])

        elif market_type == "soccer_exact_score":
            score_match = re.search(r"Exact Score:\s*(\d+-\d+)", title)
            if score_match and len(prices) >= 1:
                score = score_match.group(1)
                add_price(rows, match_id, "EXACT_SCORE", f"Exact {score}", prices[0])

        elif market_type == "soccer_anytime_goalscorer":
            player_match = re.search(r"Anytime Goalscorer:\s*(.+)", title)
            if player_match and len(prices) >= 1:
                player = player_match.group(1).strip()
                add_price(rows, match_id, "GOALSCORER", f"{player} Anytime", prices[0])

    return rows


def detect_league(slug: str) -> str:
    for prefix, league in URL_LEAGUE_MAP.items():
        if slug.startswith(prefix):
            return league
    return "Other"


def import_rows(rows: list[dict], bookmaker: str, overwrite: bool) -> dict:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    saved = 0
    skipped = 0

    for row in rows:
        if overwrite:
            cursor.execute(
                """
                DELETE FROM odds
                WHERE match_id = ? AND bookmaker = ? AND market = ? AND selection = ?
                """,
                (row["match_id"], bookmaker, row["market"], row["selection"]),
            )
        else:
            cursor.execute(
                """
                SELECT id FROM odds
                WHERE match_id = ? AND bookmaker = ? AND market = ? AND selection = ?
                """,
                (row["match_id"], bookmaker, row["market"], row["selection"]),
            )
            if cursor.fetchone():
                skipped += 1
                continue

        cursor.execute(
            """
            INSERT INTO odds (match_id, bookmaker, market, selection, odds, implied_prob)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row["match_id"],
                bookmaker,
                row["market"],
                row["selection"],
                row["odds"],
                round(1.0 / row["odds"], 4),
            ),
        )
        saved += 1

    conn.commit()
    conn.close()
    return {"saved": saved, "skipped": skipped}


def save_fixture(match_id: str, home_team: str, away_team: str, league: str, kickoff: str):
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''
        INSERT OR REPLACE INTO matches (match_id, home_team, away_team, league, kickoff, status)
        VALUES (?, ?, ?, ?, ?, 'scheduled')
    ''', (match_id, home_team, away_team, league, kickoff))

    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Scrape ALL Polymarket soccer odds")
    parser.add_argument("--days", type=int, default=7, help="Days ahead to scrape")
    parser.add_argument("--leagues", nargs="+", default=list(SUPPORTED_LEAGUES), help="Filter by league codes (default: 5 core leagues)")
    parser.add_argument("--all-leagues", action="store_true", help="Scrape ALL leagues, not just the 5 core ones")
    parser.add_argument("--no-overwrite", action="store_true", help="Skip existing odds")
    parser.add_argument("--save-csv", help="Also export to CSV file path")
    args = parser.parse_args()

    print("=" * 60)
    print("POLYMARKET FULL ODDS SCRAPER")
    print("=" * 60)

    print(f"\n[1/4] Discovering matches (next {args.days} days)...")
    matches = discover_matches(days_ahead=args.days)
    print(f"Found {len(matches)} matches")

    target_leagues = set(args.leagues) if not args.all_leagues else None
    if target_leagues:
        matches = [m for m in matches if detect_league(m["slug"]) in target_leagues]
        print(f"Filtered to {len(matches)} matches for leagues: {sorted(target_leagues)}")

    all_odds_rows = []

    print(f"\n[2/4] Fetching match pages...")
    for i, match in enumerate(matches, 1):
        safe_text = match["display_text"].encode("ascii", "ignore").decode("ascii")
        print(f"  [{i}/{len(matches)}] {safe_text} ({match['match_date']})")

        try:
            html = fetch_html(match["url"])
            meta = extract_match_metadata(html)
            objects = market_objects(html)

            home = local_team(meta["home_team"])
            away = local_team(meta["away_team"])
            match_id = f"pm_{home.lower().replace(' ', '_')}_{away.lower().replace(' ', '_')}_{match['match_date'].replace('-', '')}"

            league = detect_league(match["slug"])
            kickoff = meta["kickoff"] or f"{match['match_date']}T00:00:00Z"
            save_fixture(match_id, home, away, league, kickoff)

            rows = convert_all_markets(match_id, objects)
            all_odds_rows.extend(rows)
            print(f"    -> {len(rows)} odds rows ({len(objects)} markets)")

        except Exception as e:
            print(f"    -> ERROR: {e}")

    print(f"\n[3/4] Importing {len(all_odds_rows)} odds rows...")
    summary = import_rows(all_odds_rows, bookmaker="polymarket", overwrite=not args.no_overwrite)
    print(f"Saved: {summary['saved']}, Skipped: {summary['skipped']}")

    if args.save_csv:
        print(f"\n[4/4] Exporting to {args.save_csv}...")
        out_path = Path(args.save_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["match_id", "market", "selection", "odds"])
            writer.writeheader()
            writer.writerows(all_odds_rows)
        print("CSV export complete.")

    print("\n" + "=" * 60)
    print("MARKET SUMMARY")
    print("=" * 60)
    market_counts = {}
    for row in all_odds_rows:
        market_counts[row["market"]] = market_counts.get(row["market"], 0) + 1
    for market, count in sorted(market_counts.items(), key=lambda x: -x[1]):
        print(f"  {market}: {count} rows")

    print("\nDone!")


if __name__ == "__main__":
    main()
