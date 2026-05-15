#!/usr/bin/env python3
"""
Full Polymarket Soccer Odds Scraper

Discovers upcoming soccer matches on Polymarket, fetches supported model markets,
and imports odds into the Rollo Stake database.

Markets scraped:
- moneyline              -> 1X2
- totals                 -> OU (Over/Under goals)
- both_teams_to_score    -> BTTS
- spreads                -> AH (Asian Handicap)

Extra markets can be exported/imported with --include-extra-markets, but the
official edge model currently ignores them.

Usage:
    python scripts/scrape_polymarket_full.py --days 7
    python scripts/scrape_polymarket_full.py --days 7 --leagues EPL LaLiga SerieA
    python scripts/scrape_polymarket_full.py --days 7 --dry-run
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
from utils.match_resolver import resolve_match_id
from utils.team_normalizer import normalize_team_name


TEAM_ALIASES = {
    "Aston Villa FC": "Aston Villa",
    "Liverpool FC": "Liverpool",
    "Paris Saint-Germain FC": "PSG",
    "Racing Club de Lens": "Lens",
}

SUPPORTED_LEAGUES = {"EPL", "L1", "Bundesliga", "SerieA", "LaLiga"}
MODEL_MARKETS = {"1X2", "OU", "BTTS", "AH"}

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
    title = decode_json_text(title_match.group(1)) if title_match else ""

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
    return normalize_team_name(TEAM_ALIASES.get(name, name.strip()))


def decode_json_text(value: str) -> str:
    try:
        return json.loads(f'"{value}"')
    except json.JSONDecodeError:
        return value


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


def convert_all_markets(match_id: str, objects: list[dict], include_extra_markets: bool = False) -> tuple[list[dict], dict]:
    rows = []
    unsupported = 0
    bad_rows = 0

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
            if not include_extra_markets:
                unsupported += 1
                continue
            if title.startswith("Draw"):
                add_price(rows, match_id, "HT_1X2", "HT Draw", prices[0])
            else:
                add_price(rows, match_id, "HT_1X2", f"HT {local_team(title)} Win", prices[0])

        elif market_type == "total_corners":
            if not include_extra_markets:
                unsupported += 1
                continue
            line = line_from_text(title)
            if line is not None and len(prices) >= 2:
                add_price(rows, match_id, "CORNERS_OU", f"Over {line:g}", prices[0])
                add_price(rows, match_id, "CORNERS_OU", f"Under {line:g}", prices[1])

        elif market_type == "soccer_exact_score":
            if not include_extra_markets:
                unsupported += 1
                continue
            score_match = re.search(r"Exact Score:\s*(\d+-\d+)", title)
            if score_match and len(prices) >= 1:
                score = score_match.group(1)
                add_price(rows, match_id, "EXACT_SCORE", f"Exact {score}", prices[0])

        elif market_type == "soccer_anytime_goalscorer":
            if not include_extra_markets:
                unsupported += 1
                continue
            player_match = re.search(r"Anytime Goalscorer:\s*(.+)", title)
            if player_match and len(prices) >= 1:
                player = player_match.group(1).strip()
                add_price(rows, match_id, "GOALSCORER", f"{player} Anytime", prices[0])
        elif market_type:
            unsupported += 1

    for row in rows:
        if row["market"] not in MODEL_MARKETS and not include_extra_markets:
            bad_rows += 1

    return rows, {"unsupported_markets": unsupported, "bad_rows": bad_rows}


def detect_league(slug: str) -> str:
    for prefix, league in URL_LEAGUE_MAP.items():
        if slug.startswith(prefix):
            return league
    return "Other"


def import_rows(rows: list[dict], bookmaker: str, overwrite: bool, dry_run: bool = False) -> dict:
    if dry_run:
        return {"saved": 0, "skipped": 0}

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


def save_fixture(match_id: str, home_team: str, away_team: str, league: str, kickoff: str, dry_run: bool = False):
    if dry_run:
        return
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''
        INSERT OR REPLACE INTO matches (match_id, home_team, away_team, league, kickoff, status)
        VALUES (?, ?, ?, ?, ?, 'scheduled')
    ''', (match_id, home_team, away_team, league, kickoff))

    conn.commit()
    conn.close()


def fallback_match_id(home_team: str, away_team: str, match_date: str) -> str:
    home_key = re.sub(r"[^a-z0-9]+", "_", home_team.lower()).strip("_")
    away_key = re.sub(r"[^a-z0-9]+", "_", away_team.lower()).strip("_")
    return f"pm_{home_key}_{away_key}_{match_date.replace('-', '')}"


def resolve_or_create_match(meta: dict, match: dict, create_missing: bool, dry_run: bool) -> tuple[str, bool]:
    home = local_team(meta["home_team"])
    away = local_team(meta["away_team"])
    league = detect_league(match["slug"])
    kickoff = meta["kickoff"] or f"{match['match_date']}T00:00:00Z"
    resolved = resolve_match_id(
        {
            "home_team": home,
            "away_team": away,
            "kickoff": kickoff,
        },
        statuses=("scheduled",),
    )
    if resolved:
        return resolved, False
    if not create_missing:
        return "", False

    match_id = fallback_match_id(home, away, match["match_date"])
    save_fixture(match_id, home, away, league, kickoff, dry_run=dry_run)
    return match_id, True


def main():
    parser = argparse.ArgumentParser(description="Scrape ALL Polymarket soccer odds")
    parser.add_argument("--days", type=int, default=7, help="Days ahead to scrape")
    parser.add_argument("--leagues", nargs="+", default=list(SUPPORTED_LEAGUES), help="Filter by league codes (default: 5 core leagues)")
    parser.add_argument("--all-leagues", action="store_true", help="Scrape ALL leagues, not just the 5 core ones")
    parser.add_argument("--create-missing", action="store_true", help="Create new pm_* fixtures when no existing fixture can be resolved")
    parser.add_argument("--include-extra-markets", action="store_true", help="Also import currently unsupported markets such as corners and exact score")
    parser.add_argument("--no-overwrite", action="store_true", help="Skip existing odds")
    parser.add_argument("--dry-run", action="store_true", help="Discover, resolve, and validate without writing to the database")
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
    created_fixtures = 0
    unresolved_matches = 0
    unsupported_markets = 0
    bad_rows = 0

    print(f"\n[2/4] Fetching match pages...")
    for i, match in enumerate(matches, 1):
        safe_text = match["display_text"].encode("ascii", "ignore").decode("ascii")
        print(f"  [{i}/{len(matches)}] {safe_text} ({match['match_date']})")

        try:
            html = fetch_html(match["url"])
            meta = extract_match_metadata(html)
            objects = market_objects(html)

            match_id, created = resolve_or_create_match(
                meta,
                match,
                create_missing=args.create_missing,
                dry_run=args.dry_run,
            )
            if not match_id:
                unresolved_matches += 1
                print("    -> skipped: no matching local fixture")
                continue
            if created:
                created_fixtures += 1

            rows, validation = convert_all_markets(
                match_id,
                objects,
                include_extra_markets=args.include_extra_markets,
            )
            all_odds_rows.extend(rows)
            unsupported_markets += validation["unsupported_markets"]
            bad_rows += validation["bad_rows"]
            print(f"    -> {len(rows)} odds rows ({len(objects)} markets)")

        except Exception as e:
            print(f"    -> ERROR: {e}")

    unique_rows = {}
    for row in all_odds_rows:
        unique_rows[(row["match_id"], row["market"], row["selection"])] = row
    all_odds_rows = list(unique_rows.values())

    print(f"\n[3/4] Importing {len(all_odds_rows)} odds rows...")
    summary = import_rows(
        all_odds_rows,
        bookmaker="polymarket",
        overwrite=not args.no_overwrite,
        dry_run=args.dry_run,
    )
    print(f"Saved: {summary['saved']}, Skipped: {summary['skipped']}")
    print(
        f"Resolved validation: unresolved_matches={unresolved_matches}, "
        f"created_fixtures={created_fixtures}, unsupported_markets={unsupported_markets}, bad_rows={bad_rows}"
    )

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
