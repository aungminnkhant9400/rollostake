#!/usr/bin/env python3
"""Import Polymarket soccer prices for supported model markets."""

import argparse
import json
import re
import sqlite3
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from models.core import init_db


DEFAULT_EVENTS = [
    {
        "match_id": "manual_l1_lens_psg_2026_05_14_03_00",
        "url": "https://polymarket.com/sports/ligue-1/fl1-rcl-psg-2026-05-13",
    },
    {
        "match_id": "football_data_538146",
        "url": "https://polymarket.com/sports/epl/epl-ast-liv-2026-05-17",
    },
]


TEAM_ALIASES = {
    "Aston Villa FC": "Aston Villa",
    "Liverpool FC": "Liverpool",
    "Paris Saint-Germain FC": "PSG",
    "Racing Club de Lens": "Lens",
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
    rows.append(
        {
            "match_id": match_id,
            "market": market,
            "selection": selection,
            "odds": odds,
        }
    )


def convert_market(match_id: str, obj: dict) -> list[dict]:
    market_type = obj.get("sportsMarketType")
    title = obj.get("groupItemTitle") or obj.get("question") or ""
    outcomes = obj.get("outcomes") or []
    prices = obj.get("outcomePrices") or []
    rows = []

    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    if isinstance(prices, str):
        prices = json.loads(prices)

    if market_type == "moneyline":
        if not prices:
            return rows
        if title.startswith("Draw"):
            add_price(rows, match_id, "1X2", "Draw", prices[0])
        else:
            add_price(rows, match_id, "1X2", f"{local_team(title)} Win", prices[0])
        return rows

    if market_type == "totals":
        line = line_from_text(title)
        if line is None or len(prices) < 2:
            return rows
        add_price(rows, match_id, "OU", f"Over {line:g}", prices[0])
        add_price(rows, match_id, "OU", f"Under {line:g}", prices[1])
        return rows

    if market_type == "both_teams_to_score":
        if len(prices) < 2:
            return rows
        add_price(rows, match_id, "BTTS", "BTTS Yes", prices[0])
        add_price(rows, match_id, "BTTS", "BTTS No", prices[1])
        return rows

    if market_type == "spreads":
        line = line_from_text(title)
        if line is None or len(outcomes) < 2 or len(prices) < 2:
            return rows
        first_team = local_team(outcomes[0])
        second_team = local_team(outcomes[1])
        add_price(rows, match_id, "AH", f"{first_team} AH {line:+g}", prices[0])
        add_price(rows, match_id, "AH", f"{second_team} AH {-line:+g}", prices[1])
        return rows

    return rows


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


def collect_events(events: list[dict]) -> list[dict]:
    rows = []
    for event in events:
        html = fetch_html(event["url"])
        for obj in market_objects(html):
            rows.extend(convert_market(event["match_id"], obj))
    unique = {}
    for row in rows:
        unique[(row["match_id"], row["market"], row["selection"])] = row
    return list(unique.values())


def main():
    parser = argparse.ArgumentParser(description="Import supported Polymarket soccer odds")
    parser.add_argument("--bookmaker", default="polymarket")
    parser.add_argument("--no-overwrite", action="store_true")
    args = parser.parse_args()

    rows = collect_events(DEFAULT_EVENTS)
    summary = import_rows(rows, bookmaker=args.bookmaker, overwrite=not args.no_overwrite)
    print(f"Polymarket odds imported: saved={summary['saved']} skipped={summary['skipped']}")
    for row in sorted(rows, key=lambda r: (r["match_id"], r["market"], r["selection"])):
        print(f"{row['match_id']} | {row['market']} | {row['selection']} @{row['odds']:.4f}")


if __name__ == "__main__":
    main()
