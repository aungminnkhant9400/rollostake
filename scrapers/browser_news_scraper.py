#!/usr/bin/env python3
"""
Browser-based team news scraper using Kimi WebBridge.

Sources:
- Premier Injuries (EPL): https://www.premierinjuries.com/injury-table.php
- Manual JSON fallback: data/team_news_weekly.json

Requires the Kimi WebBridge daemon running at http://127.0.0.1:10086.
"""

import json
import sqlite3
import sys
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH, DATA_DIR
from utils.team_normalizer import normalize_team_name

WEBBRIDGE_URL = "http://127.0.0.1:10086/command"


def _wb_request(action: str, args: dict, session: str = "rollo-news") -> dict:
    """Send a command to the Kimi WebBridge daemon."""
    payload = json.dumps({"action": action, "args": args, "session": session}).encode("utf-8")
    req = urllib.request.Request(
        WEBBRIDGE_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _evaluate_js(code: str, session: str = "rollo-news") -> Optional[any]:
    """Evaluate JavaScript in the browser and return the value."""
    resp = _wb_request("evaluate", {"code": code}, session)
    if resp.get("ok") and "data" in resp:
        return resp["data"].get("value")
    return None


def webbridge_status() -> Tuple[bool, str]:
    """Check if webbridge is healthy."""
    try:
        req = urllib.request.Request(WEBBRIDGE_URL, data=b'{}', headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return True, "daemon reachable"
    except Exception as e:
        return False, str(e)


class PremierInjuriesScraper:
    """Scrape EPL injury data from Premier Injuries via browser."""

    URL = "https://www.premierinjuries.com/injury-table.php"
    SESSION = "rollo-news-pi"
    SOURCE_NAME = "premier_injuries"

    def __init__(self):
        self.items: List[Dict] = []

    def _navigate(self) -> bool:
        resp = _wb_request("navigate", {"url": self.URL, "newTab": True}, self.SESSION)
        if not resp.get("ok"):
            print(f"[PI] Navigate failed: {resp.get('error')}")
            return False
        # Wait for page load
        time.sleep(2.5)
        return True

    def _extract(self) -> List[Dict]:
        """Extract injury rows grouped by team from the page."""
        js = """
        (() => {
            const results = [];
            const tbl = document.querySelector("table");
            if (!tbl) return results;
            let currentTeam = "";
            Array.from(tbl.querySelectorAll("tr")).forEach(tr => {
                if (tr.classList.contains("heading")) {
                    const teamEl = tr.querySelector("div.injury-team");
                    currentTeam = teamEl ? teamEl.textContent.trim() : "";
                    return;
                }
                const cells = Array.from(tr.querySelectorAll("td")).map(td => td.textContent.trim()).filter(x => x);
                if (cells.length >= 2 && currentTeam) {
                    const player = cells[0].replace(/^Player/, "").trim();
                    const reason = cells[1].replace(/^Reason/, "").trim();
                    if (player && player !== "Player" && reason && reason !== "Reason") {
                        results.push({
                            team: currentTeam,
                            player: player,
                            reason: reason,
                            status: cells[5] ? cells[5].replace("Status", "").trim() : "",
                            return_date: cells[3] ? cells[3].replace("Potential Return", "").trim() : "",
                            condition: cells[4] ? cells[4].replace("Condition", "").trim() : ""
                        });
                    }
                }
            });
            return results;
        })()
        """
        raw = _evaluate_js(js, self.SESSION)
        if not raw:
            return []
        cleaned = []
        for row in raw:
            team = normalize_team_name(row.get("team", ""))
            player = row.get("player", "").strip()
            reason = row.get("reason", "").strip()
            if not team or not player or not reason:
                continue
            # Map status to our schema
            status = row.get("status", "").lower()
            if "ruled out" in status or status == "0%":
                mapped_status = "injured"
            elif "suspended" in reason.lower():
                mapped_status = "suspended"
            elif "fitness test" in row.get("condition", "").lower():
                mapped_status = "doubtful"
            elif status in ("100%", "75%"):
                mapped_status = "returning"
            else:
                mapped_status = "injured"

            cleaned.append({
                "player": player,
                "team": team,
                "status": mapped_status,
                "reason": f"{reason}. {row.get('condition', '')}".strip(". "),
                "source": self.SOURCE_NAME,
                "confidence": "high",
                "return_date": row.get("return_date", ""),
            })
        return cleaned

    def fetch(self) -> List[Dict]:
        print("[Premier Injuries] Opening browser tab...")
        if not self._navigate():
            return []
        print("[Premier Injuries] Extracting injury table...")
        items = self._extract()
        print(f"[Premier Injuries] Extracted {len(items)} items")
        return items


class BetinfScraper:
    """Scrape injury data from betinf.com for Bundesliga, Serie A, La Liga, Ligue 1."""

    LEAGUE_URLS = {
        "Bundesliga": "https://www.betinf.com/germany_injured.htm",
        "SerieA": "https://www.betinf.com/italy_injured.htm",
        "LaLiga": "https://www.betinf.com/spain_injured.htm",
        "L1": "https://www.betinf.com/france_injured.htm",
    }
    SOURCE_NAME = "betinf"

    def __init__(self, leagues: Optional[List[str]] = None):
        self.leagues = leagues or list(self.LEAGUE_URLS.keys())

    def _navigate(self, url: str, session: str) -> bool:
        resp = _wb_request("navigate", {"url": url, "newTab": True}, session)
        if not resp.get("ok"):
            print(f"[Betinf] Navigate failed: {resp.get('error')}")
            return False
        time.sleep(2.5)
        return True

    def _extract(self, session: str, league: str) -> List[Dict]:
        js = """
        (() => {
            const results = [];
            document.querySelectorAll("h3.exph").forEach(h3 => {
                const team = h3.textContent.trim();
                let next = h3.nextElementSibling;
                if (!next) return;
                const tbl = next.tagName === "TABLE" ? next : next.querySelector("table");
                if (!tbl) return;
                Array.from(tbl.querySelectorAll("tbody tr")).forEach(tr => {
                    const cells = Array.from(tr.querySelectorAll("td")).map(td => td.textContent.trim());
                    if (cells.length >= 2) {
                        const player = cells[1].replace(/\\s*\\([^)]+\\)/, "").trim();
                        const posMatch = cells[1].match(/\\(([^)]+)\\)/);
                        const position = posMatch ? posMatch[1] : "";
                        const injury = cells[4] || "";
                        const status = cells[5] || "";
                        results.push({ team, player, position, injury, status });
                    }
                });
            });
            return results;
        })()
        """
        raw = _evaluate_js(js, session)
        if not raw:
            return []
        cleaned = []
        for row in raw:
            team = normalize_team_name(row.get("team", ""))
            player = row.get("player", "").strip()
            injury = row.get("injury", "").strip()
            status_code = (row.get("status") or "").strip()
            if not team or not player:
                continue
            # Map betinf status codes
            if status_code == "s":
                mapped_status = "suspended"
                reason = f"{injury} (suspended)" if injury else "suspended"
            elif status_code == "-":
                mapped_status = "injured"
                reason = f"{injury} (ruled out)" if injury else "ruled out"
            elif status_code == "?-":
                mapped_status = "doubtful"
                reason = f"{injury} (unlikely to play)" if injury else "unlikely to play"
            elif status_code == "?":
                mapped_status = "doubtful"
                reason = f"{injury} (uncertain)" if injury else "uncertain"
            elif status_code == "?+":
                mapped_status = "returning"
                reason = f"{injury} (likely to play)" if injury else "likely to play"
            else:
                mapped_status = "injured"
                reason = injury or "injured"
            cleaned.append({
                "player": player,
                "team": team,
                "status": mapped_status,
                "reason": reason,
                "source": self.SOURCE_NAME,
                "confidence": "high",
                "return_date": "",
            })
        return cleaned

    def fetch(self) -> List[Dict]:
        all_items = []
        for league in self.leagues:
            url = self.LEAGUE_URLS.get(league)
            if not url:
                continue
            session = f"rollo-news-{league.lower()}"
            print(f"[Betinf {league}] Opening browser tab...")
            if not self._navigate(url, session):
                continue
            items = self._extract(session, league)
            print(f"[Betinf {league}] Extracted {len(items)} items")
            all_items.extend(items)
            # close tab to avoid piling up
            _wb_request("close_tab", {}, session)
        return all_items


class ManualJsonSource:
    """Read team news from a local JSON file."""

    DEFAULT_PATH = DATA_DIR / "team_news_weekly.json"

    def __init__(self, path: Optional[Path] = None):
        self.path = path or self.DEFAULT_PATH

    def fetch(self) -> List[Dict]:
        if not self.path.exists():
            return []
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[Manual JSON] Failed to read {self.path}: {e}")
            return []

        items = data.get("items", []) if isinstance(data, dict) else data
        cleaned = []
        for item in items:
            team = normalize_team_name(item.get("team", ""))
            if not team:
                continue
            cleaned.append({
                "player": item.get("player", ""),
                "team": team,
                "status": item.get("status", "injured"),
                "reason": item.get("reason", ""),
                "source": item.get("source", "manual"),
                "confidence": item.get("confidence", "medium"),
                "return_date": item.get("return_date", ""),
            })
        print(f"[Manual JSON] Loaded {len(cleaned)} items from {self.path}")
        return cleaned


class TeamNewsDB:
    """Persist team news to SQLite."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def _ensure_table(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS team_news (
                id INTEGER PRIMARY KEY,
                player TEXT,
                team TEXT,
                status TEXT,
                reason TEXT,
                source TEXT,
                confidence TEXT,
                return_date TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def clear_stale(self, days: int = 7):
        """Remove entries older than N days."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute(
            "DELETE FROM team_news WHERE fetched_at < datetime('now', '-{} day')".format(days)
        )
        deleted = c.rowcount
        conn.commit()
        conn.close()
        if deleted:
            print(f"[DB] Cleared {deleted} stale news items")

    def save(self, items: List[Dict]):
        self._ensure_table()
        if not items:
            return 0
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        inserted = 0
        for item in items:
            c.execute("""
                INSERT INTO team_news (player, team, status, reason, source, confidence, return_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item.get("player", ""),
                item["team"],
                item.get("status", "injured"),
                item.get("reason", ""),
                item.get("source", "unknown"),
                item.get("confidence", "medium"),
                item.get("return_date", ""),
            ))
            inserted += 1
        conn.commit()
        conn.close()
        print(f"[DB] Saved {inserted} news items")
        return inserted

    def summary(self) -> Dict[str, int]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT team, COUNT(*) FROM team_news GROUP BY team")
        rows = c.fetchall()
        conn.close()
        return {team: count for team, count in rows}


def run_browser_scraper(
    sources: Optional[List[str]] = None,
    manual_path: Optional[Path] = None,
    clear_stale: bool = True,
    dry_run: bool = False,
) -> List[Dict]:
    """
    Main entry point. Fetch from configured sources and save to DB.

    Args:
        sources: List of source names ('premier_injuries', 'betinf', 'manual').
                 Default is all.
        manual_path: Override path for manual JSON.
        clear_stale: Remove entries older than 7 days before saving.
        dry_run: Fetch but don't write to DB.
    """
    if sources is None:
        sources = ["premier_injuries", "betinf", "manual"]

    all_items: List[Dict] = []

    for source in sources:
        if source == "premier_injuries":
            scraper = PremierInjuriesScraper()
            items = scraper.fetch()
            all_items.extend(items)
        elif source == "betinf":
            scraper = BetinfScraper()
            items = scraper.fetch()
            all_items.extend(items)
        elif source == "manual":
            src = ManualJsonSource(path=manual_path)
            items = src.fetch()
            all_items.extend(items)
        else:
            print(f"[WARN] Unknown source: {source}")

    if not all_items:
        print("[WARN] No team news items fetched")
        return []

    # Deduplicate by (player, team) — prefer higher confidence, then newer source
    seen: Dict[Tuple[str, str], Dict] = {}
    for item in all_items:
        key = (item.get("player", "").lower(), item["team"].lower())
        existing = seen.get(key)
        if existing is None:
            seen[key] = item
            continue
        # Prefer high confidence
        conf_order = {"high": 3, "medium": 2, "low": 1}
        if conf_order.get(item.get("confidence", ""), 0) > conf_order.get(existing.get("confidence", ""), 0):
            seen[key] = item

    deduped = list(seen.values())
    print(f"[AGG] {len(all_items)} raw -> {len(deduped)} deduplicated")

    if dry_run:
        print("[DRY RUN] Would save the following items:")
        for item in deduped[:20]:
            print(f"  {item['team']}: {item.get('player','')} — {item['status']} ({item['source']})")
        if len(deduped) > 20:
            print(f"  ... and {len(deduped) - 20} more")
        return deduped

    db = TeamNewsDB()
    if clear_stale:
        db.clear_stale(days=7)
    db.save(deduped)
    summary = db.summary()
    if summary:
        print("[DB] Teams with news:")
        for team, count in sorted(summary.items(), key=lambda x: -x[1])[:10]:
            print(f"  {team}: {count}")
    return deduped


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape team news into RolloStake DB")
    parser.add_argument(
        "--sources",
        default="premier_injuries,betinf,manual",
        help="Comma-separated source list (default: premier_injuries,betinf,manual)",
    )
    parser.add_argument("--manual-path", type=Path, default=None, help="Path to manual JSON")
    parser.add_argument("--dry-run", action="store_true", help="Fetch without writing to DB")
    parser.add_argument("--keep-stale", action="store_true", help="Don't clear old entries")
    args = parser.parse_args()

    source_list = [s.strip() for s in args.sources.split(",") if s.strip()]
    run_browser_scraper(
        sources=source_list,
        manual_path=args.manual_path,
        clear_stale=not args.keep_stale,
        dry_run=args.dry_run,
    )
