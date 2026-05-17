#!/usr/bin/env python3
"""Import completed match scores and settle affected picks."""

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DB_PATH
from models.core import init_db
from utils.match_resolver import parse_kickoff_utc, resolve_match_id


def _decision(win: bool, loss: bool):
    if win:
        return "win"
    if loss:
        return "loss"
    return "push"


def _settle_selection(selection, market, home_team, away_team, home_goals, away_goals):
    total_goals = home_goals + away_goals

    if market == "1X2":
        if selection == "Draw":
            return "win" if home_goals == away_goals else "loss"
        if home_team in selection:
            return "win" if home_goals > away_goals else "loss"
        if away_team in selection:
            return "win" if away_goals > home_goals else "loss"

    if "DNB" in selection:
        if home_goals == away_goals:
            return "push"
        if home_team in selection:
            return "win" if home_goals > away_goals else "loss"
        if away_team in selection:
            return "win" if away_goals > home_goals else "loss"

    if market == "BTTS":
        both_scored = home_goals > 0 and away_goals > 0
        if "BTTS Yes" in selection:
            return "win" if both_scored else "loss"
        if "BTTS No" in selection:
            return "loss" if both_scored else "win"

    if market == "OU":
        match = re.search(r"\b(Over|Under)\s+(\d+(?:\.\d+)?)\b", selection, re.IGNORECASE)
        if match:
            direction = match.group(1).lower()
            line = float(match.group(2))
            if direction == "over":
                return _decision(total_goals > line, total_goals < line)
            return _decision(total_goals < line, total_goals > line)

    if market == "TT":
        match = re.search(r"\b(O|U|Over|Under)\s*(\d+(?:\.\d+)?)\b", selection, re.IGNORECASE)
        if match:
            if home_team in selection:
                team_goals = home_goals
            elif away_team in selection:
                team_goals = away_goals
            else:
                return None
            direction = match.group(1).lower()
            line = float(match.group(2))
            if direction in ("o", "over"):
                return _decision(team_goals > line, team_goals < line)
            return _decision(team_goals < line, team_goals > line)

    if market == "AH":
        match = re.search(r"(?:\bAH\s*)?([+-]?\d+(?:\.\d+)?)\s*$", selection, re.IGNORECASE)
        if match:
            handicap = float(match.group(1))
            if home_team in selection:
                margin = home_goals + handicap - away_goals
            elif away_team in selection:
                margin = away_goals + handicap - home_goals
            else:
                return None
            return _decision(margin > 0, margin < 0)

    return None


def _same_fixture_window(primary_kickoff: str, candidate_kickoff: str) -> bool:
    primary = parse_kickoff_utc(primary_kickoff)
    candidate = parse_kickoff_utc(candidate_kickoff)
    if primary is None or candidate is None:
        return False
    return abs((candidate - primary).total_seconds()) <= 3 * 24 * 60 * 60


def _duplicate_match_ids(c, match_id: str, home_team: str, away_team: str) -> list[str]:
    c.execute("SELECT kickoff FROM matches WHERE match_id = ?", (match_id,))
    row = c.fetchone()
    primary_kickoff = row[0] if row else ""

    c.execute(
        """
        SELECT match_id, kickoff
        FROM matches
        WHERE home_team = ? AND away_team = ?
          AND status IN ('scheduled', 'stale')
        ORDER BY kickoff
        """,
        (home_team, away_team),
    )
    duplicates = [match_id]
    for candidate_id, candidate_kickoff in c.fetchall():
        if candidate_id == match_id:
            continue
        if _same_fixture_window(primary_kickoff, candidate_kickoff):
            duplicates.append(candidate_id)
    return duplicates


def update_results(match_id, result=None, home_goals=None, away_goals=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT home_team, away_team FROM matches WHERE match_id = ?", (match_id,))
    match_row = c.fetchone()
    if not match_row:
        conn.close()
        raise ValueError(f"No match found with id {match_id}")
    home_team, away_team = match_row

    match_ids = _duplicate_match_ids(c, match_id, home_team, away_team)

    for resolved_match_id in match_ids:
        c.execute(
            """
            UPDATE matches SET status = ?, home_goals = ?, away_goals = ?
            WHERE match_id = ?
            """,
            ("completed", home_goals, away_goals, resolved_match_id),
        )

    c.execute(
        f"""
        SELECT id, match_id, selection, market, odds, stake, range_code, quality
        FROM picks
        WHERE match_id IN ({",".join("?" for _ in match_ids)})
        """,
        match_ids,
    )

    settled = []
    for pick_id, pick_match_id, selection, market, odds, stake, range_code, quality in c.fetchall():
        pick_result = None
        if home_goals is not None and away_goals is not None:
            pick_result = _settle_selection(
                selection, market, home_team, away_team, home_goals, away_goals
            )
        if pick_result is None:
            pick_result = result
        if pick_result not in {"win", "loss", "push"}:
            conn.close()
            raise ValueError(f"Could not settle pick {pick_id}: {market} {selection}")

        if pick_result == "win":
            pnl = stake * (odds - 1)
            payout = stake + pnl
        elif pick_result == "loss":
            pnl = -stake
            payout = 0
        else:
            pnl = 0
            payout = stake

        c.execute(
            """
            UPDATE picks
            SET status = 'settled', result = ?, pnl = ?, payout = ?,
                settled_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (pick_result, pnl, payout, pick_id),
        )
        c.execute("DELETE FROM results WHERE pick_id = ?", (pick_id,))
        c.execute(
            """
            INSERT INTO results
            (pick_id, match_id, range_code, quality, result, home_goals, away_goals,
             stake, odds, payout, pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (pick_id, pick_match_id, range_code, quality, pick_result, home_goals, away_goals,
             stake, odds, payout, pnl),
        )
        settled.append((pick_id, selection, pick_result, pnl))

    conn.commit()
    conn.close()

    print(f"Updated result for {match_id}: {home_team} {home_goals}-{away_goals} {away_team}")
    if len(match_ids) > 1:
        print(f"  Also marked duplicate fixture ids completed: {', '.join(match_ids[1:])}")
    for pick_id, selection, pick_result, pnl in settled:
        print(f"  Pick {pick_id} {selection}: {pick_result} ({pnl:+.2f})")


def import_results(path: str) -> dict:
    init_db()
    imported = 0
    skipped = 0
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            match_id = resolve_match_id(row, statuses=("scheduled", "stale"))
            if not match_id:
                skipped += 1
                continue
            try:
                home_goals = int(row["home_goals"])
                away_goals = int(row["away_goals"])
            except (KeyError, TypeError, ValueError):
                skipped += 1
                continue
            update_results(match_id, "auto", home_goals, away_goals)
            imported += 1
    return {"imported": imported, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(description="Import match results from CSV")
    parser.add_argument("csv_path", nargs="?", default="match_results.csv")
    args = parser.parse_args()
    summary = import_results(args.csv_path)
    print(f"Match results imported: {summary['imported']} skipped: {summary['skipped']}")


if __name__ == "__main__":
    main()
