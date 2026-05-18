#!/usr/bin/env python3
"""Study an external weekly card and save aggregate, non-pick lessons.

The output intentionally stores market-shape counts instead of exact picks.
That lets our model learn process ideas without copying another card.
"""

import argparse
import html
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.paths import DATA_DIR


def _strip_tags(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _extract_ranges(page_html: str) -> dict:
    ranges = _extract_pick_cards(page_html)
    if ranges["c"] or ranges["d"]:
        return ranges

    match = re.search(r"const\s+D\s*=\s*\{\s*c\s*:\s*(\[.*?\])\s*,\s*d\s*:\s*(\[.*?\])\s*\}\s*;", page_html, re.S)
    if not match:
        raise ValueError("Could not find the external card pick arrays.")
    return {
        "c": json.loads(match.group(1)),
        "d": json.loads(match.group(2)),
    }


def _extract_pick_cards(page_html: str) -> dict:
    return {
        "c": _extract_range_cards(page_html, "c"),
        "d": _extract_range_cards(page_html, "d"),
    }


def _extract_range_cards(page_html: str, code: str) -> list[dict]:
    section_match = re.search(
        rf'<div class="rng[^"]*" id="r-{code}">(.*?)(?=<div class="rng[^"]*" id="r-[cd]">|<script|</body>)',
        page_html,
        re.S | re.I,
    )
    if not section_match:
        return []

    section = section_match.group(1)
    cards = []
    pattern = re.compile(
        r'<div class="pn">\s*(.*?)\s*<span class="flag".*?'
        r'<div class="pm">(.*?)</div>.*?'
        r'<div class="nv accent">@([0-9.]+)</div>',
        re.S | re.I,
    )
    for rank, match in enumerate(pattern.finditer(section), 1):
        name = _strip_tags(match.group(1))
        meta = _strip_tags(match.group(2))
        matchup = re.split(r"\s*(?:·|\u00b7|\&middot;|-)\s*", meta, maxsplit=1)[0].strip()
        cards.append(
            {
                "rank": rank,
                "name": name,
                "match": matchup,
                "odds": float(match.group(3)),
                "stake": 200,
            }
        )
    return cards


def _teams(match_name: str) -> tuple[str, str]:
    parts = re.split(r"\s+vs\s+", match_name or "", flags=re.I)
    if len(parts) != 2:
        return "", ""
    return parts[0].strip(), parts[1].strip()


def _normalise_name(value: str) -> str:
    value = (value or "").lower()
    replacements = {
        "manchester united": "man utd",
        "manchester city": "man city",
        "nottingham forest": "nott forest",
        "parma calcio 1913": "parma",
        "paris saint germain": "psg",
    }
    for full, short in replacements.items():
        value = value.replace(full, short)
    return re.sub(r"[^a-z0-9]+", "", value)


def _classify(pick: dict) -> dict:
    name = str(pick.get("name") or "").strip()
    match_name = str(pick.get("match") or "").strip()
    home, away = _teams(match_name)
    market = "OTHER"
    side = "other"
    line = ""

    if re.search(r"\bDNB\b", name, re.I):
        market = "AH"
        line = "ah-0"
    elif re.search(r"\s[+-]\d+(?:\.\d+)?$", name):
        market = "AH"
        line_match = re.search(r"([+-]\d+(?:\.\d+)?)$", name)
        line = f"ah-{line_match.group(1)}" if line_match else ""
    elif re.search(r"^Under\s+\d+(?:\.\d+)?$", name, re.I):
        market = "OU"
        side = "under"
        line = "goal-line-" + re.search(r"(\d+(?:\.\d+)?)", name).group(1)
    elif re.search(r"^Over\s+\d+(?:\.\d+)?$", name, re.I):
        market = "OU"
        side = "over"
        line = "goal-line-" + re.search(r"(\d+(?:\.\d+)?)", name).group(1)
    elif re.search(r"\bU\d+(?:\.\d+)?$", name, re.I):
        market = "TT"
        side = "under"
        line = "goal-line-" + re.search(r"U(\d+(?:\.\d+)?)$", name, re.I).group(1)
    elif re.search(r"\bO\d+(?:\.\d+)?$", name, re.I):
        market = "TT"
        side = "over"
        line = "goal-line-" + re.search(r"O(\d+(?:\.\d+)?)$", name, re.I).group(1)
    elif re.search(r"\bBTTS\s+No\b", name, re.I):
        market = "BTTS"
        side = "under"
        line = "BTTS No"
    elif re.search(r"\bBTTS\s+Yes\b", name, re.I):
        market = "BTTS"
        side = "over"
        line = "BTTS Yes"
    elif re.search(r"\bWin$", name, re.I):
        market = "1X2"

    if side == "other":
        if market in ("1X2", "AH"):
            name_key = _normalise_name(name)
            home_key = _normalise_name(home)
            away_key = _normalise_name(away)
            if home_key and home_key in name_key:
                side = "home"
            elif away_key and away_key in name_key:
                side = "away"
        elif "Under" in name or re.search(r"\bU\d", name):
            side = "under"
        elif "Over" in name or re.search(r"\bO\d", name):
            side = "over"

    if market == "1X2":
        line = "1X2"

    return {
        "market": market,
        "selection_type": side,
        "line": line,
        "odds_bucket": _odds_bucket(float(pick.get("odds") or 0)),
    }


def _odds_bucket(odds: float) -> str:
    if odds <= 0:
        return ""
    if odds < 2.0:
        return "odds-1.70-1.99"
    if odds < 2.5:
        return "odds-2.00-2.49"
    if odds < 3.0:
        return "odds-2.50-2.99"
    if odds < 4.0:
        return "odds-3.00-3.99"
    return "odds-4.00-plus"


def _range_profile(picks: list[dict]) -> dict:
    market_counts = Counter()
    side_counts = Counter()
    line_counts = Counter()
    odds_counts = Counter()
    for pick in picks:
        shape = _classify(pick)
        market_counts[shape["market"]] += 1
        side_counts[shape["selection_type"]] += 1
        if shape["line"]:
            line_counts[shape["line"]] += 1
        if shape["odds_bucket"]:
            odds_counts[shape["odds_bucket"]] += 1

    total = max(len(picks), 1)
    return {
        "picks": len(picks),
        "market_counts": dict(market_counts),
        "selection_type_counts": dict(side_counts),
        "line_counts": dict(line_counts),
        "odds_bucket_counts": dict(odds_counts),
        "market_share": {key: value / total for key, value in market_counts.items()},
    }


def _empty_range_profile() -> dict:
    return {
        "picks": 0,
        "market_counts": {},
        "selection_type_counts": {},
        "line_counts": {},
        "odds_bucket_counts": {},
        "market_share": {},
    }


def _merge_range_profiles(profiles: list[dict]) -> dict:
    total = sum(item.get("picks", 0) for item in profiles)
    if total <= 0:
        return _empty_range_profile()

    merged = {
        "picks": total,
        "market_counts": dict(sum((Counter(item.get("market_counts", {})) for item in profiles), Counter())),
        "selection_type_counts": dict(sum((Counter(item.get("selection_type_counts", {})) for item in profiles), Counter())),
        "line_counts": dict(sum((Counter(item.get("line_counts", {})) for item in profiles), Counter())),
        "odds_bucket_counts": dict(sum((Counter(item.get("odds_bucket_counts", {})) for item in profiles), Counter())),
        "market_share": {},
    }
    merged["market_share"] = {
        key: value / total
        for key, value in merged["market_counts"].items()
    }
    return merged


def _source_label(source: Path) -> str:
    try:
        return str(source.relative_to(Path.cwd()))
    except ValueError:
        return str(source)


def build_profile(sources: list[Path]) -> dict:
    per_file = []
    combined = {"c": [], "d": []}
    for source in sources:
        page_html = source.read_text(encoding="utf-8", errors="replace")
        ranges = _extract_ranges(page_html)
        file_profile = {
            "source_file": _source_label(source),
            "ranges": {
                code: _range_profile(picks)
                for code, picks in ranges.items()
            },
        }
        per_file.append(file_profile)
        for code in ("c", "d"):
            combined[code].extend(ranges.get(code, []))

    profile = {
        "source_files": [_source_label(source) for source in sources],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "ranges": {
            code: _range_profile(picks)
            for code, picks in combined.items()
        },
        "per_file": per_file,
        "lessons": [
            "Use side-protected AH shapes such as DNB and +0.5 before naked long 1X2 shots.",
            "Study team totals separately from match totals.",
            "Downgrade aggressive handicap lines and unders when recent/H2H/news context fights the model.",
            "Keep external-card influence small; own settled losses must override it.",
        ],
    }
    return profile


def _expand_sources(paths: list[str]) -> list[Path]:
    sources = []
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if path.is_dir():
            sources.extend(sorted(path.glob("*.html")))
        else:
            sources.append(path)
    missing = [str(path) for path in sources if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing external card file(s): " + ", ".join(missing))
    return sources


def main():
    parser = argparse.ArgumentParser(description="Study an external weekly prediction card.")
    parser.add_argument("html_file", nargs="+", help="Path(s) to external card HTML files or folders")
    parser.add_argument("--output", default=str(DATA_DIR / "external_card_profile.json"))
    args = parser.parse_args()

    sources = _expand_sources(args.html_file)
    output = Path(args.output).expanduser().resolve()
    profile = build_profile(sources)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(profile, indent=2), encoding="utf-8")

    print(f"Saved external-card profile: {output}")
    print(f"Studied {len(sources)} file(s)")
    for code, item in profile["ranges"].items():
        print(f"Range {code.upper()}: {item['picks']} picks, markets {item['market_counts']}")


if __name__ == "__main__":
    main()
