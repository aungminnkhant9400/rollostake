#!/usr/bin/env python3
"""
Overnight AutoResearch loop.

This is the agent-style layer on top of autoresearch.py and gpu_autoresearch.py:
it runs broad experiments, reads the generated breakdowns, chooses follow-up
experiments from the strongest slices, and writes a morning report.
"""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = PROJECT_ROOT / "research" / "results"


@dataclass
class Experiment:
    name: str
    runner: str
    args: List[str]
    reason: str


@dataclass
class ExperimentResult:
    name: str
    runner: str
    reason: str
    command: List[str]
    returncode: int
    stdout_tail: str
    stderr_tail: str
    best: Optional[Dict] = None
    leaderboard_csv: Optional[str] = None
    best_config_json: Optional[str] = None
    breakdown_csv: Optional[str] = None
    best_picks_csv: Optional[str] = None
    positive_slices: Dict[str, List[Dict]] = field(default_factory=dict)


def split_csv(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def command_line(command: Sequence[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def tail(text: str, lines: int = 80) -> str:
    raw_lines = text.splitlines()
    return "\n".join(raw_lines[-lines:])


def extract_output_paths(stdout: str) -> Dict[str, str]:
    mapping = {
        "Leaderboard:": "leaderboard_csv",
        "Best config JSON:": "best_config_json",
        "Breakdown CSV:": "breakdown_csv",
        "Best picks CSV:": "best_picks_csv",
    }
    paths = {}
    for line in stdout.splitlines():
        for prefix, key in mapping.items():
            if line.startswith(prefix):
                paths[key] = line.split(":", 1)[1].strip()
    return paths


def load_best(path: Optional[str]) -> Optional[Dict]:
    if not path:
        return None
    candidate = Path(path)
    if not candidate.exists():
        return None
    with candidate.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload.get("best")


def load_breakdown(path: Optional[str]) -> List[Dict]:
    if not path:
        return []
    candidate = Path(path)
    if not candidate.exists():
        return []
    with candidate.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def positive_slices(
    rows: Sequence[Dict],
    min_picks: int,
    min_roi: float,
    group_types: Sequence[str],
) -> Dict[str, List[Dict]]:
    selected = {}
    for group_type in group_types:
        subset = []
        for row in rows:
            if row.get("group_type") != group_type:
                continue
            try:
                picks = int(row["picks"])
                roi = float(row["roi_pct"])
            except (KeyError, ValueError):
                continue
            if picks >= min_picks and roi >= min_roi:
                subset.append(row)
        subset.sort(key=lambda row: (float(row["roi_pct"]), int(row["picks"])), reverse=True)
        selected[group_type] = subset[:8]
    return selected


def run_experiment(exp: Experiment, timeout_minutes: int) -> ExperimentResult:
    runner_path = PROJECT_ROOT / "research" / exp.runner
    command = [sys.executable, str(runner_path), *exp.args]
    print(f"\n=== {exp.name} ===", flush=True)
    print(exp.reason, flush=True)
    print(command_line(command), flush=True)

    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        timeout=timeout_minutes * 60,
    )
    print(tail(completed.stdout, 30), flush=True)
    if completed.stderr:
        print(tail(completed.stderr, 20), flush=True)

    paths = extract_output_paths(completed.stdout)
    breakdown_rows = load_breakdown(paths.get("breakdown_csv"))
    result = ExperimentResult(
        name=exp.name,
        runner=exp.runner,
        reason=exp.reason,
        command=command,
        returncode=completed.returncode,
        stdout_tail=tail(completed.stdout),
        stderr_tail=tail(completed.stderr),
        best=load_best(paths.get("best_config_json")),
        leaderboard_csv=paths.get("leaderboard_csv"),
        best_config_json=paths.get("best_config_json"),
        breakdown_csv=paths.get("breakdown_csv"),
        best_picks_csv=paths.get("best_picks_csv"),
        positive_slices=positive_slices(
            breakdown_rows,
            min_picks=10,
            min_roi=2.0,
            group_types=("league", "market", "range", "selection_type", "odds_bucket", "edge_bucket"),
        ),
    )
    return result


def base_gpu_args(args, markets: str, leagues: Sequence[str], name_suffix: str = "") -> List[str]:
    cmd = [
        "--seasons",
        *args.seasons,
        "--leagues",
        *leagues,
        "--device",
        args.device,
        "--markets",
        markets,
        "--candidate-markets",
        args.candidate_markets,
        "--train-size-per-league",
        str(args.train_size_per_league),
        "--min-train-size-per-league",
        str(args.min_train_size_per_league),
        "--batch-days",
        str(args.batch_days),
        "--epochs",
        str(args.epochs),
        "--c-max-picks",
        args.c_max_picks,
        "--d-max-picks",
        args.d_max_picks,
        "--min-edges",
        args.min_edges,
        "--max-picks-per-match",
        "1",
        "--top",
        str(args.top),
    ]
    if args.quick:
        cmd.append("--quick")
    if args.refresh_cache:
        cmd.append("--refresh-cache")
    return cmd


def initial_experiments(args) -> List[Experiment]:
    leagues = args.leagues
    experiments = [
        Experiment(
            name="gpu_broad_1x2_ou",
            runner="gpu_autoresearch.py",
            args=base_gpu_args(args, "1X2,OU", leagues),
            reason="Broad GPU Dixon-Coles test across all requested leagues for 1X2 and totals.",
        ),
        Experiment(
            name="gpu_1x2_only",
            runner="gpu_autoresearch.py",
            args=base_gpu_args(args, "1X2", leagues),
            reason="Isolate 1X2 to compare against the current production-like strategy.",
        ),
        Experiment(
            name="gpu_ou_only",
            runner="gpu_autoresearch.py",
            args=base_gpu_args(args, "OU", leagues),
            reason="Isolate over/under because the user wants totals included.",
        ),
    ]
    return experiments[: args.initial_experiments]


def adaptive_experiments(args, results: Sequence[ExperimentResult]) -> List[Experiment]:
    proposed = []
    seen = set()

    for result in results:
        leagues = [
            row["group"]
            for row in result.positive_slices.get("league", [])
            if row["group"] in args.leagues
        ][:3]
        markets = [
            row["group"]
            for row in result.positive_slices.get("market", [])
            if row["group"] in ("1X2", "OU")
        ]
        if not leagues or not markets:
            continue

        market_arg = ",".join(markets)
        key = (tuple(leagues), market_arg)
        if key in seen:
            continue
        seen.add(key)

        proposed.append(
            Experiment(
                name=f"adaptive_{market_arg.lower().replace(',', '_')}_{'_'.join(leagues)}",
                runner="gpu_autoresearch.py",
                args=base_gpu_args(args, market_arg, leagues),
                reason=(
                    "Adaptive follow-up chosen from positive breakdown slices: "
                    f"leagues={','.join(leagues)} markets={market_arg}."
                ),
            )
        )

    return proposed[: args.adaptive_experiments]


def result_sort_key(result: ExperimentResult):
    if not result.best:
        return (-999999.0, -999999.0, 0)
    return (
        float(result.best.get("score", -999999.0)),
        float(result.best.get("roi_pct", -999999.0)),
        int(result.best.get("picks", 0)),
    )


def render_report(results: Sequence[ExperimentResult], started_at: datetime, finished_at: datetime) -> str:
    ranked = sorted(results, key=result_sort_key, reverse=True)
    lines = [
        "# Rollo Stake Overnight AutoResearch Report",
        "",
        f"Started: {started_at.isoformat(timespec='seconds')}",
        f"Finished: {finished_at.isoformat(timespec='seconds')}",
        f"Experiments: {len(results)}",
        "",
        "## Leaderboard",
        "",
        "| Rank | Experiment | ROI | Score | Picks | Record | Best Config |",
        "|---:|---|---:|---:|---:|---|---|",
    ]
    for rank, result in enumerate(ranked, start=1):
        best = result.best or {}
        record = f"{best.get('wins', 0)}-{best.get('losses', 0)}-{best.get('pushes', 0)}"
        lines.append(
            "| "
            f"{rank} | {result.name} | {float(best.get('roi_pct', 0)):+.2f}% | "
            f"{float(best.get('score', 0)):+.2f} | {int(best.get('picks', 0))} | "
            f"{record} | `{best.get('config_id', 'n/a')}` |"
        )

    lines.extend(["", "## Best Experiment Details", ""])
    if ranked and ranked[0].best:
        best_result = ranked[0]
        best = best_result.best
        lines.extend(
            [
                f"Best experiment: `{best_result.name}`",
                f"Reason: {best_result.reason}",
                f"Command: `{command_line(best_result.command)}`",
                f"ROI: {float(best.get('roi_pct', 0)):+.2f}%",
                f"Picks: {best.get('picks', 0)}",
                f"Record: {best.get('wins', 0)}-{best.get('losses', 0)}-{best.get('pushes', 0)}",
                f"Best config JSON: `{best_result.best_config_json}`",
                f"Breakdown CSV: `{best_result.breakdown_csv}`",
                f"Best picks CSV: `{best_result.best_picks_csv}`",
                "",
            ]
        )

    lines.extend(["## Positive Slices", ""])
    for result in ranked:
        if not result.positive_slices:
            continue
        lines.append(f"### {result.name}")
        for group_type, rows in result.positive_slices.items():
            if not rows:
                continue
            lines.append(f"- {group_type}:")
            for row in rows[:5]:
                lines.append(
                    f"  - `{row['group']}` picks={row['picks']} "
                    f"roi={float(row['roi_pct']):+.2f}% pnl={float(row['pnl']):+.2f}"
                )
        lines.append("")

    lines.extend(["## Files", ""])
    for result in results:
        lines.append(f"- `{result.name}`")
        for label, value in (
            ("leaderboard", result.leaderboard_csv),
            ("best_config", result.best_config_json),
            ("breakdown", result.breakdown_csv),
            ("best_picks", result.best_picks_csv),
        ):
            if value:
                lines.append(f"  - {label}: `{value}`")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Run an overnight adaptive AutoResearch loop")
    parser.add_argument("--seasons", nargs="+", default=["2122", "2223", "2324", "2425", "2526"])
    parser.add_argument("--leagues", nargs="+", default=["EPL", "L1", "Bundesliga", "SerieA", "LaLiga"])
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--candidate-markets", default="1X2,OU")
    parser.add_argument("--train-size-per-league", type=int, default=300)
    parser.add_argument("--min-train-size-per-league", type=int, default=120)
    parser.add_argument("--batch-days", type=int, default=14)
    parser.add_argument("--epochs", type=int, default=500)
    parser.add_argument("--c-max-picks", default="0,1,2")
    parser.add_argument("--d-max-picks", default="1,2,3")
    parser.add_argument("--min-edges", default="0.15,0.25,0.35")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--initial-experiments", type=int, default=3)
    parser.add_argument("--adaptive-experiments", type=int, default=4)
    parser.add_argument("--timeout-minutes", type=int, default=240)
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()

    started_at = datetime.now()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results: List[ExperimentResult] = []

    for exp in initial_experiments(args):
        results.append(run_experiment(exp, args.timeout_minutes))

    for exp in adaptive_experiments(args, results):
        results.append(run_experiment(exp, args.timeout_minutes))

    finished_at = datetime.now()
    timestamp = finished_at.strftime("%Y%m%d_%H%M%S")
    report_path = RESULTS_DIR / f"autoloop_report_{timestamp}.md"
    json_path = RESULTS_DIR / f"autoloop_results_{timestamp}.json"

    report_path.write_text(render_report(results, started_at, finished_at), encoding="utf-8")
    json_payload = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "results": [
            {
                **result.__dict__,
                "command": result.command,
            }
            for result in results
        ],
    }
    json_path.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")

    print(f"\nAutoLoop report: {report_path}")
    print(f"AutoLoop JSON: {json_path}")


if __name__ == "__main__":
    main()
