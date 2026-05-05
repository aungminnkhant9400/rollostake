#!/usr/bin/env python3
"""
LLM code-editing AutoResearch loop.

This is the Karpathy-style layer: an LLM proposes small code patches, the script
applies each patch in an isolated git worktree, runs GPU AutoResearch, compares
against a baseline, and writes a report with the best patch.

Safety model:
- Requires DEEPSEEK_API_KEY.
- Only allows edits to an explicit file allowlist.
- Runs in detached git worktrees under research/agent_runs/.
- Does not push or modify production by default.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = PROJECT_ROOT / "research" / "results"
RUNS_DIR = PROJECT_ROOT / "research" / "agent_runs"

DEFAULT_ALLOWED_FILES = (
    "models/torch_dixon_coles.py",
    "research/gpu_autoresearch.py",
)


@dataclass
class EvalResult:
    command: str
    returncode: int
    stdout_tail: str
    stderr_tail: str
    best: Optional[Dict] = None
    leaderboard_csv: Optional[str] = None
    best_config_json: Optional[str] = None
    breakdown_csv: Optional[str] = None
    best_picks_csv: Optional[str] = None


@dataclass
class IterationResult:
    iteration: int
    worktree: str
    hypothesis: str
    patch_path: str
    accepted: bool
    reason: str
    eval_result: Optional[EvalResult] = None
    compile_ok: bool = False
    apply_ok: bool = False
    errors: List[str] = field(default_factory=list)
    raw_response_path: Optional[str] = None


def run_cmd(
    command: Sequence[str],
    cwd: Path,
    timeout_seconds: int,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(command),
        cwd=cwd,
        text=True,
        capture_output=capture,
        timeout=timeout_seconds,
    )


def run_shell(command: str, cwd: Path, timeout_seconds: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        cwd=cwd,
        shell=True,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
    )


def tail(text: str, lines: int = 80) -> str:
    return "\n".join(text.splitlines()[-lines:])


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


def evaluate(command: str, cwd: Path, timeout_seconds: int) -> EvalResult:
    completed = run_shell(command, cwd, timeout_seconds)
    paths = extract_output_paths(completed.stdout)
    return EvalResult(
        command=command,
        returncode=completed.returncode,
        stdout_tail=tail(completed.stdout),
        stderr_tail=tail(completed.stderr),
        best=load_best(paths.get("best_config_json")),
        leaderboard_csv=paths.get("leaderboard_csv"),
        best_config_json=paths.get("best_config_json"),
        breakdown_csv=paths.get("breakdown_csv"),
        best_picks_csv=paths.get("best_picks_csv"),
    )


def metric(best: Optional[Dict], key: str, default: float = -999999.0) -> float:
    if not best:
        return default
    try:
        return float(best.get(key, default))
    except (TypeError, ValueError):
        return default


def best_summary(best: Optional[Dict]) -> str:
    if not best:
        return "No successful best config."
    return (
        f"config={best.get('config_id')} "
        f"score={metric(best, 'score'):+.2f} "
        f"roi={metric(best, 'roi_pct'):+.2f}% "
        f"picks={int(metric(best, 'picks', 0))} "
        f"record={best.get('wins', 0)}-{best.get('losses', 0)}-{best.get('pushes', 0)} "
        f"dd={metric(best, 'max_drawdown_pct'):.1f}%"
    )


def read_file_snippet(path: Path, max_chars: int) -> str:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n\n# ... truncated ..."


def latest_reports(limit: int = 3) -> str:
    paths = sorted(RESULTS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]
    chunks = []
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        best = payload.get("best")
        if best:
            chunks.append(f"{path.name}: {best_summary(best)}")
    return "\n".join(chunks) or "No prior JSON result summaries found."


def deepseek_chat(
    messages: List[Dict[str, str]],
    model: str,
    base_url: str,
    api_key: str,
    temperature: float,
    max_tokens: int,
    timeout_seconds: int,
) -> str:
    url = base_url.rstrip("/") + "/chat/completions"
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["choices"][0]["message"]["content"]


def extract_json_object(text: str) -> Dict:
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        return json.loads(fenced.group(1))
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("LLM response did not contain a JSON object")
    return json.loads(text[start : end + 1])


def proposal_prompt(
    baseline: EvalResult,
    previous_results: Sequence[IterationResult],
    allowed_files: Sequence[str],
    max_file_chars: int,
    edit_mode: str,
    repair_error: str = "",
    failed_patch: str = "",
) -> List[Dict[str, str]]:
    file_context = []
    for rel_path in allowed_files:
        path = PROJECT_ROOT / rel_path
        if path.exists():
            file_context.append(f"## {rel_path}\n```python\n{read_file_snippet(path, max_file_chars)}\n```")

    previous = []
    for result in previous_results[-5:]:
        eval_summary = best_summary(result.eval_result.best if result.eval_result else None)
        previous.append(
            f"iteration={result.iteration} accepted={result.accepted} "
            f"hypothesis={result.hypothesis} result={eval_summary} reason={result.reason}"
        )

    system = """
You are a football betting model research agent. Your job is to propose one small
code patch that could improve holdout ROI without exploding drawdown. You must
return only valid JSON.

Rules:
- Edit only files in the allowlist.
- Prefer small, testable changes.
- Keep the Dixon-Coles idea: score distribution, 1X2, and over/under.
- Do not remove safety checks, caching, output reports, or CLI compatibility.
- Do not hard-code one lucky result file or future results.
- Optimize for robust ROI, enough picks, and lower drawdown.
""".strip()

    if edit_mode == "file":
        edit_contract = """
Return JSON with this schema:
{
  "hypothesis": "short explanation",
  "file": "one exact path from the allowlist",
  "content": "complete replacement content for that file",
  "expected_effect": "what metric should improve and why",
  "risk": "what could go wrong"
}

The content field must contain the full file contents, not a diff. Do not use
markdown fences inside the JSON. Keep imports, CLI compatibility, output paths,
and existing safety checks unless the experiment needs a small targeted change.
""".strip()
    else:
        edit_contract = """
Return JSON with this schema:
{
  "hypothesis": "short explanation",
  "patch": "unified diff starting with diff --git ...",
  "expected_effect": "what metric should improve and why",
  "risk": "what could go wrong"
}
""".strip()

    user = f"""
Baseline:
{best_summary(baseline.best)}

Recent result files:
{latest_reports()}

Previous agent attempts:
{chr(10).join(previous) if previous else "None yet."}

Allowed files:
{chr(10).join(allowed_files)}

{edit_contract}

Relevant code:
{chr(10).join(file_context)}
""".strip()

    if repair_error:
        user += f"""

The previous patch failed `git apply --check` with this error:
```text
{repair_error}
```

Previous failed patch:
```diff
{failed_patch}
```

Return a corrected full unified diff. Do not return explanations outside JSON.
""".rstrip()

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def ask_for_proposal(
    baseline: EvalResult,
    previous_results: Sequence[IterationResult],
    allowed_files: Sequence[str],
    args,
    raw_path: Path,
    repair_error: str = "",
    failed_patch: str = "",
) -> Dict:
    messages = proposal_prompt(
        baseline,
        previous_results,
        allowed_files,
        args.max_file_chars,
        args.edit_mode,
        repair_error=repair_error,
        failed_patch=failed_patch,
    )
    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        raise RuntimeError(f"Missing {args.api_key_env}")
    raw = deepseek_chat(
        messages=messages,
        model=args.model,
        base_url=args.base_url,
        api_key=api_key,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        timeout_seconds=args.llm_timeout_seconds,
    )
    raw_path.write_text(raw, encoding="utf-8")
    return extract_json_object(raw)


def changed_files_from_patch(patch: str) -> List[str]:
    files = []
    for line in patch.splitlines():
        if line.startswith("+++ b/"):
            files.append(line.removeprefix("+++ b/").strip())
    return sorted(set(files))


def validate_patch_files(patch: str, allowed_files: Sequence[str]) -> None:
    changed = changed_files_from_patch(patch)
    if not changed:
        raise ValueError("Patch does not modify any files")
    disallowed = [path for path in changed if path not in set(allowed_files)]
    if disallowed:
        raise ValueError(f"Patch modifies disallowed files: {', '.join(disallowed)}")


def apply_file_replacement(
    proposal: Dict,
    worktree: Path,
    allowed_files: Sequence[str],
    patch_path: Path,
) -> List[str]:
    rel_path = str(proposal.get("file", "")).strip()
    content = proposal.get("content")
    if rel_path not in set(allowed_files):
        raise ValueError(f"File replacement targets disallowed file: {rel_path}")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("File replacement response did not include non-empty content")

    target = worktree / rel_path
    target.write_text(content, encoding="utf-8")

    diff = run_cmd(["git", "diff", "--", rel_path], worktree, 60)
    if diff.returncode != 0:
        raise RuntimeError(diff.stderr)
    if not diff.stdout.strip():
        raise ValueError("File replacement produced no diff")
    patch_path.write_text(diff.stdout, encoding="utf-8")
    return [rel_path]


def create_worktree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    run_cmd(["git", "worktree", "add", "--detach", str(path), "HEAD"], PROJECT_ROOT, 120)


def remove_worktree(path: Path) -> None:
    if path.exists():
        run_cmd(["git", "worktree", "remove", "--force", str(path)], PROJECT_ROOT, 120)


def compile_changed_files(worktree: Path, changed_files: Sequence[str], timeout_seconds: int) -> subprocess.CompletedProcess:
    py_files = [path for path in changed_files if path.endswith(".py")]
    if not py_files:
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    return run_cmd([sys.executable, "-m", "py_compile", *py_files], worktree, timeout_seconds)


def run_iteration(
    iteration: int,
    baseline: EvalResult,
    previous_results: Sequence[IterationResult],
    args,
    run_dir: Path,
) -> IterationResult:
    worktree = run_dir / f"iter_{iteration:02d}_worktree"
    patch_path = run_dir / f"iter_{iteration:02d}.patch"
    raw_path = run_dir / f"iter_{iteration:02d}_raw_response.txt"
    allowed_files = tuple(args.allowed_files)
    result = IterationResult(
        iteration=iteration,
        worktree=str(worktree),
        hypothesis="",
        patch_path=str(patch_path),
        accepted=False,
        reason="not evaluated",
    )
    result.raw_response_path = str(raw_path)

    proposal = ask_for_proposal(
        baseline,
        previous_results,
        allowed_files,
        args,
        raw_path,
    )
    result.hypothesis = str(proposal.get("hypothesis", ""))

    create_worktree(worktree)
    try:
        if args.edit_mode == "file":
            try:
                changed = apply_file_replacement(proposal, worktree, allowed_files, patch_path)
                result.apply_ok = True
            except Exception as exc:
                result.errors.append(str(exc))
                result.reason = "file_replacement_failed"
                return result
        else:
            patch = proposal.get("patch", "")
            patch_path.write_text(patch, encoding="utf-8")
            try:
                validate_patch_files(patch, allowed_files)
            except Exception as exc:
                result.errors.append(str(exc))
                result.reason = "invalid_patch_files"
                return result

            check = run_cmd(["git", "apply", "--check", str(patch_path)], worktree, 60)
            if check.returncode != 0:
                result.errors.append(check.stderr)
                if args.repair_patch:
                    repair_raw_path = run_dir / f"iter_{iteration:02d}_repair_raw_response.txt"
                    repair_patch_path = run_dir / f"iter_{iteration:02d}_repair.patch"
                    try:
                        repair = ask_for_proposal(
                            baseline,
                            previous_results,
                            allowed_files,
                            args,
                            repair_raw_path,
                            repair_error=check.stderr,
                            failed_patch=patch,
                        )
                        patch = repair.get("patch", "")
                        result.hypothesis = str(repair.get("hypothesis", result.hypothesis))
                        patch_path = repair_patch_path
                        result.patch_path = str(patch_path)
                        patch_path.write_text(patch, encoding="utf-8")
                        validate_patch_files(patch, allowed_files)
                        check = run_cmd(["git", "apply", "--check", str(patch_path)], worktree, 60)
                    except Exception as exc:
                        result.errors.append(f"repair_failed: {exc!r}")
                if check.returncode != 0:
                    result.errors.append(f"Patch path: {patch_path}")
                    result.reason = "patch_check_failed"
                    return result
            apply = run_cmd(["git", "apply", str(patch_path)], worktree, 60)
            result.apply_ok = apply.returncode == 0
            if apply.returncode != 0:
                result.errors.append(apply.stderr)
                result.reason = "patch_apply_failed"
                return result

            changed = changed_files_from_patch(patch)
        compile_result = compile_changed_files(worktree, changed, args.compile_timeout_seconds)
        result.compile_ok = compile_result.returncode == 0
        if compile_result.returncode != 0:
            result.errors.append(compile_result.stderr)
            result.reason = "compile_failed"
            return result

        eval_result = evaluate(args.eval_command, worktree, args.eval_timeout_minutes * 60)
        result.eval_result = eval_result
        if eval_result.returncode != 0:
            result.errors.append(eval_result.stderr_tail)
            result.reason = "eval_failed"
            return result

        baseline_score = metric(baseline.best, "score")
        score = metric(eval_result.best, "score")
        roi = metric(eval_result.best, "roi_pct")
        picks = metric(eval_result.best, "picks", 0)
        if score > baseline_score and roi >= args.min_roi and picks >= args.min_picks:
            result.accepted = True
            result.reason = (
                f"accepted: score {score:+.2f} > baseline {baseline_score:+.2f}, "
                f"roi={roi:+.2f}, picks={int(picks)}"
            )
        else:
            result.reason = (
                f"rejected: score={score:+.2f}, baseline={baseline_score:+.2f}, "
                f"roi={roi:+.2f}, picks={int(picks)}"
            )
        return result
    finally:
        if args.remove_rejected_worktrees and not result.accepted:
            remove_worktree(worktree)


def render_report(
    baseline: EvalResult,
    results: Sequence[IterationResult],
    started_at: datetime,
    finished_at: datetime,
) -> str:
    accepted = [result for result in results if result.accepted and result.eval_result]
    accepted.sort(key=lambda r: metric(r.eval_result.best, "score"), reverse=True)

    lines = [
        "# LLM Agent AutoResearch Report",
        "",
        f"Started: {started_at.isoformat(timespec='seconds')}",
        f"Finished: {finished_at.isoformat(timespec='seconds')}",
        "",
        "## Baseline",
        "",
        best_summary(baseline.best),
        "",
        "## Iterations",
        "",
        "| Iteration | Accepted | Result | Hypothesis |",
        "|---:|---|---|---|",
    ]
    for result in results:
        summary = best_summary(result.eval_result.best if result.eval_result else None)
        lines.append(
            f"| {result.iteration} | {result.accepted} | {summary} | "
            f"{result.hypothesis.replace('|', '/')} |"
        )
        if result.errors:
            lines.append(f"\nErrors iteration {result.iteration}:\n```text\n{chr(10).join(result.errors[-2:])}\n```\n")

    lines.extend(["", "## Best Accepted Patch", ""])
    if accepted:
        best = accepted[0]
        lines.extend(
            [
                f"Iteration: {best.iteration}",
                f"Reason: {best.reason}",
                f"Patch: `{best.patch_path}`",
                f"Worktree: `{best.worktree}`",
                f"Result: {best_summary(best.eval_result.best)}",
                f"Best config: `{best.eval_result.best_config_json}`",
                f"Breakdown: `{best.eval_result.breakdown_csv}`",
                "",
            ]
        )
    else:
        lines.append("No patch beat the baseline under the acceptance criteria.")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Run DeepSeek code-editing AutoResearch")
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--model", default=os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-pro"))
    parser.add_argument("--base-url", default=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"))
    parser.add_argument("--api-key-env", default="DEEPSEEK_API_KEY")
    parser.add_argument("--temperature", type=float, default=0.4)
    parser.add_argument("--max-tokens", type=int, default=16000)
    parser.add_argument("--llm-timeout-seconds", type=int, default=180)
    parser.add_argument("--compile-timeout-seconds", type=int, default=60)
    parser.add_argument("--eval-timeout-minutes", type=int, default=240)
    parser.add_argument("--min-roi", type=float, default=1.0)
    parser.add_argument("--min-picks", type=int, default=40)
    parser.add_argument("--max-file-chars", type=int, default=18000)
    parser.add_argument("--remove-rejected-worktrees", action="store_true")
    parser.add_argument("--repair-patch", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--edit-mode",
        choices=("file", "patch"),
        default="file",
        help="Ask the LLM for a full-file replacement by default; raw patch mode is kept as fallback.",
    )
    parser.add_argument(
        "--allowed-files",
        nargs="+",
        default=list(DEFAULT_ALLOWED_FILES),
    )
    parser.add_argument(
        "--eval-command",
        default=(
            "python research/gpu_autoresearch.py "
            "--seasons 2122 2223 2324 2425 2526 "
            "--leagues EPL L1 Bundesliga SerieA LaLiga "
            "--device cuda "
            "--markets 1X2,OU "
            "--candidate-markets 1X2,OU "
            "--train-size-per-league 300 "
            "--min-train-size-per-league 120 "
            "--batch-days 14 "
            "--epochs 500 "
            "--c-max-picks 0,1,2 "
            "--d-max-picks 1,2,3 "
            "--min-edges 0.15,0.25,0.35 "
            "--max-picks-per-match 1 "
            "--top 20"
        ),
    )
    parser.add_argument("--baseline-command", default=None)
    args = parser.parse_args()

    started_at = datetime.now()
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_dir = RUNS_DIR / started_at.strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    baseline_command = args.baseline_command or args.eval_command
    print(f"Running baseline:\n{baseline_command}", flush=True)
    baseline = evaluate(baseline_command, PROJECT_ROOT, args.eval_timeout_minutes * 60)
    print(best_summary(baseline.best), flush=True)
    if baseline.returncode != 0:
        raise SystemExit(f"Baseline failed:\n{baseline.stderr_tail}")

    results: List[IterationResult] = []
    for iteration in range(1, args.iterations + 1):
        print(f"\nStarting LLM iteration {iteration}", flush=True)
        try:
            result = run_iteration(iteration, baseline, results, args, run_dir)
        except Exception as exc:
            result = IterationResult(
                iteration=iteration,
                worktree="",
                hypothesis="",
                patch_path=str(run_dir / f"iter_{iteration:02d}.patch"),
                accepted=False,
                reason="exception",
                errors=[repr(exc)],
            )
        results.append(result)
        print(result.reason, flush=True)

        if result.accepted and result.eval_result:
            baseline = result.eval_result

    finished_at = datetime.now()
    report_path = RESULTS_DIR / f"agent_loop_report_{finished_at.strftime('%Y%m%d_%H%M%S')}.md"
    json_path = RESULTS_DIR / f"agent_loop_results_{finished_at.strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(render_report(baseline, results, started_at, finished_at), encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "started_at": started_at.isoformat(timespec="seconds"),
                "finished_at": finished_at.isoformat(timespec="seconds"),
                "run_dir": str(run_dir),
                "baseline": baseline.__dict__,
                "results": [
                    {
                        **result.__dict__,
                        "eval_result": result.eval_result.__dict__ if result.eval_result else None,
                    }
                    for result in results
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"\nAgent loop report: {report_path}")
    print(f"Agent loop JSON: {json_path}")


if __name__ == "__main__":
    main()
