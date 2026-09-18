#!/usr/bin/env python3
"""Bandit security gate — the check CI runs, runnable before you push.

The Security Scan workflow calls this script, so "clean on my machine" and
"clean in CI" are one implementation instead of two copies that drift apart.
That drift is not hypothetical: the workflow used to carry its own inline copy
of this gate, and at the same time `.bandit` was a YAML profile passed to `-c`.
Bandit ignored the skips and exclusions of that pairing without failing, and the
job went red on findings nobody had reviewed.

Usage:
    python scripts/check_bandit.py                      # the CI gate, verbatim
    python scripts/check_bandit.py --target workers/    # narrow it while iterating
    python scripts/check_bandit.py --output /tmp/bandit.json

The target, config and report paths are resolved against the repository root,
which is also the directory bandit scans, so the outcome does not depend on the
directory you invoke it from.

Assert usage (bandit's B101) under tests/ is accepted instead of reported: it is
the point of a test suite, bandit cannot scope a skip to one directory, and
skipping it globally would disarm the check for production code. Every other
finding in tests/ is reported like any other file, so tests/ does not need to be
excluded from the scan.

Exit codes:
    0  no HIGH findings. MEDIUM and LOW are reported but do not fail, which is
       exactly what CI gates on.
    1  at least one HIGH finding.
    2  bandit did not run or produced no readable report: missing tool, a config
       file it cannot parse, a crash. Always a failure, never a pass.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ".bandit"
DEFAULT_OUTPUT = "bandit-report.json"
IN_ACTIONS = os.environ.get("GITHUB_ACTIONS") == "true"

# What a dev needs to read first. Bandit emits the three severities below, and
# anything outside them sorts last rather than being dropped from the count.
SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}

# Assert usage is the point of a test suite, so B101 inside tests/ is noise - and
# at one finding per assert, enough noise to bury every other finding in the
# report. It is deliberately NOT in .bandit's global `skips`: outside tests/ an
# assert is deleted by `python -O`, which is worth reporting. Bandit has no
# per-directory skip, so the scope is applied here, where it is visible.
ACCEPTED_IN_TESTS = {"B101"}
TESTS_DIR = "tests"


def _note(level: str, message: str) -> None:
    """Emit a GitHub annotation when running in Actions, a plain line otherwise."""
    print(f"::{level}::{message}" if IN_ACTIONS else message)


def _resolve(path: str) -> Path:
    """Resolve a user-supplied path against the repository root."""
    candidate = Path(path)
    return candidate if candidate.is_absolute() else REPO_ROOT / candidate


def _one_line(text: str) -> str:
    """Collapse a bandit snippet to a single line so one finding reads as one line."""
    return " ".join(text.split())


def _is_tests_path(filename: str, target_is_tests: bool) -> bool:
    """True when a finding sits under tests/, however bandit spelled the path.

    Bandit reports paths relative to the scan root (``./tests/test_x.py``), and
    only the first component is checked: a nested ``utils/tests/`` is not the test
    suite. Scanning ``--target tests/`` directly makes every finding a test one.
    """
    if target_is_tests:
        return True
    parts = Path(filename.replace("\\", "/")).parts
    return bool(parts) and parts[0] == TESTS_DIR


def _accepted_note(count: int) -> None:
    """Say what was accepted, so the scope stays visible in every run that used it."""
    if count:
        print(f"   {count} B101 assert finding(s) under tests/ accepted by the gate (see .bandit)")


def _scan(target: str, config: Path, report: Path) -> str:
    """Run bandit; return its stderr, which is its only diagnostic output."""
    command = [sys.executable, "-m", "bandit"]

    if config.is_file():
        # `--ini` reads the [bandit] section: skips and exclude. It is NOT
        # interchangeable with `-c/--configfile`, which expects the YAML plugin
        # profile — handed an INI file there, bandit parses nothing at all.
        command += ["--ini", str(config)]

    command += ["-r", target, "-f", "json", "-o", str(report)]

    proc = subprocess.run(  # nosec B603  # fixed argv for this interpreter, no shell
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return proc.stderr or ""


def main(argv: list[str] | None = None) -> int:
    # A Windows console defaults to cp1252, where the ✅ below would raise
    # UnicodeEncodeError instead of reporting a pass. CI runs on UTF-8, so this
    # only ever fires on a developer's machine - the machine this script is for.
    with contextlib.suppress(AttributeError, ValueError):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Bandit security gate (the same one CI runs).")
    parser.add_argument(
        "--target", default=".", help="path to scan, relative to the repo root (default: the whole tree)"
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"JSON report to write, relative to the repo root (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"bandit INI config to use when that file exists (default: {DEFAULT_CONFIG})",
    )
    args = parser.parse_args(argv)

    if importlib.util.find_spec("bandit") is None:
        _note("error", "bandit is not installed — run: pip install -r requirements-dev.txt")
        return 2

    # A mistyped target is the easiest way to get a false pass: bandit does not
    # object to a directory that is not there, it just finds nothing in it.
    target = _resolve(args.target)
    if not target.exists():
        _note("error", f"Nothing to scan: '{args.target}' does not exist.")
        return 2
    target_is_tests = target.name == TESTS_DIR

    report = _resolve(args.output)
    # A report left by an earlier run would otherwise answer for this one, and
    # the "did bandit actually produce a report" check below exists to catch a
    # scan that never completed.
    report.unlink(missing_ok=True)

    stderr = _scan(args.target, _resolve(args.config), report)

    if "unable to parse config file" in stderr.lower():
        # Bandit only warns here and carries on with no skips and no exclusions,
        # so every reviewed finding in the tree is reported as if it were new.
        # That is a broken gate, not a strict one: refuse to call it a pass.
        _note("error", "bandit could not parse its config — no skips or exclusions were applied.")
        print(stderr.strip())
        print(f"  `--ini {args.config}` expects the INI format: a [bandit] section with `skips =`.")
        print("  The YAML plugin profile belongs on `-c/--configfile` instead. See .bandit for the format in use.")
        return 2

    if not report.is_file():
        print(stderr.strip())
        _note("error", "Bandit did not produce a report — see its output above.")
        return 2

    try:
        loaded = json.loads(report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _note("error", f"Could not read the bandit report at {report}: {exc}")
        return 2

    findings = loaded.get("results", [])

    # Same false-pass, other cause: a path that exists but holds no Python.
    # Bandit's `loc` is the lines of code it actually analysed.
    scanned = loaded.get("metrics", {}).get("_totals", {}).get("loc", None)
    if scanned == 0:
        _note("error", f"Bandit analysed no code under '{args.target}' — check the path.")
        return 2

    accepted = 0
    reported = []
    for issue in findings:
        if issue.get("test_id") in ACCEPTED_IN_TESTS and _is_tests_path(issue.get("filename", ""), target_is_tests):
            accepted += 1
        else:
            reported.append(issue)

    if not reported:
        print("✅ Bandit: no findings")
        _accepted_note(accepted)
        return 0

    high = 0
    medium = 0
    for issue in sorted(reported, key=lambda item: SEVERITY_ORDER.get(item.get("issue_severity", "LOW"), 3)):
        severity = issue.get("issue_severity", "LOW")
        if severity == "HIGH":
            high += 1
        elif severity == "MEDIUM":
            medium += 1

        text = _one_line(issue.get("issue_text", ""))
        location = f"{issue.get('filename')}:{issue.get('line_number')}"
        print(f"[{severity}] {issue.get('test_id')}: {location} - {text}")

    low = len(reported) - high - medium
    _note("warning", f"Bandit found {len(reported)} issue(s): {high} HIGH, {medium} MEDIUM, {low} LOW")
    _accepted_note(accepted)

    if high:
        _note("error", "High-severity Bandit findings detected.")
        print("Fix the call, or record the decision in .bandit: a per-line `# nosec <id>  # reason` on the call")
        print("(B603 is deliberately per-site, never global) or a global skip for a class that was reviewed.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
