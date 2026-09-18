#!/usr/bin/env python3
"""Lint gate — Ruff lint, Ruff format and the syntax check, as CI runs them.

The same deal as scripts/check_bandit.py: the Lint & Compile workflow calls this
script, so "clean on my machine" and "clean in CI" are one implementation rather
than two copies that drift apart. All three checks always run — a failing check
never hides the other two — and the exit code is non-zero if any of them found
something.

Usage:
    python scripts/check_lint.py                     # the CI gate, verbatim
    python scripts/check_lint.py --target workers/   # narrow it while iterating

Paths resolve against the repository root, which is also the directory the
checks run in, so the outcome does not depend on where you invoke it from. The
syntax walk prunes the same directories the workflow's `find` pruned, so it
covers the same files.

Exit codes:
    0  ruff lint clean, every file formatted, every file parses
    1  at least one check failed; the details are printed above the summary
    2  a check could not run — ruff is not installed, the target does not exist,
       ruff itself errored. Always a failure, never a pass.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG = "ruff.toml"
IN_ACTIONS = os.environ.get("GITHUB_ACTIONS") == "true"

# The `-not -path` list from the workflow's syntax step. Ruff's own discovery
# already skips these; the manual walk below has to be told.
SKIP_DIRS = {
    ".eggs",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "env",
    "node_modules",
    "venv",
}

# Printed by the summary, in this order, with what a failure is called there.
CHECKS = ("Ruff lint", "Ruff format", "py_compile")
FAIL_LABEL = {"Ruff lint": "failed", "Ruff format": "failed", "py_compile": "syntax errors"}


def _note(level: str, message: str, file: str | None = None) -> None:
    """Emit a GitHub annotation when running in Actions, a plain line otherwise."""
    if not IN_ACTIONS:
        print(message)
        return
    target = f" file={file}" if file else ""
    print(f"::{level}{target}::{message}")


def _resolve(path: str) -> Path:
    """Resolve a user-supplied path against the repository root."""
    candidate = Path(path)
    return candidate if candidate.is_absolute() else REPO_ROOT / candidate


def _rel(path: Path) -> str:
    """A repository-relative path in the separator GitHub annotations expect."""
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _run(command: list[str]) -> tuple[int, str]:
    """Run a check in the repository root and return (exit code, output)."""
    proc = subprocess.run(  # nosec B603  # fixed argv for this interpreter, no shell
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


# Statuses a check can report. "ok" rather than "pass" on purpose: bandit reads a
# dict key spelled like a password name ({"pass": "..."}) as a hardcoded
# credential, and B105 on this file would be noise in the very report it gates.
OK, FAIL, ERROR = "ok", "fail", "error"


def _classify(code: int, output: str) -> tuple[str, str]:
    """Map a check's exit code on: ruff uses 1 for findings, 2 for its own errors."""
    if code == 0:
        return OK, output
    return (FAIL if code == 1 else ERROR), output


def _ruff_lint(target: str) -> tuple[str, str]:
    command = [sys.executable, "-m", "ruff", "check", target, "--config", CONFIG]
    if IN_ACTIONS:
        # The format the workflow used inline: each violation becomes a file/line
        # annotation on the pull request instead of a line buried in the log.
        command += ["--output-format", "github"]
    return _classify(*_run(command))


def _ruff_format(target: str) -> tuple[str, str]:
    command = [sys.executable, "-m", "ruff", "format", target, "--config", CONFIG, "--check"]
    return _classify(*_run(command))


def _syntax(target: Path) -> tuple[str, str]:
    """Parse every Python file under the target and report the ones that fail."""
    if target.is_file():
        paths = [target] if target.suffix == ".py" else []
    else:
        paths = [path for path in sorted(target.rglob("*.py")) if not SKIP_DIRS.intersection(path.parts)]

    problems = []
    for path in paths:
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                ast.parse(handle.read(), filename=str(path))
        except SyntaxError as exc:
            problems.append((path, exc))
        except OSError as exc:
            return "error", f"Could not read {_rel(path)}: {exc}"

    if not problems:
        return OK, "All Python files passed syntax checks."

    lines = []
    for path, exc in problems:
        rel = _rel(path)
        if IN_ACTIONS:
            lines.append(f"::error file={rel}::SyntaxError: {exc}")
        else:
            lines.append(f"{rel}:{exc.lineno}: SyntaxError: {exc.msg}")
    lines.append(f"{len(problems)} Python file(s) contain syntax errors.")
    return FAIL, "\n".join(lines)


def _print_check(name: str, status: str, output: str) -> None:
    """Show what a check said, then its verdict on one line."""
    mark = {OK: "✅", FAIL: "❌", ERROR: "⚠️"}[status]
    verdict = {OK: "passed", FAIL: FAIL_LABEL[name], ERROR: "could not run"}[status]
    print(f"--- {name} ---")
    if output.strip():
        print(output.strip())
    print(f"{mark} {name}: {verdict}")
    print()


def main(argv: list[str] | None = None) -> int:
    # A Windows console defaults to cp1252, where the marks above would raise
    # UnicodeEncodeError instead of reporting a result. CI runs on UTF-8, so this
    # only ever fires on a developer's machine - the machine this script is for.
    with contextlib.suppress(AttributeError, ValueError):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Lint gate (the same checks CI runs).")
    parser.add_argument(
        "--target", default=".", help="path to check, relative to the repo root (default: the whole tree)"
    )
    args = parser.parse_args(argv)

    if importlib.util.find_spec("ruff") is None:
        _note("error", "ruff is not installed — run: pip install -r requirements-dev.txt")
        return 2

    target = _resolve(args.target)
    if not target.exists():
        _note("error", f"Nothing to check: '{args.target}' does not exist.")
        return 2

    # Every check runs, always, even after one fails: a developer fixing three
    # problems wants to see three problems.
    outcomes = []
    for name, check in (
        ("Ruff lint", lambda: _ruff_lint(args.target)),
        ("Ruff format", lambda: _ruff_format(args.target)),
        ("py_compile", lambda: _syntax(target)),
    ):
        status, output = check()
        outcomes.append((name, status))
        _print_check(name, status, output)
        if status == ERROR:
            _note("error", f"{name} could not run — its output is above.")

    print("========== LINT & COMPILE SUMMARY ==========")
    for name, status in outcomes:
        if status == OK:
            label = "✅ passed"
        elif status == FAIL:
            label = f"❌ {FAIL_LABEL[name]}"
        else:
            label = "⚠️ could not run"
        print(f"{name + ':':<16}{label}")
    print("============================================")

    if any(status == FAIL for _, status in outcomes):
        return 1
    if any(status == ERROR for _, status in outcomes):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
