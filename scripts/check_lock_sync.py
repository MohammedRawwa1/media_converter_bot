#!/usr/bin/env python3
"""Verify requirements.lock still covers everything requirements.txt asks for.

Why this exists
---------------
The previous CI gate re-resolved the lock from scratch with ``uv pip compile
--upgrade`` and diffed it against the committed file. ``--upgrade`` means "take
the newest version of everything", so the moment any dependency upstream
published a release the committed lock - resolved earlier, on purpose - no
longer matched, and the job failed on a commit that changed no dependency at all.
The lock header even documented the command *without* ``--upgrade`` while CI ran
it *with* it, so regenerating as documented could never satisfy the gate.

What actually has to be true is not "the lock equals today's newest resolution"
(which nothing can promise) but:

  1. every direct requirement in requirements.txt is present in the lock,
  2. at a version that satisfies the specifier written there, and
  3. carrying at least one sha256 (so ``pip install --require-hashes`` can use it).

The full transitive closure and the hashes themselves are proven by the
``pip install --require-hashes -r requirements.lock`` step that follows this one,
so this check only has to answer "did someone edit requirements.txt without
regenerating the lock?" - deterministically, and without failing because a
library released a patch version overnight.

Exit status: 0 when the lock covers requirements.txt, 1 otherwise.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

try:
    from packaging.requirements import Requirement
except ImportError:  # pragma: no cover - the CI step installs packaging first
    print("check_lock_sync: the `packaging` package is required (pip install packaging)")
    raise SystemExit(2) from None

REPO_ROOT = Path(__file__).resolve().parent.parent
REQUIREMENTS = REPO_ROOT / "requirements.txt"
LOCK = REPO_ROOT / "requirements.lock"


def _normalize(name: str) -> str:
    """PEP 503 name normalisation, so ``Flask_Cors`` and ``flask-cors`` agree."""
    return re.sub(r"[-_.]+", "-", name).strip().lower()


def _logical_lines(text: str) -> list[str]:
    """Join backslash continuations and drop blanks/comments."""
    lines: list[str] = []
    pending = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if pending:
            line = pending + line
            pending = ""
        if line.endswith("\\"):
            pending = line[:-1].rstrip() + " "
            continue
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        lines.append(stripped)
    if pending.strip():
        lines.append(pending.strip())
    return lines


def parse_requirements(text: str) -> list[Requirement]:
    """Direct requirements, comments/continuations handled."""
    out: list[Requirement] = []
    for line in _logical_lines(text):
        # Drop an environment marker: this repo's requirements carry none, and a
        # marked requirement still has to be in the lock for the deploy platform.
        line = line.split(";", 1)[0].strip()
        if not line or line.startswith("-"):  # -r/-c/--hash etc.: not a direct pin
            continue
        out.append(Requirement(line))
    return out


def parse_lock(text: str) -> dict[str, tuple[str, bool]]:
    """Map normalized name -> (locked version, has_sha256)."""
    entries: dict[str, tuple[str, bool]] = {}
    current: str | None = None
    for raw in text.splitlines():
        match = re.match(r"^\s*([A-Za-z0-9._-]+)\s*==\s*([^\s\\#]+)", raw)
        if match:
            current = _normalize(match.group(1))
            entries[current] = (match.group(2), False)
            continue
        if current and "--hash=sha256:" in raw:
            name, _version = entries[current]
            entries[current] = (name, True)
    return entries


def check(requirements_text: str | None = None, lock_text: str | None = None) -> list[str]:
    """Return a list of human-readable problems; empty means the lock is in sync.

    The texts are injectable so a test can exercise the comparison against
    fixtures instead of the repository's own files.
    """
    problems: list[str] = []
    if requirements_text is None:
        requirements_text = REQUIREMENTS.read_text(encoding="utf-8")
    if lock_text is None:
        lock_text = LOCK.read_text(encoding="utf-8")
    requirements = parse_requirements(requirements_text)
    lock = parse_lock(lock_text)

    for requirement in requirements:
        name = _normalize(requirement.name)
        entry = lock.get(name)
        if entry is None:
            problems.append(f"{requirement.name!r} is required by requirements.txt but missing from requirements.lock")
            continue
        version, has_hash = entry
        if requirement.specifier and not requirement.specifier.contains(version, prereleases=True):
            problems.append(
                f"{requirement.name}: requirements.lock pins {version}, which does not satisfy {requirement.specifier}"
            )
        if not has_hash:
            problems.append(f"{requirement.name}: requirements.lock entry {version} carries no --hash=sha256 entry")

    return problems


def main() -> int:
    problems = check()
    if problems:
        print("::error::requirements.lock is out of sync with requirements.txt")
        for problem in problems:
            print(f"  - {problem}")
        print("\nRegenerate it with the command recorded in the lock header (see README.md).")
        return 1

    locked = parse_lock(LOCK.read_text(encoding="utf-8"))

    # Plain ASCII: the script runs on Windows dev shells whose stdout is cp1252.
    print(f"OK: requirements.lock covers all of requirements.txt ({len(locked)} pinned packages, hashes present).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
