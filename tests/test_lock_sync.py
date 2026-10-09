"""The requirements.lock gate must fail on drift and never on a new upstream release.

The old gate re-resolved the lock with ``uv pip compile --upgrade`` and diffed it,
so any dependency publishing a release failed the job on a commit that changed no
dependency. These pin the replacement: a deterministic coverage check.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import check_lock_sync as mod  # noqa: E402

REQ_IN = "flask>=2.2.5,<4.0.0\nrequests==2.31.0\n"


def _lock(*entries: str) -> str:
    return "\n".join(entries) + "\n"


OK_LOCK = _lock(
    "flask==3.1.0 \\\n    --hash=sha256:aaa\n    # via -r requirements.txt",
    "requests==2.31.0 \\\n    --hash=sha256:bbb\n    # via -r requirements.txt",
)


class LockSyncTests(unittest.TestCase):
    def test_a_covered_lock_passes(self):
        self.assertEqual(mod.check(REQ_IN, OK_LOCK), [])

    def test_a_requirement_missing_from_the_lock_is_reported(self):
        problems = mod.check(REQ_IN, _lock("flask==3.1.0 \\\n    --hash=sha256:aaa"))
        self.assertTrue(any("requests" in problem and "missing" in problem for problem in problems))

    def test_a_pin_outside_the_specifier_is_reported(self):
        lock = _lock(
            "flask==4.0.0 \\\n    --hash=sha256:aaa",
            "requests==2.31.0 \\\n    --hash=sha256:bbb",
        )
        self.assertTrue(
            any("flask" in problem and "does not satisfy" in problem for problem in mod.check(REQ_IN, lock))
        )

    def test_a_pin_without_a_hash_is_reported(self):
        lock = _lock(
            "flask==3.1.0\n    # via -r requirements.txt",
            "requests==2.31.0 \\\n    --hash=sha256:bbb",
        )
        self.assertTrue(any("flask" in problem and "hash" in problem for problem in mod.check(REQ_IN, lock)))

    def test_name_normalisation_matches_the_lock(self):
        problems = mod.check("Flask_Cors>=3.0.10,<7.0.0\n", _lock("flask-cors==5.0.0 \\\n    --hash=sha256:aaa"))
        self.assertEqual(problems, [])

    def test_a_newer_upstream_release_does_not_fail_the_check(self):
        # The committed lock is older than "latest"; that must not be an error.
        lock = _lock("requests==2.31.0 \\\n    --hash=sha256:bbb")
        self.assertEqual(mod.check("requests>=2.31.0,<3.0.0\n", lock), [])

    def test_the_repository_lock_is_in_sync(self):
        self.assertEqual(mod.check(), [])
