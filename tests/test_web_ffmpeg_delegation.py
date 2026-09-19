"""The web side must not carry its own ffmpeg implementation.

Three copies of the same logic had grown here: the progress parser and runner in
``utils/ffmpeg_runner``, another parser plus its own ffmpeg invocation in
``web/ffmpeg_worker``, and the web upload fallback's inline conversion with its
own event loop. A fix in one left the others broken - which is what happened to
the runner's conflict-free progress handling and to the worker's argument
handling. These tests pin the delegation, so "one implementation" is enforced
rather than merely intended.
"""

import ast
import asyncio
import inspect
import unittest
import warnings
from pathlib import Path
from unittest.mock import patch

from source_helpers import (
    PROJECT_ROOT,
    call_keywords,
    called_methods,
    find_function,
    parse_source,
    read_source,
)

from utils import ffmpeg_runner
from web import ffmpeg_worker as web_ffmpeg_worker


class WebHelpersDelegateTests(unittest.TestCase):
    """Every helper in ``web/ffmpeg_worker`` calls the shared runner."""

    def test_progress_parser_is_the_runners_parser(self):
        self.assertIs(
            web_ffmpeg_worker._parse_out_time,
            ffmpeg_runner._parse_out_time,
            "a second parser drifts from the one the worker's progress uses",
        )

    def test_get_duration_uses_the_runners_probe(self):
        seen = []

        async def _probe(path):
            seen.append(path)
            return 12.5

        with patch.object(web_ffmpeg_worker, "probe_duration", _probe):
            duration = web_ffmpeg_worker.get_duration("clip.mp4")

        self.assertEqual(seen, ["clip.mp4"])
        self.assertEqual(duration, 12.5)

    def test_get_duration_passes_a_failed_probe_through_as_none(self):
        async def _probe(_path):
            return None

        with patch.object(web_ffmpeg_worker, "probe_duration", _probe):
            self.assertIsNone(web_ffmpeg_worker.get_duration("not-media.bin"))

    def test_convert_video_runs_the_shared_runner(self):
        calls = []
        finished = []

        async def _run_ffmpeg(*args, **kwargs):
            calls.append((args, kwargs))
            return True, "out.mp4"

        with patch.object(web_ffmpeg_worker, "run_ffmpeg", _run_ffmpeg):
            web_ffmpeg_worker.convert_video("in.mp4", "out.mp4", "job-1", 5.0, None, finished.append)

        self.assertEqual(len(calls), 1, "the conversion must go through utils.ffmpeg_runner.run_ffmpeg")
        args, kwargs = calls[0]
        self.assertEqual(args, ("in.mp4", "out.mp4", "job-1"))
        # No Redis channel: this helper serves the web threads, which track
        # progress in-process rather than on the worker's pubsub channel.
        self.assertIsNone(kwargs.get("progress_channel"))
        self.assertEqual(finished, ["out.mp4"], "the finish callback must still be called")

    def test_convert_video_reports_the_runners_progress(self):
        reported = []

        async def _run_ffmpeg(_input, _output, _job_id, *, progress_channel=None, on_progress=None):
            self.assertIsNotNone(on_progress, "the runner's progress callback must be wired up")
            on_progress(42.0, "encoding 42.0%")
            return True, "out.mp4"

        with patch.object(web_ffmpeg_worker, "run_ffmpeg", _run_ffmpeg):
            web_ffmpeg_worker.convert_video(
                "in.mp4", "out.mp4", "job-1", 5.0, lambda pct, msg: reported.append((pct, msg))
            )

        self.assertEqual(reported, [(42.0, "encoding 42.0%")])

    def test_convert_video_raises_the_runners_reason(self):
        finished = []

        async def _run_ffmpeg(*_args, **_kwargs):
            return False, "invalid data found when processing input"

        with (
            patch.object(web_ffmpeg_worker, "run_ffmpeg", _run_ffmpeg),
            self.assertRaises(RuntimeError) as raised,
        ):
            web_ffmpeg_worker.convert_video("in.mp4", "out.mp4", "job-1", 5.0, None, finished.append)

        self.assertIn("invalid data found", str(raised.exception))
        self.assertEqual(finished, [], "a failed conversion must not report success")

    def test_convert_video_keeps_its_callback_signature(self):
        parameters = list(inspect.signature(web_ffmpeg_worker.convert_video).parameters)

        self.assertEqual(
            parameters,
            ["input_path", "output_path", "job_id", "duration", "progress_cb", "finished_cb"],
            "callers pass positionally, so this contract cannot be reordered",
        )

    def test_sync_helpers_refuse_to_nest_inside_a_running_loop(self):
        # Raising beats the alternative (a nested loop that can never start), and
        # the coroutine is closed rather than left to warn on collection.
        async def _scenario():
            with warnings.catch_warnings():
                warnings.simplefilter("error", RuntimeWarning)
                with self.assertRaises(RuntimeError) as raised:
                    web_ffmpeg_worker.get_duration("clip.mp4")
            return str(raised.exception)

        message = asyncio.run(_scenario())

        self.assertIn("probe_duration", message)


class WebFallbackUsesTheSameRunnerTests(unittest.TestCase):
    """The queue-less upload fallback encodes with the worker's own runner."""

    @classmethod
    def setUpClass(cls):
        cls.src = read_source("web", "webapp.py")
        cls.tree = parse_source("web", "webapp.py")

    def test_fallback_imports_the_shared_runner(self):
        self.assertIn("from utils.ffmpeg_runner import run_ffmpeg", self.src)

    def _fallback_worker(self):
        """The queue-less path's thread body: its AST node and its source.

        ``_worker`` is nested inside the route, so it has no importable object to
        hand ``inspect``; unparsing the node is what is left, and is enough for
        both questions here.
        """
        node = find_function(self.tree, "_worker")
        return node, ast.unparse(node)

    def test_fallback_passes_the_job_ffmpeg_args(self):
        self.assertTrue(
            call_keywords(self.tree, "run_ffmpeg", "ffmpeg_args"),
            "the fallback must run the job's own ffmpeg arguments",
        )

    def test_fallback_reports_progress_through_the_runner(self):
        self.assertTrue(
            call_keywords(self.tree, "run_ffmpeg", "on_progress"),
            "the fallback must take its progress from the runner",
        )

    def test_the_worker_thread_runs_the_runner_and_keeps_no_converter(self):
        _node, worker = self._fallback_worker()

        self.assertIn("run_ffmpeg(", worker)
        self.assertNotIn(
            "ExtendedMediaConverter",
            worker,
            "a second conversion path is what let the two drift apart",
        )

    def test_the_worker_thread_no_longer_builds_its_own_loop(self):
        node, _worker = self._fallback_worker()

        # ``asyncio.run`` on the thread is all that is left; the hand-rolled
        # new_event_loop/set_event_loop/run_until_complete bracket is gone. The
        # per-thread loop helper elsewhere in the module is unaffected.
        self.assertEqual(called_methods(node, "asyncio"), {"run"})

    def test_the_fallback_still_runs_in_a_background_thread(self):
        self.assertIn("threading.Thread(target=_worker, args=(job,), daemon=True)", self.src)


class SingleFfmpegProgressLoopTests(unittest.TestCase):
    """Only one module in the project may parse an ffmpeg progress stream."""

    @classmethod
    def setUpClass(cls):
        cls.sources = {
            path: path.read_text(encoding="utf-8")
            for path in Path(PROJECT_ROOT).rglob("*.py")
            if not any(part in {"tests", ".venv", "env", "__pycache__"} for part in path.parts)
        }

    def _files_containing(self, needle: str) -> set[str]:
        return {path.relative_to(PROJECT_ROOT).as_posix() for path, text in self.sources.items() if needle in text}

    def test_only_the_runner_builds_a_progress_pipe(self):
        self.assertEqual(
            self._files_containing('"-progress"'),
            {"utils/ffmpeg_runner.py"},
            "every conversion must report through the one progress pipe",
        )

    def test_only_the_runner_parses_out_time(self):
        self.assertEqual(
            self._files_containing('"out_time"'),
            {"utils/ffmpeg_runner.py"},
            "a second out_time parser is a second set of progress bugs",
        )

    def test_only_the_runner_defines_the_parser(self):
        import re

        defining = {
            path for path, text in self.sources.items() if re.search(r"^\s*def _parse_out_time\(", text, re.MULTILINE)
        }

        self.assertEqual(
            {path.relative_to(PROJECT_ROOT).as_posix() for path in defining},
            {"utils/ffmpeg_runner.py"},
        )


if __name__ == "__main__":
    unittest.main()
