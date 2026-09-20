"""Per-user userbot resolution for the forward (too-big-for-the-Bot-API) path.

A media the Bot API cannot hand over is stored as a *forward hash*: the bot
records where the media lives and the fetch happens somewhere else — the web app,
the fetcher service, the ingest tool. Those processes have no ``Update`` in hand,
so *whose* media it is has to travel with the metadata. Without that field they
resolved the deployment's global (env-seeded) session, which is one account
fetching every user's media — the media of a second user then depends on the
first one's session still being alive and being a member of the relay chat.

These tests pin both ends: the metadata the handler writes, and every userbot
call in the runtime — so a fetch or a delivery is always scoped to a user.
"""

import ast
import asyncio
import contextlib
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from source_helpers import parse_source

from handlers import EnhancedMediaHandler

Handler = EnhancedMediaHandler

#: The userbot entry points the runtime calls. Each takes ``user_id``, which the
#: userbot layer resolves to that user's own session (see
#: ``utils.telethon_session.get_pyrogram_session_string_for_user``).
_USERBOT_CALLS = (
    "send_file_via_userbot",
    "download_forward_via_userbot",
    "download_media_to_sink",
    "download_bytes_via_userbot",
)

#: The files that call them on a user's behalf at runtime. ``scripts/`` and the
#: ``tools/`` diagnostics are deliberately not here: they are run by hand against
#: whatever session the operator configured.
_RUNTIME_FILES = (
    ("handlers.py",),
    ("workers", "ffmpeg_worker.py"),
    ("utils", "bigfile_pipeline.py"),
    ("fetcher", "service.py"),
    ("web", "webapp.py"),
)


def _userbot_calls(*path):
    """Every userbot entry-point call in a project file."""
    calls = []
    for node in ast.walk(parse_source(*path)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name in _USERBOT_CALLS:
            calls.append((getattr(node, "lineno", 0), name, node))
    return calls


def _keyword_values(call) -> dict[str, str]:
    return {kw.arg: ast.unparse(kw.value) for kw in call.keywords if kw.arg}


class ForwardMetadataTests(unittest.TestCase):
    """The forward hash records *whose* media it points at."""

    def _save(self, user_id):
        captured = {}

        async def _record(metadata):
            captured.update(metadata)
            return "fh1"

        handler = object.__new__(Handler)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=user_id))
        current_file = {
            "id": "f1",
            "name": "Big Clip.mkv",
            "type": "video",
            "size": 10**10,
            "chat_id": -100,
            "msg_id": 4321,
        }
        # No AUTO_FETCH_FORWARDS and no web upload URL in the test environment, so
        # the method ends by raising the instruction its caller turns into a reply
        # - the metadata is already written by then, which is what is under test.
        with patch("utils.forward_store.save_forward_metadata", _record), contextlib.suppress(Exception):
            asyncio.run(handler._handle_large_forward(update, None, current_file, "too big", "https://example/upload"))
        return captured

    def test_the_stored_forward_carries_the_user_it_belongs_to(self):
        metadata = self._save(7)

        self.assertEqual(metadata["user_id"], 7)
        # The rest of the record is unchanged: this added a field, not a new shape.
        self.assertEqual(metadata["file_id"], "f1")
        self.assertEqual(metadata["message_id"], 4321)
        self.assertEqual(metadata["size"], 10**10)

    def test_each_user_is_recorded_as_themselves(self):
        # The point of the field: user B's forward is fetched with B's session,
        # never with the session of whoever seeded the deployment.
        self.assertEqual(self._save(7)["user_id"], 7)
        self.assertEqual(self._save(987654321)["user_id"], 987654321)

    def test_an_update_without_a_user_records_none_instead_of_guessing(self):
        # Then the fetch keeps its previous (global-session) behaviour rather
        # than attributing the media to someone it does not know.
        metadata = self._save(None)
        self.assertIsNone(metadata["user_id"])


class UserbotScopingTests(unittest.TestCase):
    """No runtime userbot call is made without a user to resolve a session for."""

    def test_every_runtime_userbot_call_passes_a_user_id(self):
        checked = 0
        for path in _RUNTIME_FILES:
            for lineno, name, call in _userbot_calls(*path):
                values = _keyword_values(call)
                where = f"{'/'.join(path)}:{lineno} {name}"
                self.assertIn("user_id", values, f"{where} would use the deployment's session")
                # And not a literal: it has to be the acting user (the update's
                # user, or the user the job/forward metadata carries).
                self.assertTrue(
                    any(token in values["user_id"] for token in ("effective_user", "user_id", '"user_id"')),
                    f"{where} does not name a user: {values['user_id']}",
                )
                checked += 1
        self.assertGreaterEqual(checked, 10, "the userbot call sites were not found")

    def test_the_forward_fetchers_read_the_users_id_from_the_metadata(self):
        # The two processes that fetch a stored forward have no update to read it
        # from, so it has to come out of the metadata the handler wrote.
        for path in (("fetcher", "service.py"), ("web", "webapp.py")):
            calls = _userbot_calls(*path)
            self.assertTrue(calls, f"no userbot fetch found in {'/'.join(path)}")
            for _lineno, _name, call in calls:
                values = _keyword_values(call)
                # ``ast.unparse`` normalises the quotes, so the shape is checked:
                # a lookup of the metadata's own ``user_id``.
                value = values.get("user_id", "")
                self.assertIn(".get(", value, f"{'/'.join(path)}: {values}")
                self.assertIn("user_id", value, f"{'/'.join(path)}: {values}")


if __name__ == "__main__":
    unittest.main()
