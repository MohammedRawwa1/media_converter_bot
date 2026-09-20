"""Create Archive names the batch it staged, then packs it after confirmation.

The button reads the batch Apply Bulk reads, asks for the archive's name, states
what would be packed, and consumes the batch only once the archive is queued. A
ZIP too big for one Telegram send is split into numbered ``.001``/``.002``
volumes by the worker (see ``tasks.conversion_tasks.split_archive_volumes``).
"""

import asyncio
import os
import tempfile
import unittest
from unittest import mock

from source_helpers import read_source

import config
import handlers
from handlers import (
    EnhancedMediaHandler,
    _archive_default_name,
    _archive_selection,
    _archive_size_label,
    _archive_summary_text,
    _archive_total_bytes,
    _bulk_display_name,
    _sanitize_archive_name,
)
from utils import archive_split
from utils.callbacks import archive_part_key
from utils.keyboard_utils import MediaMenuBuilder


class ArchiveSelectionTests(unittest.TestCase):
    def test_it_reads_the_batch_and_normalizes_each_entry(self):
        selection = _archive_selection(["a.mp4", {"path": "b.mp3"}, {"name": "c.png"}])
        # A bare path entry is given the name it will be shown and packed under.
        self.assertEqual([_bulk_display_name(e) for e in selection], ["a.mp4", "b.mp3", "c.png"])
        self.assertEqual([e["type"] for e in selection], ["video", "audio", "photo"])

    def test_junk_entries_are_left_out(self):
        self.assertEqual(_archive_selection([None, "", 17]), [])

    def test_duplicate_entries_are_collapsed(self):
        entry = {"path": "a.mp4", "id": "x"}
        self.assertEqual(len(_archive_selection([entry, entry])), 1)

    def test_it_keeps_the_batch_order(self):
        names = [e["name"] for e in _archive_selection(["z.mp4", "a.mp4", "m.mp4"])]
        self.assertEqual(names, ["z.mp4", "a.mp4", "m.mp4"])

    def test_nothing_staged_is_an_empty_selection(self):
        self.assertEqual(_archive_selection([]), [])
        self.assertEqual(_archive_selection(None), [])


class ArchiveSizeTests(unittest.TestCase):
    def test_the_total_counts_only_the_sizes_that_are_known(self):
        self.assertEqual(_archive_total_bytes([{"size": 10}, {"size": "20"}, {}]), 30)

    def test_unusable_sizes_are_ignored_rather_than_guessed(self):
        self.assertEqual(_archive_total_bytes([{"size": "big"}, {"size": None}, {"size": -5}]), 0)

    def test_labels(self):
        self.assertEqual(_archive_size_label(0), "unknown")
        self.assertEqual(_archive_size_label(None), "unknown")
        self.assertEqual(_archive_size_label(1536), "1.5 KB")
        self.assertEqual(_archive_size_label(5 * 1024**2), "5.0 MB")
        self.assertEqual(_archive_size_label(3 * 1024**3), "3.0 GB")
        self.assertEqual(_archive_size_label(512), "512 B")


class ArchiveNameTests(unittest.TestCase):
    def test_a_typed_name_is_reduced_to_one_safe_stem(self):
        self.assertEqual(_sanitize_archive_name("  My Clips  "), "My Clips")
        # A path is never kept, and the extension the delivery adds is not doubled.
        self.assertEqual(_sanitize_archive_name("dir/sub/clips.zip"), "clips")
        self.assertEqual(_sanitize_archive_name("../evil"), "evil")
        self.assertEqual(_sanitize_archive_name("a:b*c?.mp4"), "abc.mp4")

    def test_an_unusable_name_is_empty(self):
        for raw in ("", "   ", "///", "..", None):
            with self.subTest(raw=raw):
                self.assertEqual(_sanitize_archive_name(raw), "")

    def test_the_default_name_comes_from_the_first_file(self):
        selection = _archive_selection(["Concert.mp4", "b.mp3"])
        self.assertEqual(_archive_default_name(selection), "Concert_archive")
        self.assertEqual(_archive_default_name([]), "media_archive")

    def test_the_summary_states_the_name_the_parts_and_the_batch_clearing(self):
        selection = _archive_selection([{"name": "clip.mp4", "size": 1024}])
        text = _archive_summary_text(
            selection, "My Clips", archive_split.size_value(500 * 1024**2), cap_bytes=2 * 1024**3
        )
        self.assertIn("My Clips.zip", text)
        self.assertIn("Parts:", text)
        self.assertIn("500.0 MB", text)
        self.assertIn("Packing clears the batch", text)
        self.assertIn("clip.mp4", text)


class ArchiveKeyboardTests(unittest.TestCase):
    def test_the_prompt_answers_with_its_own_triggers(self):
        """Not the generic confirm/cancel - those belong to whatever asked last."""
        kb = MediaMenuBuilder.get_archive_confirm_menu()
        data = {b.callback_data for row in kb.inline_keyboard for b in row}
        self.assertEqual(data, {"archive_confirm", "archive_cancel", "archive_part_menu"})

    def test_the_name_prompt_offers_the_default_a_part_shortcut_and_a_cancel(self):
        kb = MediaMenuBuilder.get_archive_name_menu()
        data = {b.callback_data for row in kb.inline_keyboard for b in row}
        self.assertEqual(data, {"archive_name_default", "archive_part_menu", "archive_cancel"})

    def test_the_part_picker_stays_in_the_archive_flow(self):
        """Its Back returns to the summary, and it offers the sizes plus a custom."""
        kb = MediaMenuBuilder.get_archive_part_menu("default")
        data = {b.callback_data for row in kb.inline_keyboard for b in row}
        self.assertIn("archive_part_back", data)
        self.assertIn("archive_set_part:custom", data)
        self.assertIn("archive_set_part:default", data)
        self.assertIn("archive_set_part:off", data)
        self.assertIn(archive_part_key(archive_split.preset_value(archive_split.PRESET_SIZES[0])), data)


class _FakeMessage:
    def __init__(self, text=None):
        self.text = text
        self.replies = []

    async def reply_text(self, text, **_kwargs):
        self.replies.append(text)
        return self


class _FakeQuery:
    def __init__(self):
        self.message = None


class _FakeUser:
    def __init__(self, user_id=42):
        self.id = user_id


class _FakeChat:
    def __init__(self, chat_id=7):
        self.id = chat_id


class _FakeContext:
    """A context whose ``user_data`` is the real dict the awaiting flags live on."""

    def __init__(self):
        self.user_data = {}


class _FakeUpdate:
    def __init__(self, user_id=42, chat_id=7, text=None):
        self.callback_query = _FakeQuery()
        self.effective_user = _FakeUser(user_id)
        self.effective_chat = _FakeChat(chat_id)
        self.request_id = "req1"
        self.message = _FakeMessage(text)


def _handler(edits):
    handler = object.__new__(EnhancedMediaHandler)
    handler.user_sessions = {}
    handler._persist_session = lambda *_a, **_k: None

    async def _yes(*_a, **_k):
        return True

    async def _edit(_query, text, **_kwargs):
        edits.append(text)
        return True

    async def _no_op(*_a, **_k):
        return None

    handler._require_callback = _yes
    handler.safe_edit = _edit
    handler._watch_job_progress = _no_op
    return handler


class CreateArchiveStagingTests(unittest.TestCase):
    """The button stages the batch and asks for the archive's name."""

    def _stage(self, session):
        edits = []
        handler = _handler(edits)
        update = _FakeUpdate()
        context = _FakeContext()
        enqueued = []

        async def _record(job):
            enqueued.append(job)

        with mock.patch.object(handlers, "enqueue_job", _record):
            asyncio.run(handler.create_archive(update, context, session))
        return handler, session, edits, enqueued, context

    def test_it_stages_the_batch_and_asks_for_a_name(self):
        session = {"bulk_list": [{"name": "clip.mp4", "size": 1024}, {"name": "song.mp3", "size": 512}]}
        handler, session, edits, enqueued, context = self._stage(session)

        # Nothing is packed here; the confirm does that.
        self.assertEqual(enqueued, [])
        self.assertEqual(session["archive_pending"], session["bulk_list"])
        self.assertEqual(session["archive_pending_source"], "bulk_list")
        self.assertTrue(context.user_data.get("awaiting_archive_name"))
        prompt = edits[-1]
        self.assertIn("Create Archive", prompt)
        self.assertIn("Send a name", prompt)
        self.assertIn("clip_archive", prompt)
        self.assertIn("1.5 KB", prompt)
        # The name prompt also states the current part-size setting.
        self.assertIn("Part size:", prompt)

    def test_a_single_loaded_file_is_still_something_to_pack(self):
        session = {"bulk_list": [], "current_file": {"name": "only.mp4"}}
        handler, session, edits, _, _ = self._stage(session)
        self.assertEqual([e["name"] for e in session["archive_pending"]], ["only.mp4"])

    def test_the_batch_is_preferred_over_the_loaded_file(self):
        session = {
            "bulk_list": [{"name": "batched.mp4"}],
            "current_file": {"name": "loaded.mp4"},
        }
        handler, session, edits, _, _ = self._stage(session)
        self.assertEqual([e["name"] for e in session["archive_pending"]], ["batched.mp4"])

    def test_an_empty_batch_asks_for_media_instead_of_packing_the_output_dir(self):
        session = {"bulk_list": []}
        handler, session, edits, enqueued, context = self._stage(session)
        self.assertNotIn("archive_pending", session)
        self.assertEqual(enqueued, [])
        self.assertFalse(context.user_data.get("awaiting_archive_name"))
        self.assertIn("No files to archive", edits[-1])

    def test_the_default_button_accepts_the_derived_name_and_summarizes(self):
        session = {"bulk_list": [{"name": "clip.mp4", "size": 10}]}
        handler, session, edits, _, context = self._stage(session)
        context.user_data["awaiting_archive_name"] = True
        handler.safe_edit = None

        async def _edit(_query, text, **kwargs):
            edits.append(text)
            return True

        handler.safe_edit = _edit
        update = _FakeUpdate()
        asyncio.run(handler._archive_use_default_name(update, context, session))
        self.assertEqual(session["archive_name"], "clip_archive")
        self.assertFalse(context.user_data.get("awaiting_archive_name"))
        self.assertIn("clip_archive.zip", edits[-1])


class ArchiveNameInputTests(unittest.TestCase):
    """The typed answer to the name prompt."""

    def _submit(self, session, typed):
        handler = _handler([])
        handler.user_sessions = {42: session}
        shown = []

        async def _summary(_session, name, **kwargs):
            shown.append(name)

        handler._show_archive_summary = _summary
        context = _FakeContext()
        context.user_data["awaiting_archive_name"] = True
        update = _FakeUpdate(text=typed)
        asyncio.run(handler.handle_custom_input(update, context))
        return handler, session, context, update, shown

    def test_a_typed_name_is_sanitized_stored_and_shown(self):
        session = {"archive_pending": [{"name": "clip.mp4"}]}
        handler, session, context, update, shown = self._submit(session, "My Clips")
        self.assertEqual(session["archive_name"], "My Clips")
        self.assertEqual(shown, ["My Clips"])
        self.assertFalse(context.user_data.get("awaiting_archive_name"))

    def test_an_unusable_name_keeps_the_prompt_open(self):
        session = {"archive_pending": [{"name": "clip.mp4"}]}
        handler, session, context, update, shown = self._submit(session, "///")
        self.assertNotIn("archive_name", session)
        self.assertEqual(shown, [])
        self.assertTrue(context.user_data.get("awaiting_archive_name"))
        self.assertTrue(any("no usable characters" in r for r in update.message.replies))


class ConfirmArchiveTests(unittest.TestCase):
    """Confirm packs exactly the staged selection, one member at a time."""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="archive_confirm_")
        self._local = os.path.join(self._dir, "clip.mp4")
        with open(self._local, "wb") as fh:
            fh.write(b"v")

    def tearDown(self):
        import shutil

        shutil.rmtree(self._dir, ignore_errors=True)

    def _confirm(self, session):
        edits = []
        handler = _handler(edits)
        update = _FakeUpdate()
        jobs = []

        async def _record(job):
            jobs.append(job)

        async def _ensure(*_a, **_k):
            return None

        handler._ensure_bulk_file_downloaded = _ensure

        async def _go():
            with (
                mock.patch.object(handlers, "enqueue_job", _record),
                mock.patch.object(config, "OUTPUT_PATH", self._dir, create=True),
                mock.patch.object(config, "ARCHIVE_SPLIT_MAX_BYTES", 1234, create=True),
                # The part size is a user setting; pin it so the assertion is
                # about the resolution, not about a settings file on this host.
                mock.patch.object(handlers, "_user_archive_part", lambda _uid: archive_split.DEFAULT_VALUE),
            ):
                await handler.confirm_archive(update, mock.Mock(), session)
                # Let the progress watcher's scheduled task finish before the loop
                # closes, so it is not cancelled mid-flight.
                await asyncio.sleep(0)

        asyncio.run(_go())
        return handler, session, edits, jobs

    def test_it_packs_local_files_and_storage_members(self):
        session = {
            "archive_pending": [
                {"name": "clip.mp4", "path": self._local},
                {"name": "remote.mp3", "input_key": "inputs/x/source"},
            ]
        }
        handler, session, edits, jobs = self._confirm(session)

        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job["type"], "create_archive")
        self.assertEqual(job["user_id"], 42)
        self.assertEqual([f["name"] for f in job["files"]], ["clip.mp4", "remote.mp3"])
        self.assertEqual(job["files"][0]["path"], self._local)
        self.assertEqual(job["files"][1]["input_key"], "inputs/x/source")
        # No name was typed, so the first file's stem names the archive.
        self.assertEqual(job["original_filename"], "clip.zip")
        # The worker only splits past this cap, and no part count was requested.
        self.assertEqual(job["split_max_bytes"], 1234)
        self.assertEqual(job["split_parts"], 0)
        # The staged selection is consumed, so a double press cannot re-pack it.
        self.assertNotIn("archive_pending", session)
        self.assertIn("packing 2 file(s)", edits[-1])

    def test_a_typed_name_names_the_archive(self):
        session = {
            "archive_pending": [{"name": "clip.mp4", "path": self._local}],
            "archive_name": "My Clips",
        }
        _, _, _, jobs = self._confirm(session)
        self.assertEqual(jobs[0]["original_filename"], "My Clips.zip")

    def test_a_part_count_setting_rides_the_job(self):
        session = {"archive_pending": [{"name": "clip.mp4", "path": self._local}]}
        edits = []
        handler = _handler(edits)
        update = _FakeUpdate()
        jobs = []

        async def _record(job):
            jobs.append(job)

        async def _ensure(*_a, **_k):
            return None

        handler._ensure_bulk_file_downloaded = _ensure
        parts = archive_split.parts_value(3)

        async def _go():
            with (
                mock.patch.object(handlers, "enqueue_job", _record),
                mock.patch.object(config, "OUTPUT_PATH", self._dir, create=True),
                mock.patch.object(handlers, "_user_archive_part", lambda _uid: parts),
            ):
                await handler.confirm_archive(update, mock.Mock(), session)
                await asyncio.sleep(0)

        asyncio.run(_go())
        self.assertEqual(jobs[0]["split_parts"], 3)
        self.assertEqual(jobs[0]["split_max_bytes"], 0)

    def test_the_batch_is_cleared_once_the_archive_is_queued(self):
        session = {
            "archive_pending": [{"name": "clip.mp4", "path": self._local}],
            "archive_pending_source": "bulk_list",
            "bulk_list": [{"name": "clip.mp4", "path": self._local}],
        }
        _, session, _, jobs = self._confirm(session)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(session["bulk_list"], [])

    def test_an_unreadable_member_is_named_not_silently_dropped(self):
        session = {
            "archive_pending": [
                {"name": "clip.mp4", "path": self._local},
                {"name": "gone.mp4", "path": os.path.join(self._dir, "missing.mp4")},
            ]
        }
        handler, session, edits, jobs = self._confirm(session)
        self.assertEqual([f["name"] for f in jobs[0]["files"]], ["clip.mp4"])
        self.assertIn("1 file(s) could not be read", edits[-1])

    def test_nothing_readable_stops_without_queueing_a_job(self):
        session = {"archive_pending": [{"name": "gone.mp4"}]}
        handler, session, edits, jobs = self._confirm(session)
        self.assertEqual(jobs, [])
        self.assertIn("None of the batched files could be read", edits[-1])

    def test_an_expired_request_does_not_pack_anything(self):
        session = {}
        handler, session, edits, jobs = self._confirm(session)
        self.assertEqual(jobs, [])
        self.assertIn("expired", edits[-1])


class _FakeSettings:
    """A settings store that keeps values in memory instead of on disk."""

    def __init__(self):
        self.values = {}

    def set_user_setting(self, user_id, key, value):
        self.values[(user_id, key)] = value

    def get_user_setting(self, user_id, key, default=None):
        return self.values.get((user_id, key), default)


class ArchivePartPickerTests(unittest.TestCase):
    """The summary's 📐 picker stores the shared part setting and comes back."""

    def _set_part(self, value, session=None):
        edits = []
        handler = _handler(edits)
        session = session if session is not None else {
            "archive_pending": [{"name": "clip.mp4", "size": 1024}],
            "archive_name": "My Clips",
        }
        fake = _FakeSettings()
        update = _FakeUpdate()
        context = _FakeContext()
        with mock.patch.object(handlers, "user_settings", fake):
            asyncio.run(handler._archive_set_part(update, context, session, value))
        return handler, session, edits, fake, context

    def test_a_preset_is_stored_and_the_summary_returns(self):
        _, _, edits, fake, _ = self._set_part(archive_split.parts_value(3))
        self.assertEqual(fake.values[(42, "archive_part")], archive_split.parts_value(3))
        self.assertIn("3 equal parts", edits[-1])
        self.assertIn("My Clips.zip", edits[-1])

    def test_custom_arms_the_prompt_without_storing_anything(self):
        _, _, edits, fake, context = self._set_part("custom")
        self.assertEqual(fake.values, {})
        self.assertTrue(context.user_data.get("awaiting_archive_part_size"))
        self.assertIn("Send the archive part size", edits[-1])

    def test_a_part_size_picked_before_the_name_returns_to_the_name_prompt(self):
        """No name chosen yet -> the name prompt, not a summary that assumes one."""
        _, _, edits, fake, _ = self._set_part(
            archive_split.size_value(500 * 1024**2),
            session={"archive_pending": [{"name": "clip.mp4", "size": 1024}]},
        )
        self.assertEqual(fake.values[(42, "archive_part")], archive_split.size_value(500 * 1024**2))
        self.assertIn("Send a name", edits[-1])
        self.assertIn("500.0 MB", edits[-1])
        self.assertNotIn("Pack these into a ZIP?", edits[-1])


class ArchiveWiringTests(unittest.TestCase):
    def test_the_handler_stages_a_selection_instead_of_sweeping_the_output_dir(self):
        src = read_source("handlers.py")
        self.assertIn('session["archive_pending"] = selection', src)
        self.assertIn("MediaMenuBuilder.get_archive_confirm_menu()", src)
        # The output-directory sweep is gone: an archive holds the batch now.
        self.assertNotIn("_archive_candidates(", src)

    def test_create_archive_no_longer_enqueues_directly(self):
        src = read_source("handlers.py")
        create = src.index("async def create_archive(")
        confirm = src.index("async def confirm_archive(")
        self.assertNotIn("enqueue_job(", src[create:confirm])

    def test_the_confirm_queues_the_archive_with_its_owner_and_split_cap(self):
        src = read_source("handlers.py")
        confirm = src.index("async def confirm_archive(")
        body = src[confirm : src.index("async def show_media_info(")]
        self.assertIn('"type": "create_archive",', body)
        self.assertIn('"files": sources,', body)
        self.assertIn('"user_id": user_id,', body)
        self.assertIn('"split_max_bytes":', body)
        self.assertIn('"split_parts":', body)
        # A large member is fetched by the worker from its object key, one file at
        # a time, rather than the whole set being downloaded first.
        self.assertIn('sources.append({"name": name, "input_key": key})', body)

    def test_both_prompt_answers_are_dispatched(self):
        src = read_source("handlers.py")
        self.assertIn('elif data == "archive_confirm":', src)
        self.assertIn("await self.confirm_archive(update, context, session)", src)
        self.assertIn('elif data == "archive_cancel":', src)
        self.assertIn('elif data == "archive_name_default":', src)
        self.assertIn('elif data == "archive_part_menu":', src)
        self.assertIn('data.startswith("archive_set_part:")', src)
        self.assertIn('elif data == "archive_part_back":', src)

    def test_the_cancel_keeps_the_batch(self):
        src = read_source("handlers.py")
        start = src.index('elif data == "archive_cancel":')
        # The cancel drops the staged copy only; it never clears bulk_list.
        self.assertNotIn("bulk_list", src[start : start + 400])
        self.assertIn('sess.pop("archive_pending", None)', src[start : start + 400])

    def test_the_worker_splits_an_oversized_archive_and_skips_single_output_delivery(self):
        src = read_source("workers", "ffmpeg_worker.py")
        self.assertIn("_split_and_deliver_archive(job, output_path, progress_channel)", src)
        self.assertIn("_archive_volumes_sent", src)
        self.assertIn("split_archive_volumes(", src)
        self.assertIn('job.get("split_parts")', src)


if __name__ == "__main__":
    unittest.main()
