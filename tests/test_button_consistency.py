"""Buttons and the pipe they deliver through: the invariants, checked.

Three classes of gap have shown up on real presses, and none of them is visible in
a single button's own test:

* an operation that produces a file and hands it straight to the Bot API with no
  ceiling check - the encode finished, the upload was refused, and the button
  looked broken. Every result goes through :meth:`_deliver_inline_result`, which
  is where the 50MB tier (and the userbot behind it) lives;
* a job queued with nothing watching it, so the only thing the user ever saw was
  the line that said it had been queued;
* one keyboard offering the same thing twice, under two labels.

The first two are behaviour of the shared helpers, so they are tested against the
helpers; the third is a property of the keyboards, so it is read off them.
"""

import ast
from pathlib import Path
from types import MethodType
from unittest import mock

from source_helpers import parse_source

import handlers
from handlers import EnhancedMediaHandler
from utils.keyboard_utils import MediaMenuBuilder

PROJECT_ROOT = Path(__file__).resolve().parent.parent

#: The one delivery every in-process result takes. A button that produced a file
#: and sent it with its own ``send_audio``/``send_video``/``send_document`` is
#: exactly the gap this file exists for.
INLINE_DELIVERY_HELPERS = (
    "_deliver_inline_result",
    "_send_video_result",
    "_send_audio_result",
    "_send_photo_result",
    "_send_part_via_userbot",
    "_send_audio_via_userbot",
    "_deliver_split_parts",
    "_burn_subtitle_into_current",
    "_redeliver_current_media",
)

#: The only sends that may bypass the tiers: a status *text*, and the extracted
#: subtitle, which is a few kilobytes of text by construction. Photos are not in
#: MEDIA_SENDS at all, so the screenshot paths need no entry here - keeping this
#: list short is the point, since every name on it is a place a gap could hide.
SEND_EXEMPT = {
    "_say_in_chat",
    "notify",
    "reply_text",
    "_batch_subtitle_status_query",
    "extract_subtitles",
}

#: Operations that encode in this process and deliver the file they produced.
#: Each one has to route through the shared delivery - these were the buttons that
#: failed with Telegram's own "file is too big" after finishing the work. The
#: custom-optimize branch lives in the dispatcher itself, and is covered by the
#: sweep below rather than by a name here.
INLINE_RESULT_PRODUCERS = (
    "_apply_fade",
    "_trim_current_media",
    "_merge_videos_core",
    "merge_audios",
    "normalize_audio",
    "adjust_bitrate",
    "convert_to_mp3",
)

MEDIA_SENDS = ("send_audio", "send_video", "send_document")

#: Queued jobs nobody watches. The URL commands are the exception and deliberate:
#: one reply is posted for N URLs, and one message cannot honestly show N jobs'
#: progress - the alternatives were a message per URL or a message that lies.
WATCHER_EXEMPT = {
    "bulk_url_command",
    "handle_media_message",
}


def _tree(relative: str) -> ast.Module:
    return parse_source(relative)


def _functions(tree: ast.Module) -> dict[str, list[ast.AST]]:
    out: dict[str, list[ast.AST]] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out.setdefault(node.name, []).append(node)
    return out


def _calls(func: ast.AST) -> list[str]:
    """Attribute-style call names inside *func* (``self.x``, ``context.bot.y``)."""

    def walk(node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = walk(node.value)
            return f"{base}.{node.attr}" if base else node.attr
        if isinstance(node, ast.Call):
            return walk(node.func)
        return ""

    names = []
    for node in ast.walk(func):
        if isinstance(node, ast.Call):
            names.append(walk(node.func))
    return names


def _source_of(relative: str, name: str) -> str:
    tree = _tree(relative)
    bodies = [ast.unparse(node) for node in _functions(tree).get(name, [])]
    assert bodies, f"{relative}: {name} not found - the audit would pass by finding nothing"
    return " ".join(bodies)


# ── the shared delivery: the 50MB tier and the streamed header ──────────────


class _FakeUser:
    def __init__(self, user_id=42):
        self.id = user_id


class _FakeChat:
    id = 7


class _FakeUpdate:
    def __init__(self):
        self.effective_user = _FakeUser()
        self.effective_chat = _FakeChat()


class _FakeBot:
    def __init__(self):
        self.audio = []
        self.documents = []
        self.messages = []

    async def send_audio(self, **kwargs):
        self.audio.append(kwargs)
        return mock.Mock()

    async def send_document(self, **kwargs):
        self.documents.append(kwargs)
        return mock.Mock()

    async def send_message(self, **kwargs):
        self.messages.append(kwargs)
        return mock.Mock()


class DeliveryTierTests:
    """Shared fixture: a handler whose deliveries are recorded, not performed."""

    def _handler(self):
        handler = object.__new__(EnhancedMediaHandler)
        handler.calls = {"audio_userbot": [], "file_userbot": [], "video": []}

        async def _audio_userbot(chat_id, path, caption, name, current, *, user_id=None):
            handler.calls["audio_userbot"].append((path, name))
            return True

        async def _file_userbot(chat_id, path, caption, name, as_audio, current, label, **kwargs):
            handler.calls["file_userbot"].append((path, label, kwargs.get("as_document")))
            return True

        async def _video(bot, chat_id, path, caption="", **kwargs):
            handler.calls["video"].append((path, kwargs.get("delivery_name"), kwargs.get("upload_mode")))
            return "file-id"

        handler._send_audio_via_userbot = _audio_userbot
        handler._send_part_via_userbot = _file_userbot
        handler._send_video_result = _video
        handler.deliver = MethodType(EnhancedMediaHandler._deliver_inline_result, handler)
        return handler


def test_an_over_limit_audio_result_is_carried_by_the_userbot(tmp_path, monkeypatch):
    """Over the ceiling the Bot API refuses the upload; the userbot carries it."""
    handler = DeliveryTierTests()._handler()
    path = tmp_path / "big.mp3"
    path.write_bytes(b"x" * 2048)
    monkeypatch.setattr(handlers.config, "BOT_API_MAX_BYTES", 1024, raising=False)
    monkeypatch.setattr(handlers.config, "ENABLE_USERBOT", True, raising=False)
    bot = _FakeBot()

    class _Ctx:
        pass

    ctx = _Ctx()
    ctx.bot = bot
    with mock.patch("handlers._metadata_caption", lambda *_a, **_k: "cap"):
        sent = _run(
            handler.deliver(_FakeUpdate(), ctx, str(path), {"name": "big.mp3"}, kind="audio", delivery_name="big.mp3")
        )

    assert sent is True
    assert handler.calls["audio_userbot"] == [(str(path), "big.mp3")]
    assert bot.audio == [], "the Bot API send must not be attempted over the ceiling"


def test_an_over_limit_result_with_the_userbot_off_says_so(tmp_path, monkeypatch):
    """No userbot means no delivery - and it is stated, not attempted."""
    handler = DeliveryTierTests()._handler()
    path = tmp_path / "big.mp4"
    path.write_bytes(b"x" * 4096)
    monkeypatch.setattr(handlers.config, "BOT_API_MAX_BYTES", 1024, raising=False)
    monkeypatch.setattr(handlers.config, "ENABLE_USERBOT", False, raising=False)
    monkeypatch.setattr(handlers.config, "BOT_API_MAX_MB", 50, raising=False)
    notes = []

    async def _notify(text):
        notes.append(text)

    with mock.patch("handlers._metadata_caption", lambda *_a, **_k: "cap"):
        sent = _run(
            handler.deliver(
                _FakeUpdate(),
                _Ctx(),
                str(path),
                {"name": "big.mp4"},
                kind="video",
                delivery_name="big.mp4",
                notify=_notify,
            )
        )

    assert sent is False
    assert handler.calls["file_userbot"] == []
    assert notes and "over Telegram's Bot API limit" in notes[0]


def test_a_video_result_keeps_the_streamed_delivery_and_the_upload_mode(tmp_path, monkeypatch):
    handler = DeliveryTierTests()._handler()
    path = tmp_path / "clip.mp4"
    path.write_bytes(b"x" * 10)
    monkeypatch.setattr(handlers.config, "BOT_API_MAX_BYTES", 1024, raising=False)
    with (
        mock.patch("handlers._user_upload_mode", lambda _uid: "file"),
        mock.patch("handlers._metadata_caption", lambda *_a, **_k: "cap"),
    ):
        sent = _run(
            handler.deliver(
                _FakeUpdate(), _Ctx(), str(path), {"name": "clip.mp4"}, kind="video", delivery_name="clip.mp4"
            )
        )

    assert sent is True
    assert handler.calls["video"] == [(str(path), "clip.mp4", "file")]


class _Ctx:
    """A context whose only job is to hold the bot."""

    def __init__(self):
        self.bot = _FakeBot()


def _run(coro):
    """Run a coroutine to completion without importing an event loop helper."""
    import asyncio

    return asyncio.run(coro)


def test_each_inline_result_takes_the_shared_delivery():
    """Fade, trim, merge, normalize, bitrate, mp3 and optimize all route through it."""
    missing = []
    for name in INLINE_RESULT_PRODUCERS:
        source = _source_of("handlers.py", name)
        if "_deliver_inline_result" not in source:
            missing.append(f"{name} does not call _deliver_inline_result")
    assert not missing, missing


def test_no_inline_result_producer_sends_media_itself():
    """The helpers own the send; a producer owning one is the gap coming back."""
    offenders = []
    for name in INLINE_RESULT_PRODUCERS:
        source = _source_of("handlers.py", name)
        for send in MEDIA_SENDS:
            if f"bot.{send}(" in source:
                offenders.append(f"{name} -> {send}")
    assert not offenders, offenders


def test_the_shared_delivery_reads_the_bot_api_ceiling():
    source = _source_of("handlers.py", "_deliver_inline_result")
    assert "config.BOT_API_MAX_BYTES" in source
    assert "_send_audio_via_userbot" in source
    assert "_send_part_via_userbot" in source
    assert "ENABLE_USERBOT" in source


def test_no_media_send_on_a_button_path_bypasses_the_helpers():
    """Every media send is a helper's, a status text, a photo, or ceiling-checked.

    A function that compares the output against ``BOT_API_MAX_BYTES`` and picks
    its delivery itself is the shape ``convert_audio_format`` uses, so it counts
    as tiered. Anything else sending the converted media is the gap.
    """
    offenders = []
    for node in ast.walk(_tree("handlers.py")):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name in SEND_EXEMPT or node.name in INLINE_DELIVERY_HELPERS:
            continue
        if "BOT_API_MAX_BYTES" in ast.unparse(node):
            continue
        for called in _calls(node):
            for send in MEDIA_SENDS:
                if called.endswith(f".bot.{send}"):
                    offenders.append(f"{node.name}:{node.lineno} -> {called}")
    assert not offenders, f"these sends skip the size tiers: {sorted(set(offenders))}"


# ── a queued job has something watching it ─────────────────────────────────


def test_every_queued_job_starts_a_watcher_or_goes_through_an_enqueue_helper():
    offenders = []
    for name, nodes in _functions(_tree("handlers.py")).items():
        if name in WATCHER_EXEMPT:
            continue
        for node in nodes:
            calls = _calls(node)
            if not any(c.endswith("enqueue_job") or c == "enqueue_job" for c in calls):
                continue
            watches = any("_watch_" in c for c in calls)
            # The two enqueue helpers post the job's own message and hand it to a
            # watcher themselves, and bulk apply waits inside its own loop.
            helper = any(
                c.endswith(("_enqueue_keyed_job", "_enqueue_worker_job", "_await_member_job", "_watch_batch_member"))
                for c in calls
            )
            if not (watches or helper):
                offenders.append(f"{name}:{node.lineno}")
    assert not offenders, f"queued with nothing watching it: {offenders}"


def test_the_archive_confirm_watches_its_job():
    """The archive is a queued job like any other: it gets a progress message."""
    source = _source_of("handlers.py", "confirm_archive")
    assert "enqueue_job" in source
    assert "_watch_job_progress" in source
    assert "cancel_job:" in source


# ── one keyboard, one button per thing ─────────────────────────────────────


def _keyboards():
    """Every keyboard builder, with the buttons it renders."""
    for name in dir(MediaMenuBuilder):
        if not name.startswith("get_"):
            continue
        builder = getattr(MediaMenuBuilder, name)
        if not callable(builder):
            continue
        for args in ((), ({},), ("audio",), ("video",), (1,), (1, {}), ("video", "128k")):
            try:
                markup = builder(*args)
            except Exception:
                continue
            if markup is None:
                continue
            yield name, args, [(b.text, b.callback_data) for row in markup.inline_keyboard for b in row]


def test_no_keyboard_offers_the_same_label_twice():
    """Two identical labels on one panel read as one button that does two things."""
    offenders = []
    for name, args, buttons in _keyboards():
        seen = {}
        for label, data in buttons:
            if label in seen and seen[label] != data:
                offenders.append(f"{name}{args}: {label!r} -> {seen[label]} and {data}")
            seen[label] = data
    assert not offenders, offenders


def test_no_keyboard_offers_the_same_trigger_twice():
    offenders = []
    for name, args, buttons in _keyboards():
        seen = {}
        for label, data in buttons:
            if data in seen and seen[data] != label:
                offenders.append(f"{name}{args}: {data} -> {seen[data]!r} and {label!r}")
            seen[data] = label
    assert not offenders, offenders


#: Triggers that navigate rather than act: going Back, opening a menu, cancelling.
#: They are *meant* to appear on many panels, so the one-home rule skips them.
NAVIGATION_TRIGGERS = {
    "menu_main",
    "menu_video",
    "menu_audio",
    "bulk_menu",
    "cancel",
    "archive_cancel",
    "bulk_subtitles",
    "merge_av_menu",
    "merge_options",
    "extraction_menu",
    "bitrate_menu",
    "merge_audios_menu",
    "merge_videos_menu",
    "settings_page:1",
    "settings_page:2",
    "settings_page:3",
}


NAVIGATION_PREFIXES = (
    "settings_page:",
    "archive_set_part:",
    "settings_set_",
    "bulk_set_",
    "cancel_job:",
    "cancel_dl:",
    "reorder_",
)


def _action_triggers(buttons) -> set[str]:
    """The triggers on a panel that *do* something, navigation excluded."""
    out = set()
    for _label, data in buttons:
        if not data or data in NAVIGATION_TRIGGERS:
            continue
        if any(data.startswith(prefix) for prefix in NAVIGATION_PREFIXES):
            continue
        out.add(data.split(":", 1)[0] if not data.startswith("settings_") else data)
    return out


def test_no_menu_repeats_another_menus_actions():
    """No panel is a second copy of another - which is what Advanced Tools was.

    Its four action buttons all sat on the main menu already, so opening it could
    not tell the user anything the main menu had not, and the same action had two
    homes. A panel that offers actions *plus* more of its own is fine; a panel
    whose whole action set is contained in another's is the redundancy.
    """
    menus = []
    for name, args, buttons in _keyboards():
        actions = _action_triggers(buttons)
        if actions:
            menus.append((f"{name}{args}", actions))
    offenders = []
    for left_name, left in menus:
        for right_name, right in menus:
            if left_name == right_name:
                continue
            if left and left < right:
                offenders.append(f"{left_name} repeats {right_name}: {sorted(left)}")
    assert not offenders, offenders


def test_video_tools_only_adds_what_the_main_menu_does_not():
    """The tools panel is the *additional* video tools, not a second main menu.

    Every action has one home: the main menu carries the video actions it acts on
    directly, and this panel must not repeat a single one of them.
    """
    video = _action_triggers(
        [(b.text, b.callback_data) for row in MediaMenuBuilder.get_video_tools_menu().inline_keyboard for b in row]
    )
    main = _action_triggers(
        [(b.text, b.callback_data) for row in MediaMenuBuilder.get_main_menu("video").inline_keyboard for b in row]
    )
    main_callbacks = {b.callback_data for row in MediaMenuBuilder.get_main_menu("video").inline_keyboard for b in row}
    video_callbacks = {b.callback_data for row in MediaMenuBuilder.get_video_tools_menu().inline_keyboard for b in row}
    # Repair and reorder are the video jobs the main menu has no button for, so
    # they live here; everything else on the panel is likewise not on the main menu.
    assert {"repair_video", "video_reorder"} <= video, video
    assert not (video & main), f"the video tools panel repeats the main menu: {sorted(video & main)}"
    assert "merge_options" in main_callbacks, "the merge entry point belongs on the media context menu"
    assert "merge_options" not in video_callbacks, "the merge entry point should not remain in Video Tools"
    assert "merge_view" not in main_callbacks, "the obsolete Merge And shortcut should be removed"
    assert "menu_advanced" not in main, "the Advanced Tools panel is gone, so its button must be too"


def test_every_panel_can_be_cancelled():
    """A half-started flow can always be dropped: Cancel is on every panel."""
    missing = []
    for name, _args, buttons in _keyboards():
        triggers = {data for _label, data in buttons}
        labels = {label for label, _data in buttons}
        if not triggers:
            continue
        # "cancel" (or the flow's own cancel, e.g. archive_cancel) counts.
        if not any(t == "cancel" or (t or "").endswith("cancel") for t in triggers):
            missing.append(f"{name}: {sorted(triggers)}")
        if not any("cancel" in label.lower() for label in labels):
            missing.append(f"{name}: no Cancel label")
    assert not missing, missing


def test_the_reorder_panel_moves_items_with_up_and_down():
    markup = MediaMenuBuilder.get_reorder_menu(["a.mp4", "b.mp4", "c.mp4"])
    rows = [[b.callback_data for b in row] for row in markup.inline_keyboard]
    flat = [data for row in rows for data in row]
    assert "reorder_up:1" in flat and "reorder_down:0" in flat
    # The first item cannot move up and the last cannot move down.
    assert "reorder_up:0" not in flat
    assert "reorder_down:2" not in flat


def test_the_reorder_move_is_routed_and_offered():
    source = _source_of("handlers.py", "callback_handler").replace("'", '"')
    assert '"video_reorder"' in source
    assert 'startswith("reorder_")' in source


def test_the_audio_merge_menu_starts_the_audio_merge():
    """The two typed review lists expose only their corresponding merge action."""
    audio = {
        b.callback_data
        for row in MediaMenuBuilder.get_audio_merge_review_menu(["1", "2"], True).inline_keyboard
        for b in row
    }
    video = {
        b.callback_data
        for row in MediaMenuBuilder.get_video_merge_review_menu(["1", "2"], True).inline_keyboard
        for b in row
    }
    assert "merge_audios_start" in audio
    assert "merge_videos_start" not in audio
    assert "merge_videos_start" in video
    assert "merge_audios_start" not in video
    assert not {"merge_add", "merge_view", "merge_clear"} & audio


def test_both_merge_starts_are_routed():
    """The audio start existed in the dispatcher and was offered nowhere."""
    # ``ast.unparse`` normalises the quotes, so the comparison is on the name.
    source = _source_of("handlers.py", "callback_handler").replace("'", '"')
    assert '"merge_audios_start"' in source
    assert '"merge_videos_start"' in source


def test_the_duplicate_panel_check_would_have_caught_the_old_advanced_menu():
    """Arms the check above: the old Advanced Tools set was inside the main menu's."""
    main = _action_triggers(
        [(b.text, b.callback_data) for row in MediaMenuBuilder.get_main_menu("video").inline_keyboard for b in row]
    )
    # Exactly what the panel used to offer, from the rows that were there.
    old_advanced = {"create_archive", "info", "thumbnail_grid", "edit_metadata"}
    assert old_advanced < main, "the check above would not have flagged the old panel"


# ── idempotency: one press at a time per racing action ─────────────────────


def test_the_racing_actions_map_to_one_guard_key():
    """The three fade buttons are one action; a picker is not an action at all."""
    assert handlers._inline_op_key("fade_in") == "fade"
    assert handlers._inline_op_key("fade_both") == "fade"
    assert handlers._inline_op_key("trim_audio") == "trim"
    assert handlers._inline_op_key("trim_video") == "trim"
    assert handlers._inline_op_key("normalize_audio") == "normalize"
    assert handlers._inline_op_key("merge_audios_start") == "merge_audios"
    assert handlers._inline_op_key("bitrate_320") == "bitrate"
    # A picker only reveals a menu - claiming it would refuse the second press of
    # a button that does no work.
    assert handlers._inline_op_key("bitrate_menu") is None
    assert handlers._inline_op_key("menu_main") is None
    assert handlers._inline_op_key("create_archive") is None
    assert handlers._inline_op_key(None) is None


def test_a_claimed_action_refuses_a_second_press_until_released():
    handler = object.__new__(EnhancedMediaHandler)
    session: dict = {}
    assert handler._claim_inline_op(session, "trim") is True
    assert handler._claim_inline_op(session, "trim") is False
    # A different action is not blocked by it.
    assert handler._claim_inline_op(session, "fade") is True
    handler._release_inline_op(session, "trim")
    assert handler._claim_inline_op(session, "trim") is True


def test_a_claim_left_by_a_dead_run_lapses():
    """The window is what stops a crashed run locking a button for good."""
    handler = object.__new__(EnhancedMediaHandler)
    session = {"_inline_op_trim": 1.0}
    assert handler._claim_inline_op(session, "trim") is True
    # Junk in the marker is treated as "not claimed", never as an exception.
    assert handler._claim_inline_op({"_inline_op_fade": "soon"}, "fade") is True


def test_the_dispatcher_claims_and_releases_the_action():
    source = _source_of("handlers.py", "callback_handler")
    assert "_inline_op_key(data)" in source
    assert "_claim_inline_op(session, _inline_key)" in source
    assert "_release_inline_op(_inline_session, _inline_key)" in source
    # The release has to be a finally: every branch in between returns.
    assert "finally:" in source


def test_the_bulk_menu_names_the_subtitle_rows_apart():
    """The mode row and the send-the-files row are two halves, labelled as such."""
    markup = MediaMenuBuilder.get_bulk_menu({})
    labels = {b.text: b.callback_data for row in markup.inline_keyboard for b in row}
    assert "Batch Subtitles Mode — Off" in labels
    assert labels["Batch Subtitles Mode — Off"] == "bulk_toggle:bulk_subtitles"
    assert labels["📂 Send Subtitle Files"] == "bulk_subtitles"
