"""Every delivery names the file after the media, and keeps the media's tags.

The Bot API names an uploaded file after the part it is handed, and what these
buttons hand it is an output path: ``<file id>_optimized.mp4``, a temp name, a
storage key. The media's own name is the one the user recognises, so every
delivery has to say what that name is. The same goes for the metadata - the title
and performer the file carries are what make an audio arrive in Telegram's player
as the track it is, instead of as a file called ``track`` with no artist.

Both are checked *across every button at once*, because that is how they broke:
one delivery at a time, each a little different from the last. A new delivery that
forgets its name, or drops the tags, fails here instead of in a user's chat.
"""

import ast
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import parse_source, read_source  # noqa: E402

HANDLERS = ("handlers.py",)
WORKER = ("workers", "ffmpeg_worker.py")

#: The helpers whose whole job is to name and tag one delivery. What they do
#: internally is their business; what their callers pass them is not.
DELIVERY_HELPERS = {
    "_send_video_result",
    "_send_audio_result",
    "_send_document_result",
    "_send_photo_result",
}


def _calls(tree: ast.Module, names: set[str]):
    """``(lineno, name, keywords, expansion)`` for every call to one of *names*."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name not in names:
            continue
        keywords = {kw.arg for kw in node.keywords if kw.arg}
        expansion = next(
            (ast.unparse(kw.value) for kw in node.keywords if kw.arg is None),
            "",
        )
        found.append((node.lineno, name, keywords, expansion))
    return found


def _enclosing(tree: ast.Module, lineno: int) -> str:
    """Name of the innermost function containing *lineno*."""
    best = None
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.lineno <= lineno <= (node.end_lineno or 0) and (best is None or node.lineno > best.lineno):
            best = node
    return getattr(best, "name", "")


@pytest.mark.parametrize("parts", [HANDLERS, WORKER], ids=["handlers", "worker"])
def test_a_video_delivery_says_the_name_it_wants(parts):
    """Without a name the video is called after the local output path."""
    calls = _calls(parse_source(*parts), {"_send_video_result"})
    assert calls, f"{'/'.join(parts)}: expected video deliveries to check"
    missing = [lineno for lineno, _name, keywords, _expansion in calls if "delivery_name" not in keywords]
    assert missing == [], f"{'/'.join(parts)}: video sent without a delivery_name at {missing}"


@pytest.mark.parametrize("parts", [HANDLERS, WORKER], ids=["handlers", "worker"])
def test_a_document_delivery_says_the_name_it_wants(parts):
    """The Bot API names a document after the file object it is handed."""
    tree = parse_source(*parts)
    missing = []
    for lineno, _name, keywords, _expansion in _calls(tree, {"send_document"}):
        if _enclosing(tree, lineno) in DELIVERY_HELPERS:
            continue
        if "filename" not in keywords:
            missing.append(lineno)
    assert missing == [], f"{'/'.join(parts)}: document sent without a filename at {missing}"


def test_an_audio_delivery_says_the_name_it_wants_and_keeps_the_tags():
    """A raw ``send_audio`` has to name the track *and* carry its title/performer."""
    tree = parse_source(*HANDLERS)
    problems = []
    for lineno, _name, keywords, expansion in _calls(tree, {"send_audio"}):
        if _enclosing(tree, lineno) in DELIVERY_HELPERS:
            continue
        if "filename" not in keywords:
            problems.append(f"L{lineno}: no filename")
        if "_audio_tag_kwargs" not in expansion:
            problems.append(f"L{lineno}: title/performer not taken from the source")
    assert problems == [], "; ".join(problems)


def _enclosing_node(tree: ast.Module, lineno: int):
    """The innermost function containing *lineno*, as the node itself."""
    best = None
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.lineno <= lineno <= (node.end_lineno or 0) and (best is None or node.lineno > best.lineno):
            best = node
    return best


def test_every_audio_delivery_states_how_long_the_file_is():
    """Telegram will not derive a long clip's length: the send has to state it.

    A track longer than a short clip is delivered with duration 0 unless the
    caller passes one - the player shows ``00:00`` and the progress bar never
    moves - and the answer has to come from a probe of the *output* file, because
    a re-encode and a keyframe cut both cannot promise the source's length. So
    every raw ``send_audio`` probes the file it is about to send.
    """
    tree = parse_source(*HANDLERS)
    problems = []
    for lineno, _name, _keywords, _expansion in _calls(tree, {"send_audio"}):
        if _enclosing(tree, lineno) in DELIVERY_HELPERS:
            continue
        enclosing = _enclosing_node(tree, lineno)
        if "_audio_delivery_duration" not in (ast.unparse(enclosing) if enclosing else ""):
            problems.append(f"L{lineno}: sent without a probed duration")
    assert problems == [], "; ".join(problems)


def test_the_video_metadata_is_probed_not_invented():
    """The auto-driven tags for a video: duration, dimensions and a thumbnail."""
    src = read_source("handlers.py")
    assert "from utils.ffmpeg_runner import probe_video_for_delivery" in src
    # The probe's little thumbnail is attached to the send, so the video shows
    # frames rather than a black box before it is played.
    assert '"thumb"' in src
    assert "_vid_duration" in src and "_vid_width" in src and "_vid_height" in src


def test_every_audio_name_helper_keeps_the_media_name():
    """Whatever the output is called, the delivered name comes from the media."""
    tree = parse_source(*HANDLERS)
    helpers = {"_audio_delivery_name", "_video_delivery_name", "_document_delivery_name", "_split_part_name"}
    defined = {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert helpers <= defined, f"missing name helpers: {sorted(helpers - defined)}"
    # ...and each of them reads the media's own name rather than a path.
    src = read_source("handlers.py")
    assert "_safe_media_stem" in src
    assert src.count("def _safe_media_stem") == 1


def test_the_split_delivers_its_parts_in_order():
    """One at a time, so part 1 can be watched while part 2 uploads."""
    src = read_source("handlers.py")
    assert "for index, part in enumerate(parts, 1):" in src
    # Order is the promise, so the loop must not gather or reorder the parts.
    assert "asyncio.gather" not in src.split("async def _deliver_split_parts")[1].split("async def ", 1)[0]


@pytest.mark.parametrize("parts", [HANDLERS, WORKER], ids=["handlers", "worker"])
def test_no_delivery_names_a_file_after_a_storage_path(parts):
    """The tell-tale of a leaked name: a path or a file id handed to the API."""
    tree = parse_source(*parts)
    leaks = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in {"send_document", "send_audio", "send_video", "send_media_group"}:
            continue
        for keyword in node.keywords:
            if keyword.arg != "filename":
                continue
            value = ast.unparse(keyword.value)
            # A storage path basename, a file id, a temp name: none of these is
            # the media's name.
            if "os.path.basename(" in value and "delivery_name" not in value and "_name" not in value:
                leaks.append(f"L{node.lineno}: {value}")
    assert leaks == [], f"{'/'.join(parts)}: {leaks}"
