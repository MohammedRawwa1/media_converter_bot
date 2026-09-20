"""The media's own tags have to survive every path that touches it.

``_source_metadata`` is the ffprobe verdict captured when a media is ingested -
title, performer, and everything else the caption and the audio tags are built
from. Three things used to break it, and each cost a delivered file its real
title:

* the media-cache reuse path blanked it, so the *second* operation on a media
  (the second style applied, and every batch file after the first) fell back to
  the filename;
* a batch built one generic caption for the whole run and never consulted the
  per-file tags at all;
* the large-file pipeline never populated it, because the probe that fills it
  runs in the handler and that path uploads and enqueues instead.
"""

import os
import sys

from source_helpers import read_source

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from handlers import _metadata_caption  # noqa: E402
from utils.bigfile_pipeline import IngestResult  # noqa: E402

HANDLERS = ("handlers.py",)


# ── the caption builder itself ──────────────────────────────────────────


def test_a_tagged_media_captions_itself_with_title_and_performer():
    assert _metadata_caption({"_source_metadata": {"title": "My Song", "performer": "Some Artist"}}) == (
        "My Song — Some Artist"
    )
    # The alternating key names the probe and the tag editor both produce.
    assert _metadata_caption({"_source_metadata": {"title": "T", "artist": "A"}}) == "T — A"
    assert _metadata_caption({"_source_metadata": {"performer": "Just An Artist"}}) == "Just An Artist"


def test_an_untagged_media_falls_back_to_the_given_caption():
    """The batch wording stays the fallback for a file that carries no tags."""
    assert _metadata_caption({"_source_metadata": {}}, fallback="Bulk conversion finished") == (
        "Bulk conversion finished"
    )
    assert _metadata_caption(None, fallback="Bulk conversion finished") == "Bulk conversion finished"


def test_the_probe_writes_both_tag_shapes():
    """Whatever the probe records, the caption builder has to read."""
    src = read_source("utils", "ffmpeg_runner.py")
    assert '"performer"' in src
    assert 'result["title"]' in src


# ── the reuse path must not blank the tags ──────────────────────────────


def test_the_media_cache_reuse_path_keeps_source_metadata():
    src = read_source(*HANDLERS)
    # The wipe is what made every repeat arrive with a filename caption.
    assert 'current_file["_source_metadata"] = {}' not in src
    # Every reuse path goes through the one shared adoption helper, which is
    # where the descriptor's probe verdict is carried onto the file.
    assert "async def _adopt_stored_source(" in src
    assert "_merge_cached_source_meta(current_file, _entry)" in src


def test_the_reuse_path_still_short_circuits_on_a_stored_key():
    """Keeping the tags must not have turned the reuse into a download."""
    src = read_source(*HANDLERS)
    assert 'current_file["input_key"] = _stored_key' in src
    # The gate validates the object (existence + stored size) before it
    # short-circuits, and still metadata-only, so the reuse costs no egress.
    assert "_stored_ok = await _stored_object_is_intact(" in src
    assert 'expected_size=current_file.get("size")' in src


# ── the batch path must consult the per-file tags ───────────────────────


def test_a_batch_file_keeps_its_metadriven_caption():
    src = read_source(*HANDLERS)
    assert "_bulk_tag_caption = (" in src
    assert "_metadata_caption(f)" in src
    assert 'f["_pipeline_caption"] = _bulk_tag_caption or _bulk_fallback_caption' in src


def test_the_batch_wording_is_still_the_fallback():
    """An untagged file must not change how a batch reads."""
    src = read_source(*HANDLERS)
    assert "_bulk_fallback_caption" in src
    assert "Bulk conversion finished for" in src


# ── the pipeline has to hand the probe back ─────────────────────────────


def test_an_ingest_result_can_carry_the_source_probe():
    assert IngestResult(ok=True).source_metadata is None
    carried = IngestResult(ok=True, source_metadata={"title": "T", "duration": 12.5})
    assert carried.source_metadata["title"] == "T"


def test_the_pipeline_probes_and_reports_the_source_metadata():
    tree_src = read_source("utils", "bigfile_pipeline.py")
    # Both the disk path and the streaming path hand the raw probe back.
    assert "_source_meta_raw = dict(_source_meta)" in tree_src
    assert '_source_meta_raw = dict(_stream.get("meta") or {})' in tree_src
    assert "source_metadata=_source_meta_raw or None" in tree_src


def test_the_handler_keeps_the_ingest_probe_on_the_session():
    """Without this a large file is delivered with a filename caption."""
    src = read_source(*HANDLERS)
    assert "if _ingest.source_metadata:" in src
    assert 'current_file["_source_metadata"] = dict(_ingest.source_metadata)' in src


def test_a_large_file_still_gets_a_metadriven_caption():
    """The caption is built from the metadata the ingest just supplied."""
    src = read_source(*HANDLERS)
    assert 'current_file["_pipeline_caption"] = _metadata_caption(current_file)' in src
