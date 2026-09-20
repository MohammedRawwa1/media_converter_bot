# tasks/conversion_tasks.py
import asyncio
import contextlib
import logging
import os
import tempfile

import config

# Use configured FFMPEG_PATH, fallback to FFMPEG_PATH
FFMPEG_PATH = getattr(config, "FFMPEG_PATH", "ffmpeg") or "ffmpeg"

logger = logging.getLogger(__name__)

#: Containers of the mov/mp4 family - the ones whose index (``moov``) decides
#: whether a part's duration is readable before the whole file is fetched. These
#: are exactly the containers that accept ``movflags=+faststart``; every other
#: one rejects the option, which would fail the whole split.
_FASTSTART_EXTS = frozenset({".mp4", ".m4a", ".m4v", ".mov", ".3gp", ".3g2"})

# The metadata arguments every MP3 command in this project carries. Shared with
# the audio encodes in handlers.py, so a split part and a re-encoded file cannot
# disagree about what a delivered media keeps (see utils/ffmpeg_runner.py).
try:
    from utils.ffmpeg_runner import MP3_METADATA_ARGS
except ImportError:  # pragma: no cover - the module is always present in-tree
    MP3_METADATA_ARGS: tuple[str, ...] = ("-map_metadata", "0", "-id3v2_version", "3")

# Import timeout utilities
try:
    from utils.async_timeout_wrapper import (
        DEFAULT_FFMPEG_TIMEOUT,
        run_subprocess_with_timeout,
    )
except ImportError:
    # Fallback if module not available
    async def run_subprocess_with_timeout(cmd, timeout_seconds=18000, operation_name="Operation"):
        if create_checked_subprocess_exec is not None:
            process = await create_checked_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        else:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
            return stdout, stderr, process.returncode
        except TimeoutError:
            process.kill()
            raise

    DEFAULT_FFMPEG_TIMEOUT = 18000

# Ensure `create_checked_subprocess_exec` is available even when
# `utils.async_timeout_wrapper` imported successfully above. Doing this
# here guarantees the symbol exists for the rest of this module.
try:
    from utils.process_utils import create_checked_subprocess_exec
except Exception:
    create_checked_subprocess_exec = None


async def _spawn_process(*cmd, **kwargs):
    """Spawn subprocess preferring the safe helper when available.

    This centralizes the checked/normalized subprocess creation so the
    rest of this module can call `_spawn_process(...)` and avoid
    duplicating the same conditional logic.
    """
    if "create_checked_subprocess_exec" in globals() and create_checked_subprocess_exec is not None:
        return await create_checked_subprocess_exec(*cmd, **kwargs)
    return await asyncio.create_subprocess_exec(*cmd, **kwargs)


def _split_metadata_args(source_meta: dict | None, stem: str) -> list[str]:
    """The ``-metadata`` pairs that give a split part's header something to show.

    ``-map_metadata 0`` copies whatever the source carried, and for a tagged media
    that is the whole answer. It is *no* answer for a media that carries nothing:
    an ordinary ``Module 02.mp3`` with no ID3 at all produces parts whose only tag
    is the encoder's, so a player - and Telegram, which reads the header before it
    decides what to show - displays a file name and no title. That is ffmpeg's
    default, and it cannot be talked out of it: the tags have to be stated.

    So the part is told what it is: the media's own name when the source had no
    title of its own, and the artist it did carry when there was one. The source's
    own title is restated when it exists, which leaves a tagged media's parts
    exactly as ``-map_metadata 0`` already made them.

    Returns ``[]`` for a source whose tags could not be read at all, which leaves
    the command exactly as it was: a verdict nobody could read is not evidence of
    an empty one, and overwriting a real title with the file name would be worse
    than the blank this exists to fix.
    """
    if not isinstance(source_meta, dict):
        return []
    title = str(source_meta.get("title") or "").strip() or stem
    artist = str(source_meta.get("performer") or source_meta.get("artist") or "").strip()
    args = ["-metadata", f"title={title}"]
    if artist:
        args += ["-metadata", f"artist={artist}"]
    return args


#: The audio containers that are given per-part tags after the cut, which is why
#: a split of one of these costs one extra ffmpeg run per part - a stream copy,
#: never a re-encode (see ``_stamp_split_part``). Checked container by container:
#: mp3/m4a/flac/wav report the tags as format tags and ogg/opus as stream tags,
#: which is what a player reads either way; wma keeps them in its ASF header.
#: The set is the delivery path's audio list too - a container this bot hands over
#: as an audio file is one whose parts are numbered like audio (``.aac`` and
#: ``.wma`` are both delivered as audio, but only the one with a tag carrier is
#: here).
#:
#: Raw ADTS (``.aac``) is absent on purpose - it has no tag carrier at all, so a
#: pass there would be a process that changes nothing. Video is absent for the
#: mirror image of that reason: its parts would have to be re-muxed whole to hold
#: a title nothing displays, which is a second full copy of a multi-gigabyte part
#: for a field the caption already carries.
_SPLIT_TAG_AUDIO_EXTS = frozenset({".mp3", ".m4a", ".flac", ".ogg", ".opus", ".wav", ".wma"})

#: Containers whose tags are *stream*-level, where a global ``-metadata`` is
#: ignored for a key the stream already carries - and ``-map_metadata 0`` has just
#: copied the cut's own title onto that stream. Verified against ogg/opus: the
#: global form there silently kept the un-numbered title while ``-metadata:s:a:0``
#: replaced it. Every other container writes the tags globally, where the same
#: override does win.
_STREAM_TAG_SPLIT_EXTS = frozenset({".ogg", ".opus"})


def _split_tag_value(source_meta: dict, *candidates: str) -> str:
    """The first of *candidates* this probe verdict states, or ``""``.

    The probe and the tag editor do not agree on every key name (``performer``
    from ffprobe, ``artist`` from an edit, ``band`` for an album artist), so the
    aliases are read in one place instead of each call site guessing.
    """
    for candidate in candidates:
        value = str(source_meta.get(candidate) or "").strip()
        if value:
            return value
    return ""


def _part_tag_args(source_meta: dict | None, stem: str, index: int, total: int, flag: str = "-metadata") -> list[str]:
    """The ``-metadata`` pairs that make one part its own track.

    The segment muxer writes one set of *global* tags to every part - ffmpeg has
    no per-segment metadata option - so a part cannot be told which one it is by
    the command that cut it. This is what the second write states: the media's
    title (or its own name, when it carries none) with the part number on it, the
    number itself, and the album the parts share - so a player lists
    ``Module 02 (part 1/2)`` and ``Module 02 (part 2/2)`` as tracks 1 and 2 of
    *Module 02*, instead of showing the same media twice with no relation at all
    between its parts.

    The album is the source's own when it has one - a real album is not worth
    overwriting with a file name - and the media's name when it does not, which is
    the case this exists for: an untagged media has no album to group its parts
    under, so they arrive as unrelated files.

    The album artist is what the source says it is, and the artist it names when
    it names no album artist of its own: that is the pair players actually group an
    album by, so a part that carried one artist beside a blank album artist was
    grouped by its track artists instead - and any library that files such a media
    under "Various Artists" did so for every part of it. A source that names
    neither gets none, exactly as it gets no artist: the media's *name* is not an
    artist, and inventing one would file the parts under a performer nobody knows.

    Returns ``[]`` for a source whose tags could not be read at all, exactly like
    :func:`_split_metadata_args`: the part then keeps whatever ffmpeg made of it.
    """
    if not isinstance(source_meta, dict):
        return []
    base = str(source_meta.get("title") or "").strip() or stem
    album = str(source_meta.get("album") or "").strip() or stem
    label = f"part {index}/{total}"
    args = [
        flag,
        f"title={base} ({label})",
        flag,
        f"track={index}/{total}",
        flag,
        f"album={album}",
    ]
    artist = _split_tag_value(source_meta, "performer", "artist", "artists", "author")
    album_artist = _split_tag_value(source_meta, "album_artist", "albumartist", "band")
    if album_artist or artist:
        # The artist is the album artist only when the source named no other, which
        # is what a file with one artist and no album-artist field means.
        args += [flag, f"album_artist={album_artist or artist}"]
    if artist:
        args += [flag, f"artist={artist}"]
    return args


async def _stamp_split_part(
    part: str,
    *,
    source_meta: dict | None,
    stem: str,
    index: int,
    total: int,
    ext: str,
) -> bool:
    """Write one part's own title, track number, album and album artist into its header.

    A stream copy: the header is rewritten and the frames are copied through, so
    nothing is decoded and the length the part states for itself is untouched.

    The rewrite stages through a sibling file that keeps the part's own
    extension - ffmpeg picks the muxer from it, and a ``.tmp`` name has no format
    ffmpeg will write at all - and is swapped in only once it has succeeded. That
    is what keeps a failed pass from costing the bytes the split already
    produced: the caller keeps the part exactly as ffmpeg wrote it.
    """
    # Which option writes the tag depends on where the container keeps it: a
    # stream-tagged one has to be told on the stream, or the title copied from the
    # cut simply wins (see _STREAM_TAG_SPLIT_EXTS).
    flag = "-metadata:s:a:0" if ext in _STREAM_TAG_SPLIT_EXTS else "-metadata"
    tags = _part_tag_args(source_meta, stem, index, total, flag)
    if not tags:
        return False

    root, extension = os.path.splitext(part)
    staged = f"{root}.tagging{extension}"
    cmd = [FFMPEG_PATH, "-y", "-i", part, "-c", "copy"]
    # The same recipe the audio encodes use, so a part and a re-encoded file
    # cannot disagree about the version of ID3 they are written with.
    cmd += list(MP3_METADATA_ARGS) if ext == ".mp3" else ["-map_metadata", "0"]
    cmd += tags
    cmd.append(staged)
    try:
        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.debug(
                "split: could not tag %s: %s",
                os.path.basename(part),
                stderr.decode("utf-8", errors="ignore")[-200:],
            )
            return False
        os.replace(staged, part)
        return True
    except Exception:
        logger.debug("split: tagging %s failed", os.path.basename(part))
        return False
    finally:
        with contextlib.suppress(OSError):
            if os.path.exists(staged):
                os.remove(staged)


async def _stamp_split_parts(parts: list[str], *, source_meta: dict | None, stem: str, ext: str) -> None:
    """Give every part of a split its own title, track number, album and album artist, one at a time.

    Best-effort by design: the parts are already the media the user asked for, so
    one that cannot be tagged is delivered as ffmpeg wrote it rather than failing
    the split that produced it.
    """
    if not isinstance(source_meta, dict):
        return
    total = len(parts)
    for index, part in enumerate(parts, 1):
        await _stamp_split_part(part, source_meta=source_meta, stem=stem, index=index, total=total, ext=ext)


async def _probe_split_source_meta(input_path: str) -> dict | None:
    """The source's own title/performer, or ``None`` when it cannot be read.

    One local ffprobe, and only for a media whose verdict the caller does not
    already have: the ingest probes every file it stores, so a split of one of
    those passes that verdict in and never reaches this.

    ``None`` covers both ways of having no answer: a probe that raised and a probe
    that came back with nothing at all. An empty verdict is a probe that could not
    read the file - a missing ffprobe, a file it refuses - not evidence that the
    media carries no tags, and stating a file name as a title on that evidence
    would overwrite the real one ffmpeg was about to copy.
    """
    try:
        from utils.ffmpeg_runner import probe_media

        meta = await probe_media(input_path)
    except Exception:
        logger.debug("split: could not probe the source's own tags")
        return None
    return dict(meta) if isinstance(meta, dict) and meta else None


def _validate_input_file(input_path: str) -> tuple[bool, str]:
    """Validate input file exists and is readable."""
    if not input_path:
        return False, "Input path cannot be empty"

    if not os.path.exists(input_path):
        return False, f"Input file not found: {input_path}"

    if not os.path.isfile(input_path):
        return False, f"Input path is not a file: {input_path}"

    if not os.access(input_path, os.R_OK):
        return False, f"Input file is not readable: {input_path}"

    return True, ""


def _validate_output_path(output_path: str) -> tuple[bool, str]:
    """Validate output path is writable."""
    if not output_path:
        return False, "Output path cannot be empty"

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            return False, f"Cannot create output directory: {str(e)}"

    return True, ""


async def convert_video_to_mp3(
    input_path: str, output_path: str, bitrate: str = "192k", timeout_seconds: int = 18000
) -> tuple[bool, str]:
    """Convert video file to MP3 audio asynchronously."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-vn",
            "-acodec",
            "libmp3lame",
            "-ab",
            bitrate,
            "-ar",
            "48000",
            "-ac",
            "2",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        try:
            # Timeout protection - FFmpeg can hang if ffprobe crashes
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except TimeoutError:
            logger.warning(f"FFmpeg timeout after {timeout_seconds}s, killing process")
            process.kill()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                logger.error("Failed to kill FFmpeg process")
            return False, f"Conversion timeout (> {timeout_seconds}s)"

        if process.returncode == 0:
            logger.info(f"Successfully converted {input_path} to MP3")
            return True, "Conversion successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            logger.error(f"Conversion failed: {error}")
            return False, error

    except asyncio.CancelledError:
        logger.error("Conversion was cancelled")
        with contextlib.suppress(Exception):
            process.kill()
        return False, "Conversion was cancelled"
    except Exception as e:
        logger.error(f"Exception in convert_video_to_mp3: {e}")
        return False, str(e)


async def compress_video(
    input_path: str, output_path: str, preset: str = "medium", crf: int = 23, timeout_seconds: int = 18000
) -> tuple[bool, str]:
    """Compress video asynchronously using preset."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    # Validate CRF range
    if not (0 <= crf <= 51):
        logger.error(f"Invalid CRF value: {crf}. Must be 0-51")
        return False, "CRF must be between 0-51"

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-c:v",
            "libx264",
            "-preset",
            preset,
            "-crf",
            str(crf),
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        try:
            # Timeout protection
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except TimeoutError:
            logger.warning(f"Compression timeout after {timeout_seconds}s")
            process.kill()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                logger.error("Failed to kill compression process")
            return False, f"Compression timeout (> {timeout_seconds}s)"

        if process.returncode == 0:
            logger.info(f"Successfully compressed {input_path}")
            return True, "Compression successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except asyncio.CancelledError:
        logger.error("Compression was cancelled")
        with contextlib.suppress(Exception):
            process.kill()
        return False, "Compression was cancelled"
    except Exception as e:
        logger.error(f"Exception in compress_video: {e}")
        return False, str(e)


async def extract_audio(
    input_path: str, output_path: str, format: str = "mp3", bitrate: str = "192k", timeout_seconds: int = 18000
) -> tuple[bool, str]:
    """Extract audio from video asynchronously with timeout protection."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    try:
        codec = "libmp3lame" if format == "mp3" else "copy"

        cmd = ["ffmpeg", "-y", "-i", input_path, "-vn", "-acodec", codec, "-ab", bitrate, output_path]

        try:
            stdout, stderr, returncode = await run_subprocess_with_timeout(
                cmd, timeout_seconds=timeout_seconds, operation_name="Audio Extraction"
            )
        except Exception as e:
            logger.error(f"Audio extraction subprocess error: {e}")
            return False, str(e)

        if returncode == 0:
            logger.info(f"Successfully extracted audio from {input_path}")
            return True, "Extraction successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            logger.error(f"Audio extraction failed: {error}")
            return False, error

    except asyncio.CancelledError:
        logger.error("Audio extraction was cancelled")
        return False, "Audio extraction was cancelled"
    except Exception as e:
        logger.error(f"Exception in extract_audio: {e}")
        return False, str(e)


async def merge_videos(video_paths: list[str], output_path: str, timeout_seconds: int = 18000) -> tuple[bool, str]:
    """Merge multiple videos asynchronously with timeout protection."""
    # Validate inputs
    if not video_paths:
        return False, "No video paths provided"

    for video_path in video_paths:
        valid, error = _validate_input_file(video_path)
        if not valid:
            return False, f"Invalid video file: {error}"

    valid, error = _validate_output_path(output_path)
    if not valid:
        return False, error

    concat_file = None
    try:
        # Create concat file
        concat_content = "\n".join([f"file '{os.path.abspath(path)}'" for path in video_paths])

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write(concat_content)
            concat_file = f.name

        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", concat_file, "-c", "copy", output_path]

        try:
            stdout, stderr, returncode = await run_subprocess_with_timeout(
                cmd, timeout_seconds=timeout_seconds, operation_name="Video Merge"
            )
        except Exception as e:
            logger.error(f"Video merge subprocess error: {e}")
            return False, str(e)

        if returncode == 0:
            logger.info(f"Successfully merged {len(video_paths)} videos")
            return True, "Merge successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            logger.error(f"Video merge failed: {error}")
            return False, error

    except asyncio.CancelledError:
        logger.error("Video merge was cancelled")
        return False, "Video merge was cancelled"
    except Exception as e:
        logger.error(f"Exception in merge_videos: {e}")
        return False, str(e)
    finally:
        if concat_file and os.path.exists(concat_file):
            with contextlib.suppress(Exception):
                os.unlink(concat_file)


async def merge_audios(audio_paths: list[str], output_path: str, timeout_seconds: int = 18000) -> tuple[bool, str]:
    """Merge multiple audio files asynchronously with timeout protection."""
    # Validate inputs
    if not audio_paths:
        return False, "No audio paths provided"

    for audio_path in audio_paths:
        valid, error = _validate_input_file(audio_path)
        if not valid:
            return False, f"Invalid audio file: {error}"

    valid, error = _validate_output_path(output_path)
    if not valid:
        return False, error

    concat_file = None
    try:
        cmd = ["ffmpeg", "-y"]
        for path in audio_paths:
            cmd.extend(["-i", path])

        # Build filter
        filter_complex = ""
        for i in range(len(audio_paths)):
            filter_complex += f"[{i}:a]"
        filter_complex += f"concat=n={len(audio_paths)}:v=0:a=1[out]"

        cmd.extend(["-filter_complex", filter_complex, "-map", "[out]", "-c:a", "libmp3lame", "-q:a", "2", output_path])

        try:
            stdout, stderr, returncode = await run_subprocess_with_timeout(
                cmd, timeout_seconds=timeout_seconds, operation_name="Audio Merge"
            )
        except Exception as e:
            logger.error(f"Audio merge subprocess error: {e}")
            return False, str(e)

        if returncode == 0:
            logger.info(f"Successfully merged {len(audio_paths)} audio files")
            return True, "Merge successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            logger.error(f"Audio merge failed: {error}")
            return False, error

    except asyncio.CancelledError:
        logger.error("Audio merge was cancelled")
        return False, "Audio merge was cancelled"
    except Exception as e:
        logger.error(f"Exception in merge_audios: {e}")
        return False, str(e)
    finally:
        if concat_file and os.path.exists(concat_file):
            with contextlib.suppress(Exception):
                os.unlink(concat_file)


#: The slideshow's own encode defaults, used when the batch that asked for it
#: named no quality of its own. This is the recipe ``create_slideshow`` has
#: always used.
_SLIDESHOW_CRF = 23
_SLIDESHOW_PRESET = "medium"
_SLIDESHOW_CRF_MIN = 18
_SLIDESHOW_CRF_MAX = 51
_SLIDESHOW_PRESETS = frozenset(
    {"ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"}
)


def _slideshow_crf(value) -> int:
    """A quality for the slideshow encode, or its own default.

    ``None`` and anything unusable fall back, so the plan's quality can only
    ever replace the default with a value libx264 accepts.
    """
    try:
        crf = int(value)
    except (TypeError, ValueError):
        return _SLIDESHOW_CRF
    return crf if _SLIDESHOW_CRF_MIN <= crf <= _SLIDESHOW_CRF_MAX else _SLIDESHOW_CRF


def _slideshow_preset(value) -> str:
    """An x264 preset for the slideshow encode, or its own default."""
    text = str(value or "").strip().lower()
    return text if text in _SLIDESHOW_PRESETS else _SLIDESHOW_PRESET


async def create_slideshow(
    image_paths: list[str],
    output_path: str,
    seconds_per_image: float = 3.0,
    music_path: str | None = None,
    timeout_seconds: int = 18000,
    crf: int | None = None,
    preset: str | None = None,
) -> tuple[bool, str]:
    """Build a slideshow video from images, one shown for ``seconds_per_image``.

    Every image is looped for the fixed duration and scaled onto a common
    1280x720 canvas (letterboxed, never stretched) so images of different sizes
    can be concatenated in a single pass. The result is a widely-playable H.264
    MP4. When ``music_path`` names a readable audio file it is looped underneath
    the slideshow and cut at the video's end.

    ``crf``/``preset`` carry the *plan's* quality when the batch that asked for
    the slideshow also asked to Compress or Optimize. Without them the slideshow
    encoded at its own defaults while the apply reported the user's quality as
    applied. ``None`` keeps those defaults, which is what a plain slideshow uses.
    """
    if not image_paths:
        return False, "No image paths provided"

    for image_path in image_paths:
        valid, error = _validate_input_file(image_path)
        if not valid:
            return False, f"Invalid image file: {error}"

    valid, error = _validate_output_path(output_path)
    if not valid:
        return False, error

    try:
        duration = float(seconds_per_image)
    except (TypeError, ValueError):
        duration = 3.0
    # A zero or negative duration would produce an empty video.
    duration = duration if duration > 0 else 3.0

    width, height = 1280, 720
    cmd = [FFMPEG_PATH, "-y"]
    for image_path in image_paths:
        cmd.extend(["-loop", "1", "-t", f"{duration}", "-i", image_path])

    # Optional background music. It is looped so a short track still covers the
    # whole slideshow, and -shortest ends the output with the video rather than
    # letting the audio keep running.
    use_music = bool(music_path) and os.path.isfile(str(music_path))
    if use_music:
        cmd.extend(["-stream_loop", "-1", "-i", music_path])

    chains = [
        (
            f"[{i}:v]scale={width}:{height}:force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,fps=30[v{i}]"
        )
        for i in range(len(image_paths))
    ]
    concat_inputs = "".join(f"[v{i}]" for i in range(len(image_paths)))
    chains.append(f"{concat_inputs}concat=n={len(image_paths)}:v=1:a=0[slideshow]")

    cmd.extend(
        [
            "-filter_complex",
            ";".join(chains),
            "-map",
            "[slideshow]",
            "-c:v",
            "libx264",
            "-preset",
            _slideshow_preset(preset),
            "-crf",
            str(_slideshow_crf(crf)),
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
        ]
    )
    if use_music:
        # The music is the input right after the images, so its audio stream index
        # is len(image_paths).
        cmd.extend(["-map", f"{len(image_paths)}:a", "-c:a", "aac", "-b:a", "192k", "-shortest"])
    cmd.append(output_path)

    try:
        _stdout, stderr, returncode = await run_subprocess_with_timeout(
            cmd, timeout_seconds=timeout_seconds, operation_name="Slideshow"
        )
    except asyncio.CancelledError:
        logger.error("Slideshow was cancelled")
        return False, "Slideshow was cancelled"
    except Exception as e:
        logger.error(f"Slideshow subprocess error: {e}")
        return False, str(e)

    if returncode == 0:
        logger.info(f"Successfully built a slideshow from {len(image_paths)} image(s)")
        return True, "Slideshow created"

    error = stderr.decode("utf-8", errors="ignore")[:200]
    logger.error(f"Slideshow failed: {error}")
    return False, error


async def take_screenshot(input_path: str, output_path: str, time: str = "00:00:01") -> tuple[bool, str]:
    """Take screenshot from video asynchronously."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    try:
        cmd = ["ffmpeg", "-y", "-ss", time, "-i", input_path, "-vframes", "1", "-q:v", "2", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully took screenshot at {time}")
            return True, "Screenshot taken"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in take_screenshot: {e}")
        return False, str(e)


async def change_resolution(input_path: str, output_path: str, width: int, height: int) -> tuple[bool, str]:
    """Change video resolution asynchronously."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    # Validate dimensions
    if width <= 0 or height <= 0:
        error = f"Invalid dimensions: {width}x{height}. Both must be > 0"
        logger.error(error)
        return False, error

    if width % 2 != 0 or height % 2 != 0:
        logger.warning(f"Resolution {width}x{height} not even. FFmpeg may adjust.")

    try:
        cmd = ["ffmpeg", "-y", "-i", input_path, "-filter:v", f"scale={width}:{height}", "-c:a", "copy", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully changed resolution to {width}x{height}")
            return True, "Resolution changed"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in change_resolution: {e}")
        return False, str(e)


async def trim_media(input_path: str, output_path: str, start_time: str, end_time: str) -> tuple[bool, str]:
    """Trim video or audio asynchronously.

    The cut is a stream copy, so what the source carried has to be carried across
    rather than regenerated, and two flags are what do it:

    * ``-map_metadata 0`` states the copy outright. ffmpeg's default copies the
      global tags but not all of them - a real mp4 cut came back without its
      ``creation_time``, which is the date a player shows for the file.
    * ``-map 0`` keeps *every* stream. The default selection picks one video and
      one audio track, so a range cut of a subtitled video quietly arrived with
      no subtitle track at all. It is added only when the cut lands in the
      source's own container: a different one (the caller can ask for one) may be
      unable to hold the streams the source had, and a stream that cannot be
      written fails the whole run where leaving it out at least produces the cut.
    """
    try:
        duration_parts = end_time.split(":")
        duration_seconds = int(duration_parts[0]) * 3600 + int(duration_parts[1]) * 60 + float(duration_parts[2])
        start_parts = start_time.split(":")
        start_seconds = int(start_parts[0]) * 3600 + int(start_parts[1]) * 60 + float(start_parts[2])
        duration = duration_seconds - start_seconds

        same_container = os.path.splitext(str(input_path))[1].lower() == os.path.splitext(str(output_path))[1].lower()
        cmd = [FFMPEG_PATH, "-y", "-ss", start_time, "-i", input_path, "-t", str(duration)]
        if same_container:
            cmd += ["-map", "0"]
        cmd += ["-map_metadata", "0", "-c", "copy", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully trimmed media from {start_time} to {end_time}")
            return True, "Trim successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in trim_media: {e}")
        return False, str(e)


async def split_media_segments(
    input_path: str,
    output_dir: str,
    segment_seconds: float,
    *,
    ext: str = ".mp4",
    stem: str = "part",
    source_meta: dict | None = None,
) -> tuple[bool, list[str], str]:
    """Cut one media file into parts of *segment_seconds*, one file per part.

    This is the stream-copy split, and it is the same command for a video and an
    audio file - only the extension of the parts differs::

        ffmpeg -i in.mp4 -c copy -map 0 -segment_time 3600 -f segment \\
               -segment_start_number 1 -reset_timestamps 1 out/Concert.%03d.mp4

    ``-c copy`` means no re-encode: the parts are the original bytes, cut at
    segment boundaries, so splitting an hour-long file costs seconds, not hours.
    ``-map 0`` keeps every stream of the source (video, audio, subtitle tracks),
    ``-reset_timestamps 1`` restarts each part at 00:00 so a player shows the
    part's own duration instead of an hour-long timeline, and
    ``-segment_start_number 1`` numbers the parts from **001** rather than the
    segment muxer's own default of 000 - the numbering a user expects next to the
    "part 1/N" caption that goes out with each file.

    Every part carries its own duration *up front*, because a player that streams a
    part (Telegram's, above all) reads the header before the bytes: the mov/mp4
    family writes its index (the ``moov`` atom) at the **end** of the file by
    default, so the player shows ``00:00 / 00:00`` until the whole part has been
    fetched, and only the part it happened to download fully shows a length. Passing
    ``movflags=+faststart`` per segment is what moves that index to the front, so the
    duration is visible in the preview immediately. It has to travel as
    ``-segment_format_options``: the flag applied to the ``segment`` muxer itself
    (``-movflags +faststart``) never reaches the per-part muxer and changes nothing.
    Any other container refuses the flag outright (``-segment_format_options
    movflags=+faststart`` on an MKV fails the whole run), so it is only added for the
    containers it belongs to - the ones whose header is the ``moov`` atom.

    Cuts can only land on the source's keyframes, so a part can come out longer
    than *segment_seconds* (and a source whose keyframes are farther apart than
    that produces a single part). Re-encoding would fix the exact length and cost
    the whole point of this command, so the caller explains the result instead.

    The tags are stated on the command line rather than left to ffmpeg, because a
    media that carries none produces parts that carry none (see
    :func:`_split_metadata_args`). *source_meta* is the verdict the caller already
    holds - the ingest's own probe - and a caller without one costs a single local
    ffprobe here.

    Returns ``(ok, parts, error)``: the sorted list of files that were written, so
    a caller can deliver them in order, and the ffmpeg stderr tail when it failed.

    *stem* is the media's own name, so a split of ``Concert.mp4`` yields
    ``Concert.001.mp4``, ``Concert.002.mp4``, ... - the numbering Telegram users
    expect from a self-extracting archive, and the original name the user
    recognises instead of a storage path. The caller owns *output_dir* and is
    expected to give a fresh one per run, which is what keeps parts apart.
    """
    ok, message = _validate_input_file(input_path)
    if not ok:
        return False, [], message

    try:
        segment = float(segment_seconds)
    except (TypeError, ValueError):
        return False, [], "invalid segment length"
    if segment <= 0:
        return False, [], "invalid segment length"

    ext = ext if str(ext).startswith(".") else f".{ext}"
    # Traversal hardening: the stem ends up in a path, so only its file name part
    # is kept.
    safe_stem = os.path.basename(str(stem or "part")) or "part"
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        return False, [], f"cannot create the output directory: {e}"

    # A float-typed second is what ``-segment_time`` takes; ``3600`` and
    # ``01:00:00`` are the same to ffmpeg, and the number cannot be misread.
    segment_arg = f"{segment:.3f}".rstrip("0").rstrip(".")
    # ``Stem.%03d.ext``: ffmpeg numbers the parts itself, so the files on disk are
    # already named the way they are delivered - nothing has to be renamed later,
    # and no delivery can fall back to naming a part after its storage path.
    _pattern = os.path.join(output_dir, f"{safe_stem}.%03d{ext}")
    cmd = [
        FFMPEG_PATH,
        "-y",
        "-i",
        input_path,
        "-c",
        "copy",
        "-map",
        "0",
        "-segment_time",
        segment_arg,
        "-f",
        "segment",
        # 001, not the segment muxer's default 000: the parts are delivered under
        # their own names, next to a "part 1/N" caption.
        "-segment_start_number",
        "1",
        "-reset_timestamps",
        "1",
        # Presentation timestamps for every part, so a cut that lands on a
        # keyframe still yields a file with a coherent timeline.
        "-fflags",
        "+genpts",
    ]

    # How each part's header has to be written, by container. The mov/mp4 family
    # (and only it) takes ``movflags=+faststart``; ``-segment_format_options``
    # forwards the option to the muxer of each individual part, which is the only
    # form that works - ``-movflags`` on the segment muxer is silently ignored.
    ext_lower = ext.lower()
    if ext_lower in _FASTSTART_EXTS:
        cmd.extend(["-segment_format_options", "movflags=+faststart"])
    elif ext_lower == ".mp3":
        # The Xing/Info header ffmpeg writes for a CBR mp3 already carries the
        # part's frame count - which is the duration a streaming player reads -
        # so only the tags themselves are pinned here: the media's metadata is
        # copied onto the command line instead of left to ffmpeg's default, and
        # it is written as ID3v2.3, the version Windows Explorer and older
        # players read. Both come from MP3_METADATA_ARGS, which the audio
        # encodes use too - a split part and a re-encoded file must carry the
        # same tags, and neither may lose them.
        cmd.extend(MP3_METADATA_ARGS)
    if ext_lower not in _FASTSTART_EXTS:
        # Every part starts at zero regardless of what the cut's timestamps were.
        cmd.extend(["-avoid_negative_ts", "make_zero"])
    if ext_lower != ".mp3":
        # Stated for every other container. ffmpeg copies the *global* tags by
        # default, but that is not the whole verdict: a real mp4 part came back
        # without its ``creation_time`` - the date every player's properties panel
        # shows - until this was passed, and the mov family is where a file's
        # other format-level fields live too. The mp3 branch above carries the
        # same flag in MP3_METADATA_ARGS, so it is not repeated there.
        cmd.extend(["-map_metadata", "0"])

    # What every part states about itself, whichever container it is cut into.
    # The caller's verdict is the ingest's own probe, so a media that came through
    # the pipe costs nothing to describe; only a file nothing has probed yet is
    # read here.
    _meta = source_meta if isinstance(source_meta, dict) and source_meta else None
    if _meta is None:
        # ``or None``: an empty verdict is a probe that could not read the file, and
        # stating the stem as the title on that evidence would overwrite the real
        # one ffmpeg was about to copy off the source.
        _meta = await _probe_split_source_meta(input_path) or None
    cmd.extend(_split_metadata_args(_meta, safe_stem))

    # Add the output pattern as the last argument
    cmd.append(_pattern)

    try:
        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _stdout, stderr = await process.communicate()
    except Exception as e:
        logger.error(f"Exception in split_media_segments: {e}")
        return False, [], str(e)

    if process.returncode != 0:
        error = stderr.decode("utf-8", errors="ignore")[-300:]
        logger.error(f"split_media_segments failed: {error}")
        return False, [], error

    prefix = f"{safe_stem}."
    parts = sorted(
        os.path.join(output_dir, name)
        for name in os.listdir(output_dir)
        if name.startswith(prefix) and name.endswith(ext) and os.path.getsize(os.path.join(output_dir, name)) > 0
    )
    if not parts:
        return False, [], "ffmpeg produced no parts"

    logger.info(f"Split media into {len(parts)} part(s) of ~{segment_arg}s")

    # The cut gives every part the same global tags; the parts a player lists are
    # the audio ones, so each of those is then given the number it is (one stream
    # copy per part). A video part is left alone - see _SPLIT_TAG_AUDIO_EXTS.
    if ext_lower in _SPLIT_TAG_AUDIO_EXTS:
        await _stamp_split_parts(parts, source_meta=_meta, stem=safe_stem, ext=ext_lower)

    return True, parts, ""


async def repair_video(input_path: str, output_path: str) -> tuple[bool, str]:
    """Repair corrupted video asynchronously."""
    try:
        cmd = ["ffmpeg", "-y", "-i", input_path, "-c", "copy", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully repaired video")
            return True, "Repair successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in repair_video: {e}")
        return False, str(e)


async def optimize_video(input_path: str, output_path: str, preset: str = "slow", crf: int = 23) -> tuple[bool, str]:
    """Optimize video for web asynchronously."""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-c:v",
            "libx264",
            "-preset",
            preset,
            "-crf",
            str(crf),
            "-movflags",
            "+faststart",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully optimized video")
            return True, "Optimization successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in optimize_video: {e}")
        return False, str(e)


async def create_thumbnail_grid(input_path: str, output_path: str, rows: int = 3, cols: int = 3) -> tuple[bool, str]:
    """Create thumbnail grid asynchronously."""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-vf",
            f"select=not(mod(n\\,{(rows * cols) + 1})),scale=160:-1,tile={cols}x{rows}",
            "-frames:v",
            "1",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully created thumbnail grid")
            return True, "Thumbnail grid created"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in create_thumbnail_grid: {e}")
        return False, str(e)


async def generate_sample(input_path: str, output_path: str, duration: int = 30) -> tuple[bool, str]:
    """Generate sample/preview asynchronously."""
    try:
        # For mp4 outputs, re-encode to H.264/AAC and add movflags for streaming
        if output_path.lower().endswith(".mp4"):
            cmd = [
                "ffmpeg",
                "-y",
                "-i",
                input_path,
                "-t",
                str(duration),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "28",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-movflags",
                "+faststart",
                output_path,
            ]
        else:
            cmd = ["ffmpeg", "-y", "-i", input_path, "-t", str(duration), "-c", "copy", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully generated {duration}s sample")
            return True, "Sample generated"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in generate_sample: {e}")
        return False, str(e)


async def extract_streams(input_path: str, output_dir: str) -> tuple[bool, dict[str, str]]:
    """Extract all streams asynchronously."""
    try:
        # Validate input path before probing
        valid, error = _validate_input_file(input_path)
        if not valid:
            logger.error(error)
            return False, {}

        # First get probe info
        cmd_probe = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", input_path]

        process = await _spawn_process(*cmd_probe, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        import json

        probe_data = json.loads(stdout.decode())
        streams = probe_data.get("streams", [])

        extracted = {}

        for i, stream in enumerate(streams):
            codec_type = stream.get("codec_type", "unknown")

            if codec_type == "video":
                output = os.path.join(output_dir, f"stream_video_{i}.mp4")
                cmd = ["ffmpeg", "-y", "-i", input_path, "-map", f"0:v:{i}", "-c:v", "copy", "-an", output]
            elif codec_type == "audio":
                output = os.path.join(output_dir, f"stream_audio_{i}.aac")
                cmd = ["ffmpeg", "-y", "-i", input_path, "-map", f"0:a:{i}", "-c:a", "copy", "-vn", output]
            elif codec_type == "subtitle":
                output = os.path.join(output_dir, f"stream_subtitle_{i}.srt")
                cmd = ["ffmpeg", "-y", "-i", input_path, "-map", f"0:s:{i}", "-c:s", "srt", output]
            else:
                continue

            proc = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

            _, _ = await proc.communicate()

            if proc.returncode == 0:
                extracted[f"{codec_type}_{i}"] = output

        return True, extracted

    except Exception as e:
        logger.error(f"Exception in extract_streams: {e}")
        return False, {}


#: The audio targets this bot converts to, and the codec each one is written with.
#: ONE table: the in-process converter below reads it, and so does the worker job
#: the handler queues for a source too large to convert here - those used to be two
#: tables, and the in-process one had no ``m4a`` entry at all.
AUDIO_FORMAT_CODECS: dict[str, str] = {
    "mp3": "libmp3lame",
    "wav": "pcm_s16le",
    "aac": "aac",
    "m4a": "aac",
    "flac": "flac",
    "ogg": "libvorbis",
    "opus": "libopus",
}

#: The targets whose encoder takes a bitrate. PCM has no bitrate to set, and flac
#: refuses one outright ("Codec AVOption b ... has not been used for any stream").
_AUDIO_BITRATE_TARGETS = frozenset({"mp3", "aac", "m4a", "ogg", "opus"})

#: The name ffprobe reports for each target's own audio stream.
#:
#: This is what lets a request whose source already *is* the target recognise
#: itself before anything is fetched - the codec the table above names is the one
#: ffmpeg *writes* with, which is not the name a probe reads back. It is not derived
#: from :data:`AUDIO_FORMAT_CODECS` because the mapping is not one to one:
#: ``libmp3lame`` writes what ffprobe calls ``mp3`` and ``libvorbis`` writes
#: ``vorbis``, and both ``aac`` and ``m4a`` report ``aac``, because m4a is an AAC
#: stream inside an MP4 container.
AUDIO_FORMAT_PROBE_CODECS: dict[str, str] = {
    "mp3": "mp3",
    "wav": "pcm_s16le",
    "aac": "aac",
    "m4a": "aac",
    "flac": "flac",
    "ogg": "vorbis",
    "opus": "opus",
}


def audio_format_takes_bitrate(target_format: str) -> bool:
    """Whether the encoder for *target_format* accepts a bitrate.

    PCM has no bitrate to set and flac refuses one outright, so a request to one
    of those has no bitrate of its own to read back from /usersettings - which is
    the question the audio-format button asks before it compares a source against
    the target it would produce.
    """
    return str(target_format or "").strip().lower() in _AUDIO_BITRATE_TARGETS


def audio_format_ffmpeg_args(target_format: str, bitrate: str = "128k") -> list[str] | None:
    """The ffmpeg arguments one audio target is encoded with, or ``None`` for a target nothing knows.

    ``m4a`` is the entry that has to be stated: it is AAC inside an MP4 container,
    and ffmpeg writes nothing else into that container - the converter used to have
    no entry for it and fell back to ``libmp3lame``, so the M4A button failed
    outright ("Nothing was written into output file, because at least one of its
    streams received no packets") for every file small enough to be converted in
    process, while the very same media handed to the worker converted fine.

    A target this table does not know is refused rather than written as an MP3
    under another extension, which is what the old fallback did - the caller gets
    ``None`` and says so.

    The tag recipe travels with the codec, because the tags are part of the result:
    ``-map_metadata 0`` states the copy instead of leaving it to an invisible
    default, and an MP3 is written as ID3v2.3 (see :data:`MP3_METADATA_ARGS`) - the
    version the properties panel a user actually opens reads.
    """
    target = str(target_format or "").lower()
    codec = AUDIO_FORMAT_CODECS.get(target)
    if codec is None:
        return None
    args = ["-c:a", codec]
    if target in _AUDIO_BITRATE_TARGETS:
        args += ["-b:a", str(bitrate)]
    args += list(MP3_METADATA_ARGS) if target == "mp3" else ["-map_metadata", "0"]
    return args


async def convert_audio_format(
    input_path: str, output_path: str, target_format: str = "mp3", bitrate: str = "192k"
) -> tuple[bool, str]:
    """Convert audio to *target_format*, keeping the media's own tags.

    The codec comes from :func:`audio_format_ffmpeg_args`, so this and the worker
    job encode a target the same way - a small file and a large one are the same
    button - and a target the table does not know is refused instead of being
    written as an MP3 under the wrong extension.
    """
    try:
        args = audio_format_ffmpeg_args(target_format, bitrate)
        if args is None:
            logger.warning("convert_audio_format: unsupported target %r", target_format)
            return False, f"unsupported audio format: {target_format}"

        cmd = ["ffmpeg", "-y", "-i", input_path, *args, output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully converted to {target_format}")
            return True, "Conversion successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in convert_audio_format: {e}")
        return False, str(e)


async def adjust_bitrate(
    input_path: str, output_path: str, video_bitrate: str = "5000k", audio_bitrate: str = "128k"
) -> tuple[bool, str]:
    """Adjust video and audio bitrate asynchronously."""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-b:v",
            video_bitrate,
            "-b:a",
            audio_bitrate,
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully adjusted bitrate")
            return True, "Bitrate adjusted"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in adjust_bitrate: {e}")
        return False, str(e)


async def normalize_audio(input_path: str, output_path: str) -> tuple[bool, str]:
    """Normalize audio volume asynchronously."""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-filter:a",
            "loudnorm=I=-20:TP=-1.5:LRA=11",
            "-c:v",
            "copy",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully normalized audio")
            return True, "Audio normalized"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in normalize_audio: {e}")
        return False, str(e)


async def apply_fade(
    input_path: str,
    output_path: str,
    fade_in_duration: float = 0.0,
    fade_out_duration: float = 0.0,
) -> tuple[bool, str]:
    """Apply a fade-in and/or fade-out to the audio track of a media file.

    Counterpart of ``ExtendedMediaConverter.apply_fade`` for the worker, which
    has no converter instance. A fade-out starts relative to the end of the
    media, so the duration has to be probed from the resolved source first -
    that is why this runs as its own job type rather than as a static
    ``ffmpeg_args`` list built by the caller (which has no source to probe on a
    cache repeat).
    """
    try:
        fade_in_duration = float(fade_in_duration or 0.0)
        fade_out_duration = float(fade_out_duration or 0.0)
    except (TypeError, ValueError):
        return False, "invalid fade duration"

    if fade_in_duration <= 0 and fade_out_duration <= 0:
        logger.warning("apply_fade: both durations are zero, nothing to do")
        return False, "no fade duration"

    afilters = []
    if fade_in_duration > 0:
        afilters.append(f"afade=t=in:st=0:d={fade_in_duration}")

    if fade_out_duration > 0:
        duration = 0.0
        try:
            from utils.ffmpeg_runner import probe_duration

            duration = float(await probe_duration(input_path) or 0.0)
        except Exception:
            logger.debug("apply_fade: duration probe failed for %s", input_path)
        if duration <= 0:
            logger.error("apply_fade: cannot determine media duration for fade-out")
            return False, "cannot determine media duration"
        fade_out_start = max(0.0, duration - fade_out_duration)
        afilters.append(f"afade=t=out:st={fade_out_start}:d={fade_out_duration}")

    cmd = [
        FFMPEG_PATH,
        "-y",
        "-i",
        input_path,
        "-af",
        ",".join(afilters),
        "-c:v",
        "copy",
        output_path,
    ]

    try:
        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _stdout, stderr = await process.communicate()
    except Exception as e:
        logger.error(f"Exception in apply_fade: {e}")
        return False, str(e)

    if process.returncode == 0:
        logger.info("Successfully applied fade (%s)", ",".join(afilters))
        return True, "Fade applied"

    return False, stderr.decode("utf-8", errors="ignore")[:200]


async def extract_subtitles(input_path: str, output_path: str) -> tuple[bool, str]:
    """Extract subtitles from video asynchronously."""
    try:
        cmd = ["ffmpeg", "-y", "-i", input_path, "-map", "0:s:0", "-c:s", "srt", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully extracted subtitles")
            return True, "Subtitles extracted"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in extract_subtitles: {e}")
        return False, str(e)


async def edit_metadata(input_path: str, output_path: str, metadata: dict[str, str]) -> tuple[bool, str]:
    """Edit media metadata asynchronously."""
    try:
        cmd = ["ffmpeg", "-y", "-i", input_path, "-c", "copy", "-map_metadata", "-1"]

        for key, value in metadata.items():
            cmd.extend(["-metadata", f"{key}={value}"])

        cmd.append(output_path)

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully edited metadata")
            return True, "Metadata updated"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in edit_metadata: {e}")
        return False, str(e)


def _unique_arcname(name: str, used: set[str]) -> str:
    """A name that no earlier member already took, unique inside the archive.

    Two media often share a basename (``video.mp4`` from two folders). A zip can
    hold both names, but extractors write one over the other, so the archive
    silently loses a file. Append ``_1``, ``_2`` ... before the extension instead.
    """
    base = os.path.basename(str(name or "")) or "file"
    stem, ext = os.path.splitext(base)
    candidate = base
    counter = 1
    while candidate.lower() in used:
        candidate = f"{stem}_{counter}{ext}"
        counter += 1
    used.add(candidate.lower())
    return candidate


def _archive_compression_level() -> int:
    """The deflate level for created archives (``ARCHIVE_COMPRESSION_LEVEL``).

    Defaults to 6 - the level zlib itself defaults to, and the point past which a
    level costs encode time for almost no size. Media is already compressed, so a
    high level buys nothing and a level of 0 (store) makes the archive as large as
    its contents; both are still selectable for a caller that wants them.
    """
    try:
        level = int(os.environ.get("ARCHIVE_COMPRESSION_LEVEL", "6") or 6)
    except (TypeError, ValueError):
        return 6
    return max(0, min(9, level))


async def create_archive(
    sources: list,
    output_path: str,
    *,
    fetch=None,
    on_progress=None,
) -> tuple[bool, str]:
    """Write the given media into a ZIP, one file at a time.

    ``sources`` may mix local paths and dicts. A dict that carries no usable
    ``path`` is resolved by ``fetch`` - an async callable returning a local path -
    which is how a file that lives in object storage joins the archive without the
    whole set being downloaded first. A fetched copy is removed immediately after
    it is written, so peak disk is one member and peak memory is one read buffer,
    the same shape the batch runs files in.

    The archive is written with ZIP64 enabled: enough large media push it past the
    classic 4 GiB / 65535-entry limits, and a run that produced an unreadable
    archive is worse than a slow one. Names are made unique because extractors
    overwrite on a collision, which would silently drop a file.

    ``on_progress(done, total)`` - awaited when given - lets the worker publish a
    per-file percentage instead of a bar that sits at 5% until the whole pack is
    done. Returns ``(ok, message)``; a run that wrote nothing is a failure.
    """
    try:
        import zipfile

        entries = [src for src in (sources or []) if src]
        if not entries:
            return False, "no files to archive"

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        used: set[str] = set()
        failed: list[str] = []
        written = 0
        level = _archive_compression_level()

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True, compresslevel=level) as zipf:
            for index, src in enumerate(entries, start=1):
                entry = src if isinstance(src, dict) else {"path": src}
                name = str(
                    entry.get("name")
                    or os.path.basename(str(entry.get("path") or entry.get("input_key") or ""))
                    or f"file_{index}"
                )
                path = entry.get("path")
                fetched = None
                if not (path and os.path.exists(path)) and fetch is not None:
                    try:
                        path = await fetch(entry)
                        fetched = path
                    except Exception:
                        logger.exception("create_archive: could not fetch source for %s", name)
                        path = None
                if not path or not os.path.exists(path):
                    failed.append(name)
                    continue
                try:
                    zipf.write(path, _unique_arcname(name, used))
                    written += 1
                except Exception:
                    logger.exception("create_archive: could not add %s", name)
                    failed.append(name)
                finally:
                    # Only the copy this loop fetched is removed; a path the caller
                    # passed in is the caller's to own.
                    if fetched:
                        with contextlib.suppress(OSError):
                            os.remove(fetched)
                if on_progress is not None:
                    with contextlib.suppress(Exception):
                        await on_progress(index, len(entries))

        if not written:
            reason = "no files could be archived"
            if failed:
                reason += f" ({len(failed)} unavailable)"
            logger.error("create_archive: %s", reason)
            return False, reason

        logger.info("Successfully created archive with %d/%d files", written, len(entries))
        if failed:
            return True, f"Archive created ({len(failed)} of {len(entries)} skipped)"
        return True, "Archive created"

    except Exception as e:
        logger.error(f"Exception in create_archive: {e}")
        return False, str(e)


# Read size for the volume splitter. Large enough to move a multi-gigabyte
# archive without a syscall per kilobyte, small enough that the loop holds a few
# megabytes rather than the whole part.
_ARCHIVE_SPLIT_CHUNK = 8 * 1024 * 1024


def _volume_name(archive_filename: str, index: int) -> str:
    """The name of one volume: ``myclips.zip`` -> ``myclips.zip.001``.

    The numbered suffix is the scheme 7-Zip, WinRAR and the mobile extractors
    share, and it is what tells a recipient the pieces belong together and which
    one to open. The base name (including ``.zip``) is preserved exactly, because
    a set whose volumes disagree on the name cannot be joined.
    """
    base = os.path.basename(str(archive_filename or "")) or "archive.zip"
    return f"{base}.{index:03d}"


def _split_archive_sync(archive_path: str, archive_filename: str, max_bytes: int, remove_source: bool) -> list[str]:
    """Split one finished archive into volumes of at most *max_bytes* bytes."""
    try:
        total = os.path.getsize(archive_path)
    except OSError:
        return [archive_path]

    cap = int(max_bytes or 0)
    # Nothing to do when the archive already fits one send, or when splitting is
    # switched off: the caller gets the single file back and delivers it as-is.
    if cap <= 0 or total <= cap:
        return [archive_path]

    out_dir = os.path.dirname(archive_path) or "."
    volumes: list[str] = []
    index = 1
    try:
        with open(archive_path, "rb") as src:
            while True:
                target = os.path.join(out_dir, _volume_name(archive_filename, index))
                remaining = cap
                written = 0
                with open(target, "wb") as dst:
                    while remaining > 0:
                        chunk = src.read(min(_ARCHIVE_SPLIT_CHUNK, remaining))
                        if not chunk:
                            break
                        dst.write(chunk)
                        written += len(chunk)
                        remaining -= len(chunk)
                if written == 0:
                    with contextlib.suppress(OSError):
                        os.remove(target)
                    break
                volumes.append(target)
                index += 1
    except Exception:
        # A half-written set is worse than none: drop what was created so the
        # caller is left with the whole archive it can still deliver or retry.
        for volume in volumes:
            with contextlib.suppress(OSError):
                os.remove(volume)
        raise

    if remove_source and volumes:
        with contextlib.suppress(OSError):
            os.remove(archive_path)
    logger.info("Split %s (%d bytes) into %d volume(s) of up to %d bytes", archive_filename, total, len(volumes), cap)
    return volumes


async def split_archive_volumes(
    archive_path: str,
    *,
    archive_filename: str,
    max_bytes: int,
    remove_source: bool = True,
) -> list[str]:
    """Split a finished archive into ``.001``, ``.002`` ... volumes.

    Only a delivery concern: a multi-volume set is one archive cut into parts
    small enough to each fit a single Telegram send, and it extracts only when
    *every* part is present under the same name and the recipient opens the
    first one. So the split happens only when it has to - an archive at or below
    *max_bytes* comes straight back as a one-element list, and a ``max_bytes`` of
    0 switches splitting off entirely.

    The split keeps peak disk at the archive plus one volume: the source file is
    read once, sequentially, and (by default) removed once every part is on disk.
    The read is offloaded so a multi-gigabyte archive never blocks the loop.
    """
    return await asyncio.to_thread(_split_archive_sync, archive_path, archive_filename, max_bytes, remove_source)
