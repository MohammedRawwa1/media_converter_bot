#!/usr/bin/env python3
"""Local indexer + search tool for quick-preview MP4 videos.

Features:
- Scan a directory for video files (.mp4, .mkv, .webm, .avi, .mov).
- Extract basic metadata using `ffprobe` (duration, width, height, codec).
- Generate a one-frame thumbnail (quick preview) using `ffmpeg`.
- Save an index JSON with tokenized filename fields for fast local search.
- Provide a small search CLI using a ranking heuristic.

Requirements:
- Python 3.8+
- `ffmpeg` / `ffprobe` available in PATH (optional: indexing still works without them).

Usage examples:
  python scripts/local_indexer.py scan --root storage/input --index index.json --thumbs storage/thumbnails --recursive
  python scripts/local_indexer.py search --index index.json --query "interstellar 1080p"
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# --- Tokenizer (adapted from tg-index project notes) ---------------------------------
_sep_re = re.compile(r"[\.\-_\[\]\(\)]+")
_space_re = re.compile(r"\s+")
_camel_re = re.compile(r'(?<!^)(?=[A-Z])')

_release_junk = set([
    'bluray', 'bdrip', 'brrip', 'dvd', 'dvdrip', 'web', 'webrip', 'web-dl', 'xvid', 'x264', 'x265',
    'h264', 'h265', 'hevc', 'hdrip', 'aac', 'ac3', 'mp3', 'remux', 'proper', 'yify', 'ettv', 'rarbg',
    'dvdr', 'limited', 'internal', 'subbed', 'dubbed', 'repack', 'hc', 'hdr', 'uhd', '720p', '1080p', '2160p',
])


def _split_camel(s: str) -> List[str]:
    return _camel_re.sub(' ', s).split()


def normalize_and_classify(filename: str) -> Dict[str, Any]:
    base = filename.rsplit('/', 1)[-1]
    s = base
    s = s.replace('\n', ' ')
    s = _sep_re.sub(' ', s)
    s = ' '.join(_split_camel(s))
    s = s.lower()
    s = _space_re.sub(' ', s).strip()

    parts = s.split(' ')

    title_tokens: List[str] = []
    quality_tokens: List[str] = []
    codec_tokens: List[str] = []
    year: Optional[int] = None
    other: List[str] = []

    for p in parts:
        if not p:
            continue
        if p.isdigit() and len(p) == 4 and 1900 <= int(p) <= 2100:
            year = int(p)
            continue
        if re.match(r'^\d{3,4}p$', p):
            quality_tokens.append(p)
            continue
        if p in ('x264', 'x265', 'h264', 'h265', 'hevc'):
            codec_tokens.append(p)
            continue
        if p in _release_junk:
            continue
        if re.match(r'^[a-z0-9]+$', p):
            title_tokens.append(p)
            continue
        other.append(p)

    def uniq(seq: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return {
        'title_tokens': uniq(title_tokens),
        'quality_tokens': uniq(quality_tokens),
        'codec_tokens': uniq(codec_tokens),
        'year': year,
        'other': uniq(other),
    }


def tokenize_query(query: str) -> List[str]:
    q = _sep_re.sub(' ', query)
    q = ' '.join(_split_camel(q))
    q = q.lower().strip()
    tokens = [t for t in _space_re.split(q) if t]
    return tokens


# --- FFprobe / FFmpeg helpers --------------------------------------------------------


def ffprobe_info(path: Path) -> Dict[str, Optional[Any]]:
    """Return dict with duration (seconds), width, height, codec_name or empty dict on failure."""
    try:
        cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-show_streams',
            '-of', 'json',
            str(path),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0 or not proc.stdout:
            return {}
        data = json.loads(proc.stdout)
        duration = None
        if data.get('format') and data['format'].get('duration'):
            try:
                duration = float(data['format']['duration'])
            except Exception:
                duration = None
        width = None
        height = None
        codec = None
        for s in data.get('streams', []):
            if s.get('codec_type') == 'video':
                width = s.get('width')
                height = s.get('height')
                codec = s.get('codec_name')
                break
        return {'duration': duration, 'width': width, 'height': height, 'codec': codec}
    except Exception:
        return {}


def create_thumbnail(path: Path, thumb_path: Path, time_offset: str = '00:00:01') -> bool:
    """Create a single-frame thumbnail with ffmpeg. Returns True on success."""
    try:
        cmd = [
            'ffmpeg',
            '-y',
            '-ss', time_offset,
            '-i', str(path),
            '-frames:v', '1',
            '-q:v', '2',
            str(thumb_path),
        ]
        proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return proc.returncode == 0 and thumb_path.exists()
    except Exception:
        return False


def make_id(path: Path) -> str:
    st = path.stat()
    key = f"{str(path.resolve())}:{int(st.st_mtime)}".encode('utf8')
    return hashlib.sha1(key).hexdigest()[:12]


# --- Indexing / Searching logic ------------------------------------------------------


def save_index(index_path: Path, docs: List[Dict[str, Any]]) -> None:
    payload = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'count': len(docs),
        'files': docs,
    }
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with open(index_path, 'w', encoding='utf8') as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def load_index(index_path: Path) -> Dict[str, Any]:
    if not index_path.exists():
        return {'generated_at': None, 'count': 0, 'files': []}
    with open(index_path, 'r', encoding='utf8') as fh:
        return json.load(fh)


def scan_directory(root: Path, index_path: Path, thumbs_dir: Path, recursive: bool = True) -> None:
    exts = {'.mp4', '.mkv', '.webm', '.avi', '.mov'}
    files: List[Dict[str, Any]] = []
    if recursive:
        iterator = root.rglob('*')
    else:
        iterator = root.glob('*')

    thumbs_dir.mkdir(parents=True, exist_ok=True)

    for p in iterator:
        if not p.is_file():
            continue
        if p.suffix.lower() not in exts:
            continue
        try:
            stat = p.stat()
        except Exception:
            continue
        fname = p.name
        token_struct = normalize_and_classify(fname)
        meta = ffprobe_info(p)

        doc_id = make_id(p)
        thumb_name = f"{doc_id}.jpg"
        thumb_path = thumbs_dir / thumb_name
        if not thumb_path.exists():
            create_thumbnail(p, thumb_path)

        doc = {
            'id': doc_id,
            'path': str(p.resolve()),
            'filename': fname,
            'extension': p.suffix.lower().lstrip('.'),
            'size': stat.st_size,
            'mtime': int(stat.st_mtime),
            'duration': meta.get('duration') if meta else None,
            'width': meta.get('width') if meta else None,
            'height': meta.get('height') if meta else None,
            'codec': meta.get('codec') if meta else None,
            'title_tokens': token_struct.get('title_tokens', []),
            'quality_tokens': token_struct.get('quality_tokens', []),
            'codec_tokens': token_struct.get('codec_tokens', []),
            'year': token_struct.get('year'),
            'thumbnail': str(thumb_path.resolve()) if thumb_path.exists() else None,
        }
        files.append(doc)

    save_index(index_path, files)
    print(f"Indexed {len(files)} files -> {index_path}")


def search_index(index_path: Path, query: str, page: int = 1, per_page: int = 10) -> Dict[str, Any]:
    idx = load_index(index_path)
    docs = idx.get('files', [])
    tokens = tokenize_query(query)
    if not tokens:
        return {'results': [], 'total': 0}

    results: List[Dict[str, Any]] = []
    qlower = query.lower()
    for doc in docs:
        score = 0.0
        doc_titles = [t.lower() for t in doc.get('title_tokens', [])]
        matched = sum(1 for t in tokens if t.lower() in doc_titles)
        score += matched * 10
        for qt in doc.get('quality_tokens', []):
            if qt and any(qt == t.lower() for t in tokens):
                score += 6
        for cd in doc.get('codec_tokens', []):
            if cd and any(cd == t.lower() for t in tokens):
                score += 5
        if doc.get('year') and any(str(doc.get('year')) == t for t in tokens):
            score += 8
        if qlower and qlower in doc.get('filename', '').lower():
            score += 3
        fname_len = len(doc.get('filename', ''))
        score -= fname_len / 200.0
        if score > 0:
            doc['_score'] = score
            results.append(doc)

    # fallback: prefix/filename match
    if not results:
        for doc in docs:
            score = 0.0
            doc_titles = [t.lower() for t in doc.get('title_tokens', [])]
            matched = sum(1 for t in tokens if any(tt.startswith(t.lower()) for tt in doc_titles))
            score += matched * 8
            if qlower and qlower in doc.get('filename', '').lower():
                score += 2
            fname_len = len(doc.get('filename', ''))
            score -= fname_len / 300.0
            if score > 0:
                doc['_score'] = score
                results.append(doc)

    results.sort(key=lambda r: (r.get('_score', 0), r.get('mtime', 0)), reverse=True)
    total = len(results)
    start = (page - 1) * per_page
    end = start + per_page
    return {'results': results[start:end], 'total': total, 'page': page, 'per_page': per_page}


def make_tg_message_link(chat_id: int, message_id: int) -> str:
    """Construct a Telegram message link for a chat_id/message_id.

    For supergroups/channels stored as '-100...' the link uses the `/c/{id}` form.
    This is the best-effort URL; public channels may require username-based links.
    """
    s = str(chat_id)
    if s.startswith('-100'):
        base = s[4:]
        return f'https://t.me/c/{base}/{message_id}'
    base = s.lstrip('-')
    return f'https://t.me/c/{base}/{message_id}'


def export_markdown(index_path: Path, query: Optional[str] = None, output: Optional[Path] = None, per_page: int = 100) -> str:
    """Export matching index entries as a Markdown list of links.

    - If `query` is provided we run a search and export results.
    - If no `query` is provided the entire index is exported.
    - Each link text prefers tokenized title, falling back to filename.
    - Link target prefers Telegram message URLs when `chat_id`/`message_id` exist,
      otherwise falls back to a `file://` URI for the local file path.
    """
    idx = load_index(index_path)
    if query:
        res = search_index(index_path, query, page=1, per_page=per_page)
        docs = res.get('results', [])
    else:
        docs = idx.get('files', [])
    lines: List[str] = []
    for doc in docs:
        title_tokens = doc.get('title_tokens') or []
        display = ' '.join(title_tokens) if title_tokens else doc.get('filename', '')
        display = (display.strip() or doc.get('filename', '')).replace('\n', ' ')
        # prefer Telegram message link when available
        url = ''
        if doc.get('chat_id') and doc.get('message_id'):
            try:
                url = make_tg_message_link(doc.get('chat_id'), doc.get('message_id'))
            except Exception:
                url = ''
        if not url:
            try:
                url = Path(doc.get('path', '')).as_uri()
            except Exception:
                url = doc.get('path', '') or ''
        lines.append(f'[{display}]({url})')
    block = '\n'.join(lines)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, 'w', encoding='utf8') as fh:
            fh.write(block)
        print(f'Wrote {len(lines)} links to {output}')
    else:
        print(block)
    return block


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description='Local video indexer and search')
    sub = p.add_subparsers(dest='cmd')

    scan_p = sub.add_parser('scan', help='Scan a directory and build an index')
    scan_p.add_argument('--root', required=True, help='Directory to scan')
    scan_p.add_argument('--index', required=True, help='Path to write index JSON')
    scan_p.add_argument('--thumbs', required=True, help='Directory to store thumbnails')
    scan_p.add_argument('--no-recursive', dest='recursive', action='store_false', default=True)

    search_p = sub.add_parser('search', help='Search an existing index')
    search_p.add_argument('--index', required=True, help='Index JSON path')
    search_p.add_argument('--query', required=True, help='Search query')
    search_p.add_argument('--page', type=int, default=1)
    search_p.add_argument('--per-page', type=int, default=10)

    export_p = sub.add_parser('export', help='Export matching entries as Markdown links')
    export_p.add_argument('--index', required=True, help='Index JSON path')
    export_p.add_argument('--query', required=False, help='Search query')
    export_p.add_argument('--output', required=False, help='Write markdown to file')
    export_p.add_argument('--all', action='store_true', help='Export entire index if set')
    export_p.add_argument('--per-page', type=int, default=100)

    args = p.parse_args(argv)
    if args.cmd == 'scan':
        root = Path(args.root)
        index_path = Path(args.index)
        thumbs = Path(args.thumbs)
        if not root.exists() or not root.is_dir():
            print('Root directory does not exist:', root)
            return 2
        scan_directory(root, index_path, thumbs, recursive=bool(args.recursive))
        return 0
    elif args.cmd == 'search':
        index_path = Path(args.index)
        if not index_path.exists():
            print('Index not found:', index_path)
            return 2
        res = search_index(index_path, args.query, page=args.page, per_page=args.per_page)
        print(f"Results: {res.get('total',0)}")
        for i, r in enumerate(res.get('results', []), start=1 + (args.page - 1) * args.per_page):
            fname = r.get('filename', '-')
            display = fname if len(fname) <= 120 else fname[:117] + '...'
            print(f"{i}) {display} [{r.get('_score',0):.1f}] — {r.get('path')}")
        return 0
    elif args.cmd == 'export':
        index_path = Path(args.index)
        if not index_path.exists():
            print('Index not found:', index_path)
            return 2
        out_path = Path(args.output) if getattr(args, 'output', None) else None
        # if --all provided or no query specified, export entire index
        if getattr(args, 'all', False) or not getattr(args, 'query', None):
            export_markdown(index_path, query=None, output=out_path, per_page=args.per_page)
        else:
            export_markdown(index_path, query=args.query, output=out_path, per_page=args.per_page)
        return 0
    else:
        p.print_help()
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
