import os
import json
import uuid
from datetime import datetime
from typing import Optional

try:
    import config
except Exception:
    config = None


def _forwards_dir() -> str:
    base = None
    if config is not None:
        base = getattr(config, "STORAGE_PATH", None)
    if not base:
        base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage")
    path = os.path.join(base, "forwards")
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass
    return path


def save_forward_metadata(metadata: dict) -> str:
    """Persist metadata about a forwarded (undownloadable) message and return a short id."""
    fid = uuid.uuid4().hex
    data = dict(metadata)
    data.setdefault("created_at", datetime.utcnow().isoformat())
    p = os.path.join(_forwards_dir(), f"{fid}.json")
    try:
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
    except Exception:
        # best-effort: try to write somewhere else
        try:
            tmp = os.path.join(os.path.dirname(__file__), f"{fid}.json")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False)
            return fid
        except Exception:
            raise
    return fid


def load_forward_metadata(fid: str) -> Optional[dict]:
    p = os.path.join(_forwards_dir(), f"{fid}.json")
    if not os.path.exists(p):
        # fallback to local file in utils dir
        alt = os.path.join(os.path.dirname(__file__), f"{fid}.json")
        if os.path.exists(alt):
            p = alt
        else:
            return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def delete_forward_metadata(fid: str) -> bool:
    p = os.path.join(_forwards_dir(), f"{fid}.json")
    try:
        if os.path.exists(p):
            os.remove(p)
            return True
    except Exception:
        pass
    return False
