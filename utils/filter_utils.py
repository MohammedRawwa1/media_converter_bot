"""Helper utilities to create PTB `filters` compatibly across versions.

Provide a single function to build a media filter (video|audio|document)
defensively so the code works across PTB versions and import variants.
"""



def build_media_filter(filters_module) -> object | None:
    """Return a combined media filter (video|audio|document) or
    `filters_module.ALL` if specific filters aren't available.

    The function checks that candidate attributes on `filters_module`
    look like PTB Filter-like objects (support `data_filter` or `__or__`
    or provide a callable `.filter`). If none are usable, it falls
    back to `filters_module.ALL`.
    """

    def _get_filter(*names):
        for n in names:
            f = getattr(filters_module, n, None)
            if f is None:
                continue
            if hasattr(f, "data_filter") or hasattr(f, "__or__"):
                return f
            if callable(getattr(f, "filter", None)):
                return f
        return None

    f_video = _get_filter("VIDEO", "Video", "video")
    f_audio = _get_filter("AUDIO", "Audio", "audio")
    f_document = _get_filter("DOCUMENT", "Document", "document")

    media_filter = None
    for f in (f_video, f_audio, f_document):
        if f is None:
            continue
        try:
            media_filter = f if media_filter is None else (media_filter | f)
        except Exception:
            # If combination fails for a candidate, skip it
            continue

    if media_filter is None:
        # If no specific media filters are available, do not fall back to ALL.
        # Returning None allows the caller to apply a safer text-excluding filter.
        return None

    return media_filter
