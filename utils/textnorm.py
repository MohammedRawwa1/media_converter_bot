"""Unicode-aware text normalization for media names and search queries.

A Latin-only pattern such as ``[^a-z0-9]`` deletes every Arabic, Persian,
Cyrillic, Greek, Hebrew or CJK word, so a filename written in one of those
scripts indexed with zero tokens and could never match a query - and a query
in the same script was reduced to an empty token list.

This module is the single place that decides what a "word" is:

* every Unicode letter or digit is a word character, i.e. all scripts at once;
* case is folded with ``casefold`` (not just ``lower``) so mixed-case Latin
  titles match however the user typed them;
* Arabic-script orthographic variants are unified, because the same word is
  routinely written several ways: diacritics (tashkeel) are dropped, alef
  variants (alef/hamza forms) unify to bare alef, alef maqsura to yaa, taa
  marbuta to haa, tatweel is removed, and the Persian/Urdu look-alikes
  yaa/kaaf fold onto their Arabic forms;
* bidi controls, zero-width marks and other format/control characters are
  dropped instead of being treated as letters.

Pure text helpers only - no I/O, safe to import anywhere.
"""

import re
import unicodedata

# A run of letters/digits of any script. ``_`` is a ``\w`` character but is
# release/junk punctuation for our purposes, so it is excluded explicitly.
WORD_RE = re.compile(r"[^\W_]+", re.UNICODE)

# Anything that is not a letter or digit -> word separator.
_NON_WORD_SEP_RE = re.compile(r"[\W_]+", re.UNICODE)
_SPACE_RE = re.compile(r"\s+")

# Arabic diacritics (harakat/tashkeel) and Quranic annotation marks.
_AR_DIACRITICS_RE = re.compile("[\u0610-\u061a\u064b-\u065f\u0670\u06d6-\u06dc\u06df-\u06e8\u06ea-\u06ed]")
_TATWEEL_RE = re.compile("\u0640")

# Orthographic variants that read as the same word to a searcher.
_AR_FOLD = str.maketrans(
    {
        "\u0623": "\u0627",  # alef with hamza above -> alef
        "\u0625": "\u0627",  # alef with hamza below -> alef
        "\u0622": "\u0627",  # alef with madda -> alef
        "\u0671": "\u0627",  # alef wasla -> alef
        "\u0649": "\u064a",  # alef maqsura -> yaa
        "\u0626": "\u064a",  # yaa with hamza -> yaa
        "\u0624": "\u0648",  # waw with hamza -> waw
        "\u0629": "\u0647",  # taa marbuta -> haa
        "\u06a9": "\u0643",  # Persian/Urdu kaaf -> Arabic kaaf
        "\u06cc": "\u064a",  # Persian/Urdu yaa -> Arabic yaa
        "\u06d2": "\u064a",  # Urdu bari yaa -> yaa
        "\u06be": "\u0647",  # Urdu do-chashmi heh -> haa
        "\u06c1": "\u0647",  # Urdu heh goal -> haa
        "\u06ba": "\u0646",  # Urdu noon ghunna -> noon
        "\u0679": "\u062a",  # Urdu tteh -> taa
        "\u0688": "\u062f",  # Urdu ddal -> daal
        "\u0691": "\u0631",  # Urdu rreh -> reh
    }
)


def strip_arabic_diacritics(text: str) -> str:
    """Remove tashkeel/Quranic marks so vocalized and plain text match."""
    return _AR_DIACRITICS_RE.sub("", text or "")


def fold_digits(text: str) -> str:
    """Map decimal digits of any script to ASCII 0-9 (e.g. ٢٠٢٣ -> 2023)."""
    if not text:
        return ""
    if not any(ch.isdigit() and not ch.isascii() for ch in text):
        return text
    out: list[str] = []
    for ch in text:
        if ch.isdigit() and not ch.isascii():
            try:
                out.append(str(unicodedata.digit(ch)))
                continue
            except (TypeError, ValueError):
                pass
        out.append(ch)
    return "".join(out)


def clean_text(text: str | None) -> str:
    """NFKC-normalize, casefold and fold Arabic variants; keep punctuation."""
    if not text:
        return ""
    s = unicodedata.normalize("NFKC", str(text))
    # Drop format/control characters (bidi overrides, zero-width marks, ...).
    s = "".join(ch for ch in s if not unicodedata.category(ch).startswith("C"))
    s = fold_digits(s)
    s = s.casefold()
    s = _TATWEEL_RE.sub("", s)
    s = strip_arabic_diacritics(s)
    return s.translate(_AR_FOLD)


def normalize_text(text: str | None) -> str:
    """Case-folded, word-only normalized form with single spaces.

    Words of every script are preserved; punctuation and separators become
    single spaces. This is the canonical form used for exact/phrase matching,
    dedupe keys and trigram generation.
    """
    s = clean_text(text)
    s = _NON_WORD_SEP_RE.sub(" ", s)
    return _SPACE_RE.sub(" ", s).strip()


def tokenize(text: str | None) -> list[str]:
    """Split any-script text into normalized word tokens."""
    norm = normalize_text(text)
    return norm.split() if norm else []
