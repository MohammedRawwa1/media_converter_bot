"""A menu that writes HTML has to tell Telegram to render it.

The pickers under Bulk stated their headings with ``<b>``/``<i>`` but sent them
without ``parse_mode``, so the tags arrived literally: the menu read
``<b>Bulk compress quality</b>`` instead of showing a heading. Telegram renders
markup only when the message says which markup it is, so a call that carries a
tag has to carry ``parse_mode`` as well.

Nothing here encodes or converts - it pins one invariant across every menu in
``handlers.py``, so a new HTML menu cannot be added without saying how it is
meant to be read.
"""

import ast

from source_helpers import parse_source

HANDLERS = "handlers.py"

#: The tags this bot's menu text uses, and so the ones Telegram would render.
_TAGS = ("<b>", "<i>", "<code>", "<pre>", "<u>", "<s>", "<a href")

#: The calls that put a message on screen.
_MESSAGE_CALLS = ("safe_edit", "edit_message_text")


def _text_arguments(call: ast.Call) -> list[str]:
    """Every argument of *call* that could be the message body, as source."""
    values = [ast.unparse(node) for node in call.args[1:]]
    values += [ast.unparse(kw.value) for kw in call.keywords if kw.arg in ("text", "message")]
    return values


def test_every_menu_that_writes_html_asks_telegram_to_render_it():
    offenders = []
    for node in ast.walk(parse_source(HANDLERS)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr not in _MESSAGE_CALLS:
            continue
        if any(kw.arg == "parse_mode" for kw in node.keywords):
            continue
        for value in _text_arguments(node):
            if any(tag in value for tag in _TAGS):
                offenders.append(value[:90])
    assert offenders == [], "HTML sent without parse_mode:\n" + "\n".join(offenders)


def test_the_bulk_pickers_are_the_menus_this_pins():
    """The four Bulk pickers state a heading, so all four name their markup.

    Pinned by name as well as by the scan above, so the scan cannot pass by the
    menus being deleted rather than fixed.
    """
    body = ast.unparse(parse_source(HANDLERS))

    for heading in (
        "Bulk compress quality",
        "Bulk optimize preset",
        "Bulk Extract Audio bitrate",
        "Slideshow seconds per photo",
    ):
        assert heading in body, heading
        assert f"<b>{heading}</b>" in body, heading
