"""Every callback branch: no bare ``update.message``, and no button without a handler.

The Media Forwarder's "internal error" was one line in a callback-reachable
method: ``update.message.reply_text(...)``. A callback press carries no
``update.message`` — the message it was made on lives on
``update.callback_query.message`` — so that read raised ``AttributeError`` *after*
the work had been done: the media was re-sent and the user was told it failed.

Two audits pin both halves of "does this button work?":

* the callback entry points' call graphs are walked, and no function on them may
  read ``update.message`` unless a guard in the same function has already
  established that there is one (``getattr(update, "message", None)``, an
  ``update and update.message`` test, or the ``_Reply`` shape that only takes the
  message path when there is no callback query);
* every ``callback_data`` the keyboards offer is matched against the triggers the
  handlers route — an alias map, an exact trigger or a prefix — so a button that
  would silently do nothing fails here instead of in the chat.
"""

import ast
from pathlib import Path

from source_helpers import parse_source

PROJECT_ROOT = Path(__file__).resolve().parent.parent

#: The functions Telegram calls for a press, and the files they live in.
CALLBACK_ENTRIES = {
    "handlers.py": ("callback_handler",),
    "main.py": ("confirm_callback", "session_status_callback"),
}

#: How small a call graph still counts as "the audit ran". handlers.py's dispatch
#: reaches the whole button surface; main.py's two entry points reach a handful of
#: helpers. A traverse that finds less than this has silently broken.
MIN_REACHABLE = {"handlers.py": 60, "main.py": 4}

#: Triggers a *keyboard* offers. Test-file/table-driven callbacks (``mp3q_key``
#: and friends) are covered by the prefix list the handlers route on.
KEYBOARD_FILES = ("utils/keyboard_utils.py", "handlers.py", "main.py")

#: Where an offered trigger is routed: the handler's dispatch, plus ``main.py``'s
#: two pattern handlers (``^cf[mn]`` confirmations, ``^st:`` session status).
ROUTING_FILES = ("handlers.py", "main.py")


def _tree(relative: str) -> ast.Module:
    return parse_source(relative)


def _parents(tree: ast.Module) -> dict[ast.AST, ast.AST]:
    parents: dict[ast.AST, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _functions(tree: ast.Module) -> dict[str, list[ast.AST]]:
    """Every (async) function in the file, by name - nested ones included."""
    out: dict[str, list[ast.AST]] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out.setdefault(node.name, []).append(node)
    return out


def _callees(func: ast.AST, takes_update: set[str]) -> set[str]:
    """Names called from *func*: ``self.X`` plus any function taking an update."""
    names: set[str] = set()
    for node in ast.walk(func):
        if not isinstance(node, ast.Call):
            continue
        called = node.func
        if isinstance(called, ast.Attribute) and isinstance(called.value, ast.Name) and called.value.id == "self":
            names.add(called.attr)
        elif isinstance(called, ast.Name) and called.id in takes_update:
            names.add(called.id)
    return names


def _reachable(tree: ast.Module, entries) -> dict[str, list[ast.AST]]:
    """The functions reachable from the callback entry points in *tree*.

    Takes the tree rather than a path so the caller's parent map and the nodes it
    walks are the same objects - two parses of one file would compare nowhere.
    """
    funcs = _functions(tree)
    takes_update = {
        name for name, nodes in funcs.items() if any(arg.arg == "update" for node in nodes for arg in node.args.args)
    }
    seen: set[str] = set()
    stack = [name for entry in entries if entry in funcs for name in _callees(funcs[entry][0], takes_update)]
    while stack:
        name = stack.pop()
        if name in seen or name not in funcs:
            continue
        seen.add(name)
        for node in funcs[name]:
            stack.extend(_callees(node, takes_update))
    return {name: funcs[name] for name in seen}


def _guards(read: ast.AST, parents: dict[ast.AST, ast.AST]) -> list[str]:
    """The tests enclosing *read*, innermost first."""
    tests, node = [], parents.get(read)
    while node is not None:
        if isinstance(node, ast.If):
            tests.append(ast.unparse(node.test))
        elif isinstance(node, ast.BoolOp):
            tests.append(ast.unparse(node))
        node = parents.get(node)
    return tests


def _is_guarded(read: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    """Whether a ``update.message`` read sits behind a check that one exists.

    Three shapes qualify: the ``getattr`` probe
    (``getattr(update, "message", None)``), a truthiness test
    (``update and update.message``), and ``self.query is None`` — the ``_Reply``
    helper's split between a command (a message to reply to) and a press (which it
    answers through the query instead).
    """
    for test in _guards(read, parents):
        stripped = test.strip()
        if "message" in stripped and "getattr" in stripped:
            return True
        if stripped in ("update.message", "update and update.message", "update.message is not None"):
            return True
        if "self.query is None" in stripped:
            return True
    return False


def _message_reads(func: ast.AST) -> list[ast.Attribute]:
    """Every ``update.message`` read in *func*."""
    return [
        node
        for node in ast.walk(func)
        if isinstance(node, ast.Attribute)
        and node.attr == "message"
        and isinstance(node.value, ast.Name)
        and node.value.id == "update"
    ]


# ── audit 1: no unguarded update.message on a callback path ────────────────


def test_no_callback_reachable_function_reads_update_message_unguarded():
    unguarded, checked = [], 0
    for relative, entries in CALLBACK_ENTRIES.items():
        tree = _tree(relative)
        parents = _parents(tree)
        reachable = _reachable(tree, entries)
        # The traversal has to have found a real call graph, or the audit would
        # pass by finding nothing at all.
        assert len(reachable) >= MIN_REACHABLE[relative], f"{relative}: only {len(reachable)} reachable function(s)"
        for name, nodes in reachable.items():
            for func in nodes:
                for read in _message_reads(func):
                    checked += 1
                    if not _is_guarded(read, parents):
                        unguarded.append(f"{relative}:{read.lineno} in {name}")
    assert checked > 0, "no update.message reads were inspected - the audit did not run"
    assert not unguarded, f"a press has no update.message; guard these: {sorted(unguarded)}"


def test_the_forwarder_press_path_is_walked_by_the_audit():
    # The bug this audit exists for: ``_redeliver_current_media`` must be on the
    # graph, and must not read ``update.message`` at all any more - its notes go
    # through the one helper that answers a press.
    reachable = _reachable(_tree("handlers.py"), CALLBACK_ENTRIES["handlers.py"])
    assert "_redeliver_current_media" in reachable
    assert "forward_batch" in reachable
    source = " ".join(ast.unparse(node) for node in reachable["_redeliver_current_media"])
    assert "update.message" not in source
    assert "_reply_to_press" in source


# ── audit 2: every offered trigger is routed ───────────────────────────────


def _literal_prefix(node: ast.AST, constants: dict[str, str]) -> str | None:
    """The (constant prefix of the) trigger a ``callback_data=`` expression makes."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _literal_prefix(node.left, constants), _literal_prefix(node.right, constants)
        if left is None:
            return None
        return left + (right or "")
    if isinstance(node, ast.JoinedStr):
        prefix = ""
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                prefix += value.value
            else:
                break
        return prefix or None
    return None


def _callbacks_constants() -> dict[str, str]:
    tree = _tree("utils/callbacks.py")
    return {
        target.id: node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
        for target in node.targets
        if isinstance(target, ast.Name)
    }


def _offered() -> dict[str, list[str]]:
    constants = _callbacks_constants()
    offered: dict[str, list[str]] = {}
    for relative in KEYBOARD_FILES:
        for node in ast.walk(_tree(relative)):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg != "callback_data":
                    continue
                value = _literal_prefix(keyword.value, constants)
                if value:
                    offered.setdefault(value, []).append(f"{relative}:{node.lineno}")
    return offered


def _routing() -> tuple[set[str], set[str], dict[str, str]]:
    """``(exact triggers, prefix triggers, aliases)`` from the handlers."""
    exact, prefixes, aliases = set(), set(), {}
    for relative in ROUTING_FILES:
        tree = _tree(relative)
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare) and isinstance(node.left, ast.Name) and node.left.id in ("data", "action"):
                for op, comparator in zip(node.ops, node.comparators, strict=True):
                    if isinstance(op, ast.Eq) and isinstance(comparator, ast.Constant):
                        exact.add(comparator.value)
                    if isinstance(op, ast.In) and isinstance(comparator, (ast.Tuple, ast.Set, ast.List)):
                        exact.update(element.value for element in comparator.elts if isinstance(element, ast.Constant))
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "startswith"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                prefixes.add(node.args[0].value)
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
                pairs = {
                    key.value: value.value
                    for key, value in zip(node.value.keys, node.value.values, strict=True)
                    if isinstance(key, ast.Constant)
                    and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                }
                # The alias table is the one big literal mapping.
                if len(pairs) > 5:
                    aliases.update(pairs)
    return exact, prefixes, aliases


def _is_routed(trigger: str, exact: set[str], prefixes: set[str], aliases: dict[str, str], _seen=None) -> bool:
    _seen = _seen or set()
    if trigger in exact or trigger in _seen:
        return True
    _seen.add(trigger)
    for candidate in (trigger, trigger.split(":", 1)[0], trigger.rsplit(":", 1)[0]):
        if candidate in exact:
            return True
        alias = aliases.get(candidate)
        if alias and _is_routed(alias, exact, prefixes, aliases, _seen):
            return True
        if any(candidate.startswith(prefix) for prefix in prefixes):
            return True
    return False


def test_every_offered_callback_trigger_is_routed():
    offered = _offered()
    exact, prefixes, aliases = _routing()
    assert len(offered) > 90, f"only {len(offered)} triggers found - the keyboards were not scanned"
    dead = sorted(trigger for trigger in offered if not _is_routed(trigger, exact, prefixes, aliases))
    assert not dead, f"a press on these would do nothing: {dead}"


def test_the_new_batch_forward_is_both_offered_and_routed():
    # Guards the audit itself: a trigger added to a keyboard with no handler has
    # to fail here, which is the shape it is built to catch.
    offered = _offered()
    assert "bulk_forward" in offered
    exact, prefixes, aliases = _routing()
    assert _is_routed("bulk_forward", exact, prefixes, aliases)
