"""Helpers for the tests that assert on this project's source.

A few behaviours are only visible in the source: a keyword argument that has to
reach a Telegram call, a helper a script has to reuse. Those assertions used to
match raw text, so every reformat was a chance to break them - a wrapped argument
or a normalised quote was enough. Everything here compares a *flattened* form, and
:func:`call_keywords` walks the AST where the shape of a call is the point.
"""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# The layout a formatter is free to add inside brackets: a space after an opener,
# a line break before a closer, a magic trailing comma before one.
_BRACKET_LAYOUT = (
    (re.compile(r"\(\s+"), "("),
    (re.compile(r"\[\s+"), "["),
    (re.compile(r"\{\s+"), "{"),
    (re.compile(r"\s+\)"), ")"),
    (re.compile(r"\s+\]"), "]"),
    (re.compile(r"\s+\}"), "}"),
    (re.compile(r"\s+,"), ","),
    (re.compile(r",\s*([)\]}])"), r"\1"),
)


def flatten(text: str) -> str:
    """Collapse layout, so a snippet matches however the formatter wrapped it.

    Whitespace runs become single spaces and single quotes become double quotes -
    the formatter does both - and the layout it adds inside brackets is dropped.
    The tokens of the snippet keep their order, which is what the assertions are
    about. Call it on both the snippet and the text it is looked for in.
    """
    collapsed = " ".join(str(text).replace("'", '"').split())
    for pattern, replacement in _BRACKET_LAYOUT:
        collapsed = pattern.sub(replacement, collapsed)
    return collapsed


class FlatSource(str):
    """A project file's text, compared with layout flattened on *both* sides.

    ``"snippet" in src``, ``src.count(...)`` and ``src.index(...)`` all flatten the
    snippet as they look for it, so an assertion can keep reading like the source
    it is about while no longer caring how the formatter wrapped or quoted it.
    """

    def __contains__(self, snippet: object) -> bool:
        return super().__contains__(flatten(snippet))

    def count(self, snippet: str, *args: int) -> int:
        return super().count(flatten(snippet), *args)

    def index(self, snippet: str, *args: int) -> int:
        return super().index(flatten(snippet), *args)

    def find(self, snippet: str, *args: int) -> int:
        return super().find(flatten(snippet), *args)


def read_source(*parts: str) -> FlatSource:
    """The text of a project file, ready for ``assertIn``/``count``/``index``."""
    return FlatSource(flatten(source_text(*parts)))


def source_text(*parts: str) -> str:
    """The raw text of a project file, for the scans that work on real lines."""
    return (PROJECT_ROOT.joinpath(*parts)).read_text(encoding="utf-8")


def parse_source(*parts: str) -> ast.Module:
    """The parsed module of a project file."""
    return ast.parse(source_text(*parts))


def read_object_source(obj: object) -> FlatSource:
    """The flattened source of a function or class, as ``inspect`` reports it."""
    return FlatSource(flatten(inspect.getsource(obj)))


def call_keywords(tree: ast.Module, function: str, keyword: str) -> list[str]:
    """Every value passed as ``keyword=`` to any ``function(...)`` call.

    Values are returned unparsed, so wrapping the argument over several lines or
    dropping a trailing comma cannot change the answer.
    """
    values: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node.func) == function:
            values.extend(ast.unparse(kw.value) for kw in node.keywords if kw.arg == keyword)
    return values


def assigned_values(tree: ast.Module, name: str) -> list[str]:
    """Unparsed right-hand sides of every ``name = ...``, whatever its layout.

    A long expression gets wrapped in parentheses by the formatter, which no
    amount of whitespace collapsing can undo, so those are compared as the values
    they are rather than as the text they happen to be written as.
    """
    values: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            values.append(ast.unparse(node.value))
    return values


def find_function(node: ast.AST, name: str) -> ast.AST:
    """The (async) function or method definition called ``name``.

    Taken from the tree rather than sliced out of the text, so the answer does
    not depend on where the method sits or how deeply it is indented.
    """
    for child in ast.walk(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and child.name == name:
            return child
    raise AssertionError(f"no function named {name!r} in the module")


def defined_functions(tree: ast.Module) -> set[str]:
    """Every function and method name defined in a module."""
    return {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def compared_literals(node: ast.AST, name: str, op: type[ast.cmpop] = ast.Eq) -> list[str]:
    """String literals compared to ``name``, as in ``name == "value"``.

    ``op`` picks the comparison - ``ast.Eq`` for the exact triggers, ``ast.In``
    for membership - and either side of it may hold the literal.
    """
    values: list[str] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Compare):
            continue
        operands = [child.left, *child.comparators]
        for index, operator in enumerate(child.ops):
            if not isinstance(operator, op):
                continue
            for literal, other in (
                (operands[index + 1], operands[index]),
                (operands[index], operands[index + 1]),
            ):
                if (
                    isinstance(other, ast.Name)
                    and other.id == name
                    and isinstance(literal, ast.Constant)
                    and isinstance(literal.value, str)
                ):
                    values.append(literal.value)
    return values


def called_string_args(node: ast.AST, dotted: str) -> list[str]:
    """The first string argument of every ``dotted(...)`` call.

    ``dotted`` is the call as written - ``"data.startswith"`` - so a trigger that
    is matched by a prefix is read without depending on the quotes or the layout.
    """
    values: list[str] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Call) or _dotted_name(child.func) != dotted:
            continue
        if child.args and isinstance(child.args[0], ast.Constant) and isinstance(child.args[0].value, str):
            values.append(child.args[0].value)
    return values


def called_methods(node: ast.AST, receiver: str) -> set[str]:
    """Names of the methods called on ``receiver``, as in ``receiver.method()``."""
    names: set[str] = set()
    for child in ast.walk(node):
        if (
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Attribute)
            and isinstance(child.func.value, ast.Name)
            and child.func.value.id == receiver
        ):
            names.add(child.func.attr)
    return names


def literal_dict(node: ast.AST, name: str) -> dict[str, str]:
    """The string-to-string mapping a literal ``name = {...}`` defines."""
    mapping: dict[str, str] = {}
    for child in ast.walk(node):
        if not isinstance(child, ast.Assign) or not isinstance(child.value, ast.Dict):
            continue
        if not any(isinstance(target, ast.Name) and target.id == name for target in child.targets):
            continue
        for key, value in zip(child.value.keys, child.value.values, strict=True):
            if (
                isinstance(key, ast.Constant)
                and isinstance(key.value, str)
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            ):
                mapping[key.value] = value.value
        return mapping
    return {}


def is_and_operand(tree: ast.Module, name: str) -> bool:
    """True when ``name`` is an operand of an ``and`` expression."""
    for node in ast.walk(tree):
        if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And)):
            continue
        if any(isinstance(value, ast.Name) and value.id == name for value in node.values):
            return True
    return False


def _dotted_name(func: ast.expr) -> str | None:
    """``a.b.c`` for a name or attribute chain, ``None`` for anything else."""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        base = _dotted_name(func.value)
        return f"{base}.{func.attr}" if base else None
    return None


def _call_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None
