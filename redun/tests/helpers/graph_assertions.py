"""
Helpers for inspecting redun expression trees without executing them.

A redun workflow function returns a lazy ``Expression`` tree. These helpers
let graph-level tests assert on the shape of that tree without invoking the
scheduler.

The traversal is structural: it recurses into ``args`` and ``kwargs`` of any
``ApplyExpression`` and into the contents of standard Python containers
(``list``, ``tuple``, ``dict``, ``set``).
"""

from typing import Any, Iterable, List, Tuple

from redun.expression import ApplyExpression, TaskExpression


def _walk(node: Any) -> Iterable[Any]:
    """Yield every descendant node of ``node``, including itself."""
    yield node
    if isinstance(node, ApplyExpression):
        yield from _walk(node.args)
        yield from _walk(node.kwargs)
    elif isinstance(node, dict):
        for k, v in node.items():
            yield from _walk(k)
            yield from _walk(v)
    elif isinstance(node, (list, tuple, set, frozenset)):
        for item in node:
            yield from _walk(item)


def count_calls(expr: Any, task_name: str) -> int:
    """Count occurrences of ``task_name`` calls in the expression tree."""
    return sum(
        1
        for n in _walk(expr)
        if isinstance(n, TaskExpression) and _matches(n.task_name, task_name)
    )


def all_call_args(expr: Any, task_name: str) -> List[Tuple[tuple, dict]]:
    """Return ``(args, kwargs)`` for every call to ``task_name``."""
    return [
        (n.args, n.kwargs)
        for n in _walk(expr)
        if isinstance(n, TaskExpression) and _matches(n.task_name, task_name)
    ]


def assert_structure(expr: Any, expected: Any) -> None:
    """
    Assert the expression tree's shape matches ``expected``.

    The expected-shape language is a nested tuple of task names:

    - A string matches a ``TaskExpression`` with that ``task_name``
      (matched by bare name or fully-qualified name; see ``_matches``).
    - A tuple ``(name, *children)`` matches a ``TaskExpression`` with
      ``task_name == name`` whose argument tree contains the given children
      (recursively).
    - ``None`` matches anything (wildcard).

    Raises ``AssertionError`` with a description of the first mismatch.
    """
    _assert_shape(expr, expected, path="<root>")


def _assert_shape(node: Any, expected: Any, path: str) -> None:
    if expected is None:
        return
    if isinstance(expected, str):
        match = next(
            (
                n
                for n in _walk(node)
                if isinstance(n, TaskExpression) and _matches(n.task_name, expected)
            ),
            None,
        )
        if match is None:
            raise AssertionError(f"{path}: expected a call to {expected!r}, none found")
        return
    if isinstance(expected, tuple) and expected:
        head, *children = expected
        if not isinstance(head, str):
            raise AssertionError(
                f"{path}: shape tuple must start with a task name (str), got {head!r}"
            )
        match = next(
            (
                n
                for n in _walk(node)
                if isinstance(n, TaskExpression) and _matches(n.task_name, head)
            ),
            None,
        )
        if match is None:
            raise AssertionError(f"{path}: expected a call to {head!r}, none found")
        for i, child in enumerate(children):
            _assert_shape(
                (match.args, match.kwargs),
                child,
                path=f"{path}/{head}[{i}]",
            )
        return
    raise AssertionError(f"{path}: unsupported expected-shape entry {expected!r}")


def _matches(actual: str, query: str) -> bool:
    """Task-name match: exact, or bare-name match against a qualified name."""
    return actual == query or actual.split(".")[-1] == query
