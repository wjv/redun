"""
Self-tests for the test scaffolding itself.

Exercises the pytest markers and shared fixtures defined in
``redun/tests/conftest.py``. If these break, the rest of the marker-based
test layering won't be reliable.
"""

import pytest

from redun import Scheduler, task
from redun.backends.db import RedunBackendDb
from redun.tests.helpers.graph_assertions import (
    all_call_args,
    assert_structure,
    count_calls,
)


redun_namespace = "redun.tests.test_scaffolding"


@pytest.mark.unit
def test_unit_marker_runs() -> None:
    """The ``unit`` marker registers and collects tests."""
    assert 1 + 1 == 2


@pytest.mark.smoke
def test_redun_scheduler_is_hermetic(redun_scheduler: Scheduler) -> None:
    """``redun_scheduler`` yields a Scheduler with an in-memory SQLite backend."""
    backend = redun_scheduler.backend
    assert isinstance(backend, RedunBackendDb)
    assert backend.engine is not None
    assert "sqlite" in str(backend.engine.url)
    assert ":memory:" in str(backend.engine.url)


@pytest.mark.unit
def test_tmp_workspace_is_per_test(tmp_workspace) -> None:
    """``tmp_workspace`` yields a writable per-test directory."""
    p = tmp_workspace / "hello.txt"
    p.write_text("hi")
    assert p.read_text() == "hi"


@task
def _add_one(x: int) -> int:
    return x + 1


@task
def _double(x: int) -> int:
    return x * 2


@task
def _flow(start: int) -> int:
    return _double(_add_one(start))


@pytest.mark.graph
def test_graph_helpers_walk_expression_tree() -> None:
    """``count_calls`` and ``all_call_args`` traverse the expression tree."""
    expr = _flow(3)
    assert count_calls(expr, "_flow") == 1
    assert count_calls(expr, "_add_one") == 0  # not yet expanded
    assert all_call_args(expr, "_flow") == [((3,), {})]


@pytest.mark.graph
def test_assert_structure_matches_top_level_call() -> None:
    """``assert_structure`` matches a single-task expected shape."""
    expr = _flow(3)
    assert_structure(expr, "_flow")
    with pytest.raises(AssertionError):
        assert_structure(expr, "_does_not_exist")
