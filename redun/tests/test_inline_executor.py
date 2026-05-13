"""Tests for the ``InlineExecutor``."""

import pytest

from redun import Scheduler, task
from redun.executors.inline import InlineExecutor


redun_namespace = "redun.tests.test_inline_executor"


@task(executor="inline")
def _double(x: int) -> int:
    return x * 2


@task(executor="inline")
def _add(a: int, b: int) -> int:
    return a + b


@task(executor="inline")
def _pipeline(start: int) -> int:
    return _double(_add(start, 1))


@pytest.mark.unit
def test_inline_executor_registered() -> None:
    """``executor='inline'`` resolves to :class:`InlineExecutor`."""
    from redun.executors.base import get_executor_class

    assert get_executor_class("inline") is InlineExecutor


@pytest.mark.smoke
def test_inline_executor_runs_single_task(redun_scheduler: Scheduler) -> None:
    """A single inline task executes synchronously and returns the result."""
    assert redun_scheduler.run(_double(7)) == 14


@pytest.mark.smoke
def test_inline_executor_runs_pipeline(redun_scheduler: Scheduler) -> None:
    """A pipeline of inline tasks resolves correctly."""
    # _pipeline(5) -> _double(_add(5, 1)) -> _double(6) -> 12
    assert redun_scheduler.run(_pipeline(5)) == 12


@pytest.mark.smoke
def test_inline_executor_propagates_exceptions(redun_scheduler: Scheduler) -> None:
    """An exception inside an inline task surfaces to the scheduler caller."""

    @task(executor="inline")
    def _boom() -> int:
        raise ValueError("kaboom")

    with pytest.raises(ValueError, match="kaboom"):
        redun_scheduler.run(_boom())
