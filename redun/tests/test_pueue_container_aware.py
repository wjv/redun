"""Tests for the orthogonality refactor's effect on ``PueueExecutor``.

Covers:

- Task-level ``container=`` wraps the submitted command in
  ``apptainer exec``.
- Executor-level ``default_container`` is the fallback when the task
  doesn't specify one.
- The scratch directory is always bind-mounted into the container.
- Without a task-level ``container``, the legacy
  ``container_type``/``image`` path still works (back-compat).
- ``binds`` and ``passthrough_env`` from the task options are honoured.
"""

from typing import cast
from unittest.mock import Mock, patch

import pytest

from redun import task
from redun.config import Config
from redun.executors.pueue import PueueExecutor
from redun.expression import TaskExpression
from redun.scheduler import Job, Scheduler
from redun.tests.utils import use_tempdir


redun_namespace = "redun.tests.test_pueue_container_aware"


@task
def _task_plain(x: int) -> int:
    return x


@task(container="task_image.sif")
def _task_with_container(x: int) -> int:
    return x


@task(container="task_image.sif", binds=["/data"], passthrough_env=["FOO_VAR"])
def _task_with_binds_and_env(x: int) -> int:
    return x


def _make_executor(scheduler: Scheduler, **extra_config: str) -> PueueExecutor:
    section = {"scratch": ".redun_scratch", **extra_config}
    config = Config({"pueue": section})
    return PueueExecutor("pueue", scheduler, config=config["pueue"])


@use_tempdir
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
def test_task_container_wraps_command(
    _thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """A task with ``container=`` produces an ``apptainer exec`` command."""
    pueue_add_mock.return_value = 1
    executor = _make_executor(scheduler)

    expr = cast(TaskExpression[int], _task_with_container(5))
    job = Job(_task_with_container, expr)
    job.eval_hash = "hash_with_container"
    job.args = ((5,), {})
    executor.submit(job)

    shell_cmd = pueue_add_mock.call_args[0][0]
    assert "apptainer" in shell_cmd
    assert "exec" in shell_cmd
    assert "task_image.sif" in shell_cmd


@use_tempdir
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
def test_executor_default_container_used_when_task_does_not_set_one(
    _thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """``default_container`` is the fallback when a task omits ``container=``."""
    pueue_add_mock.return_value = 2
    executor = _make_executor(scheduler, default_container="exec_image.sif")

    expr = cast(TaskExpression[int], _task_plain(7))
    job = Job(_task_plain, expr)
    job.eval_hash = "hash_default"
    job.args = ((7,), {})
    executor.submit(job)

    shell_cmd = pueue_add_mock.call_args[0][0]
    assert "exec_image.sif" in shell_cmd


@use_tempdir
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
def test_task_container_overrides_executor_default(
    _thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """Task-level ``container`` takes precedence over executor default."""
    pueue_add_mock.return_value = 3
    executor = _make_executor(scheduler, default_container="default_image.sif")

    expr = cast(TaskExpression[int], _task_with_container(9))
    job = Job(_task_with_container, expr)
    job.eval_hash = "hash_override"
    job.args = ((9,), {})
    executor.submit(job)

    shell_cmd = pueue_add_mock.call_args[0][0]
    assert "task_image.sif" in shell_cmd
    assert "default_image.sif" not in shell_cmd


@use_tempdir
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
def test_scratch_dir_is_always_bind_mounted(
    _thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """A containerised pueue command always binds the scratch dir."""
    pueue_add_mock.return_value = 4
    executor = _make_executor(scheduler)

    expr = cast(TaskExpression[int], _task_with_container(1))
    job = Job(_task_with_container, expr)
    job.eval_hash = "hash_scratch"
    job.args = ((1,), {})
    executor.submit(job)

    shell_cmd = pueue_add_mock.call_args[0][0]
    assert executor._scratch_prefix in shell_cmd


@use_tempdir
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
def test_task_binds_and_env_are_passed_through(
    _thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
    monkeypatch,
) -> None:
    """Task-level ``binds`` and ``passthrough_env`` show up in the command."""
    monkeypatch.setenv("FOO_VAR", "foo_value")
    pueue_add_mock.return_value = 5
    executor = _make_executor(scheduler)

    expr = cast(TaskExpression[int], _task_with_binds_and_env(1))
    job = Job(_task_with_binds_and_env, expr)
    job.eval_hash = "hash_binds_env"
    job.args = ((1,), {})
    executor.submit(job)

    shell_cmd = pueue_add_mock.call_args[0][0]
    assert "/data:/data" in shell_cmd
    assert "FOO_VAR=foo_value" in shell_cmd


@use_tempdir
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
def test_no_container_means_no_apptainer(
    _thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """With no container at task or executor level, command is unwrapped."""
    pueue_add_mock.return_value = 6
    executor = _make_executor(scheduler)

    expr = cast(TaskExpression[int], _task_plain(2))
    job = Job(_task_plain, expr)
    job.eval_hash = "hash_plain"
    job.args = ((2,), {})
    executor.submit(job)

    shell_cmd = pueue_add_mock.call_args[0][0]
    assert "apptainer" not in shell_cmd


@pytest.mark.unit
def test_local_with_container_is_rejected_at_definition() -> None:
    """`executor='local'` + container= is deferred and raises at definition time."""
    with pytest.raises(NotImplementedError, match="executor='local'.*container"):

        @task(executor="local", container="img.sif")
        def _bad() -> int:
            return 1
