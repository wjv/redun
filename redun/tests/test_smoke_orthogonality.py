"""End-to-end smoke tests for the executor orthogonality refactor.

Exercises the user-visible payoff: ``@task(executor='inline')`` cleanly
composes into a pipeline, the scheduler caches re-runs, and the retired
standalone Apptainer executor surfaces an actionable migration error.

These are the minimum demonstrations of the refactor's correctness. A
container-using counterpart (``executor='pueue', container='X'``) is
left as a TODO until a test image and a pueue daemon are available in
the test environment.
"""

import pytest

from redun import Scheduler
from redun.executors.base import ExecutorError


redun_namespace = "redun.tests.test_smoke_orthogonality"


@pytest.mark.smoke
def test_minimal_workflow_runs_inline(
    redun_scheduler: Scheduler, minimal_workflow
) -> None:
    """The inline workflow produces (start + 1) * 2 via the scheduler."""
    result = redun_scheduler.run(minimal_workflow.workflow(5))
    assert result == 12


@pytest.mark.smoke
def test_minimal_workflow_caches_on_rerun(
    redun_scheduler: Scheduler, minimal_workflow, monkeypatch
) -> None:
    """Re-running with identical input hits the cache (task bodies don't re-execute)."""
    calls = {"add_one": 0, "double": 0}

    orig_add = minimal_workflow.add_one.func
    orig_double = minimal_workflow.double.func

    def spy_add(x: int) -> int:
        calls["add_one"] += 1
        return orig_add(x)

    def spy_double(x: int) -> int:
        calls["double"] += 1
        return orig_double(x)

    monkeypatch.setattr(minimal_workflow.add_one, "func", spy_add)
    monkeypatch.setattr(minimal_workflow.double, "func", spy_double)

    assert redun_scheduler.run(minimal_workflow.workflow(5)) == 12
    assert calls == {"add_one": 1, "double": 1}

    # Second run with identical input should hit the cache; spies don't
    # increment.
    assert redun_scheduler.run(minimal_workflow.workflow(5)) == 12
    assert calls == {"add_one": 1, "double": 1}


@pytest.mark.smoke
def test_retired_apptainer_executor_surfaces_migration_error() -> None:
    """Constructing the retired ApptainerExecutor raises an actionable error."""
    from redun.executors.apptainer import ApptainerExecutor

    with pytest.raises(ExecutorError, match="executor='pueue'"):
        ApptainerExecutor()


@pytest.mark.smoke
@pytest.mark.skip(
    reason=(
        "TODO: requires a test Apptainer image and a pueued daemon in the "
        "test environment to exercise `executor='pueue', container='X'` "
        "end-to-end. Tracked for the post-merge integration-test work."
    )
)
def test_pueue_with_container_end_to_end() -> None:
    """Placeholder for the containerised pueue smoke test."""
