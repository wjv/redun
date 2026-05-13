"""Tests for the retired standalone ``ApptainerExecutor``.

The original executor's behaviour is now covered by
``test_container_aware.py`` (command wrapping) and
``test_pueue_container_aware.py`` (end-to-end via Pueue). What remains
here is a single check that the migration stub surfaces an actionable
error pointing users at the new API.
"""

import pytest

from redun.executors.apptainer import ApptainerExecutor
from redun.executors.base import ExecutorError


redun_namespace = "redun.tests.test_apptainer_executor"


@pytest.mark.unit
def test_apptainer_executor_instantiation_raises_migration_error() -> None:
    with pytest.raises(ExecutorError, match="task-level option"):
        ApptainerExecutor()


@pytest.mark.unit
def test_apptainer_executor_message_names_replacement_apis() -> None:
    """The migration message should mention both pueue and inline as targets."""
    try:
        ApptainerExecutor()
    except ExecutorError as e:
        message = str(e)
        assert "executor='pueue'" in message
        assert "executor='inline'" in message
        assert "container=" in message
    else:  # pragma: no cover
        pytest.fail("ApptainerExecutor() should have raised")
