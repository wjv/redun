"""
The standalone ``ApptainerExecutor`` has been retired.

After the orthogonality refactor in the EVA fork, containerisation is a
task-level concern, orthogonal to the choice of host executor:

.. code-block:: python

    @task(executor="pueue", container="my_image.sif")
    def my_task(...):
        ...

Use ``executor="pueue"`` for containerised work, or ``executor="inline"``
for in-process housekeeping tasks. The mixin
:class:`~redun.executors.container_aware.ContainerAware` handles the
``apptainer exec`` command wrapping that the standalone executor used to
provide.

Importing or instantiating ``ApptainerExecutor`` raises an actionable
error pointing to the new API. See
``.claude/redun-orthogonality-and-testability-handover.md`` for the full
design rationale.
"""

from configparser import SectionProxy
from typing import Optional

from redun.executors.base import Executor, ExecutorError, register_executor
from redun.scheduler import Scheduler


_MIGRATION_MESSAGE = (
    "The standalone ApptainerExecutor has been retired. "
    "Containerisation is now a task-level option: "
    "`@task(executor='pueue', container='my_image.sif')`. "
    "For in-process tasks, use `@task(executor='inline')`."
)


@register_executor("apptainer")
class ApptainerExecutor(Executor):
    """Migration stub for the retired standalone Apptainer executor.

    Raises :class:`ExecutorError` on instantiation with a pointer to the
    task-level ``container=`` option introduced by the orthogonality
    refactor. Kept only so that ``executor="apptainer"`` in stale code
    yields an actionable error rather than the generic
    "Unknown executor" message.
    """

    def __init__(
        self,
        name: str = "apptainer",
        scheduler: Optional[Scheduler] = None,
        config: Optional[SectionProxy] = None,
    ):
        raise ExecutorError(_MIGRATION_MESSAGE)
