"""
``InlineExecutor``: synchronous, in-process task execution.

Unlike :class:`~redun.executors.local.LocalExecutor` (thread / process pools),
``InlineExecutor`` runs each task on the scheduler's own thread, synchronously
inside :meth:`submit`. This is intentionally distinct from the parallel local
modes:

- Concurrency is always 1.
- A long-running inline task blocks the scheduler — surface that property in
  the executor name rather than burying it in a mode flag.
- No subprocess overhead; ideal for housekeeping tasks (small Python helpers,
  database lookups, file moves) and for tests that want to exercise scheduler
  behaviour without the cost of pool startup.

``InlineExecutor`` does NOT inherit :class:`ContainerAware`. Container
wrapping makes no sense for a function call; setting ``container=`` on an
inline task is rejected at task-definition time (see commit 3).
"""

import asyncio
from configparser import SectionProxy
from typing import Optional

from redun.executors.base import Executor, ExecutorError, register_executor
from redun.scheduler import Job, Scheduler


@register_executor("inline")
class InlineExecutor(Executor):
    """Synchronous, in-process executor.

    Suitable for tiny housekeeping tasks and tests. Runs each submitted job
    on the calling thread; results are delivered to the scheduler before
    :meth:`submit` returns.
    """

    def __init__(
        self,
        name: str = "inline",
        scheduler: Optional[Scheduler] = None,
        config: Optional[SectionProxy] = None,
    ):
        super().__init__(name=name, scheduler=scheduler)

    def supports_async(self) -> bool:
        return True

    def submit(self, job: Job) -> None:
        assert job.args is not None
        assert self._scheduler is not None
        args, kwargs = job.args

        try:
            if job.task.is_async():
                result = asyncio.run(job.task.func(*args, **kwargs))
            else:
                result = job.task.func(*args, **kwargs)
        except Exception as error:
            self._scheduler.reject_job(job, error)
            return

        self._scheduler.done_job(job, result)

    def submit_script(self, job: Job) -> None:
        assert self._scheduler is not None
        self._scheduler.reject_job(
            job,
            ExecutorError(
                "InlineExecutor does not support script tasks; "
                "use a host executor with ContainerAware (Local, Pueue) instead."
            ),
        )

    def scratch_root(self) -> str:
        raise NotImplementedError(
            "InlineExecutor runs in-process and has no scratch directory."
        )
