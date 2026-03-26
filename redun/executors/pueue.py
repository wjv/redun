"""
Executor for submitting jobs to a Pueue daemon.

Pueue is a CLI task management tool for queuing and running shell commands.
This executor targets a fork that adds job-slot-based resource management
(``pueued --jobs N``, ``pueue add --jobs N``).

Jobs are submitted via ``pueue add``, monitored via ``pueue status --json``,
and results are exchanged through a shared scratch directory on the local
filesystem.

Configuration example::

    [executors.pueue]
    type = pueue
    scratch = /shared/scratch/redun
    job_monitor_interval = 2.0
    group = default
    jobs = 1
    code_package = true
    code_includes = **/*.py
    code_excludes =

    # Optional container wrapping.
    container_type = apptainer
    image = /path/to/container.sif
    no_home = true
"""

import json
import logging
import os
import re
import subprocess
import threading
import time
from collections import OrderedDict
from configparser import SectionProxy
from typing import Any, Dict, Iterator, List, Optional, Tuple

from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.executors.container import ContainerRunner, get_container_runner
from redun.executors.scratch import (
    SCRATCH_OUTPUT,
    SCRATCH_STATUS,
    get_job_scratch_dir,
    get_job_scratch_file,
    parse_job_error,
    parse_job_result,
)
from redun.file import File
from redun.scheduler import Job, Scheduler
from redun.scripting import get_task_command
from redun.task import Task

SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"


class PueueError(Exception):
    pass


def pueue_add(
    command: str,
    group: Optional[str] = None,
    jobs: int = 1,
    label: Optional[str] = None,
    working_directory: Optional[str] = None,
) -> int:
    """Submit a command to pueue and return the task ID.

    Parameters
    ----------
    command
        The shell command to enqueue.
    group
        Pueue group to submit to.
    jobs
        Number of job slots this task requires.
    label
        Optional label for the task.
    working_directory
        Working directory for the task.

    Returns
    -------
    The pueue task ID (integer).
    """
    args = ["pueue", "add", "--print-task-id"]

    if group:
        args.extend(["--group", group])
    if jobs != 1:
        args.extend(["--jobs", str(jobs)])
    if label:
        args.extend(["--label", label])
    if working_directory:
        args.extend(["--working-directory", working_directory])

    args.extend(["--", command])

    try:
        output = subprocess.check_output(args, stderr=subprocess.PIPE).decode("utf8").strip()
    except subprocess.CalledProcessError as exc:
        raise PueueError(
            f"pueue add failed (exit {exc.returncode}): "
            f"{exc.stderr.decode('utf8', errors='replace') if exc.stderr else ''}"
        ) from exc

    try:
        return int(output)
    except ValueError:
        raise PueueError(f"Could not parse pueue task ID from output: {output!r}")


def pueue_status() -> dict:
    """Query pueue daemon for current status.

    Returns
    -------
    Parsed JSON dict with ``tasks`` and ``groups`` keys.
    """
    try:
        output = subprocess.check_output(
            ["pueue", "status", "--json"], stderr=subprocess.PIPE
        ).decode("utf8")
    except subprocess.CalledProcessError as exc:
        raise PueueError(
            f"pueue status failed (exit {exc.returncode}): "
            f"{exc.stderr.decode('utf8', errors='replace') if exc.stderr else ''}"
        ) from exc

    return json.loads(output)


def get_pueue_task_status(task_info: dict) -> Optional[str]:
    """Extract the terminal status from a pueue task dict.

    Returns
    -------
    ``"SUCCEEDED"`` if done with Success, ``"FAILED"`` if done with a failure
    result, or None if the task is still running/queued.
    """
    status = task_info.get("status", {})

    if isinstance(status, str):
        # Queued, Running, Paused, Stashed, Locked — still in progress.
        return None

    if "Done" in status:
        result = status["Done"].get("result")
        if result == "Success":
            return SUCCEEDED
        else:
            return FAILED

    return None


def iter_pueue_job_status(
    pending_tasks: Dict[int, "Job"],
) -> Iterator[dict]:
    """Poll pueue for the status of pending tasks.

    Yields status dicts for tasks that have reached a terminal state.
    """
    if not pending_tasks:
        return

    status_data = pueue_status()
    all_tasks = status_data.get("tasks", {})

    for pueue_id, redun_job in list(pending_tasks.items()):
        pueue_id_str = str(pueue_id)
        task_info = all_tasks.get(pueue_id_str)
        if task_info is None:
            # Task no longer exists in pueue — treat as failed.
            yield {
                "pueue_id": pueue_id,
                "status": FAILED,
                "logs": f"Pueue task {pueue_id} no longer exists.\n",
            }
            continue

        terminal_status = get_pueue_task_status(task_info)
        if terminal_status is not None:
            yield {
                "pueue_id": pueue_id,
                "status": terminal_status,
                "logs": "",
            }


@register_executor("pueue")
class PueueExecutor(Executor):
    """Executor for submitting jobs to a Pueue daemon.

    Jobs are submitted via the ``pueue`` CLI and monitored by polling
    ``pueue status --json``. Task arguments and results are exchanged
    through pickle files in a shared scratch directory.
    """

    def __init__(
        self,
        name: str,
        scheduler: Optional[Scheduler] = None,
        config: Optional[SectionProxy] = None,
    ):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("PueueExecutor requires config.")
        self._config = config

        # Required config.
        self._scratch_prefix_rel = config["scratch"]
        self._scratch_prefix_abs: Optional[str] = None
        self._interval = config.getfloat("job_monitor_interval", fallback=2.0)

        # Pueue-specific config.
        self._group = config.get("group", fallback=None)
        self._jobs = config.getint("jobs", fallback=1)

        # Optional container wrapping.
        self._image = config.get("image", fallback=None)
        self._container_runner: Optional[ContainerRunner] = None

        # Code packaging.
        self._code_package = parse_code_package_config(config)
        self._code_file: Optional[File] = None

        # Default task options.
        self._default_job_options: Dict[str, Any] = {
            "vcpus": config.getint("vcpus", fallback=1),
            "gpus": config.getint("gpus", fallback=0),
            "memory": config.getint("memory", fallback=4),
        }

        self._is_running = False
        self._pending_jobs: Dict[int, Job] = OrderedDict()
        self._thread: Optional[threading.Thread] = None

    def set_scheduler(self, scheduler: "Scheduler") -> None:
        super().set_scheduler(scheduler)
        self._container_runner = get_container_runner(self._config)

        self._default_job_options["volumes"] = self._parse_volumes(
            self._config.get("volumes", fallback="[]")
        )

    def _parse_volumes(self, volumes_str: str) -> List[List[str]]:
        """Parse bind mount specifications from JSON string."""
        try:
            volumes = json.loads(volumes_str)
        except json.decoder.JSONDecodeError:
            raise ValueError("Invalid 'volumes' syntax. Expected JSON list of path pairs.")

        assert self._scheduler
        configdir = self._scheduler.config.configdir
        return [
            [os.path.abspath(os.path.join(configdir, host_path)), container_path]
            for host_path, container_path in volumes
        ]

    @property
    def _scratch_prefix(self) -> str:
        if not self._scratch_prefix_abs:
            if os.path.isabs(self._scratch_prefix_rel):
                self._scratch_prefix_abs = self._scratch_prefix_rel
            else:
                assert self._scheduler
                base_dir = os.path.abspath(self._scheduler.config.configdir)
                self._scratch_prefix_abs = os.path.normpath(
                    os.path.join(base_dir, self._scratch_prefix_rel)
                )
        assert self._scratch_prefix_abs
        return self._scratch_prefix_abs

    def _start(self) -> None:
        """Start the monitoring thread if not already running."""
        os.makedirs(self._scratch_prefix, exist_ok=True)

        if not self._is_running:
            self._is_running = True
            self._thread = threading.Thread(target=self._monitor, daemon=False)
            self._thread.start()

    def stop(self) -> None:
        """Stop the executor and monitoring thread."""
        self._is_running = False
        if (
            self._thread
            and self._thread.is_alive()
            and threading.get_ident() != self._thread.ident
        ):
            self._thread.join()

    def _monitor(self) -> None:
        """Monitor thread that polls pueue for job completion."""
        assert self._scheduler

        try:
            while self._is_running and self._pending_jobs:
                for job_status in iter_pueue_job_status(self._pending_jobs):
                    self._process_job_status(job_status)
                time.sleep(self._interval)

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_job_status(self, job_status: dict) -> None:
        """Process a completed pueue job."""
        assert self._scheduler

        pueue_id = job_status["pueue_id"]
        redun_job = self._pending_jobs.pop(pueue_id)

        if job_status["status"] == SUCCEEDED:
            result, exists = parse_job_result(self._scratch_prefix, redun_job)
            if exists:
                self._scheduler.done_job(redun_job, result)
            else:
                self._scheduler.reject_job(
                    redun_job,
                    FileNotFoundError(
                        get_job_scratch_file(self._scratch_prefix, redun_job, SCRATCH_OUTPUT)
                    ),
                )
        elif job_status["status"] == FAILED:
            error, error_traceback = parse_job_error(self._scratch_prefix, redun_job)
            if job_status.get("logs"):
                error_traceback.logs = [
                    line + "\n" for line in job_status["logs"].split("\n")
                ]
            self._scheduler.reject_job(redun_job, error, error_traceback=error_traceback)

    def _build_command(
        self, job: Job, args: tuple, kwargs: dict, job_options: dict
    ) -> List[str]:
        """Build the command to run, optionally wrapped in a container."""
        if not job.task.script:
            command = get_oneshot_command(
                self._scratch_prefix,
                job,
                job.task,
                args=args,
                kwargs=kwargs,
                job_options=job_options,
                code_file=self._code_file,
            )
        else:
            command = get_script_task_command(
                self._scratch_prefix,
                job,
                get_task_command(job.task, args, kwargs),
                as_mount=True,
            )

        # Optionally wrap in a container.
        if self._container_runner and self._image:
            volumes = job_options.get("volumes", []) + [
                (self._scratch_prefix, self._scratch_prefix)
            ]
            command = self._container_runner.wrap_command(
                command,
                image=self._image,
                volumes=volumes,
                memory=job_options.get("memory"),
                vcpus=job_options.get("vcpus"),
                gpus=job_options.get("gpus"),
            )

        return command

    def _submit(self, job: Job) -> None:
        """Submit a Job to the Pueue executor."""
        assert self._scheduler
        assert job.args
        args, kwargs = job.args

        if self._code_package is not False and self._code_file is None:
            code_package = self._code_package or {}
            assert isinstance(code_package, dict)
            self._code_file = package_code(self._scratch_prefix, code_package)

        job_options: dict = {
            **self._default_job_options,
            **job.get_options(),
        }

        try:
            command = self._build_command(job, args, kwargs, job_options)

            # Join command into a single shell string for pueue.
            shell_command = _shell_join(command)

            jobs_slots = job_options.get("jobs", self._jobs)
            label = f"redun:{job.eval_hash}" if job.eval_hash else None

            pueue_id = pueue_add(
                shell_command,
                group=self._group,
                jobs=jobs_slots,
                label=label,
            )

        except (PueueError, OSError) as exc:
            self._scheduler.reject_job(job, exc)
            return

        job_dir = get_job_scratch_dir(self._scratch_prefix, job)
        self.log(
            "submit redun job {redun_job} as pueue task {pueue_id}:\n"
            "  pueue_id   = {pueue_id}\n"
            "  scratch    = {job_dir}\n".format(
                redun_job=job.id,
                pueue_id=pueue_id,
                job_dir=job_dir,
            )
        )
        self._pending_jobs[pueue_id] = job
        self._start()

    def submit(self, job: Job) -> None:
        """Submit a Job to the executor."""
        return self._submit(job)

    def submit_script(self, job: Job) -> None:
        """Submit a script Job to the executor."""
        return self._submit(job)

    def scratch_root(self) -> str:
        return self._scratch_prefix


def _shell_join(command: List[str]) -> str:
    """Join a command list into a shell-safe string.

    Uses shlex.join on Python 3.8+ (available since 3.8).
    """
    import shlex

    return shlex.join(command)
