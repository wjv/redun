"""
Executor for running jobs in Apptainer (formerly Singularity) containers.

Apptainer is the standard unprivileged container runtime in HPC environments.
Unlike Docker, it has no daemon — containers run as foreground processes. This
executor tracks jobs via subprocess.Popen objects and polls for completion.

Configuration example::

    [executors.apptainer]
    type = apptainer
    image = /path/to/container.sif
    scratch = .redun_scratch
    job_monitor_interval = 0.5
    vcpus = 1
    memory = 4
    gpus = 0
    no_home = true
    gpu_type = nvidia
    extra_args =
    code_package = true
    code_includes = **/*.py
    code_excludes =
    volumes = []
"""

import json
import logging
import os
import subprocess
import threading
import time
from collections import OrderedDict
from configparser import SectionProxy
from typing import Any, Dict, Iterator, List, Optional, Tuple

from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.executors.container import ApptainerRunner
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


class ApptainerError(Exception):
    pass


def run_apptainer(
    command: List[str],
    image: str,
    volumes: List[Tuple[str, str]] = [],
    env: Dict[str, str] = {},
    no_home: bool = True,
    gpu_type: str = "nvidia",
    gpus: int = 0,
    extra_args: Optional[List[str]] = None,
) -> subprocess.Popen:
    """Launch an Apptainer container as a background subprocess.

    Parameters
    ----------
    command
        Command to run inside the container.
    image
        Path to a SIF file or an Apptainer-compatible image URI.
    volumes
        List of (host_path, container_path) bind mount pairs.
    env
        Environment variables to set inside the container.
    no_home
        If True, do not mount the user's home directory.
    gpu_type
        GPU type: "nvidia" or "rocm".
    gpus
        Number of GPUs required (any value > 0 enables GPU passthrough).
    extra_args
        Additional arguments to pass to apptainer exec.

    Returns
    -------
    A Popen object for the running container process.
    """
    runner = ApptainerRunner(
        no_home=no_home,
        gpu_type=gpu_type,
        extra_args=extra_args or [],
    )
    full_command = runner.wrap_command(
        command,
        image=image,
        volumes=volumes,
        env=env,
        gpus=gpus,
    )
    return subprocess.Popen(
        full_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def get_apptainer_job_options(job_options: dict, scratch_path: str) -> dict:
    """Extract Apptainer-specific job options and add scratch as a bind mount."""
    keys = ["volumes", "no_home", "gpu_type", "gpus", "extra_args", "env"]
    options = {key: job_options[key] for key in keys if key in job_options}
    options["volumes"] = options.get("volumes", []) + [(scratch_path, scratch_path)]
    return options


def iter_job_status(
    scratch_prefix: str, pending_jobs: "OrderedDict[int, _ApptainerJob]"
) -> Iterator[dict]:
    """Check status of running Apptainer subprocesses.

    Yields status dicts for jobs that have finished.
    """
    for pid, apptainer_job in list(pending_jobs.items()):
        proc = apptainer_job.proc
        returncode = proc.poll()
        if returncode is not None:
            # Process has exited. Collect output.
            stdout = proc.stdout.read().decode("utf8", errors="replace") if proc.stdout else ""
            stderr = proc.stderr.read().decode("utf8", errors="replace") if proc.stderr else ""
            logs = stdout + stderr

            redun_job = apptainer_job.job
            status_file = File(get_job_scratch_file(scratch_prefix, redun_job, SCRATCH_STATUS))
            output_file = File(get_job_scratch_file(scratch_prefix, redun_job, SCRATCH_OUTPUT))

            if status_file.exists():
                succeeded = status_file.read().strip() == "ok"
            else:
                succeeded = output_file.exists()

            status = SUCCEEDED if succeeded else FAILED
            yield {"pid": pid, "status": status, "logs": logs}


class _ApptainerJob:
    """Tracks a running Apptainer subprocess and its associated redun Job."""

    __slots__ = ("proc", "job")

    def __init__(self, proc: subprocess.Popen, job: Job):
        self.proc = proc
        self.job = job


@register_executor("apptainer")
class ApptainerExecutor(Executor):
    """Executor for running jobs in local Apptainer containers.

    Each job is launched as a foreground ``apptainer exec`` subprocess. A monitor
    thread polls for process completion and reads results from scratch files.
    """

    def __init__(
        self,
        name: str,
        scheduler: Optional[Scheduler] = None,
        config: Optional[SectionProxy] = None,
    ):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("ApptainerExecutor requires config.")
        self._config = config

        # Required config.
        self._image = config["image"]
        self._scratch_prefix_rel = config["scratch"]
        self._scratch_prefix_abs: Optional[str] = None
        self._interval = config.getfloat("job_monitor_interval", fallback=0.5)

        # Optional config.
        self._code_package = parse_code_package_config(config)
        self._code_file: Optional[File] = None

        # Default task options.
        self._default_job_options: Dict[str, Any] = {
            "vcpus": config.getint("vcpus", fallback=1),
            "gpus": config.getint("gpus", fallback=0),
            "memory": config.getint("memory", fallback=4),
            "no_home": config.getboolean("no_home", fallback=True),
            "gpu_type": config.get("gpu_type", fallback="nvidia"),
        }

        extra_args_str = config.get("extra_args", fallback="")
        self._default_job_options["extra_args"] = extra_args_str.split() if extra_args_str else []

        self._is_running = False
        self._pending_jobs: OrderedDict[int, _ApptainerJob] = OrderedDict()
        self._thread: Optional[threading.Thread] = None

    def set_scheduler(self, scheduler: "Scheduler") -> None:
        super().set_scheduler(scheduler)
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
        """Monitor thread that polls Apptainer subprocesses for completion."""
        assert self._scheduler

        try:
            while self._is_running and self._pending_jobs:
                jobs = iter_job_status(self._scratch_prefix, self._pending_jobs)
                for job in jobs:
                    self._process_job_status(job)
                time.sleep(self._interval)

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_job_status(self, job: dict) -> None:
        """Process a completed Apptainer job."""
        assert self._scheduler

        pid = job["pid"]
        apptainer_job = self._pending_jobs.pop(pid)
        redun_job = apptainer_job.job

        if job["status"] == SUCCEEDED:
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
        elif job["status"] == FAILED:
            error, error_traceback = parse_job_error(self._scratch_prefix, redun_job)
            error_traceback.logs = [line + "\n" for line in job["logs"].split("\n")]
            self._scheduler.reject_job(redun_job, error, error_traceback=error_traceback)

    def _submit(self, job: Job) -> None:
        """Submit a Job to the Apptainer executor."""
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
        image: str = job_options.pop("image", self._image)

        try:
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
                    self._scratch_prefix, job, get_task_command(job.task, args, kwargs),
                    as_mount=True,
                )

            apptainer_options = get_apptainer_job_options(job_options, self._scratch_prefix)
            proc = run_apptainer(command, image=image, **apptainer_options)

        except (ApptainerError, OSError) as exc:
            error, error_traceback = parse_job_error(self._scratch_prefix, job)
            self._scheduler.reject_job(job, error, error_traceback=error_traceback)
            return

        job_dir = get_job_scratch_dir(self._scratch_prefix, job)
        self.log(
            "submit redun job {redun_job} as Apptainer process {pid}:\n"
            "  pid        = {pid}\n"
            "  image      = {image}\n"
            "  scratch    = {job_dir}\n".format(
                redun_job=job.id,
                pid=proc.pid,
                image=image,
                job_dir=job_dir,
            )
        )
        self._pending_jobs[proc.pid] = _ApptainerJob(proc, job)
        self._start()

    def submit(self, job: Job) -> None:
        """Submit a Job to the executor."""
        return self._submit(job)

    def submit_script(self, job: Job) -> None:
        """Submit a script Job to the executor."""
        return self._submit(job)

    def scratch_root(self) -> str:
        return self._scratch_prefix
