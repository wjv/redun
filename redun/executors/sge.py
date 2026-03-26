"""
Executor for submitting jobs to a Sun Grid Engine (SGE) cluster.

Jobs are submitted via ``qsub``, monitored via ``qstat``, and results are
exchanged through pickle files in a shared scratch directory (NFS, etc.).

Configuration example::

    [executors.sge]
    type = sge
    scratch = /shared/nfs/redun_scratch
    job_monitor_interval = 10.0
    queue = all.q
    parallel_environment = smp
    project = myproject
    vcpus = 1
    memory = 4
    gpus = 0
    extra_qsub_args =
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
import xml.etree.ElementTree as ET
from collections import OrderedDict
from configparser import SectionProxy
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

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


class SGEError(Exception):
    pass


def qsub_submit(
    script_path: str,
    job_name: str,
    queue: Optional[str] = None,
    parallel_environment: Optional[str] = None,
    project: Optional[str] = None,
    vcpus: int = 1,
    memory: int = 4,
    gpus: int = 0,
    extra_args: Optional[List[str]] = None,
) -> str:
    """Submit a script to SGE via qsub and return the job ID.

    Parameters
    ----------
    script_path
        Path to the shell script to submit.
    job_name
        SGE job name.
    queue
        SGE queue name.
    parallel_environment
        Parallel environment (e.g. ``smp``), used with vcpus.
    project
        SGE project for accounting.
    vcpus
        Number of slots to request.
    memory
        Memory per slot in GB (passed as ``h_vmem``).
    gpus
        Number of GPUs to request.
    extra_args
        Additional arguments to pass to qsub.

    Returns
    -------
    The SGE job ID string (e.g. ``"12345"``).
    """
    args = ["qsub", "-N", job_name, "-l", f"h_vmem={memory}G"]

    if queue:
        args.extend(["-q", queue])
    if parallel_environment and vcpus > 1:
        args.extend(["-pe", parallel_environment, str(vcpus)])
    if project:
        args.extend(["-P", project])
    if gpus > 0:
        args.extend(["-l", f"gpu={gpus}"])
    if extra_args:
        args.extend(extra_args)

    args.append(script_path)

    try:
        output = subprocess.check_output(args, stderr=subprocess.PIPE).decode("utf8").strip()
    except subprocess.CalledProcessError as exc:
        raise SGEError(
            f"qsub failed (exit {exc.returncode}): "
            f"{exc.stderr.decode('utf8', errors='replace') if exc.stderr else ''}"
        ) from exc

    # Parse: "Your job 12345 ("name") has been submitted"
    match = re.search(r"Your job (\d+)", output)
    if not match:
        raise SGEError(f"Could not parse SGE job ID from qsub output: {output!r}")
    return match.group(1)


def qstat_running_jobs() -> Set[str]:
    """Query qstat for the set of currently active (queued/running) job IDs.

    Returns
    -------
    Set of job ID strings that are still in the queue.
    """
    try:
        output = subprocess.check_output(
            ["qstat", "-xml"], stderr=subprocess.PIPE
        ).decode("utf8")
    except subprocess.CalledProcessError as exc:
        raise SGEError(
            f"qstat failed (exit {exc.returncode}): "
            f"{exc.stderr.decode('utf8', errors='replace') if exc.stderr else ''}"
        ) from exc

    running: Set[str] = set()
    try:
        root = ET.fromstring(output)
        for job_elem in root.iter("job_list"):
            jid = job_elem.findtext("JB_job_number")
            if jid:
                running.add(jid.strip())
    except ET.ParseError:
        # Fallback: parse plain-text qstat output.
        for line in output.strip().splitlines():
            parts = line.split()
            if parts and parts[0].isdigit():
                running.add(parts[0])

    return running


def iter_sge_job_status(
    scratch_prefix: str, pending_jobs: Dict[str, "Job"]
) -> Iterator[dict]:
    """Poll SGE for the status of pending jobs.

    For SGE, a job that is no longer in ``qstat`` output has finished.
    We determine success/failure by checking scratch files, which is more
    reliable across SGE configurations than parsing ``qacct``.

    Yields status dicts for jobs that have reached a terminal state.
    """
    if not pending_jobs:
        return

    running = qstat_running_jobs()

    for sge_id, redun_job in list(pending_jobs.items()):
        if sge_id in running:
            continue  # Still active.

        # Job is no longer in qstat — it has finished.
        status_file = File(get_job_scratch_file(scratch_prefix, redun_job, SCRATCH_STATUS))
        output_file = File(get_job_scratch_file(scratch_prefix, redun_job, SCRATCH_OUTPUT))

        if status_file.exists():
            succeeded = status_file.read().strip() == "ok"
        else:
            succeeded = output_file.exists()

        status = SUCCEEDED if succeeded else FAILED
        yield {
            "sge_id": sge_id,
            "status": status,
            "logs": "" if succeeded else f"SGE job {sge_id} failed.\n",
        }


@register_executor("sge")
class SGEExecutor(Executor):
    """Executor for submitting jobs to a Sun Grid Engine cluster.

    Jobs are submitted via ``qsub`` and monitored by polling ``qstat``.
    When a job disappears from ``qstat``, its result is read from the
    shared scratch directory.
    """

    def __init__(
        self,
        name: str,
        scheduler: Optional[Scheduler] = None,
        config: Optional[SectionProxy] = None,
    ):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("SGEExecutor requires config.")
        self._config = config

        # Required config.
        self._scratch_prefix_rel = config["scratch"]
        self._scratch_prefix_abs: Optional[str] = None
        self._interval = config.getfloat("job_monitor_interval", fallback=10.0)

        # SGE-specific config.
        self._queue = config.get("queue", fallback=None)
        self._parallel_environment = config.get("parallel_environment", fallback=None)
        self._project = config.get("project", fallback=None)

        extra_args_str = config.get("extra_qsub_args", fallback="")
        self._extra_qsub_args = extra_args_str.split() if extra_args_str else []

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
        self._pending_jobs: Dict[str, Job] = OrderedDict()
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
        """Monitor thread that polls qstat for job completion."""
        assert self._scheduler

        try:
            while self._is_running and self._pending_jobs:
                for job_status in iter_sge_job_status(
                    self._scratch_prefix, self._pending_jobs
                ):
                    self._process_job_status(job_status)
                time.sleep(self._interval)

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_job_status(self, job_status: dict) -> None:
        """Process a completed SGE job."""
        assert self._scheduler

        sge_id = job_status["sge_id"]
        redun_job = self._pending_jobs.pop(sge_id)

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

    def _write_submit_script(self, job: Job, command: List[str]) -> str:
        """Write a submit script to scratch and return its path."""
        import shlex

        job_dir = get_job_scratch_dir(self._scratch_prefix, job)
        os.makedirs(job_dir, exist_ok=True)
        script_path = os.path.join(job_dir, "submit.sh")

        with open(script_path, "w") as f:
            f.write("#!/bin/bash\n")
            f.write(shlex.join(command) + "\n")
        os.chmod(script_path, 0o755)

        return script_path

    def _submit(self, job: Job) -> None:
        """Submit a Job to the SGE executor."""
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
            script_path = self._write_submit_script(job, command)

            job_name = f"redun_{job.eval_hash[:12]}" if job.eval_hash else f"redun_{job.id}"

            sge_id = qsub_submit(
                script_path,
                job_name=job_name,
                queue=self._queue,
                parallel_environment=self._parallel_environment,
                project=self._project,
                vcpus=job_options.get("vcpus", 1),
                memory=job_options.get("memory", 4),
                gpus=job_options.get("gpus", 0),
                extra_args=self._extra_qsub_args or None,
            )

        except (SGEError, OSError) as exc:
            self._scheduler.reject_job(job, exc)
            return

        job_dir = get_job_scratch_dir(self._scratch_prefix, job)
        self.log(
            "submit redun job {redun_job} as SGE job {sge_id}:\n"
            "  sge_id     = {sge_id}\n"
            "  job_name   = {job_name}\n"
            "  scratch    = {job_dir}\n".format(
                redun_job=job.id,
                sge_id=sge_id,
                job_name=job_name,
                job_dir=job_dir,
            )
        )
        self._pending_jobs[sge_id] = job
        self._start()

    def submit(self, job: Job) -> None:
        """Submit a Job to the executor."""
        return self._submit(job)

    def submit_script(self, job: Job) -> None:
        """Submit a script Job to the executor."""
        return self._submit(job)

    def scratch_root(self) -> str:
        return self._scratch_prefix
