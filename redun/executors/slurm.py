"""
Executor for submitting jobs to a Slurm cluster.

Jobs are submitted via ``sbatch``, monitored via ``sacct``, and results are
exchanged through pickle files in a shared scratch directory (NFS, Lustre,
GPFS, etc.).

Configuration example::

    [executors.slurm]
    type = slurm
    scratch = /shared/lustre/redun_scratch
    job_monitor_interval = 10.0
    partition = compute
    account = myaccount
    qos = normal
    time_limit = 01:00:00
    vcpus = 1
    memory = 4
    gpus = 0
    nodes = 1
    extra_sbatch_args =
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

# Slurm terminal states from sacct.
_SLURM_SUCCESS_STATES = {"COMPLETED"}
_SLURM_FAILURE_STATES = {
    "FAILED", "TIMEOUT", "CANCELLED", "CANCELLED+", "NODE_FAIL",
    "PREEMPTED", "OUT_OF_MEMORY",
}


class SlurmError(Exception):
    pass


def sbatch_submit(
    script_path: str,
    job_name: str,
    partition: Optional[str] = None,
    account: Optional[str] = None,
    qos: Optional[str] = None,
    time_limit: Optional[str] = None,
    vcpus: int = 1,
    memory: int = 4,
    gpus: int = 0,
    nodes: int = 1,
    extra_args: Optional[List[str]] = None,
) -> str:
    """Submit a batch script to Slurm and return the job ID.

    Parameters
    ----------
    script_path
        Path to the shell script to submit.
    job_name
        Slurm job name (used for identification and reuniting).
    partition
        Slurm partition.
    account
        Slurm account for billing.
    qos
        Quality of service.
    time_limit
        Wall-clock time limit (e.g. ``01:00:00``).
    vcpus
        CPUs per task.
    memory
        Memory in GB.
    gpus
        Number of GPUs.
    nodes
        Number of nodes.
    extra_args
        Additional arguments to pass to sbatch.

    Returns
    -------
    The Slurm job ID string (e.g. ``"12345"``).
    """
    args = ["sbatch", f"--job-name={job_name}", f"--cpus-per-task={vcpus}", f"--mem={memory}G"]

    if partition:
        args.append(f"--partition={partition}")
    if account:
        args.append(f"--account={account}")
    if qos:
        args.append(f"--qos={qos}")
    if time_limit:
        args.append(f"--time={time_limit}")
    if nodes != 1:
        args.append(f"--nodes={nodes}")
    if gpus > 0:
        args.append(f"--gres=gpu:{gpus}")
    if extra_args:
        args.extend(extra_args)

    args.append(script_path)

    try:
        output = subprocess.check_output(args, stderr=subprocess.PIPE).decode("utf8").strip()
    except subprocess.CalledProcessError as exc:
        raise SlurmError(
            f"sbatch failed (exit {exc.returncode}): "
            f"{exc.stderr.decode('utf8', errors='replace') if exc.stderr else ''}"
        ) from exc

    # Parse: "Submitted batch job 12345"
    match = re.search(r"Submitted batch job (\d+)", output)
    if not match:
        raise SlurmError(f"Could not parse Slurm job ID from sbatch output: {output!r}")
    return match.group(1)


def sacct_query(job_ids: List[str]) -> Dict[str, str]:
    """Query Slurm accounting for job states.

    Parameters
    ----------
    job_ids
        List of Slurm job IDs to query.

    Returns
    -------
    Dict mapping job ID to state string (e.g. ``{"12345": "COMPLETED"}``).
    """
    if not job_ids:
        return {}

    try:
        output = subprocess.check_output(
            [
                "sacct",
                "-j", ",".join(job_ids),
                "--format=JobID,State",
                "--noheader",
                "--parsable2",
            ],
            stderr=subprocess.PIPE,
        ).decode("utf8")
    except subprocess.CalledProcessError as exc:
        raise SlurmError(
            f"sacct failed (exit {exc.returncode}): "
            f"{exc.stderr.decode('utf8', errors='replace') if exc.stderr else ''}"
        ) from exc

    states: Dict[str, str] = {}
    for line in output.strip().splitlines():
        parts = line.split("|")
        if len(parts) >= 2:
            job_id = parts[0].strip()
            state = parts[1].strip()
            # sacct returns sub-jobs (e.g. "12345.batch"). We only want the main entry.
            if "." not in job_id and job_id in job_ids:
                states[job_id] = state
    return states


def iter_slurm_job_status(
    scratch_prefix: str, pending_jobs: Dict[str, "Job"]
) -> Iterator[dict]:
    """Poll Slurm for the status of pending jobs.

    Yields status dicts for jobs that have reached a terminal state.
    """
    if not pending_jobs:
        return

    states = sacct_query(list(pending_jobs.keys()))

    for slurm_id, redun_job in list(pending_jobs.items()):
        state = states.get(slurm_id)
        if not state:
            continue

        if state in _SLURM_SUCCESS_STATES:
            yield {"slurm_id": slurm_id, "status": SUCCEEDED, "state": state, "logs": ""}
        elif state in _SLURM_FAILURE_STATES:
            yield {
                "slurm_id": slurm_id,
                "status": FAILED,
                "state": state,
                "logs": f"Slurm job {slurm_id} ended with state: {state}\n",
            }


@register_executor("slurm")
class SlurmExecutor(Executor):
    """Executor for submitting jobs to a Slurm cluster.

    Jobs are submitted via ``sbatch`` and monitored by polling ``sacct``.
    Task arguments and results are exchanged through pickle files in a
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
            raise ValueError("SlurmExecutor requires config.")
        self._config = config

        # Required config.
        self._scratch_prefix_rel = config["scratch"]
        self._scratch_prefix_abs: Optional[str] = None
        self._interval = config.getfloat("job_monitor_interval", fallback=10.0)

        # Slurm-specific config.
        self._partition = config.get("partition", fallback=None)
        self._account = config.get("account", fallback=None)
        self._qos = config.get("qos", fallback=None)
        self._time_limit = config.get("time_limit", fallback=None)
        self._nodes = config.getint("nodes", fallback=1)

        extra_args_str = config.get("extra_sbatch_args", fallback="")
        self._extra_sbatch_args = extra_args_str.split() if extra_args_str else []

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
        """Monitor thread that polls sacct for job completion."""
        assert self._scheduler

        try:
            while self._is_running and self._pending_jobs:
                for job_status in iter_slurm_job_status(
                    self._scratch_prefix, self._pending_jobs
                ):
                    self._process_job_status(job_status)
                time.sleep(self._interval)

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_job_status(self, job_status: dict) -> None:
        """Process a completed Slurm job."""
        assert self._scheduler

        slurm_id = job_status["slurm_id"]
        redun_job = self._pending_jobs.pop(slurm_id)

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
        """Submit a Job to the Slurm executor."""
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

            slurm_id = sbatch_submit(
                script_path,
                job_name=job_name,
                partition=self._partition,
                account=self._account,
                qos=self._qos,
                time_limit=self._time_limit,
                vcpus=job_options.get("vcpus", 1),
                memory=job_options.get("memory", 4),
                gpus=job_options.get("gpus", 0),
                nodes=self._nodes,
                extra_args=self._extra_sbatch_args or None,
            )

        except (SlurmError, OSError) as exc:
            self._scheduler.reject_job(job, exc)
            return

        job_dir = get_job_scratch_dir(self._scratch_prefix, job)
        self.log(
            "submit redun job {redun_job} as Slurm job {slurm_id}:\n"
            "  slurm_id   = {slurm_id}\n"
            "  job_name   = {job_name}\n"
            "  scratch    = {job_dir}\n".format(
                redun_job=job.id,
                slurm_id=slurm_id,
                job_name=job_name,
                job_dir=job_dir,
            )
        )
        self._pending_jobs[slurm_id] = job
        self._start()

    def submit(self, job: Job) -> None:
        """Submit a Job to the executor."""
        return self._submit(job)

    def submit_script(self, job: Job) -> None:
        """Submit a script Job to the executor."""
        return self._submit(job)

    def scratch_root(self) -> str:
        return self._scratch_prefix
