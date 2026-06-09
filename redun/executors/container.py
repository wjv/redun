"""
Container runner abstraction for wrapping commands in container invocations.

Container runners are lightweight command wrappers, not executors. They take a command
and return it wrapped in a container CLI invocation (e.g. apptainer exec, docker run).

Scheduler executors (Pueue, SGE, Slurm) can optionally use a container runner to wrap
commands before submitting them to the scheduler, enabling composability between
container runtimes and job schedulers.
"""

from configparser import SectionProxy
from typing import Dict, List, Optional, Protocol, Tuple


class ContainerRunner(Protocol):
    """Protocol for container command wrappers."""

    def wrap_command(
        self,
        command: List[str],
        image: str,
        volumes: List[Tuple[str, str]] = [],
        env: Dict[str, str] = {},
        memory: Optional[int] = None,
        vcpus: Optional[int] = None,
        gpus: Optional[int] = None,
    ) -> List[str]:
        """Wrap a command for execution inside a container.

        Parameters
        ----------
        command
            The command to run inside the container.
        image
            Container image path or reference.
        volumes
            List of (host_path, container_path) bind mount pairs.
        env
            Environment variables to set inside the container.
        memory
            Memory limit in GB.
        vcpus
            CPU limit.
        gpus
            Number of GPUs required.

        Returns
        -------
        The full CLI command including the container runtime invocation.
        """
        ...


class ApptainerRunner:
    """Wraps commands for execution inside Apptainer (formerly Singularity) containers.

    Builds commands of the form:
        apptainer exec [--no-home] [--bind h:c] [--env K=V] [--nv] image.sif command
    """

    def __init__(
        self,
        no_home: bool = True,
        gpu_type: str = "nvidia",
        extra_args: Optional[List[str]] = None,
    ):
        self.no_home = no_home
        self.gpu_type = gpu_type
        self.extra_args = extra_args or []

    def wrap_command(
        self,
        command: List[str],
        image: str,
        volumes: List[Tuple[str, str]] = [],
        env: Dict[str, str] = {},
        memory: Optional[int] = None,
        vcpus: Optional[int] = None,
        gpus: Optional[int] = None,
    ) -> List[str]:
        args = ["apptainer", "exec"]

        if self.no_home:
            args.append("--no-home")

        for host, container in volumes:
            args.extend(["--bind", f"{host}:{container}"])

        for key, value in env.items():
            args.extend(["--env", f"{key}={value}"])

        if gpus and gpus > 0:
            if self.gpu_type == "nvidia":
                args.append("--nv")
            elif self.gpu_type == "rocm":
                args.append("--rocm")

        args.extend(self.extra_args)
        args.append(image)
        args.extend(command)
        return args


class DockerRunner:
    """Wraps commands for execution inside Docker containers.

    Builds commands of the form:
        docker run --rm [-v h:c] [-e K=V] [--memory Xg] [--cpus Y] image command
    """

    def __init__(self, extra_args: Optional[List[str]] = None):
        self.extra_args = extra_args or []

    def wrap_command(
        self,
        command: List[str],
        image: str,
        volumes: List[Tuple[str, str]] = [],
        env: Dict[str, str] = {},
        memory: Optional[int] = None,
        vcpus: Optional[int] = None,
        gpus: Optional[int] = None,
    ) -> List[str]:
        # ``-i`` (interactive) attaches the container's stdin to the
        # caller's. Required for the multi-stage `script(Pipe(...))`
        # case so that stage N+1 actually receives stage N's stdout
        # over the bash pipe — without ``-i`` Docker hands the
        # container process an empty stdin regardless of what bash
        # is piping. Apptainer's ``exec`` attaches stdin by default,
        # so this is a Docker-only quirk. Always-on is safe: ``-i``
        # on a stage with nothing to read just attaches an unused
        # stdin. (NOT ``-t``: a TTY would corrupt binary pipe data.)
        args = ["docker", "run", "--rm", "-i"]

        for host, container in volumes:
            args.extend(["-v", f"{host}:{container}"])

        for key, value in env.items():
            args.extend(["-e", f"{key}={value}"])

        if memory is not None:
            args.append(f"--memory={memory}g")
        if vcpus is not None:
            args.append(f"--cpus={vcpus}")
        if gpus and gpus > 0:
            args.extend(["--gpus", "all"])

        # Bypass the image's ENTRYPOINT and invoke the requested binary
        # directly. Apptainer's ``exec`` already does this; injecting
        # ``--entrypoint`` here makes Docker behave consistently. Without
        # this, an image with ``ENTRYPOINT=["foo"]`` would receive the
        # requested command as args to foo (often nonsensically).
        # Placed before ``extra_args`` so a user can still override via
        # ``extra_container_args = --entrypoint <X>`` — Docker honours
        # the last ``--entrypoint`` flag.
        if command:
            args.extend(["--entrypoint", command[0]])

        args.extend(self.extra_args)
        # Accept Apptainer-style `docker://...` references for cross-runtime
        # portability — Docker rejects the prefix as "invalid reference
        # format"; Apptainer reads it natively. Stripping here keeps the
        # task-level `container=` string portable across hosts.
        args.append(image.removeprefix("docker://"))
        args.extend(command[1:])
        return args


def get_container_runner(config: SectionProxy) -> Optional[ContainerRunner]:
    """Create a ContainerRunner from executor config, if container_type is specified.

    Parameters
    ----------
    config
        Executor config section. Reads ``container_type`` (apptainer, docker, or absent),
        plus runner-specific options like ``no_home``, ``gpu_type``, ``extra_container_args``.

    Returns
    -------
    A ContainerRunner instance, or None if no container_type is configured.
    """
    container_type = config.get("container_type", fallback=None)
    if not container_type:
        return None

    extra_args_str = config.get("extra_container_args", fallback="")
    extra_args = extra_args_str.split() if extra_args_str else []

    if container_type == "apptainer":
        return ApptainerRunner(
            no_home=config.getboolean("no_home", fallback=True),
            gpu_type=config.get("gpu_type", fallback="nvidia"),
            extra_args=extra_args,
        )
    elif container_type == "docker":
        return DockerRunner(extra_args=extra_args)
    else:
        raise ValueError(f"Unknown container_type: {container_type!r}")
