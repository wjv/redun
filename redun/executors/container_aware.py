"""
``ContainerAware`` mixin: adds runtime-agnostic container wrapping to host
executors.

Host executors that inherit ``ContainerAware`` gain support for the task-level
options ``container``, ``binds``, and ``passthrough_env``, plus the matching
executor-level config defaults ``default_container``, ``default_bind``, and
``default_passthrough_env``. The container *runtime* (Apptainer vs Docker)
is selected per executor via the ``container_type`` config key — the same
``@task(container="...")`` declaration can run unchanged across an
Apptainer-only host and a Docker-only host, with each host's executor
config picking the local runtime.

The mixin is concerned only with *command transformation*: given a command
list and a task's option dict, return the command wrapped in a container
invocation (``apptainer exec`` or ``docker run``) if a container image is
resolved, else the command unchanged. It does not interact with job
submission, monitoring, or scheduling — those remain the host executor's
responsibility.

Determinism
-----------
The wrapped command is built deterministically:

- Bind specifications are sorted lexically before being passed to the
  underlying runner.
- Environment variables passed through are sorted lexically by key.

This ensures that two runs of identical input produce identical command
output, supporting cache-hash stability (see ``container`` in the task
option cache hash, §4.3 of the orthogonality handover spec). The runtime
choice (Apptainer vs Docker) does **not** affect the cache hash — image
string is what determines what code runs; runtime is just how the host
happens to invoke it.
"""

import os
from configparser import SectionProxy
from typing import Iterable, List, Optional, Tuple

from redun.executors.container import (
    ApptainerRunner,
    ContainerRunner,
    get_container_runner,
)


def _parse_bind(spec: str) -> Tuple[str, str]:
    """Parse an Apptainer-style bind spec into a ``(host, container)`` pair.

    Accepts either ``/host/path`` (mounts the host path at the same path
    inside the container) or ``/host/path:/container/path``.
    """
    if ":" in spec:
        host, container = spec.split(":", 1)
    else:
        host = container = spec
    return host, container


class ContainerAware:
    """Mixin for host executors that adds Apptainer container wrapping.

    Subclasses must call :meth:`_load_container_config` from their own
    ``__init__`` to populate the executor-level defaults from their
    config section. Tasks then opt in per-call via the ``container``,
    ``binds``, and ``passthrough_env`` task options.
    """

    # Executor-level defaults; populated by ``_load_container_config``.
    default_container: Optional[str] = None
    default_binds: List[str] = []
    default_passthrough_env: List[str] = []

    # The underlying command-wrapper. Populated per-instance by
    # ``_load_container_config`` from the ``container_type`` config key
    # (apptainer | docker); class-level default preserves today's
    # Apptainer-only behaviour when no key is configured. Named
    # distinctly from any pre-existing ``_container_runner`` attribute
    # on host executors to avoid attribute-shadowing collisions.
    _container_runtime: ContainerRunner = ApptainerRunner()

    def _load_container_config(self, config: Optional[SectionProxy]) -> None:
        """Populate executor-level defaults from a config section.

        Reads the keys ``default_container``, ``default_bind``,
        ``default_passthrough_env``, and ``container_type``. The middle
        two are comma-separated. Missing keys yield the sentinel
        defaults (no container; empty lists; Apptainer runtime).
        """
        if config is None:
            return
        self.default_container = config.get("default_container", fallback=None) or None
        self.default_binds = _split_csv(config.get("default_bind", fallback=""))
        self.default_passthrough_env = _split_csv(
            config.get("default_passthrough_env", fallback="")
        )

        # Runtime selection (apptainer vs docker) via the shared factory;
        # falls back to the Apptainer default when ``container_type`` is
        # absent. The factory raises on unknown values.
        runner = get_container_runner(config)
        if runner is not None:
            self._container_runtime = runner

    def _resolve_container(self, task_options: dict) -> Optional[str]:
        """Return the container image to use, or ``None`` for no wrapping.

        Task-level ``container`` overrides the executor-level default. A
        task-level ``None`` falls back to the default.
        """
        value = task_options.get("container", None)
        return self.default_container if value is None else value

    def _resolve_binds(self, task_options: dict) -> List[str]:
        """Return the bind specifications for the task.

        Task-level ``binds`` overrides the executor-level default. A
        task-level ``None`` falls back to the default; an explicit empty
        list overrides to nothing.
        """
        value = task_options.get("binds", None)
        return list(self.default_binds) if value is None else list(value)

    def _resolve_passthrough_env(self, task_options: dict) -> List[str]:
        """Return the names of environment variables to expose to the container.

        Task-level ``passthrough_env`` overrides the executor-level default.
        A task-level ``None`` falls back to the default; an explicit empty
        list overrides to nothing.
        """
        value = task_options.get("passthrough_env", None)
        return list(self.default_passthrough_env) if value is None else list(value)

    def _substitute_pipeline_markers(
        self, bash_body: str, stages: tuple, task_options: dict
    ) -> str:
        """Replace ``__REDUN_PIPELINE_STAGE_<i>__`` markers in ``bash_body``
        with per-stage container-wrapped invocations.

        Executor-time substitution is necessary because per-stage container
        wrapping needs the host's resolved runtime (Apptainer vs Docker per
        ``container_type``), which lives on this ``ContainerAware`` instance
        and is not knowable in ``script()`` at workflow-author time.

        **Security/correctness note:** ``bash_body`` must originate from
        :func:`redun.scripting._build_pipeline_bash_body`. Do not pass
        user-supplied or external text through this function — the marker
        pattern is improbable but not guaranteed unique in arbitrary text,
        and a stray collision would silently corrupt the script.

        ``stages`` is a tuple of ``(argv, container_or_None)`` pairs as
        emitted by ``script()`` into the ``_pipeline_stages`` task option.
        ``argv`` is either a str (shell command, runs via ``bash -c`` —
        the container image must therefore have ``bash`` on PATH) or a
        tuple of strings (argv list, invoked directly — no shell required
        in the container).

        Bare stages (``container is None``) are passed through with no
        wrapping. Per-stage ``binds`` and ``passthrough_env`` inherit from
        task-level options uniformly (no per-stage override in Phase 2).
        """
        import shlex

        for i, (argv, container) in enumerate(stages):
            marker = f"__REDUN_PIPELINE_STAGE_{i}__"
            # Normalise argv to a list[str] for the runner.
            if isinstance(argv, str):
                # A str argv is interpreted as a shell command: wrap it in
                # `bash -c ...` so the shell parses redirects, expansions,
                # etc. inside the container. NOTE: the container image
                # must have `bash` on PATH. Pass argv as a tuple/list to
                # avoid this requirement.
                argv_list = ["bash", "-c", argv]
            else:
                argv_list = list(argv)

            if container is None:
                # Bare stage. The substituted string is the argv joined
                # by shlex (so it's a single shell-quoted token sequence).
                substitution = shlex.join(argv_list)
            else:
                stage_options = {**task_options, "container": container}
                wrapped = self._wrap_command_for_container(argv_list, stage_options)
                substitution = shlex.join(wrapped)

            bash_body = bash_body.replace(marker, substitution)

        return bash_body

    def _wrap_command_for_container(
        self, cmd: List[str], task_options: dict
    ) -> List[str]:
        """Wrap ``cmd`` in a container invocation if appropriate.

        Returns the wrapped command, or ``cmd`` unchanged if no container is
        resolved. The exact wrapping (``apptainer exec`` vs ``docker run``)
        depends on the executor's configured ``container_type``. Binds and
        environment-variable names are sorted lexically for reproducibility.
        """
        image = self._resolve_container(task_options)
        if image is None:
            return cmd

        binds = self._resolve_binds(task_options)
        env_keys = self._resolve_passthrough_env(task_options)

        volumes = sorted(_parse_bind(b) for b in binds)
        env = {k: os.environ[k] for k in sorted(set(env_keys)) if k in os.environ}

        return self._container_runtime.wrap_command(
            list(cmd), image=image, volumes=volumes, env=env
        )


def _split_csv(raw: str) -> List[str]:
    """Split a comma-separated config value into a list, dropping blanks."""
    return [item.strip() for item in raw.split(",") if item.strip()]


__all__ = ["ContainerAware"]
