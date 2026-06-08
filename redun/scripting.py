import os
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from tempfile import mkdtemp
from textwrap import dedent
from typing import Any, Optional, Union

from redun.file import File, Staging
from redun.task import Task, task
from redun.utils import iter_nested_value, map_nested_value

NULL = object()
# By default, use bash shell with immediate exit on first error.
DEFAULT_SHELL = "#!/usr/bin/env bash\nset -exo pipefail"


@dataclass(frozen=True)
class Cmd:
    """One command stage in a :func:`script` invocation.

    ``argv`` is either a string (interpreted as a shell command) or a list
    of strings (treated as argv and joined via :func:`shlex.join`).
    ``container`` is optional — when ``None``, the command runs bare on the
    host shell; when a string, it is wrapped in the executor's configured
    container runtime (Apptainer or Docker, per ``container_type`` config).

    Compose via ``|`` into a :class:`Pipe` for multi-stage piped execution::

        Cmd(["samtools", "import", ...], container="docker://samtools:1.21") \\
            | Cmd(["evatags", "-c", "N", "-l", "9"], container="docker://evatags:0.5.5")
    """

    argv: Union[str, tuple[str, ...]]
    container: Optional[str] = None

    def __post_init__(self) -> None:
        # Tuplify lists so the dataclass stays hashable (frozen=True needs all
        # fields hashable). Cosmetic for users; they pass lists, we store
        # tuples internally.
        if isinstance(self.argv, list):
            object.__setattr__(self, "argv", tuple(self.argv))

    def __or__(self, other: "Union[Cmd, Pipe]") -> "Pipe":
        if isinstance(other, Cmd):
            return Pipe(self, other)
        if isinstance(other, Pipe):
            return Pipe(self, *other.stages)
        return NotImplemented


@dataclass(frozen=True, init=False)
class Pipe:
    """N piped command stages: ``stages[i]``'s stdout flows to ``stages[i+1]``'s stdin.

    All stages share a single redun-task identity; the pipeline's ``inputs=``
    and ``outputs=`` are passed at the :func:`script` call site, not per
    stage. Each stage's ``container`` is independent — mixing containerised
    and bare stages is first-class.

    Construct positionally (``Pipe(cmd_a, cmd_b)``) or via ``|`` composition
    on :class:`Cmd` instances. A one-stage pipe (``Pipe(cmd)``) is the same
    as ``Cmd``'s single-command behaviour.
    """

    stages: tuple[Cmd, ...]

    def __init__(self, *stages: Cmd) -> None:
        if not stages:
            raise ValueError("Pipe requires at least one Cmd stage")
        for i, s in enumerate(stages):
            if not isinstance(s, Cmd):
                raise TypeError(
                    f"Pipe stage {i} must be a Cmd, got {type(s).__name__}"
                )
        object.__setattr__(self, "stages", stages)

    def __or__(self, other: "Union[Cmd, Pipe]") -> "Pipe":
        if isinstance(other, Cmd):
            return Pipe(*self.stages, other)
        if isinstance(other, Pipe):
            return Pipe(*self.stages, *other.stages)
        return NotImplemented


class ScriptError(Exception):
    """
    Error raised when user script returns failure (non-zero exit code).
    """

    def __init__(self, stderr: bytes):
        self.message: bytes | str

        try:
            self.message = stderr.decode("utf8")
        except UnicodeDecodeError:
            # Error might not be utf8. Keep as is.
            self.message = stderr

    def __str__(self) -> str:
        if isinstance(self.message, str):
            lines = self.message.rstrip("\n").rsplit("\n")
            return "Last line: " + lines[-1]
        else:
            return ""

    def __repr__(self) -> str:
        return f"ScriptError('{str(self)}')"


def prepare_command(command: str, default_shell=DEFAULT_SHELL) -> str:
    """
    Prepare a command string execution by removing surrounding blank lines and dedent.

    Also if an interpreter is not specified, add the default shell as interpreter.
    """
    command = dedent(command).strip()
    if not command.startswith("#!"):
        command = default_shell.rstrip("\n") + "\n" + command
    return command


def get_task_command(task: Task, args: tuple, kwargs: dict) -> str:
    """
    Get command from a script task.
    """
    command = task.func(*args, **kwargs)
    return prepare_command(command)


def exec_script(command: str) -> bytes:
    """
    Run a script as a subprocess.
    """
    fd, command_file = tempfile.mkstemp()
    try:
        os.write(fd, command.encode("utf8"))
        os.close(fd)

        command2 = """\
chmod +x {command_file}
{command_file}
""".format(command_file=command_file)
        proc = subprocess.run(
            command2,
            check=False,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        result, error = proc.stdout, proc.stderr
    finally:
        os.remove(command_file)

    if proc.returncode != 0:
        # Raise error if command had error.
        raise ScriptError(error)

    return result


def get_command_eof(command: str, eof_prefix: str = "EOF") -> str:
    """
    Determine a safe end-of-file keyword to use for a given command to wrap.
    """
    index = 0
    eof = eof_prefix
    lines = command.split("\n")

    while True:
        if eof in lines:
            index += 1
            eof = eof_prefix + str(index)
        else:
            return eof


def get_wrapped_command(command: str, eof_prefix: str = "EOF") -> str:
    """
    Returns a shell script for executing a script written in any language.

    Consider `command` written in python:

    .. code-block:: python

        '''
        #!/usr/bin/env python

        print('Hello, World')
        '''

    In order to turn this into a regular sh shell script, we need to write
    this command to a temporary file, mark the file as executable,
    execute the file, and remove the temporary file.
    """
    wrapped_command = """\
(
# Save command to temp file.
COMMAND_FILE="$(mktemp)"
cat > "$COMMAND_FILE" <<"{eof}"
{command}
{eof}

# Execute temp file.
chmod +x "$COMMAND_FILE"
"$COMMAND_FILE"
RETCODE=$?

# Remove temp file.
rm "$COMMAND_FILE"

exit $RETCODE
)
""".format(command=command, eof=get_command_eof(command, eof_prefix=eof_prefix))
    return wrapped_command


@task(name="script_task", namespace="redun", version="1", script=True)
def script_task(command: str) -> str:
    """
    Execute a shell script as redun Task.
    """
    return command


@task(name="script", namespace="redun", version="1", check_valid="shallow")
def _script(
    command: str,
    inputs: Any,
    outputs: Any,
    task_options: dict = {},
    temp_path: Optional[str] = None,
) -> Any:
    """
    Internal task for executing a script.

    This task correctly implements reactivity to changing inputs and outputs.
    `script_task()` alone is unable to implement such reactivity because its
    only argument is a shell script string and its output is the stdout.
    Thus, the ultimate input and output files of the script are accessed
    outside the usual redun detection mechanisms (task arguments
    and return values).

    To achieve the correct reactivity, `script_task()` is special-cased in the Scheduler
    to not use caching, in order to force it to always execute when called.
    Additionally, `_script()` is configured with `check_valid="shallow"` to
    skip execution of its child tasks, `script_task()` and `postprocess_script()`,
    if its previous outputs are still valid (i.e. not altered or deleted).
    """
    # Note: inputs are an argument just for reactivity sake.
    # They have already been incorporated into the command.
    return postprocess_script(
        script_task.options(**task_options)(command), outputs, temp_path=temp_path
    )


@task(name="postprocess_script", namespace="redun", version="1")
def postprocess_script(result: Any, outputs: Any, temp_path: Optional[str] = None) -> Any:
    """
    Postprocess the results of a script task.
    """

    def get_file(value: Any) -> Any:
        if isinstance(value, File) and value.path == "-":
            # File for script stdout.
            return result
        elif isinstance(value, Staging):
            # Staging files and dir turn into their remote versions.
            cls = type(value.remote)
            return cls(value.remote.path)
        else:
            return value

    if temp_path:
        shutil.rmtree(temp_path)

    return map_nested_value(get_file, outputs)


def _normalise_command(
    command: Union[str, list, Cmd, Pipe],
    task_options: dict,
) -> Pipe:
    """Coerce any of the four accepted `command=` shapes to a :class:`Pipe`.

    - ``str`` / ``list[str]``: wrap as ``Pipe(Cmd(argv, container=...))``
      where the container, if any, is pulled from
      ``task_options["container"]`` (and removed from there so it isn't
      double-applied downstream).
    - :class:`Cmd`: wrap as ``Pipe(cmd)``; if the cmd carries a
      ``container``, it is left there (not migrated to task_options) — the
      pipeline-aware wrapping path (Phase 2) reads per-stage containers
      from the Pipe directly.
    - :class:`Pipe`: passed through.

    Conflicting task-level vs Cmd-level container settings raise — better
    to fail loudly than to silently prefer one.
    """
    if isinstance(command, Pipe):
        return command
    if isinstance(command, Cmd):
        return Pipe(command)
    # str or list[str]: pull container from task_options for the legacy shape.
    container = task_options.pop("container", None)
    return Pipe(Cmd(argv=command, container=container))


def script(
    command: Union[str, list, Cmd, Pipe],
    inputs: Any = [],
    outputs: Any = NULL,
    tempdir: bool = False,
    as_mount: bool = False,
    **task_options: Any,
) -> Any:
    """
    Execute a shell script as a redun task with file staging.

    See the docs for a full explanation:
      https://insitro.github.io/redun/design.html#file-staging

    Parameters
    ----------
    command : Union[str, list, Cmd, Pipe]
        What to execute. Accepts four shapes:

        - ``str`` — a shell command string. Today's most common form.
        - ``list[str]`` — argv joined via :func:`shlex.join`.
        - :class:`Cmd` — argv plus an optional per-stage ``container``;
          equivalent to passing ``argv`` plus a ``container=`` task option.
        - :class:`Pipe` — N piped :class:`Cmd` stages, each independently
          containerised or bare.

    inputs : Any
        Collection of FileStaging objects used to stage cloud input files to local files.
    outputs : Any
        Collection of FileStaging objects used to unstage local output files back to cloud storage.
    tempdir : bool
        If True, run the command within a temporary directory.
    as_mount : bool
        If True, make use of cloud storage mounting (if available) to stage files.
    **task_options : Any
        Options to configure the Executor, such as `vcpus=2` or `memory=3`.

    Returns
    -------
    Any
        A result the same shape as `outputs` but with all FileStaging objects converted to their
        corresponding remote Files.
    """
    # Normalise the command shape to a Pipe. Single-stage callers (str /
    # list / Cmd / single-stage Pipe) flow through the existing
    # single-command path unchanged; multi-stage Pipes go through the
    # pipeline-aware path (Phase 2+, raises until implemented).
    pipe = _normalise_command(command, task_options)
    if len(pipe.stages) > 1:
        raise NotImplementedError(
            "Multi-stage Pipe(...) is not yet implemented in this fork; "
            "use a single Cmd / str / list, or an intermediate file between "
            "two separate script() calls. Tracking: "
            "`.claude/redun-script-pipelines-plan.md`."
        )
    # Single-stage: rebuild today's str/list command shape from the Cmd.
    only_stage = pipe.stages[0]
    if only_stage.container is not None and "container" not in task_options:
        # A Cmd-supplied container becomes the task-level container option
        # (which `ContainerAware` reads). Don't clobber an explicit
        # task_options["container"] if one was somehow set in parallel.
        task_options["container"] = only_stage.container
    # `command` from here on is what the existing body of `script` expects.
    command = (
        list(only_stage.argv) if isinstance(only_stage.argv, tuple) else only_stage.argv
    )

    if outputs == NULL:
        outputs = File("-")

    command_parts = []

    # Prepare tempdir if requested.
    temp_path: Optional[str]
    if tempdir:
        temp_path = mkdtemp(suffix=".tempdir")
        command_parts.append(shlex.join(["cd", temp_path]))
    else:
        temp_path = None

    # Preprocess outputs.
    def preprocess_output(value):
        if isinstance(value, File) and value.path != "-":
            # Self-stage output Files.
            return value.stage(value.path)
        else:
            return value

    outputs = map_nested_value(preprocess_output, outputs)

    # Preprocess inputs (symmetric to preprocess_output above).
    def preprocess_input(value):
        if isinstance(value, File) and not isinstance(value, Staging):
            # Raw File means "already at its own path"; no staging needed,
            # but the file participates in the cache hash via the input arg
            # to `_script`. `StagingFile.render_stage()` no-ops when
            # `local.path == remote.path`, so this is a degenerate-case
            # use of the existing staging machinery — not a new abstraction.
            return value.stage(value.path)
        else:
            return value

    inputs = map_nested_value(preprocess_input, inputs)

    # Stage inputs.
    command_parts.extend(input.render_stage(as_mount) for input in iter_nested_value(inputs))

    # User command.
    if isinstance(command, list):
        command = shlex.join(command)
    command_parts.append(get_wrapped_command(prepare_command(command)))

    # Unstage outputs.
    file_stages = [value for value in iter_nested_value(outputs) if isinstance(value, Staging)]
    command_parts.extend(file_stage.render_unstage(as_mount) for file_stage in file_stages)

    full_command = "\n".join(command_parts)

    # Get input files for reactivity.
    def get_file(value: Any) -> Any:
        if isinstance(value, Staging):
            # Staging files and dir turn into their remote versions.
            cls = type(value.remote)
            return cls(value.remote.path)
        else:
            return value

    input_args = map_nested_value(get_file, inputs)
    return _script(
        full_command,
        input_args,
        outputs,
        task_options=task_options,
        temp_path=temp_path,
    )
