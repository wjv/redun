import ast
import re
from itertools import chain
from typing import Any, Optional

from redun.backends.db import Argument, Execution, Job, Tag, Task, Value
from redun.tags import format_tag_value
from redun.utils import format_timestamp, trim_string

NULL = object()

# Max length for a single scalar argument preview in the (compact) job-list
# view. The detail screen renders the full value; this only trims the
# one-line summary so a long command body or path doesn't dominate the row.
JOB_LIST_ARG_MAX = 40

# Heredoc inside `get_wrapped_command`: `cat > "$COMMAND_FILE" <<"EOF"` …
# `EOF`, where the marker is EOF / EOF1 / … (chosen to avoid collision).
_HEREDOC_RE = re.compile(r'<<"(EOF\d*)"\n(.*?)\n\1\n', re.DOTALL)
# `# stages: <python-tuple-literal>` comment embedded by
# `_build_pipeline_bash_body` for multi-stage pipelines.
_STAGES_RE = re.compile(r"^# stages: (.+)$", re.MULTILINE)


def summarise_script_command(command: str) -> str:
    """Extract a short, legible summary of a script task's user command.

    A script task's single positional argument is the full wrapped bash
    body (kilobytes — staging, the user command in a heredoc, unstaging).
    Rendering that verbatim makes the console job list a wall of shell.
    This pulls out just the user-meaningful part:

    - Multi-stage pipelines (a ``# stages: <repr>`` comment is present):
      render ``argv0 | argv1 | …`` from the recorded stage argvs (the
      bash body itself only has ``__REDUN_PIPELINE_STAGE_<i>__`` markers
      at this point, which aren't useful).
    - Single-stage: the first non-boilerplate line of the heredoc
      (skipping shebang, comments, and ``set -…`` lines).

    Falls back to the raw command if neither shape is recognised.
    """
    heredoc = _HEREDOC_RE.search(command)
    body = heredoc.group(2) if heredoc else command

    stages_match = _STAGES_RE.search(body)
    if stages_match:
        try:
            stages = ast.literal_eval(stages_match.group(1))
            parts = []
            for argv, _container in stages:
                argv_str = " ".join(argv) if isinstance(argv, (tuple, list)) else str(argv)
                parts.append(argv_str)
            return " | ".join(parts)
        except (ValueError, SyntaxError):
            pass  # Fall through to line-scan.

    for line in body.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("set "):
            continue
        return stripped

    return command


def rich_escape(text: str) -> str:
    """
    Escape a string for rich.

    Unfortunately, `rich.markup.escape()` doesn't catch all issues.
    """
    return text.replace("[", r"\[")


def format_link(link_pattern: str, tags: dict[str, Any]) -> Optional[str]:
    """
    Format a link pattern using a tag dictionary.
    """

    def replace(match: re.Match) -> str:
        # Trim {{ }} brackets.
        key_regex = match[0][2:-2]

        # Parse out key and optional regex.
        if ":" in key_regex:
            key, regex = key_regex.split(":", 1)
        else:
            key, regex = key_regex, ""

        # Fetch value
        value = tags.get(key, NULL)
        if value is NULL:
            raise KeyError(key)
        value = str(value)

        if regex:
            # If a regex is given, reformat the value according to the regex.
            match2 = re.match(regex, value)
            if match2:
                try:
                    # Try a named group first.
                    value = match2["val"]
                except IndexError:
                    value = match2[1]
            else:
                raise KeyError(key)
        return value

    try:
        # Replace every instance of '{{varible}}' and '{{variable:regex}}'.
        return re.sub(r"\{\{([^}]|\}[^}])+\}\}", replace, link_pattern)
    except KeyError:
        return None


def get_links(link_patterns: list[str], tags: list[Tag]) -> list[str]:
    """
    Get links from a list of link patterns and redun tags.
    """
    tags_dict: dict[str, Any] = {tag.key: tag.value for tag in tags}
    return list(
        filter(
            None,
            [format_link(link_pattern, tags_dict) for link_pattern in link_patterns],
        )
    )


def format_tags(tags: list[Tag], max_length: int = 100, color="#9999cc") -> str:
    """
    Format a set of tags.
    """
    if not tags:
        return ""

    def format_tag_key_value(key: str, value: Any) -> str:
        key = trim_string(key, max_length=max_length)
        value_str = trim_string(format_tag_value(value), max_length=max_length)
        return f"[{color}]{rich_escape(key)}={rich_escape(value_str)}[/]"

    tags = sorted(tags, key=lambda tag: tag.key)

    return ", ".join(format_tag_key_value(tag.key, tag.value) for tag in tags)


def format_arguments(args: list[Argument], max_length: int = 200) -> str:
    """
    Display CallNode arguments.

    For example, if `args` has 2 positional and 1 keyword argument, we would
    display that as:

        'prog', 10, extra_file=File(path=prog.c, hash=763bc10f)

    `max_length` bounds each individual value preview. Callers wanting a
    compact one-line summary (the job list) pass a smaller value than the
    detail screens.
    """
    pos_args = sorted(
        [arg for arg in args if arg.arg_position is not None],
        key=lambda arg: arg.arg_position,
    )
    kw_args = sorted([arg for arg in args if arg.arg_key is not None], key=lambda arg: arg.arg_key)

    text = ", ".join(
        chain(
            (trim_string(repr(arg.value.preview), max_length=max_length) for arg in pos_args),
            (
                "{}={}".format(
                    arg.arg_key, trim_string(repr(arg.value.preview), max_length=max_length)
                )
                for arg in kw_args
            ),
        )
    )
    return text


def _is_script_task(task: Task) -> bool:
    """True for redun's internal script tasks (`redun.script` /
    `redun.script_task`), whose sole positional arg is a wrapped bash body."""
    return task.namespace == "redun" and task.name in ("script", "script_task")


def format_job(job: Job) -> str:
    """
    Format a redun Job into a string representation.

    For the compact job-list view: script tasks render a short summary of
    the user command (not the kilobyte wrapper body), and every argument
    preview is trimmed hard so the salient leading positional stays
    visible. The detail screens render the full arguments separately.
    """
    if not job.call_hash:
        return f"[bold]{rich_escape(job.task.fullname)}[/][#999999]()[/]"

    if _is_script_task(job.task):
        # The first positional arg is the wrapped command body. Show a
        # legible summary of the user command instead of the whole wrapper.
        pos = sorted(
            (a for a in job.call_node.arguments if a.arg_position is not None),
            key=lambda a: a.arg_position,
        )
        if pos:
            summary = summarise_script_command(str(pos[0].value.preview))
            args = trim_string(summary, max_length=JOB_LIST_ARG_MAX)
        else:
            args = ""
    else:
        args = format_arguments(job.call_node.arguments, max_length=JOB_LIST_ARG_MAX)

    return f"[bold]{rich_escape(job.task.fullname)}[/][#999999]({rich_escape(args)})[/]"


def format_traceback(job: Job) -> str:
    """
    Format the call stack from Execution down to the given Job.
    """

    # Determine job stack.
    job_stack = []
    current_job = job
    while current_job:
        job_stack.append(current_job)
        current_job = current_job.parent_job

    parts = ["[@click=screen.click_exec]Exec {exec}[/] > ".format(exec=job.execution.id[:8])]

    if len(job_stack) > 2:
        parts.append(
            "({num_jobs} {unit}) > ".format(
                num_jobs=len(job_stack) - 2,
                unit="Jobs" if len(job_stack) - 2 > 1 else "Job",
            )
        )
    if len(job_stack) > 1:
        parts.append(
            "[@click=screen.click_parent_job]Job {job_id} {task_name}[/] > ".format(
                job_id=job_stack[1].id[:8],
                task_name=job_stack[1].task.name,
            )
        )
    parts.append(
        "[bold]Job {job_id} {task_name}[/]".format(
            job_id=job_stack[0].id[:8],
            task_name=job_stack[0].task.name,
        )
    )
    if job.child_jobs:
        parts.append(f" > [@click=screen.children]{len(job.child_jobs)} child jobs[/]")

    return f"[bold]Traceback:[/b] {''.join(parts)}"


def style_status(status: str) -> str:
    """
    Returns styled text for a job/execution status.
    """
    status2style = {
        "DONE": "white on #55aa55",
        "FAILED": "white on #aa5555",
        "CACHED": "white on #5555aa",
        "RUN ": "black on #aaaa55",
    }
    if status in status2style:
        return f"[{status2style[status]}]{status.center(6)}[/]"
    else:
        return f"[{status.center(6)}]"


def format_record(record: Any) -> str:
    """
    Format a redun repo record (e.g. Execution, Job, etc) into a string.
    """
    if isinstance(record, Execution):
        return (
            f"Exec {record.id[:8]} {style_status(record.status)} "
            f"{format_timestamp(record.job.start_time)}"
            f"[[bold]{record.job.task.namespace or 'no namespace'}[/bold]] "
        )
    elif isinstance(record, Job):
        return (
            f"Job {record.id[:8]} {style_status(record.status)} "
            f"{format_timestamp(record.start_time)} "
        ) + format_job(record)
    elif isinstance(record, Task):
        return f"Task {record.hash[:8]} {record.fullname}"
    elif isinstance(record, Value):
        return f"Value {record.value_hash[:8]} {rich_escape(trim_string(repr(record.preview)))}"
    else:
        return rich_escape(repr(record))
