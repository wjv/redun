import boto3

from redun import File, task
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.scheduler import Job
from redun.tests.utils import mock_s3, use_tempdir


@task
def task1(x):
    return x


@use_tempdir
def test_get_oneshot_command() -> None:
    """
    Should be able to generate a shell command for redun oneshot.
    """

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    code_file = File("scratch/code.tar.gz")

    command = get_oneshot_command(
        "scratch",
        job,
        task1,
        (10,),
        {},
        {},
        code_file=code_file,
    )
    assert command == [
        "redun",
        "--check-version",
        ">=0.4.1",
        "oneshot",
        "redun.tests.test_executor_command",
        "--code",
        "scratch/code.tar.gz",
        "--input",
        "scratch/jobs/eval_hash/input",
        "--output",
        "scratch/jobs/eval_hash/output",
        "--error",
        "scratch/jobs/eval_hash/error",
        "task1",
    ]


@use_tempdir
def test_get_script_task_command_local() -> None:
    """
    Should be able to generate a shell command for a redun script task.
    """
    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"

    command = get_script_task_command("scratch", job, "myprog -x 1 input.txt")
    assert command == [
        "bash",
        "-c",
        "-o",
        "pipefail",
        "\n"
        "cp scratch/jobs/eval_hash/input .task_command\n"
        "chmod +x .task_command\n"
        "./.task_command 2> >(tee .task_error >&2) | tee .task_output\n"
        "RETCODE=${PIPESTATUS[0]}\n"
        'if [ "$RETCODE" -eq 0 ]; then\n'
        "    cp .task_output scratch/jobs/eval_hash/output\n"
        "    cp .task_error scratch/jobs/eval_hash/error\n"
        "    echo ok | cat > scratch/jobs/eval_hash/status\n"
        "else\n"
        "    [ -f .task_output ] && cp .task_output scratch/jobs/eval_hash/output\n"
        "    [ -f .task_error ] && cp .task_error scratch/jobs/eval_hash/error\n"
        "    echo fail | cat > scratch/jobs/eval_hash/status\n"
        "    \n"
        "fi\n"
        'exit "$RETCODE"\n',
    ]


@use_tempdir
@mock_s3
def test_get_script_task_command_s3() -> None:
    """
    Should be able to generate a shell command for a redun script task.
    """
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="bucket")

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"

    command = get_script_task_command("s3://bucket/scratch", job, "myprog -x 1 input.txt")
    assert command == [
        "bash",
        "-c",
        "-o",
        "pipefail",
        "\n"
        "aws s3 cp --no-progress s3://bucket/scratch/jobs/eval_hash/input .task_command\n"
        "chmod +x .task_command\n"
        "./.task_command 2> >(tee .task_error >&2) | tee .task_output\n"
        "RETCODE=${PIPESTATUS[0]}\n"
        'if [ "$RETCODE" -eq 0 ]; then\n'
        "    aws s3 cp --no-progress .task_output s3://bucket/scratch/jobs/eval_hash/output\n"
        "    aws s3 cp --no-progress .task_error s3://bucket/scratch/jobs/eval_hash/error\n"
        "    echo ok | aws s3 cp --no-progress - s3://bucket/scratch/jobs/eval_hash/status\n"
        "else\n"
        "    [ -f .task_output ] && aws s3 cp --no-progress .task_output "
        "s3://bucket/scratch/jobs/eval_hash/output\n"
        "    [ -f .task_error ] && aws s3 cp --no-progress .task_error "
        "s3://bucket/scratch/jobs/eval_hash/error\n"
        "    echo fail | aws s3 cp --no-progress - s3://bucket/scratch/jobs/eval_hash/status\n"
        "    \n"
        "fi\n"
        'exit "$RETCODE"\n',
    ]


@use_tempdir
def test_get_script_task_command_propagates_failure_exit_code() -> None:
    """Wrapper must exit non-zero when the user command exits non-zero.

    Regression: the old ``(A) && (B) || (C)`` template caused C's exit code
    (always 0, from successful cleanup) to dominate when A failed —
    silently masking script-task failures from the executor's monitor
    (notably pueue, which would mark the task ``Success`` despite a
    non-zero user exit code). See back-channel q4-to-redun.md 2026-06-05.
    """
    import os
    import subprocess

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"

    # Construct the wrapper for a user command that exits 1.
    failing_user_command = "echo out before fail; echo err before fail >&2; exit 1"
    command = get_script_task_command("scratch", job, failing_user_command)

    # Create the scratch dir layout the wrapper expects.
    scratch_dir = "scratch/jobs/eval_hash"
    os.makedirs(scratch_dir, exist_ok=True)
    with open(f"{scratch_dir}/input", "w") as f:
        f.write(f"#!/bin/bash\n{failing_user_command}\n")

    # Run the wrapper. Capture its exit code; check the status sentinel.
    result = subprocess.run(command, capture_output=True)
    assert result.returncode == 1, (
        f"wrapper exit code was {result.returncode}, expected 1; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    with open(f"{scratch_dir}/status") as f:
        assert f.read().strip() == "fail"


@use_tempdir
def test_get_script_task_command_propagates_success_exit_code() -> None:
    """Wrapper must exit 0 when the user command exits 0 (smoke check for the
    happy path under the new structure)."""
    import os
    import subprocess

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"

    user_command = "echo hello"
    command = get_script_task_command("scratch", job, user_command)

    scratch_dir = "scratch/jobs/eval_hash"
    os.makedirs(scratch_dir, exist_ok=True)
    with open(f"{scratch_dir}/input", "w") as f:
        f.write(f"#!/bin/bash\n{user_command}\n")

    result = subprocess.run(command, capture_output=True)
    assert result.returncode == 0, (
        f"wrapper exit code was {result.returncode}; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    with open(f"{scratch_dir}/status") as f:
        assert f.read().strip() == "ok"
