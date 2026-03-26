from typing import cast
from unittest.mock import Mock, patch

from redun import File, task
from redun.config import Config
from redun.executors.apptainer import ApptainerExecutor
from redun.expression import TaskExpression
from redun.scheduler import Job, Scheduler, Traceback
from redun.tests.utils import use_tempdir
from redun.utils import pickle_dumps


@task
def task1(x: int) -> int:
    return x


@use_tempdir
@patch("redun.executors.apptainer.run_apptainer")
@patch("threading.Thread")
@patch("redun.executors.apptainer.iter_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_apptainer(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    run_apptainer_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """
    ApptainerExecutor should run jobs.

    We patch threading.Thread to prevent the monitor thread from starting
    and call the monitor code ourselves.
    """
    # Simulate a Popen-like object.
    mock_proc = Mock()
    mock_proc.pid = 12345
    run_apptainer_mock.return_value = mock_proc

    config = Config(
        {
            "apptainer": {
                "image": "/path/to/image.sif",
                "scratch": ".redun_scratch",
            }
        }
    )
    executor = ApptainerExecutor("apptainer", scheduler, config=config["apptainer"])

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Verify run_apptainer was called with expected arguments.
    scratch_dir = executor._scratch_prefix
    assert run_apptainer_mock.called
    call_args = run_apptainer_mock.call_args
    command = call_args[0][0]  # First positional arg is the command list.
    assert "redun" in command
    assert "oneshot" in command
    assert call_args[1]["image"] == "/path/to/image.sif"
    # Scratch should be in the volumes.
    volumes = call_args[1].get("volumes", [])
    assert (scratch_dir, scratch_dir) in volumes

    # Simulate output file created by job.
    output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate process completing successfully.
    iter_job_status_mock.return_value = [
        {"pid": 12345, "status": "SUCCEEDED", "logs": ""},
    ]

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns result to scheduler.
    scheduler.done_job.assert_called_with(job, 10)  # type: ignore


@use_tempdir
@patch("redun.executors.apptainer.run_apptainer")
@patch("threading.Thread")
@patch("redun.executors.apptainer.iter_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_apptainer_failure(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    run_apptainer_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """ApptainerExecutor should handle job failures."""
    mock_proc = Mock()
    mock_proc.pid = 12346
    run_apptainer_mock.return_value = mock_proc

    config = Config(
        {
            "apptainer": {
                "image": "/path/to/image.sif",
                "scratch": ".redun_scratch",
            }
        }
    )
    executor = ApptainerExecutor("apptainer", scheduler, config=config["apptainer"])

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(11))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash2"
    job.args = ((11,), {})
    executor.submit(job)

    scratch_dir = executor._scratch_prefix

    # Simulate error file created by job.
    error = ValueError("Boom")
    error_traceback = Traceback.from_error(error)
    error_file = File(f"{scratch_dir}/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, error_traceback)), mode="wb")

    # Simulate process failing.
    iter_job_status_mock.return_value = [
        {"pid": 12346, "status": "FAILED", "logs": "something went wrong"},
    ]

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job was rejected.
    scheduler.reject_job.call_args[:2] == (job, error)  # type: ignore
