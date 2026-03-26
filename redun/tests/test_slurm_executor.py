import os
from typing import cast
from unittest.mock import Mock, patch

from redun import File, task
from redun.config import Config
from redun.executors.slurm import (
    SlurmExecutor,
    sacct_query,
    sbatch_submit,
)
from redun.expression import TaskExpression
from redun.scheduler import Job, Scheduler, Traceback
from redun.tests.utils import use_tempdir
from redun.utils import pickle_dumps


@task
def task1(x: int) -> int:
    return x


class TestSlurmHelpers:
    @patch("subprocess.check_output")
    def test_sbatch_submit(self, mock_check_output: Mock) -> None:
        """sbatch_submit should parse job ID from sbatch output."""
        mock_check_output.return_value = b"Submitted batch job 98765\n"
        job_id = sbatch_submit(
            "/tmp/submit.sh",
            job_name="redun_abc123",
            partition="compute",
            vcpus=4,
            memory=16,
        )
        assert job_id == "98765"

        args = mock_check_output.call_args[0][0]
        assert "sbatch" in args
        assert "--job-name=redun_abc123" in args
        assert "--partition=compute" in args
        assert "--cpus-per-task=4" in args
        assert "--mem=16G" in args

    @patch("subprocess.check_output")
    def test_sbatch_submit_with_gpus(self, mock_check_output: Mock) -> None:
        """sbatch_submit should add GPU resource request."""
        mock_check_output.return_value = b"Submitted batch job 111\n"
        sbatch_submit("/tmp/submit.sh", job_name="test", gpus=2)

        args = mock_check_output.call_args[0][0]
        assert "--gres=gpu:2" in args

    @patch("subprocess.check_output")
    def test_sacct_query(self, mock_check_output: Mock) -> None:
        """sacct_query should parse job states from sacct output."""
        mock_check_output.return_value = (
            b"12345|COMPLETED\n"
            b"12345.batch|COMPLETED\n"
            b"12346|FAILED\n"
            b"12346.batch|FAILED\n"
        )
        states = sacct_query(["12345", "12346"])
        assert states == {"12345": "COMPLETED", "12346": "FAILED"}

    @patch("subprocess.check_output")
    def test_sacct_query_ignores_substeps(self, mock_check_output: Mock) -> None:
        """sacct_query should only return main job entries, not sub-steps."""
        mock_check_output.return_value = (
            b"100|COMPLETED\n"
            b"100.batch|COMPLETED\n"
            b"100.extern|COMPLETED\n"
        )
        states = sacct_query(["100"])
        assert states == {"100": "COMPLETED"}


@use_tempdir
@patch("redun.executors.slurm.sbatch_submit")
@patch("threading.Thread")
@patch("redun.executors.slurm.iter_slurm_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_slurm(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    sbatch_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """SlurmExecutor should submit and monitor jobs."""
    sbatch_mock.return_value = "98765"

    config = Config(
        {
            "slurm": {
                "scratch": ".redun_scratch",
                "partition": "gpu",
                "account": "mylab",
            }
        }
    )
    executor = SlurmExecutor("slurm", scheduler, config=config["slurm"])

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Verify sbatch was called.
    assert sbatch_mock.called
    call_kwargs = sbatch_mock.call_args[1]
    assert call_kwargs["partition"] == "gpu"
    assert call_kwargs["account"] == "mylab"
    assert call_kwargs["job_name"] == "redun_eval_hash"

    # Verify a submit script was written.
    scratch_dir = executor._scratch_prefix
    script_path = sbatch_mock.call_args[0][0]
    assert os.path.exists(script_path)

    # Simulate output file created by job.
    output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate Slurm reporting success.
    iter_job_status_mock.return_value = [
        {"slurm_id": "98765", "status": "SUCCEEDED", "state": "COMPLETED", "logs": ""},
    ]

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns result to scheduler.
    scheduler.done_job.assert_called_with(job, 10)  # type: ignore


@use_tempdir
@patch("redun.executors.slurm.sbatch_submit")
@patch("threading.Thread")
@patch("redun.executors.slurm.iter_slurm_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_slurm_failure(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    sbatch_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """SlurmExecutor should handle job failures."""
    sbatch_mock.return_value = "99999"

    config = Config(
        {
            "slurm": {
                "scratch": ".redun_scratch",
            }
        }
    )
    executor = SlurmExecutor("slurm", scheduler, config=config["slurm"])

    expr = cast(TaskExpression[int], task1(11))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash2"
    job.args = ((11,), {})
    executor.submit(job)

    scratch_dir = executor._scratch_prefix

    # Simulate error file.
    error = ValueError("Boom")
    error_traceback = Traceback.from_error(error)
    error_file = File(f"{scratch_dir}/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, error_traceback)), mode="wb")

    iter_job_status_mock.return_value = [
        {"slurm_id": "99999", "status": "FAILED", "state": "TIMEOUT", "logs": "job timed out"},
    ]

    executor._monitor()

    scheduler.reject_job.call_args[:2] == (job, error)  # type: ignore


@use_tempdir
@patch("redun.executors.slurm.sbatch_submit")
@patch("threading.Thread")
@patch("redun.executors.slurm.iter_slurm_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_slurm_with_container(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    sbatch_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """SlurmExecutor should wrap commands in a container when configured."""
    sbatch_mock.return_value = "77777"

    config = Config(
        {
            "slurm": {
                "scratch": ".redun_scratch",
                "container_type": "apptainer",
                "image": "/path/to/image.sif",
            }
        }
    )
    executor = SlurmExecutor("slurm", config=config["slurm"])
    executor.set_scheduler(scheduler)

    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash3"
    job.args = ((10,), {})
    executor.submit(job)

    # Read the generated submit script and verify it contains apptainer.
    script_path = sbatch_mock.call_args[0][0]
    with open(script_path) as f:
        script_content = f.read()
    assert "apptainer" in script_content
    assert "/path/to/image.sif" in script_content
