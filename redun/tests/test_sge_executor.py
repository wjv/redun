import os
from typing import cast
from unittest.mock import Mock, patch

from redun import File, task
from redun.config import Config
from redun.executors.sge import (
    SGEExecutor,
    qsub_submit,
    qstat_running_jobs,
)
from redun.expression import TaskExpression
from redun.scheduler import Job, Scheduler, Traceback
from redun.tests.utils import use_tempdir
from redun.utils import pickle_dumps


@task
def task1(x: int) -> int:
    return x


class TestSGEHelpers:
    @patch("subprocess.check_output")
    def test_qsub_submit(self, mock_check_output: Mock) -> None:
        """qsub_submit should parse job ID from qsub output."""
        mock_check_output.return_value = b'Your job 54321 ("redun_abc") has been submitted\n'
        job_id = qsub_submit(
            "/tmp/submit.sh",
            job_name="redun_abc",
            queue="all.q",
            vcpus=4,
            memory=8,
        )
        assert job_id == "54321"

        args = mock_check_output.call_args[0][0]
        assert "qsub" in args
        assert "redun_abc" in args
        assert "-q" in args
        assert "all.q" in args
        assert "h_vmem=8G" in " ".join(args)

    @patch("subprocess.check_output")
    def test_qsub_submit_with_pe(self, mock_check_output: Mock) -> None:
        """qsub_submit should add parallel environment when vcpus > 1."""
        mock_check_output.return_value = b'Your job 100 ("test") has been submitted\n'
        qsub_submit(
            "/tmp/submit.sh",
            job_name="test",
            parallel_environment="smp",
            vcpus=8,
        )

        args = mock_check_output.call_args[0][0]
        assert "-pe" in args
        pe_idx = args.index("-pe")
        assert args[pe_idx + 1] == "smp"
        assert args[pe_idx + 2] == "8"

    @patch("subprocess.check_output")
    def test_qsub_submit_no_pe_single_cpu(self, mock_check_output: Mock) -> None:
        """qsub_submit should not add -pe when vcpus == 1."""
        mock_check_output.return_value = b'Your job 101 ("test") has been submitted\n'
        qsub_submit("/tmp/submit.sh", job_name="test", parallel_environment="smp", vcpus=1)

        args = mock_check_output.call_args[0][0]
        assert "-pe" not in args

    @patch("subprocess.check_output")
    def test_qstat_running_jobs_xml(self, mock_check_output: Mock) -> None:
        """qstat_running_jobs should parse XML output."""
        mock_check_output.return_value = b"""<?xml version='1.0'?>
<job_info>
  <queue_info>
    <job_list state="running">
      <JB_job_number>12345</JB_job_number>
    </job_list>
    <job_list state="running">
      <JB_job_number>12346</JB_job_number>
    </job_list>
  </queue_info>
  <job_info>
    <job_list state="pending">
      <JB_job_number>12347</JB_job_number>
    </job_list>
  </job_info>
</job_info>"""
        running = qstat_running_jobs()
        assert running == {"12345", "12346", "12347"}


@use_tempdir
@patch("redun.executors.sge.qsub_submit")
@patch("threading.Thread")
@patch("redun.executors.sge.iter_sge_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_sge(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    qsub_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """SGEExecutor should submit and monitor jobs."""
    qsub_mock.return_value = "54321"

    config = Config(
        {
            "sge": {
                "scratch": ".redun_scratch",
                "queue": "all.q",
                "parallel_environment": "smp",
            }
        }
    )
    executor = SGEExecutor("sge", scheduler, config=config["sge"])

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Verify qsub was called.
    assert qsub_mock.called
    call_kwargs = qsub_mock.call_args[1]
    assert call_kwargs["queue"] == "all.q"
    assert call_kwargs["parallel_environment"] == "smp"
    assert call_kwargs["job_name"] == "redun_eval_hash"

    # Verify a submit script was written.
    scratch_dir = executor._scratch_prefix
    script_path = qsub_mock.call_args[0][0]
    assert os.path.exists(script_path)

    # Simulate output file created by job.
    output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate SGE reporting success.
    iter_job_status_mock.return_value = [
        {"sge_id": "54321", "status": "SUCCEEDED", "logs": ""},
    ]

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns result to scheduler.
    scheduler.done_job.assert_called_with(job, 10)  # type: ignore


@use_tempdir
@patch("redun.executors.sge.qsub_submit")
@patch("threading.Thread")
@patch("redun.executors.sge.iter_sge_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_sge_failure(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    qsub_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """SGEExecutor should handle job failures."""
    qsub_mock.return_value = "55555"

    config = Config(
        {
            "sge": {
                "scratch": ".redun_scratch",
            }
        }
    )
    executor = SGEExecutor("sge", scheduler, config=config["sge"])

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
        {"sge_id": "55555", "status": "FAILED", "logs": "SGE job failed"},
    ]

    executor._monitor()

    scheduler.reject_job.call_args[:2] == (job, error)  # type: ignore


@use_tempdir
@patch("redun.executors.sge.qsub_submit")
@patch("threading.Thread")
@patch("redun.executors.sge.iter_sge_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_sge_with_container(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    qsub_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """SGEExecutor should wrap commands in a container when configured."""
    qsub_mock.return_value = "66666"

    config = Config(
        {
            "sge": {
                "scratch": ".redun_scratch",
                "container_type": "apptainer",
                "image": "/path/to/image.sif",
            }
        }
    )
    executor = SGEExecutor("sge", config=config["sge"])
    executor.set_scheduler(scheduler)

    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash3"
    job.args = ((10,), {})
    executor.submit(job)

    # Read the generated submit script and verify it contains apptainer.
    script_path = qsub_mock.call_args[0][0]
    with open(script_path) as f:
        script_content = f.read()
    assert "apptainer" in script_content
    assert "/path/to/image.sif" in script_content
