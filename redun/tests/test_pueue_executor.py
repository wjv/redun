import json
from typing import cast
from unittest.mock import Mock, patch

from redun import File, task
from redun.config import Config
from redun.executors.pueue import (
    PueueError,
    PueueExecutor,
    PueueVersion,
    get_pueue_task_status,
    get_pueue_version,
    iter_pueue_job_status,
    pueue_add,
)
from redun.expression import TaskExpression
from redun.scheduler import Job, Scheduler, Traceback
from redun.tests.utils import use_tempdir
from redun.utils import pickle_dumps


@task
def task1(x: int) -> int:
    return x


class TestPueueHelpers:
    @patch("subprocess.check_output")
    def test_pueue_add(self, mock_check_output: Mock) -> None:
        """pueue_add should parse task ID from --print-task-id output."""
        mock_check_output.return_value = b"42\n"
        task_id = pueue_add("echo hello", group="default", jobs=2, label="test")
        assert task_id == 42

        # Verify CLI arguments.
        args = mock_check_output.call_args[0][0]
        assert "pueue" in args
        assert "add" in args
        assert "--print-task-id" in args
        assert "--group" in args
        assert "default" in args
        assert "--jobs" in args
        assert "2" in args
        assert "--label" in args
        assert "test" in args
        assert "echo hello" in args

    @patch("subprocess.check_output")
    def test_pueue_add_no_optional_args(self, mock_check_output: Mock) -> None:
        """pueue_add should omit optional flags when not specified."""
        mock_check_output.return_value = b"7\n"
        task_id = pueue_add("ls -la")
        assert task_id == 7

        args = mock_check_output.call_args[0][0]
        assert "--group" not in args
        # jobs=1 is the default, should not pass --jobs.
        assert "--jobs" not in args
        assert "--label" not in args

    def test_get_pueue_task_status_success(self) -> None:
        task_info = {
            "status": {
                "Done": {
                    "result": "Success",
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-01T00:01:00Z",
                }
            }
        }
        assert get_pueue_task_status(task_info) == "SUCCEEDED"

    def test_get_pueue_task_status_failed(self) -> None:
        task_info = {
            "status": {
                "Done": {
                    "result": {"Failed": 1},
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-01T00:01:00Z",
                }
            }
        }
        assert get_pueue_task_status(task_info) == "FAILED"

    def test_get_pueue_task_status_running(self) -> None:
        task_info = {
            "status": {
                "Running": {
                    "start": "2026-01-01T00:00:00Z",
                }
            }
        }
        assert get_pueue_task_status(task_info) is None

    def test_get_pueue_task_status_queued(self) -> None:
        task_info = {"status": {"Queued": {"enqueued_at": "2026-01-01T00:00:00Z"}}}
        assert get_pueue_task_status(task_info) is None

    def test_get_pueue_task_status_killed(self) -> None:
        task_info = {"status": {"Done": {"result": "Killed"}}}
        assert get_pueue_task_status(task_info) == "FAILED"

    def test_get_pueue_task_status_dependency_failed(self) -> None:
        # When an upstream pueue task fails, downstream dependents get
        # ``result: "DependencyFailed"`` — a string, not a dict.
        task_info = {"status": {"Done": {"result": "DependencyFailed"}}}
        assert get_pueue_task_status(task_info) == "FAILED"

    def test_get_pueue_task_status_stashed(self) -> None:
        task_info = {"status": {"Stashed": {"enqueue_at": None}}}
        assert get_pueue_task_status(task_info) is None


class TestPueueVersion:
    def setup_method(self) -> None:
        get_pueue_version.cache_clear()

    def teardown_method(self) -> None:
        get_pueue_version.cache_clear()

    @patch("subprocess.check_output")
    def test_parses_standard_output(self, mock_check_output: Mock) -> None:
        mock_check_output.return_value = b"pueue 4.0.4\n"
        v = get_pueue_version()
        assert (v.major, v.minor, v.patch) == (4, 0, 4)
        assert v.suffix == ""
        assert str(v) == "4.0.4"

    @patch("subprocess.check_output")
    def test_parses_fork_suffix(self, mock_check_output: Mock) -> None:
        mock_check_output.return_value = b"pueue 4.0.4-eva.2\n"
        v = get_pueue_version()
        assert (v.major, v.minor, v.patch) == (4, 0, 4)
        assert v.suffix == "-eva.2"
        assert str(v) == "4.0.4-eva.2"

    @patch("subprocess.check_output")
    def test_unparseable_output_raises(self, mock_check_output: Mock) -> None:
        mock_check_output.return_value = b"hello world\n"
        try:
            get_pueue_version()
        except PueueError as exc:
            assert "could not parse" in str(exc)
        else:
            assert False, "expected PueueError"

    @patch("subprocess.check_output")
    def test_missing_binary_raises(self, mock_check_output: Mock) -> None:
        mock_check_output.side_effect = FileNotFoundError("pueue")
        try:
            get_pueue_version()
        except PueueError as exc:
            assert "could not run" in str(exc)
        else:
            assert False, "expected PueueError"


class TestIterPueueJobStatusMissingTask:
    """Tests for ``iter_pueue_job_status`` when ``task_info is None`` —
    i.e. the pueue daemon has lost visibility of a pending task (auto-
    trim, ``pueue clean``, daemon restart, or a transient race).

    Regression for Q4's 2026-06-11 intermittent failures: ~6% of sibling
    ``script(Cmd | Cmd)`` tasks raised ScriptError despite the wrapper
    having exited cleanly and the BAM being on disk. The executor was
    blindly classifying ``task_info is None`` as FAILED — now it
    consults the per-job scratch ``status`` file the wrapper writes as
    ground truth."""

    def _make_job(self, eval_hash: str = "abc123"):
        @task
        def t():
            return 1

        job = Job(t, cast(TaskExpression[int], t()))
        job.eval_hash = eval_hash
        return job

    @patch("redun.executors.pueue.pueue_status")
    def test_missing_task_with_ok_scratch_yields_success(
        self, mock_status: Mock, tmp_path
    ) -> None:
        """``status: ok`` in scratch + pueue task missing → SUCCEEDED."""
        from redun.executors.pueue import iter_pueue_job_status

        mock_status.return_value = {"tasks": {}}
        job = self._make_job()
        job_dir = tmp_path / "jobs" / job.eval_hash
        job_dir.mkdir(parents=True)
        (job_dir / "status").write_text("ok\n")

        results = list(iter_pueue_job_status({42: job}, str(tmp_path)))
        assert len(results) == 1
        assert results[0]["status"] == "SUCCEEDED"

    @patch("redun.executors.pueue.pueue_status")
    def test_missing_task_with_fail_scratch_yields_failed(
        self, mock_status: Mock, tmp_path
    ) -> None:
        """``status: fail`` in scratch + pueue task missing → FAILED."""
        from redun.executors.pueue import iter_pueue_job_status

        mock_status.return_value = {"tasks": {}}
        job = self._make_job()
        job_dir = tmp_path / "jobs" / job.eval_hash
        job_dir.mkdir(parents=True)
        (job_dir / "status").write_text("fail\n")

        results = list(iter_pueue_job_status({42: job}, str(tmp_path)))
        assert len(results) == 1
        assert results[0]["status"] == "FAILED"

    @patch("redun.executors.pueue.pueue_status")
    def test_missing_task_with_no_scratch_yields_failed(
        self, mock_status: Mock, tmp_path
    ) -> None:
        """No scratch status file at all → FAILED (the genuine "lost"
        case — wrapper never reached the status-write step)."""
        from redun.executors.pueue import iter_pueue_job_status

        mock_status.return_value = {"tasks": {}}
        job = self._make_job()

        results = list(iter_pueue_job_status({42: job}, str(tmp_path)))
        assert len(results) == 1
        assert results[0]["status"] == "FAILED"


@use_tempdir
@patch(
    "redun.executors.pueue.get_pueue_version",
    new=Mock(return_value=PueueVersion(4, 0, 4, "-eva.2")),
)
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
@patch("redun.executors.pueue.iter_pueue_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_pueue(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """
    PueueExecutor should submit and monitor jobs.

    We patch threading.Thread to prevent the monitor thread from starting
    and call the monitor code ourselves.
    """
    pueue_add_mock.return_value = 42

    config = Config(
        {
            "pueue": {
                "scratch": ".redun_scratch",
                "group": "compute",
                "jobs": "2",
            }
        }
    )
    executor = PueueExecutor("pueue", scheduler, config=config["pueue"])

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Verify pueue_add was called.
    assert pueue_add_mock.called
    call_kwargs = pueue_add_mock.call_args[1]
    assert call_kwargs["group"] == "compute"
    assert call_kwargs["jobs"] == 2
    assert call_kwargs["label"] == "redun:eval_hash"
    # `working_directory` is the per-job scratch dir so the wrapper's
    # `.task_command` / `.task_output` / `.task_error` files land in
    # scratch rather than leaking into pueued's cwd. (Q4 back-channel
    # request, 2026-06-09.)
    assert call_kwargs["working_directory"].endswith("/jobs/eval_hash")

    # Simulate output file created by job.
    scratch_dir = executor._scratch_prefix
    output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate pueue reporting success.
    iter_job_status_mock.return_value = [
        {"pueue_id": 42, "status": "SUCCEEDED", "logs": ""},
    ]

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns result to scheduler.
    scheduler.done_job.assert_called_with(job, 10)  # type: ignore


@use_tempdir
@patch(
    "redun.executors.pueue.get_pueue_version",
    new=Mock(return_value=PueueVersion(4, 0, 4, "-eva.2")),
)
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
@patch("redun.executors.pueue.iter_pueue_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_pueue_failure(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """PueueExecutor should handle job failures."""
    pueue_add_mock.return_value = 99

    config = Config(
        {
            "pueue": {
                "scratch": ".redun_scratch",
            }
        }
    )
    executor = PueueExecutor("pueue", scheduler, config=config["pueue"])

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

    # Simulate pueue reporting failure.
    iter_job_status_mock.return_value = [
        {"pueue_id": 99, "status": "FAILED", "logs": "task failed"},
    ]

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job was rejected.
    scheduler.reject_job.call_args[:2] == (job, error)  # type: ignore


@use_tempdir
@patch(
    "redun.executors.pueue.get_pueue_version",
    new=Mock(return_value=PueueVersion(4, 0, 4, "-eva.2")),
)
@patch("redun.executors.pueue.pueue_add")
@patch("threading.Thread")
@patch("redun.executors.pueue.iter_pueue_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_pueue_with_container(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    pueue_add_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """PueueExecutor should wrap commands in a container when configured."""
    pueue_add_mock.return_value = 55

    config = Config(
        {
            "pueue": {
                "scratch": ".redun_scratch",
                "container_type": "apptainer",
                "image": "/path/to/image.sif",
            }
        }
    )
    executor = PueueExecutor("pueue", config=config["pueue"])
    executor.set_scheduler(scheduler)

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash3"
    job.args = ((10,), {})
    executor.submit(job)

    # Verify the command passed to pueue_add contains apptainer.
    call_args = pueue_add_mock.call_args
    command_str = call_args[0][0]
    assert "apptainer" in command_str
    assert "exec" in command_str
    assert "/path/to/image.sif" in command_str
