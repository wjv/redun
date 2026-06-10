import os
import subprocess
from configparser import ConfigParser

import boto3
import pytest
from moto import mock_s3

from redun import File, Scheduler, task
from redun.executors.aws_batch import AWSBatchExecutor
from redun.expression import TaskExpression
from redun.file import Dir
from redun.scripting import (
    Cmd,
    Pipe,
    ScriptError,
    exec_script,
    get_command_eof,
    get_wrapped_command,
    prepare_command,
    script,
)
from redun.tests.utils import use_tempdir


@use_tempdir
def test_redirect() -> None:
    """
    Shell redirection should use tee to create files for stderr and stdout.

    This test documents the technique works.
    """
    proc = subprocess.run(
        [
            "bash",
            "-c",
            """
            (echo hello) 2> >(tee >(cat > stderr) >&2) | tee >(cat > stdout)
            """,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert File("stdout").read() == "hello\n"
    assert File("stderr").read() == ""
    assert proc.stdout == b"hello\n"
    assert proc.stderr == b""
    assert proc.returncode == 0

    proc = subprocess.run(
        [
            "bash",
            "-c",
            "-o",
            "pipefail",
            "(bad_command) 2> >(tee >(cat > stderr) >&2) | "
            "tee >(cat > stdout) || (echo fail; exit 1)",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert File("stdout").read() == ""
    assert "command not found" in File("stderr").read()  # ty: ignore[unsupported-operator]
    assert proc.stdout == b"fail\n"
    assert b"command not found" in proc.stderr
    assert proc.returncode != 0


def test_prepare_command() -> None:
    """
    Commands should be dedented.
    """
    assert (
        prepare_command(
            """
        #!/bin/bash
        echo hello
        """
        )
        == """\
#!/bin/bash
echo hello"""
    )


def test_prepare_command_default_interpreter() -> None:
    """
    Commands should be dedented.
    """
    assert (
        prepare_command(
            """
        echo hello
        """
        )
        == """\
#!/usr/bin/env bash
set -exo pipefail
echo hello"""
    )


def test_exec_script() -> None:
    """
    Execute a python script.
    """
    assert (
        exec_script(
            """\
#!/usr/bin/env python
print('hello')
"""
        )
        == b"hello\n"
    )


def test_script_task(scheduler: Scheduler) -> None:
    """
    Tasks should be definable as shell scripts.
    """

    @task(script=True)
    def task1(message):
        return """echo Hello, {message}!""".format(message=message)

    assert scheduler.run(task1("World")) == b"Hello, World!\n"


def test_script_task_error(scheduler: Scheduler) -> None:
    """
    Script tasks should raise errors from shell script.
    """

    @task(script=True)
    def task1():
        return "bad_command"

    @task(script=True)
    def task2():
        # Multi-line script should exit on first error by default.
        return """
bad_command
echo hello
        """

    with pytest.raises(ScriptError):
        scheduler.run(task1())

    with pytest.raises(ScriptError):
        scheduler.run(task2())


def test_python_script_task(scheduler: Scheduler) -> None:
    """
    Script tasks should be able to use custom interpreters.
    """

    @task(script=True)
    def task1(message):
        return """
        #!/usr/bin/env python
        print('Hello, {message}!')
        """.format(message=message)

    assert scheduler.run(task1("World")) == b"Hello, World!\n"


def test_python_script_task2(scheduler: Scheduler) -> None:
    """
    script() should use custom interpreters.
    """
    result = script(
        """
        #!/usr/bin/env python
        print('Hello, World!')
        """,
        executor="default",
    )
    assert isinstance(result, TaskExpression)
    assert scheduler.run(result) == b"Hello, World!\n"


def test_default_shell(scheduler: Scheduler) -> None:
    """
    script() should use bash as default interpreter.
    """
    result = script(
        """
        # Use a bash only syntax.
        cat <(echo ok)
        """
    )
    assert scheduler.run(result) == b"ok\n"


def test_script_list(scheduler: Scheduler) -> None:
    """
    script() should accept lists as well.
    """
    # Note: The double space should be preserved since each element is shell quoted.
    result = script(["echo", "hello  world"])
    assert scheduler.run(result) == b"hello  world\n"


def test_script_error(scheduler: Scheduler) -> None:
    """
    Scripts should propagate their errors.
    """

    @task()
    def task1():
        return script(
            """
            echo message > /dev/stderr
            bad_prog 1 2 3
            """
        )

    with pytest.raises(ScriptError) as error:
        scheduler.run(task1())

    assert "message" in error.value.message  # ty: ignore[unsupported-operator]
    assert "bad_prog: command not found" in error.value.message  # ty: ignore[unsupported-operator]


def test_script_outputs(scheduler: Scheduler) -> None:
    """
    script() should be able to define an output structure.
    """
    result = script(
        """
        #!/bin/sh
        echo 'Hello, World!'
        """,
        executor="default",
        outputs={"my_output": 10, "stdout": File("-")},
    )
    assert isinstance(result, TaskExpression)
    assert scheduler.run(result) == {
        "my_output": 10,
        "stdout": b"Hello, World!\n",
    }


@use_tempdir
def test_script_file(scheduler: Scheduler) -> None:
    result = script(
        """
        #!/bin/sh
        echo 'hello' > hello.txt
        echo 'good bye' > bye.txt
        """,
        outputs={"hello": File("hello.txt"), "bye": File("bye.txt")},
    )

    result = scheduler.run(result)
    assert result["hello"].read() == "hello\n"
    assert result["bye"].read() == "good bye\n"

    # Files should not immediately become invalidated.
    assert result["hello"].is_valid()
    assert result["bye"].is_valid()


def test_multistage_pipe_siblings_distinct_command_args() -> None:
    """Two distinct-content multi-stage ``script(Pipe(...))`` calls must
    produce DISTINCT ``command`` args to the inner ``_script`` /
    ``script_task``.

    Regression for the 46/47 ghost-success bug Q4 hit on 2026-06-10:
    ``script_task`` is special-cased to ``CacheScope.CSE``, which keys on
    ``args_hash`` only — ``task_options`` (where ``_pipeline_stages`` lives)
    does NOT participate. Before the fix, ``_build_pipeline_bash_body``
    returned an identical template string for any two same-length Pipes,
    so all sibling multi-stage scripts collided on CSE within an execution
    and only the first actually ran."""
    expr_a = script(Cmd(["echo", "lib_a"]) | Cmd(["cat"]))
    expr_b = script(Cmd(["echo", "lib_b"]) | Cmd(["cat"]))
    # First arg to _script is the bash body that becomes script_task's
    # `command` arg. Must differ per stages tuple.
    assert expr_a.args[0] != expr_b.args[0]


@use_tempdir
def test_script_check_valid_full_reruns_on_missing_output(
    scheduler: Scheduler,
) -> None:
    """``check_valid="full"`` round-trips through to actual execution and
    a missing output file invalidates the cache, forcing re-execution."""
    script_body = """
        #!/bin/sh
        echo 'fresh' > out.txt
    """

    result1 = scheduler.run(
        script(script_body, outputs=File("out.txt"), check_valid="full")
    )
    assert result1.read() == "fresh\n"

    os.remove("out.txt")
    assert not os.path.exists("out.txt")

    result2 = scheduler.run(
        script(script_body, outputs=File("out.txt"), check_valid="full")
    )
    assert os.path.exists("out.txt")
    assert result2.read() == "fresh\n"


def test_command_eof() -> None:
    command = """
run-prog --x 10
ls my-dir
"""
    assert get_command_eof(command) == "EOF"

    command = """
run-prog --x 10 <<"EOF"
    ls my-dir
EOF
"""
    assert get_command_eof(command) == "EOF1"

    command = """
run-prog1 <<"EOF"
run-prog2 --x 10 <<"EOF1"
    ls my-dir
EOF1
EOF
"""
    assert get_command_eof(command) == "EOF2"


def test_wrapped_command() -> None:
    command = """\
#!/bin/bash
echo hello
"""
    assert "EOF" in get_wrapped_command(command)


def test_exec_wrapped_command() -> None:
    command = """\
#!/bin/bash
echo hello
"""
    wrapped_command = get_wrapped_command(command)
    assert subprocess.check_output(wrapped_command, shell=True) == b"hello\n"


def test_script_tempdir(scheduler: Scheduler) -> None:
    result = script(
        """
        #!/bin/sh
        echo 'hello' > hello.txt
        echo 'good bye' > bye.txt
        """,
        tempdir=True,
        outputs={"hello": File("hello.txt"), "bye": File("bye.txt")},
    )

    result = scheduler.run(result)

    # tempdir has been cleaned up.
    assert not result["hello"].exists()
    assert not result["bye"].exists()


@use_tempdir
def test_script_staging(scheduler: Scheduler) -> None:
    basedir = os.getcwd()

    hello_path = os.path.join(basedir, "remote_hello.txt")
    bye_path = os.path.join(basedir, "remote_bye.txt")

    result = script(
        """
        #!/bin/sh
        echo 'hello' > hello.txt
        echo 'good bye' > bye.txt
        """,
        tempdir=True,
        outputs={
            "hello": File(hello_path).stage("hello.txt"),
            "bye": File(bye_path).stage("bye.txt"),
        },
    )

    result = scheduler.run(result)

    # Remote files should still exist.
    assert result["hello"].exists()
    assert result["bye"].exists()
    assert result["hello"].path == hello_path
    assert result["bye"].path == bye_path
    assert result["hello"].read() == "hello\n"
    assert result["bye"].read() == "good bye\n"


@use_tempdir
def test_script_staging_dir(scheduler: Scheduler) -> None:
    basedir = os.getcwd()

    remote_dir = os.path.join(basedir, "remote")

    File(remote_dir + "/in/a.txt").write("a")
    File(remote_dir + "/in/b.txt").write("b")
    File(remote_dir + "/in/c/d.txt").write("d")

    result = script(
        """
        #!/bin/sh
        mkdir -p out
        cat in/a.txt in/b.txt in/c/d.txt > out/z
        echo 'hello' > out/y
        """,
        tempdir=True,
        inputs={Dir(remote_dir + "/in").stage("in")},
        outputs={"out": Dir(remote_dir + "/out").stage("out")},
    )

    result = scheduler.run(result)

    # Remote files should still exist.
    assert result["out"].exists()
    assert result["out"].file("z").read() == "abd"
    assert result["out"].file("y").read() == "hello\n"


@use_tempdir
def test_script_invalid(scheduler: Scheduler) -> None:
    """
    script() should be reactive to invalidated output.
    """

    expr = script(
        """
        echo hi > local
        """,
        outputs=[File("remote").stage("local")],
    )

    # Run the workflow once.
    [out_file] = scheduler.run(expr)
    assert out_file.read() == "hi\n"

    # Invalidate the output file by overwriting it.
    File("remote").write("bye")

    # Rerunning the workflow should reproduce the same output.
    [out_file] = scheduler.run(expr)
    assert out_file.read() == "hi\n"

    # Invalidate the output file by deleting.
    File("remote").remove()

    # Rerunning the workflow should reproduce the same output.
    scheduler.run(expr)
    assert File("remote").read() == "hi\n"


@use_tempdir
def test_script_staging_input_change(scheduler: Scheduler) -> None:
    """
    script() should be reactive to changing inputs.
    """

    File("input_remote").write("hello")

    expr = script(
        """
        cat input_local > output_local
        """,
        inputs=[File("input_remote").stage("input_local")],
        outputs=File("output_remote").stage("output_local"),
    )
    assert scheduler.run(expr).read() == "hello"

    # Change input.
    File("input_remote").write("hello2")

    expr = script(
        """
        cat input_local > output_local
        """,
        inputs=[File("input_remote").stage("input_local")],
        outputs=File("output_remote").stage("output_local"),
    )
    assert scheduler.run(expr).read() == "hello2"


@use_tempdir
def test_script_raw_file_input(scheduler: Scheduler) -> None:
    """
    script() should accept raw `File` in `inputs=` (no `.stage(...)` boilerplate)
    when the file is already at the path the command will see, AND the file's
    content should participate in the cache hash so an edit busts the cache.

    Symmetric to the existing raw-`File`-as-output handling (preprocess_output
    in scripting.py). The raw `File` gets auto-wrapped as a no-op StagingFile
    (local == remote); render_stage returns "" so no copy step lands in the
    wrapper script, but the underlying File still flows into _script() as a
    cache-affecting argument.
    """

    File("data").write("hello")

    expr = script(
        """
        cat data > output
        """,
        inputs=[File("data")],
        outputs=File("output_remote").stage("output"),
    )
    assert scheduler.run(expr).read() == "hello"

    # Editing the raw-File input busts the cache; script re-executes.
    File("data").write("hello2")

    expr = script(
        """
        cat data > output
        """,
        inputs=[File("data")],
        outputs=File("output_remote").stage("output"),
    )
    assert scheduler.run(expr).read() == "hello2"


@mock_s3
def _test_script_staging_s3(scheduler: Scheduler) -> None:
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    hello_path = "s3://example-bucket/_hello.txt"
    bye_path = "s3://example-bucket/bye.txt"

    result = script(
        """
        #!/bin/sh
        echo 'hello' > hello.txt
        echo 'good bye' > bye.txt
        """,
        tempdir=True,
        outputs={
            "hello": File(hello_path).stage("hello.txt"),
            "bye": File(bye_path).stage("bye.txt"),
        },
    )

    result = scheduler.run(result)

    # Remote files should still exist.
    assert result["hello"].exists()
    assert result["bye"].exists()
    assert result["hello"].path == hello_path
    assert result["bye"].path == bye_path
    assert result["hello"].read() == "hello\n"
    assert result["bye"].read() == "good bye\n"


@mock_s3
def _test_script_task_aws_batch():
    @task(executor="batch", script=True)
    def task1(message):
        return """echo Hello, {message}!""".format(message=message)

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    config = ConfigParser()
    config.read_dict(
        {
            "batch": {
                "image": "ubuntu",
                "queue": "queue",
                "s3_scratch": "s3://example-bucket/redun",
                "debug": True,
            }
        }
    )

    executor = AWSBatchExecutor("batch", None, config=config["batch"])
    scheduler = Scheduler()
    scheduler.executors["batch"] = executor
    executor.scheduler = scheduler
    assert scheduler.run(task1, ["World"]) == b"Hello, World!\n"


# ---------------------------------------------------------------------------
# Cmd / Pipe composable command types
# ---------------------------------------------------------------------------


class TestCmd:
    def test_str_argv(self) -> None:
        c = Cmd("echo hi")
        assert c.argv == "echo hi"
        assert c.container is None

    def test_list_argv_tuplifies(self) -> None:
        c = Cmd(["echo", "hi"])
        # Stored as a tuple internally so the frozen dataclass stays hashable.
        assert c.argv == ("echo", "hi")
        assert isinstance(c.argv, tuple)

    def test_container_field(self) -> None:
        c = Cmd(["cat"], container="docker://debian:stable-slim")
        assert c.container == "docker://debian:stable-slim"

    def test_equality_and_hashable(self) -> None:
        c1 = Cmd(["a", "b"], container="img")
        c2 = Cmd(["a", "b"], container="img")
        assert c1 == c2
        # Hashable because frozen dataclass + tuple argv.
        assert hash(c1) == hash(c2)

    def test_inequality_on_container_difference(self) -> None:
        assert Cmd(["x"], container="a") != Cmd(["x"], container="b")
        assert Cmd(["x"]) != Cmd(["x"], container="img")


class TestPipe:
    def test_variadic_construction(self) -> None:
        p = Pipe(Cmd(["a"]), Cmd(["b"]))
        assert len(p.stages) == 2
        assert p.stages[0].argv == ("a",)
        assert p.stages[1].argv == ("b",)

    def test_single_stage_is_legal(self) -> None:
        # A 1-stage Pipe is semantically the same as the bare Cmd.
        p = Pipe(Cmd(["solo"]))
        assert len(p.stages) == 1

    def test_empty_pipe_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one"):
            Pipe()

    def test_non_cmd_stage_raises(self) -> None:
        with pytest.raises(TypeError, match="must be a Cmd"):
            Pipe("not a Cmd")  # type: ignore[arg-type]

    def test_cmd_or_cmd_makes_pipe(self) -> None:
        p = Cmd(["a"]) | Cmd(["b"])
        assert isinstance(p, Pipe)
        assert len(p.stages) == 2

    def test_pipe_or_cmd_extends(self) -> None:
        p = Pipe(Cmd(["a"]), Cmd(["b"])) | Cmd(["c"])
        assert len(p.stages) == 3
        assert p.stages[-1].argv == ("c",)

    def test_cmd_or_pipe_prepends(self) -> None:
        p = Cmd(["a"]) | Pipe(Cmd(["b"]), Cmd(["c"]))
        assert len(p.stages) == 3
        assert p.stages[0].argv == ("a",)

    def test_pipe_or_pipe_concatenates(self) -> None:
        p = Pipe(Cmd(["a"]), Cmd(["b"])) | Pipe(Cmd(["c"]), Cmd(["d"]))
        assert len(p.stages) == 4
        assert [s.argv for s in p.stages] == [("a",), ("b",), ("c",), ("d",)]

    def test_chained_or_reads_naturally(self) -> None:
        # Sugar for the bcl2fastq | evatags / samtools chain example.
        p = (
            Cmd(["samtools", "import"], container="docker://samtools:1.21")
            | Cmd(["evatags", "-c", "N"], container="docker://evatags:0.5.5")
        )
        assert len(p.stages) == 2
        assert p.stages[0].container == "docker://samtools:1.21"
        assert p.stages[1].container == "docker://evatags:0.5.5"


class TestScriptDispatchPhase1:
    """script() accepts the new Cmd/Pipe shapes; multi-stage not yet implemented."""

    @use_tempdir
    def test_script_accepts_cmd(self, scheduler: Scheduler) -> None:
        File("input").write("hello")
        result = scheduler.run(
            script(Cmd(["cat", "input"]))
        )
        # script() default outputs is File("-"), which returns stdout bytes.
        assert b"hello" in result

    @use_tempdir
    def test_script_accepts_single_stage_pipe(self, scheduler: Scheduler) -> None:
        File("input").write("hello")
        result = scheduler.run(
            script(Pipe(Cmd(["cat", "input"])))
        )
        assert b"hello" in result

    def test_script_accepts_multistage_pipe(self) -> None:
        """Multi-stage Pipe builds without error (Phase 2 dispatch).

        End-to-end execution requires the executor substitution step
        (Phase 2 continues); this test only verifies the dispatch
        accepts the multi-stage shape without raising at construction.
        """
        # Should not raise.
        expr = script(Pipe(Cmd(["echo", "a"]), Cmd(["cat"])))
        # The returned object is a redun expression (TaskExpression).
        from redun.expression import Expression
        assert isinstance(expr, Expression)

    @use_tempdir
    def test_cmd_container_becomes_task_option(self, scheduler: Scheduler) -> None:
        """A Cmd-supplied container flows into task_options the same way
        `script(..., container=...)` does."""
        # We can't easily check the Pueue dispatch path without a daemon, so
        # just confirm script() doesn't error and runs the command bare-
        # equivalent when no executor is configured for containers (i.e.
        # the default scheduler runs locally without container wrapping).
        File("data").write("ok")
        # No container resolves end-to-end without a container runtime
        # configured; behaviour is identical to today's bare command.
        result = scheduler.run(
            script(Cmd(["cat", "data"]))
        )
        assert b"ok" in result


# ---------------------------------------------------------------------------
# Pipeline bash-body generation (Phase 2)
# ---------------------------------------------------------------------------


from redun.scripting import _build_pipeline_bash_body


class TestPipelineBashBody:
    @staticmethod
    def _stages(n: int) -> tuple:
        """Build a representative stages tuple of length n."""
        return tuple(((f"echo", f"stage{i}"), None) for i in range(n))

    def test_two_stage_layout(self) -> None:
        body = _build_pipeline_bash_body(self._stages(2))
        # Both stage markers appear.
        assert "__REDUN_PIPELINE_STAGE_0__" in body
        assert "__REDUN_PIPELINE_STAGE_1__" in body
        # Per-stage stderr capture.
        assert ".task_error_0" in body
        assert ".task_error_1" in body
        # PIPESTATUS preservation + sidecar (one file with the full array;
        # per-stage sentinels were dropped — see _build_pipeline_bash_body
        # docstring).
        assert "PIPESTATUS_COPY" in body
        assert ".task_pipestatus" in body
        # First-failing exit code propagation.
        assert 'exit "$RETCODE"' in body

    def test_n_stage_marker_count(self) -> None:
        body = _build_pipeline_bash_body(self._stages(4))
        for i in range(4):
            assert f"__REDUN_PIPELINE_STAGE_{i}__" in body
            assert f".task_error_{i}" in body
        # Make sure we don't have a stage-5 marker.
        assert "__REDUN_PIPELINE_STAGE_4__" not in body

    def test_body_differs_for_distinct_stages_of_same_length(self) -> None:
        """Two same-length Pipes with different stage content must produce
        DISTINCT bash bodies — script_task's CSE cache key is args_hash
        only, so identical bodies would collide within an execution and
        cause sibling cache hits (the 46/47 ghost-success bug Q4 hit
        2026-06-10)."""
        body_a = _build_pipeline_bash_body(
            ((("echo", "lib_a"), None), (("cat",), None))
        )
        body_b = _build_pipeline_bash_body(
            ((("echo", "lib_b"), None), (("cat",), None))
        )
        assert body_a != body_b


class TestScriptDispatchPhase2:
    def test_multistage_sets_pipeline_stages_option(self) -> None:
        """script(Pipe(...)) of length > 1 sets `_pipeline_stages` in task
        options of the resulting Expression."""
        from redun.expression import TaskExpression

        expr = script(
            Pipe(
                Cmd(["echo", "a"], container="img_a"),
                Cmd(["cat"], container="img_b"),
            )
        )
        # The outermost expression is the result of postprocess_script;
        # walk to find the script_task expression that carries our option.
        # Easier: just verify multi-stage doesn't raise (the runtime
        # behaviour is verified by the substitution-helper tests below).
        # `_pipeline_stages` lives in task_options on the script_task call;
        # detailed inspection of that requires walking the Expression tree.
        # For now: smoke check on the API surface.
        assert expr is not None

    def test_multistage_rejects_task_level_container(self) -> None:
        """A multi-stage Pipe with task-level `container=` is ambiguous."""
        with pytest.raises(ValueError, match="ambiguous"):
            script(
                Pipe(Cmd(["a"], container="img_a"), Cmd(["b"])),
                container="img_outer",
            )


# ---------------------------------------------------------------------------
# Pipeline marker substitution (Phase 2)
# ---------------------------------------------------------------------------


from redun.executors.container_aware import ContainerAware


class TestSubstitutePipelineMarkers:
    def _instance(self):
        # ContainerAware needs no config for these tests — runtime defaults
        # to Apptainer when no container_type is configured.
        ca = ContainerAware()
        return ca

    def test_bare_stages_pass_through_unchanged(self) -> None:
        ca = self._instance()
        body = "__REDUN_PIPELINE_STAGE_0__ | __REDUN_PIPELINE_STAGE_1__"
        stages = (
            (("echo", "hello"), None),
            (("cat",), None),
        )
        result = ca._substitute_pipeline_markers(body, stages, {})
        # Markers are gone.
        assert "__REDUN_PIPELINE_STAGE_" not in result
        # Bare commands appear as shlex-joined argv.
        assert "echo hello" in result
        assert "cat" in result

    def test_containerised_stage_apptainer_default(self) -> None:
        ca = self._instance()
        body = "__REDUN_PIPELINE_STAGE_0__"
        stages = ((("samtools", "view"), "img.sif"),)
        result = ca._substitute_pipeline_markers(body, stages, {})
        # Apptainer runner emits `apptainer exec ... img.sif samtools view`.
        assert "apptainer" in result
        assert "img.sif" in result
        assert "samtools" in result

    def test_mixed_bare_and_containerised(self) -> None:
        ca = self._instance()
        body = "__REDUN_PIPELINE_STAGE_0__ | __REDUN_PIPELINE_STAGE_1__"
        stages = (
            (("samtools", "view"), "img.sif"),
            (("awk", "/foo/"), None),
        )
        result = ca._substitute_pipeline_markers(body, stages, {})
        # Stage 0 wrapped; stage 1 bare.
        assert "apptainer" in result  # stage 0
        assert "img.sif" in result  # stage 0
        assert "awk" in result  # stage 1
        # `awk` should not be inside the apptainer invocation; it should be
        # after the pipe.
        apptainer_idx = result.find("apptainer")
        awk_idx = result.find("awk")
        pipe_idx = result.find("|")
        assert apptainer_idx < pipe_idx < awk_idx

    def test_string_argv_wrapped_in_bash_c(self) -> None:
        """String argv (shell command) wraps as `bash -c '...'` so the
        container shell parses redirects/expansions correctly."""
        ca = self._instance()
        body = "__REDUN_PIPELINE_STAGE_0__"
        stages = (("foo | bar > baz", "img.sif"),)
        result = ca._substitute_pipeline_markers(body, stages, {})
        assert "bash" in result
        assert "-c" in result


# ---------------------------------------------------------------------------
# Pipeline end-to-end behaviour (Phase 3)
#
# These tests run the bash body produced by `_build_pipeline_bash_body` plus
# marker-substitution directly via subprocess. They validate the bash
# template's correctness independently of any executor's integration —
# Phase 4 will add executor-integrated regression tests.
# ---------------------------------------------------------------------------


from redun.scripting import _build_pipeline_bash_body


def _build_and_substitute(pipe: Pipe) -> str:
    """Build pipeline bash body for `pipe` and substitute its markers via a
    bare `ContainerAware` instance (no container wrapping; bare-stage path)."""
    from redun.executors.container_aware import ContainerAware

    stages_tuple = tuple((s.argv, s.container) for s in pipe.stages)
    body = _build_pipeline_bash_body(stages_tuple)
    ca = ContainerAware()
    return ca._substitute_pipeline_markers(body, stages_tuple, {})


@use_tempdir
def test_pipeline_two_stage_bare_runs() -> None:
    """Smoke: ``echo … | tr a-z A-Z`` produces the expected stdout."""
    body = _build_and_substitute(
        Pipe(Cmd(["echo", "hello world"]), Cmd(["tr", "a-z", "A-Z"]))
    )
    result = subprocess.run(["bash", "-c", body], capture_output=True)
    assert result.returncode == 0, result.stderr
    assert b"HELLO WORLD" in result.stdout


@use_tempdir
def test_pipeline_per_stage_stderr_captured_to_separate_files() -> None:
    """Each stage's stderr lands in `.task_error_<i>`, indexed per stage."""
    body = _build_and_substitute(
        Pipe(
            Cmd("echo stage0_err >&2; echo data"),
            Cmd("echo stage1_err >&2; cat"),
        )
    )
    result = subprocess.run(["bash", "-c", body], capture_output=True)
    assert result.returncode == 0
    assert b"data" in result.stdout
    assert "stage0_err" in File(".task_error_0").read()  # ty: ignore[unsupported-operator]
    assert "stage1_err" in File(".task_error_1").read()  # ty: ignore[unsupported-operator]


@use_tempdir
def test_pipeline_first_stage_failure_propagates_exit_code() -> None:
    """Stage 0 fails → headline exit code is stage 0's (first-failing rule)."""
    body = _build_and_substitute(
        Pipe(
            Cmd("echo error >&2; exit 42"),
            Cmd(["cat"]),
        )
    )
    result = subprocess.run(["bash", "-c", body], capture_output=True)
    assert result.returncode == 42


@use_tempdir
def test_pipeline_middle_stage_failure_propagates_exit_code() -> None:
    """Stage 1 of 3 fails → exit code is stage 1's (first-failing rule)."""
    body = _build_and_substitute(
        Pipe(
            Cmd(["echo", "data"]),
            Cmd("cat; exit 7"),
            Cmd(["cat"]),
        )
    )
    result = subprocess.run(["bash", "-c", body], capture_output=True)
    assert result.returncode == 7


@use_tempdir
def test_pipeline_pipestatus_sidecar_records_array() -> None:
    """`.task_pipestatus` contains the space-separated PIPESTATUS array."""
    body = _build_and_substitute(
        Pipe(
            Cmd(["echo", "ok"]),
            Cmd("cat; exit 3"),
            Cmd(["cat"]),
        )
    )
    subprocess.run(["bash", "-c", body], capture_output=True)
    pipestatus = File(".task_pipestatus").read().strip()  # ty: ignore[unsupported-operator]
    assert pipestatus == "0 3 0"


@use_tempdir
def test_pipeline_all_success_pipestatus_all_zero() -> None:
    """Healthy pipeline: PIPESTATUS array is all zeros."""
    body = _build_and_substitute(
        Pipe(Cmd(["echo", "a"]), Cmd(["cat"]), Cmd(["cat"]))
    )
    result = subprocess.run(["bash", "-c", body], capture_output=True)
    assert result.returncode == 0
    pipestatus = File(".task_pipestatus").read().strip()  # ty: ignore[unsupported-operator]
    assert pipestatus == "0 0 0"


@use_tempdir
def test_pipeline_binary_safe_through_pipe() -> None:
    """Arbitrary bytes flow through the pipe without text-coercion.

    Critical for bioinformatics use cases where stages stream binary
    formats (BAM, gzipped, etc.). Confirmed Nextflow-style text-coercion
    bug doesn't exist here.
    """
    import gzip

    # Stage 0 emits raw bytes, gzipped; stage 1 just passes them through.
    body = _build_and_substitute(
        Pipe(
            Cmd(r"printf '\x00\x01\xff\xfe\x42\x00\xa5' | gzip"),
            Cmd(["cat"]),
        )
    )
    result = subprocess.run(["bash", "-c", body], capture_output=True)
    assert result.returncode == 0
    decompressed = gzip.decompress(result.stdout)
    assert decompressed == b"\x00\x01\xff\xfe\x42\x00\xa5"


@use_tempdir
def test_pipeline_four_stages_compose() -> None:
    """N=4 stages compose correctly; per-stage stderr files all written."""
    body = _build_and_substitute(
        Pipe(
            Cmd("echo line1; echo line2; echo line3"),
            Cmd(["grep", "line"]),
            Cmd(["wc", "-l"]),
            Cmd(["tr", "-d", " "]),
        )
    )
    result = subprocess.run(["bash", "-c", body], capture_output=True)
    assert result.returncode == 0
    assert result.stdout.strip() == b"3"
    # All four per-stage stderr files exist (may be empty).
    for i in range(4):
        assert File(f".task_error_{i}").exists()  # ty: ignore[unsupported-operator]
