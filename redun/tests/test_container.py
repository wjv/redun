from unittest.mock import patch

from redun.config import Config
from redun.executors.container import (
    ApptainerRunner,
    DockerRunner,
    get_container_runner,
)


class TestApptainerRunner:
    def test_basic_command(self) -> None:
        runner = ApptainerRunner()
        result = runner.wrap_command(
            ["echo", "hello"],
            image="/path/to/image.sif",
        )
        assert result == [
            "apptainer", "exec", "--no-home",
            "/path/to/image.sif",
            "echo", "hello",
        ]

    def test_no_home_disabled(self) -> None:
        runner = ApptainerRunner(no_home=False)
        result = runner.wrap_command(["ls"], image="image.sif")
        assert result == ["apptainer", "exec", "image.sif", "ls"]

    def test_volumes(self) -> None:
        runner = ApptainerRunner()
        result = runner.wrap_command(
            ["cmd"],
            image="image.sif",
            volumes=[("/host/data", "/container/data"), ("/tmp", "/tmp")],
        )
        assert "--bind" in result
        assert "/host/data:/container/data" in result
        assert "/tmp:/tmp" in result

    def test_environment_variables(self) -> None:
        runner = ApptainerRunner()
        result = runner.wrap_command(
            ["cmd"],
            image="image.sif",
            env={"FOO": "bar", "BAZ": "qux"},
        )
        assert "--env" in result
        assert "FOO=bar" in result
        assert "BAZ=qux" in result

    def test_nvidia_gpu(self) -> None:
        runner = ApptainerRunner(gpu_type="nvidia")
        result = runner.wrap_command(["cmd"], image="image.sif", gpus=1)
        assert "--nv" in result

    def test_rocm_gpu(self) -> None:
        runner = ApptainerRunner(gpu_type="rocm")
        result = runner.wrap_command(["cmd"], image="image.sif", gpus=2)
        assert "--rocm" in result

    def test_no_gpu_flag_when_zero(self) -> None:
        runner = ApptainerRunner()
        result = runner.wrap_command(["cmd"], image="image.sif", gpus=0)
        assert "--nv" not in result
        assert "--rocm" not in result

    def test_extra_args(self) -> None:
        runner = ApptainerRunner(extra_args=["--cleanenv", "--writable-tmpfs"])
        result = runner.wrap_command(["cmd"], image="image.sif")
        assert "--cleanenv" in result
        assert "--writable-tmpfs" in result
        # Extra args should appear before image.
        cleanenv_idx = result.index("--cleanenv")
        image_idx = result.index("image.sif")
        assert cleanenv_idx < image_idx


class TestDockerRunner:
    def test_basic_command(self) -> None:
        runner = DockerRunner()
        result = runner.wrap_command(["echo", "hello"], image="my-image:latest")
        assert result == [
            "docker", "run", "--rm",
            "my-image:latest",
            "echo", "hello",
        ]

    def test_volumes(self) -> None:
        runner = DockerRunner()
        result = runner.wrap_command(
            ["cmd"],
            image="img",
            volumes=[("/host", "/container")],
        )
        assert "-v" in result
        assert "/host:/container" in result

    def test_resource_limits(self) -> None:
        runner = DockerRunner()
        result = runner.wrap_command(
            ["cmd"],
            image="img",
            memory=8,
            vcpus=4,
            gpus=1,
        )
        assert "--memory=8g" in result
        assert "--cpus=4" in result
        assert "--gpus" in result

    def test_env_vars(self) -> None:
        runner = DockerRunner()
        result = runner.wrap_command(
            ["cmd"],
            image="img",
            env={"KEY": "val"},
        )
        assert "-e" in result
        assert "KEY=val" in result

    def test_strips_docker_prefix(self) -> None:
        """`docker://...` refs are accepted (Apptainer-style cross-runtime portability)."""
        runner = DockerRunner()
        result = runner.wrap_command(["cmd"], image="docker://my-image:tag")
        assert "docker://my-image:tag" not in result
        assert "my-image:tag" in result

    def test_passes_bare_ref_unchanged(self) -> None:
        """Bare refs without the prefix pass through verbatim."""
        runner = DockerRunner()
        result = runner.wrap_command(["cmd"], image="my-image:tag")
        assert "my-image:tag" in result


class TestGetContainerRunner:
    def test_no_container_type(self) -> None:
        config = Config({"exec": {}})
        assert get_container_runner(config["exec"]) is None

    def test_apptainer(self) -> None:
        config = Config({"exec": {"container_type": "apptainer"}})
        runner = get_container_runner(config["exec"])
        assert isinstance(runner, ApptainerRunner)

    def test_docker(self) -> None:
        config = Config({"exec": {"container_type": "docker"}})
        runner = get_container_runner(config["exec"])
        assert isinstance(runner, DockerRunner)

    def test_unknown_raises(self) -> None:
        config = Config({"exec": {"container_type": "podman"}})
        try:
            get_container_runner(config["exec"])
            assert False, "Should have raised ValueError"
        except ValueError as exc:
            assert "podman" in str(exc)

    def test_apptainer_options(self) -> None:
        config = Config({
            "exec": {
                "container_type": "apptainer",
                "no_home": "false",
                "gpu_type": "rocm",
                "extra_container_args": "--cleanenv --writable-tmpfs",
            }
        })
        runner = get_container_runner(config["exec"])
        assert isinstance(runner, ApptainerRunner)
        assert runner.no_home is False
        assert runner.gpu_type == "rocm"
        assert runner.extra_args == ["--cleanenv", "--writable-tmpfs"]
