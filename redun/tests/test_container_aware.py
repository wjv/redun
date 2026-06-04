"""Unit tests for the ``ContainerAware`` mixin."""

from configparser import ConfigParser

import pytest

from redun.executors.container_aware import ContainerAware


redun_namespace = "redun.tests.test_container_aware"


def _config(**values: str):
    """Build a ConfigParser SectionProxy from kwargs."""
    cfg = ConfigParser()
    cfg["executor"] = values
    return cfg["executor"]


def _instance(**defaults: str) -> ContainerAware:
    """Build a bare ContainerAware loaded with the given defaults."""
    inst = ContainerAware()
    inst._load_container_config(_config(**defaults) if defaults else None)
    return inst


# ---------------------------------------------------------------------------
# _resolve_container


@pytest.mark.unit
def test_resolve_container_uses_task_value() -> None:
    inst = _instance(default_container="default.sif")
    assert inst._resolve_container({"container": "task.sif"}) == "task.sif"


@pytest.mark.unit
def test_resolve_container_falls_back_to_executor_default() -> None:
    inst = _instance(default_container="default.sif")
    assert inst._resolve_container({}) == "default.sif"
    assert inst._resolve_container({"container": None}) == "default.sif"


@pytest.mark.unit
def test_resolve_container_unset_returns_none() -> None:
    inst = _instance()
    assert inst._resolve_container({}) is None


# ---------------------------------------------------------------------------
# _resolve_binds


@pytest.mark.unit
def test_resolve_binds_task_none_uses_default() -> None:
    inst = _instance(default_bind="/a, /b")
    assert inst._resolve_binds({}) == ["/a", "/b"]
    assert inst._resolve_binds({"binds": None}) == ["/a", "/b"]


@pytest.mark.unit
def test_resolve_binds_task_empty_list_overrides_default() -> None:
    inst = _instance(default_bind="/a, /b")
    assert inst._resolve_binds({"binds": []}) == []


@pytest.mark.unit
def test_resolve_binds_task_list_overrides_default() -> None:
    inst = _instance(default_bind="/a, /b")
    assert inst._resolve_binds({"binds": ["/x"]}) == ["/x"]


# ---------------------------------------------------------------------------
# _resolve_passthrough_env


@pytest.mark.unit
def test_resolve_passthrough_env_triad() -> None:
    inst = _instance(default_passthrough_env="PATH, HOME")
    assert inst._resolve_passthrough_env({}) == ["PATH", "HOME"]
    assert inst._resolve_passthrough_env({"passthrough_env": None}) == ["PATH", "HOME"]
    assert inst._resolve_passthrough_env({"passthrough_env": []}) == []
    assert inst._resolve_passthrough_env({"passthrough_env": ["PG"]}) == ["PG"]


# ---------------------------------------------------------------------------
# _wrap_command_for_container


@pytest.mark.unit
def test_wrap_command_no_container_returns_unchanged() -> None:
    inst = _instance()
    assert inst._wrap_command_for_container(["echo", "hi"], {}) == ["echo", "hi"]


@pytest.mark.unit
def test_wrap_command_produces_apptainer_exec() -> None:
    inst = _instance()
    result = inst._wrap_command_for_container(
        ["echo", "hi"], {"container": "img.sif"}
    )
    # Underlying ApptainerRunner uses --no-home by default.
    assert result == ["apptainer", "exec", "--no-home", "img.sif", "echo", "hi"]


@pytest.mark.unit
def test_wrap_command_includes_binds_sorted_lexically() -> None:
    inst = _instance()
    result = inst._wrap_command_for_container(
        ["cmd"],
        {"container": "img.sif", "binds": ["/zeta", "/alpha", "/middle:/m"]},
    )
    # Binds sorted lexically: /alpha < /middle:/m < /zeta.
    assert result == [
        "apptainer",
        "exec",
        "--no-home",
        "--bind",
        "/alpha:/alpha",
        "--bind",
        "/middle:/m",
        "--bind",
        "/zeta:/zeta",
        "img.sif",
        "cmd",
    ]


@pytest.mark.unit
def test_wrap_command_passthrough_env_uses_environ(monkeypatch) -> None:
    monkeypatch.setenv("FOO_VAR", "foo_value")
    monkeypatch.setenv("BAR_VAR", "bar_value")
    monkeypatch.delenv("MISSING_VAR", raising=False)
    inst = _instance()
    result = inst._wrap_command_for_container(
        ["cmd"],
        {
            "container": "img.sif",
            "passthrough_env": ["FOO_VAR", "MISSING_VAR", "BAR_VAR"],
        },
    )
    # Env vars sorted lexically by key, missing ones silently dropped.
    assert "--env" in result
    bar_i = result.index("BAR_VAR=bar_value")
    foo_i = result.index("FOO_VAR=foo_value")
    assert bar_i < foo_i
    assert "MISSING_VAR" not in " ".join(result)


@pytest.mark.unit
def test_wrap_command_is_deterministic_across_calls() -> None:
    inst = _instance()
    options = {"container": "img.sif", "binds": ["/b", "/a"]}
    first = inst._wrap_command_for_container(["x"], options)
    second = inst._wrap_command_for_container(["x"], options)
    assert first == second


# ---------------------------------------------------------------------------
# container_type — runtime selection (apptainer vs docker)


@pytest.mark.unit
def test_container_runtime_defaults_to_apptainer() -> None:
    """No ``container_type`` configured → Apptainer (preserves prior behaviour)."""
    from redun.executors.container import ApptainerRunner

    inst = _instance(default_container="img.sif")
    assert isinstance(inst._container_runtime, ApptainerRunner)


@pytest.mark.unit
def test_container_runtime_apptainer_explicit() -> None:
    """``container_type = apptainer`` → ``ApptainerRunner``, honours ``no_home``."""
    from redun.executors.container import ApptainerRunner

    inst = _instance(container_type="apptainer", no_home="false")
    assert isinstance(inst._container_runtime, ApptainerRunner)
    assert inst._container_runtime.no_home is False


@pytest.mark.unit
def test_container_runtime_docker_when_configured() -> None:
    """``container_type = docker`` → ``DockerRunner``; wrap produces ``docker run``."""
    from redun.executors.container import DockerRunner

    inst = _instance(container_type="docker", default_container="img:tag")
    assert isinstance(inst._container_runtime, DockerRunner)

    wrapped = inst._wrap_command_for_container(["echo", "hi"], {})
    assert wrapped[0] == "docker"
    assert "img:tag" in wrapped
    assert "echo" in wrapped and "hi" in wrapped


@pytest.mark.unit
def test_container_runtime_unknown_raises() -> None:
    """Unknown ``container_type`` value surfaces a clear error."""
    with pytest.raises(ValueError, match="Unknown container_type"):
        _instance(container_type="podman")
