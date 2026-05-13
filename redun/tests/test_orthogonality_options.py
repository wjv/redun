"""Tests for the ``container``, ``binds``, and ``passthrough_env`` task options.

Covers validation at task-definition time and the cache-hash behaviour
(handover spec §4.3, §5.2, §6.4).
"""

import pytest

from redun import task


redun_namespace = "redun.tests.test_orthogonality_options"


# ---------------------------------------------------------------------------
# Type validation


@pytest.mark.unit
def test_container_accepts_str_or_none() -> None:
    @task(container="img.sif")
    def _t1() -> int:
        return 1

    @task(container=None)
    def _t2() -> int:
        return 2

    assert _t1.get_task_option("container") == "img.sif"
    assert _t2.get_task_option("container") is None


@pytest.mark.unit
def test_container_rejects_non_string() -> None:
    with pytest.raises(TypeError, match="`container` must be a str or None"):

        @task(container=42)  # type: ignore[arg-type]
        def _bad() -> int:
            return 1


@pytest.mark.unit
def test_binds_accepts_list_of_str_or_none() -> None:
    @task(binds=["/a", "/b:/mnt/b"])
    def _t() -> int:
        return 1

    assert _t.get_task_option("binds") == ["/a", "/b:/mnt/b"]


@pytest.mark.unit
def test_binds_rejects_non_list() -> None:
    with pytest.raises(TypeError, match="`binds` must be a list of str"):

        @task(binds="/a")  # type: ignore[arg-type]
        def _bad() -> int:
            return 1


@pytest.mark.unit
def test_binds_rejects_list_of_non_strings() -> None:
    with pytest.raises(TypeError, match="`binds` must be a list of str"):

        @task(binds=["/a", 7])  # type: ignore[list-item]
        def _bad() -> int:
            return 1


@pytest.mark.unit
def test_passthrough_env_rejects_non_list() -> None:
    with pytest.raises(TypeError, match="`passthrough_env` must be a list of str"):

        @task(passthrough_env="PATH")  # type: ignore[arg-type]
        def _bad() -> int:
            return 1


# ---------------------------------------------------------------------------
# Inline + container is forbidden


@pytest.mark.unit
def test_inline_with_container_is_rejected() -> None:
    with pytest.raises(ValueError, match="executor='inline'.*container"):

        @task(executor="inline", container="img.sif")
        def _bad() -> int:
            return 1


@pytest.mark.unit
def test_inline_without_container_is_fine() -> None:
    @task(executor="inline")
    def _t() -> int:
        return 1

    assert _t.get_task_option("executor") == "inline"


@pytest.mark.unit
def test_local_with_container_is_deferred() -> None:
    """`executor='local'` + container= is deferred in the EVA fork; raise."""
    with pytest.raises(NotImplementedError, match="executor='local'.*container"):

        @task(executor="local", container="img.sif")
        def _t() -> int:
            return 1


# ---------------------------------------------------------------------------
# Cache-hash behaviour (handover §4.3, §6.4)


# For hash tests, use `version=` to pin source hashing out of the picture
# and isolate the effect of container/binds/passthrough_env options.


@pytest.mark.unit
def test_container_change_changes_task_hash() -> None:
    @task(name="_t_c", version="1", container="alpha.sif")
    def _t_alpha() -> int:
        return 1

    @task(name="_t_c", version="1", container="beta.sif")
    def _t_beta() -> int:
        return 1

    assert _t_alpha.hash != _t_beta.hash


@pytest.mark.unit
def test_no_container_matches_unconfigured_task_hash() -> None:
    """A task with `container=None` hashes the same as one without the option."""

    @task(name="_t_baseline", version="1")
    def _without() -> int:
        return 1

    @task(name="_t_baseline", version="1", container=None)
    def _with_none() -> int:
        return 1

    assert _without.hash == _with_none.hash


@pytest.mark.unit
def test_binds_change_does_not_change_task_hash() -> None:
    @task(name="_t_binds", version="1", container="img.sif", binds=["/a"])
    def _t_a() -> int:
        return 1

    @task(name="_t_binds", version="1", container="img.sif", binds=["/b"])
    def _t_b() -> int:
        return 1

    assert _t_a.hash == _t_b.hash


@pytest.mark.unit
def test_passthrough_env_change_does_not_change_task_hash() -> None:
    @task(name="_t_env", version="1", container="img.sif", passthrough_env=["FOO"])
    def _t_foo() -> int:
        return 1

    @task(name="_t_env", version="1", container="img.sif", passthrough_env=["BAR"])
    def _t_bar() -> int:
        return 1

    assert _t_foo.hash == _t_bar.hash


@pytest.mark.unit
def test_override_binds_excluded_from_hash() -> None:
    """`.options(binds=...)` must not affect the hash either."""

    @task(name="_t_ovr", container="img.sif")
    def _t() -> int:
        return 1

    base = _t.hash
    assert _t.options(binds=["/x"]).hash == base
    assert _t.options(passthrough_env=["X"]).hash == base


@pytest.mark.unit
def test_override_container_changes_hash() -> None:
    """`.options(container=...)` must change the hash."""

    @task(name="_t_ovr_container", container="alpha.sif")
    def _t() -> int:
        return 1

    base = _t.hash
    assert _t.options(container="beta.sif").hash != base
