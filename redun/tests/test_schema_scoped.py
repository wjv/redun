"""Tests for the same-database schema-scoped Postgres deployment.

See ``.claude/redun-schema-scoped-deployment.md`` for the design. The fork
exposes a ``[backend] db_schema`` config key that confines all of redun's
DDL/DML to one Postgres schema via a single-entry ``search_path``.
"""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse

import pytest
import sqlalchemy as sa

from redun.backends.db import RedunBackendDb, RedunDatabaseError
from redun.config import create_config_section


def _setup_pg(monkeypatch, pg) -> str:
    """Strip credentials from the testcontainers URL into env vars.

    Redun refuses URIs that embed a password and reads ``REDUN_DB_USERNAME``
    / ``REDUN_DB_PASSWORD`` instead.
    """
    raw = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://", 1)
    parts = urlparse(raw)
    monkeypatch.setenv("REDUN_DB_USERNAME", parts.username or "")
    monkeypatch.setenv("REDUN_DB_PASSWORD", parts.password or "")
    netloc = f"{parts.hostname}:{parts.port}" if parts.port else parts.hostname
    return urlunparse(parts._replace(netloc=netloc))


@pytest.mark.docker
def test_db_schema_routes_migrations_into_named_schema(pg_container, monkeypatch):
    """Tables and ``alembic_version`` land in the configured schema only."""
    uri = _setup_pg(monkeypatch, pg_container)
    bootstrap_url = pg_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
    bootstrap = sa.create_engine(bootstrap_url, future=True)
    with bootstrap.begin() as conn:
        conn.execute(sa.text("CREATE SCHEMA redun"))

    config = create_config_section(
        {"db_uri": uri, "db_schema": "redun", "automigrate": "True"}
    )
    backend = RedunBackendDb(config=config)
    backend.load()

    with bootstrap.connect() as conn:
        in_redun = set(
            r[0]
            for r in conn.execute(
                sa.text(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'redun'"
                )
            )
        )
        in_public = set(
            r[0]
            for r in conn.execute(
                sa.text(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                )
            )
        )

    assert "task" in in_redun
    assert "execution" in in_redun
    assert "alembic_version" in in_redun
    assert in_public == set(), f"public schema should be untouched, got {in_public}"


@pytest.mark.docker
def test_db_schema_assertion_fails_when_schema_missing(pg_container, monkeypatch):
    """If the schema doesn't exist, ``current_schema()`` falls back and we error."""
    uri = _setup_pg(monkeypatch, pg_container)

    config = create_config_section(
        {"db_uri": uri, "db_schema": "nonexistent", "automigrate": "False"}
    )
    backend = RedunBackendDb(config=config)
    with pytest.raises(RedunDatabaseError, match="current_schema"):
        backend.load()


@pytest.mark.unit
def test_db_schema_rejected_for_sqlite():
    """Configuring db_schema on a SQLite backend is a config error."""
    config = create_config_section(
        {"db_uri": "sqlite:///:memory:", "db_schema": "redun"}
    )
    with pytest.raises(RedunDatabaseError, match="only meaningful for Postgres"):
        RedunBackendDb(config=config)


@pytest.mark.unit
def test_db_schema_absent_is_unchanged():
    """When db_schema is unset, no search_path option is added."""
    config = create_config_section({"db_uri": "sqlite:///:memory:"})
    backend = RedunBackendDb(config=config)
    assert backend.db_schema is None
    assert "options" not in backend.connect_args
