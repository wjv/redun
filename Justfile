# Justfile — common dev recipes for the EVA redun fork.
#
# The full test suite lives under `redun/tests/`. Tests are layered by pytest
# marker rather than by directory; see `redun/tests/README.md` for the
# conventions.

# Run the fast tests (unit + graph). Targeted for tight feedback loops.
test:
    pytest -m "unit or graph" -v

# Run fast tests plus the smoke layer (still hermetic; in-memory SQLite).
test-smoke:
    pytest -m "unit or graph or smoke" -v

# Run everything, including Docker-dependent tests (testcontainers).
test-all:
    pytest -v

# Run the full historical suite, mirroring what tox runs.
test-full:
    pytest -v --ignore redun/experimental redun
