# Tests for the EVA redun fork

This directory holds both upstream-derived tests and tests specific to the
fork. Tests are layered by **pytest marker**, not by directory: the layout
stays flat and `pytest -m "<marker>"` picks the right slice.

## The three layers (plus one)

| Marker  | What it tests                                                      | Speed       | When it runs       |
|---------|--------------------------------------------------------------------|-------------|--------------------|
| `unit`  | Pure-Python code: helpers, task-function bodies via `task.func`.   | sub-second  | Every commit       |
| `graph` | Redun expression-tree shape, built without invoking the scheduler. | sub-second  | Every commit       |
| `smoke` | Real scheduler invocation against tiny fixtures + SQLite memory.   | seconds     | CI / local         |
| `docker`| Tests requiring a Docker daemon (e.g. testcontainers Postgres).    | tens of sec | Opt-in / when avail|

A fifth conceptual layer — regression-level tests against larger fixtures
— is acknowledged but explicitly out of scope until the rebuilt pipeline
has real tasks to test.

## When to add a test at which layer

- **A pure-Python helper or a `@task` body that does something interesting
  with its arguments.** → `unit`. Call `task.func(args)` directly, no
  scheduler needed.
- **A workflow with structure: branching, fan-out, conditional task
  invocation.** → `graph`. Build the expression with `make_workflow(...)`,
  assert via `helpers.graph_assertions.count_calls` /
  `all_call_args` / `assert_structure`. No tasks run.
- **End-to-end correctness: does the scheduler run this workflow with the
  expected result, and does caching work?** → `smoke`. Use the
  `redun_scheduler` fixture (in-memory SQLite, hermetic).
- **Anything genuinely needing Postgres semantics, real container
  runtimes, etc.** → `smoke` *and* `docker`. Skipped where Docker is
  unavailable.

## Running tests

From the project root (where `Justfile` lives):

```sh
just test         # unit + graph        (fast)
just test-smoke   # + smoke             (still hermetic)
just test-all     # + docker            (needs Docker)
just test-full    # the historical suite tox runs
```

Or directly:

```sh
pytest -m "unit or graph" -v
pytest -m smoke -v
pytest redun/tests/test_task.py::test_task_hash
```

## Fixtures

Defined in `redun/tests/conftest.py`:

- **`redun_scheduler`** — fresh `Scheduler` with `sqlite:///:memory:`
  backend per test. Use when you need real scheduler behaviour but no
  persistent state.
- **`scheduler`** — pre-existing fixture; can be Postgres-backed when
  `REDUN_TEST_POSTGRES` is set. Prefer `redun_scheduler` for new
  hermetic tests.
- **`tmp_workspace`** — per-test temp directory (alias of pytest's
  built-in `tmp_path`).
- **`pg_container`** — `testcontainers.postgres.PostgresContainer`,
  skipped if `testcontainers` or Docker is unavailable. Use **only**
  alongside `@pytest.mark.docker`.

For the testcontainers escape-hatch pattern, the working reference is
the `pg-http-proxy` project — same pattern (lazy import, skip on
missing Docker, container-per-test).

## Helpers

`redun/tests/helpers/graph_assertions.py` exposes:

- `count_calls(expr, task_name)` — number of `task_name` calls in the tree.
- `all_call_args(expr, task_name)` — `(args, kwargs)` for every such call.
- `assert_structure(expr, expected)` — nested-tuple shape check
  (`("workflow", ("fastq2bam", "trim"))`).

Task names are matched by full or bare name (`"redun.foo.bar"` or
`"bar"`).

## Adding a fixture

1. Put it in `redun/tests/conftest.py` if it's broadly useful, or in a
   sibling `conftest.py` if it's scoped to a particular subtree.
2. For workflow fixtures that need to import as modules, put them under
   `redun/tests/fixtures/` and add a fixture in `conftest.py` that
   imports the module.
3. Document the fixture in the table above so future authors find it.

## Notes on conventions

- This fork keeps tests flat under `redun/tests/` (matching upstream)
  rather than a top-level `tests/` directory. The handover spec
  originally drafted the top-level form; we use markers instead so the
  upstreaming path stays clean.
- The `@pytest.mark.docker` skip path must always be graceful — a
  developer without Docker should be able to run `just test-all` and
  see Docker tests skipped, not error.
