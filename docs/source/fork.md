# Using the EVA fork

This page documents the user-facing additions in this fork of [redun](https://github.com/insitro/redun). It assumes baseline familiarity with redun (tasks, lazy expressions, the cache); see the upstream docs for that.

The fork's headline change: **`container=` is now a task option, orthogonal to `executor=`**. You compose them freely.

If you are using an AI coding agent to author a workflow against this fork, read this whole page once before starting. It is intentionally terse and example-heavy.

---

## The mental model

Two orthogonal axes:

| Axis | Meaning | Values |
|------|---------|--------|
| `executor=` | *Where* the task runs (the scheduling backend). | `"pueue"`, `"inline"`, `"local"`, `"slurm"`, `"sge"`, … |
| `container=` | *How* the task runs (Apptainer image, or none). | `"path/to/image.sif"` or `None` |

The orthogonality matters: any sensible `(executor, container)` combination should just work. Two cells are currently special:

- `executor="inline", container=...` is forbidden — raises at task-definition time. Inline tasks run in-process; container wrapping makes no sense for a Python function call.
- `executor="local", container=...` is deferred (see [TODO](#deferred-todo)) — raises `NotImplementedError` at task-definition time. Use `executor="pueue"` for now.

---

## The two task forms you will actually write

### Pueue task with a container (the common case)

```python
from redun import task, File

@task(executor="pueue", container="leehom.sif", jobs=4)
def mergetrim(sample: str, raw_bam: File) -> File:
    output = f"{RESULTS}/final/{sample}.bam"
    subprocess.check_call([
        "leeHom", "-t", "4",
        "-fq1", raw_bam.path,
        "-o", output,
    ])
    return File(output)
```

Note: **no manual `apptainer exec` in the task body.** The `container=` option causes redun to wrap the underlying `redun oneshot` command in `apptainer exec leehom.sif …`. Inside the container, the task body runs as plain Python and shells out without further wrapping.

### Inline task (small housekeeping)

```python
@task(executor="inline")
def merge_results(parts: list[File]) -> File:
    out_path = f"{RESULTS}/merged.txt"
    with open(out_path, "w") as f:
        for p in parts:
            f.write(open(p.path).read())
    return File(out_path)
```

Use `executor="inline"` for:
- File moves, simple text manipulation.
- Database lookups returning a Python value.
- Glue tasks that just route arguments between other tasks.
- Anything where subprocess overhead would dominate the actual work.

Inline tasks run synchronously on the scheduler thread. They block the scheduler while running; keep them quick.

---

## Task options reference (fork additions)

| Option | Type | Default | Cache-affecting? |
|--------|------|---------|------------------|
| `container` | `str \| None` | `None` (from executor default) | **yes** — image change invalidates cache |
| `binds` | `list[str] \| None` | `None` (from executor default) | no |
| `passthrough_env` | `list[str] \| None` | `None` (from executor default) | no |

Semantics:

- **Task-level overrides executor-level default.** `None` means "use executor default"; `[]` means "explicitly empty, override default to nothing".
- **`binds` accept either `/host/path` or `/host/path:/container/path`.** Single-path form mounts at the same path inside the container.
- **`passthrough_env` is a list of variable *names*.** The current process's env value at submission time is passed through.

The container scratch directory is always bind-mounted automatically; you don't need to include it in `binds`.

### Why `binds` and `passthrough_env` aren't in the cache hash

Bind mounts and env-var exposure affect what the container can *reach*, not what the code *does*. Treating them as cache-relevant would invalidate caches for orthogonal reasons (e.g. moving from `/mnt/ngs_data` on one host to `/data` on another with the same files).

If a task is genuinely sensitive to a specific environment variable, declare that variable as a task argument and pass it through explicitly — that puts it in the cache hash where it belongs.

---

## Configuration: `redun.ini`

Minimal pueue executor with executor-level container defaults:

```ini
[executors.pueue]
type = pueue
scratch = .redun_scratch
group = compute
jobs = 1

# Optional executor-level defaults (overridable per task)
default_container = common.sif
default_bind = /mnt/ngs_data, /mnt/scratch
default_passthrough_env = PATH

[executors.inline]
type = inline
```

The `default_*` keys are read by `ContainerAware` and provide fallbacks when a task doesn't specify the option directly. Per-task options always override.

Legacy back-compat: the older `container_type = apptainer` / `image = X.sif` config is still honoured for pueue, but only when no task-level `container=` is resolved.

### Same-database schema-scoped deployment (Postgres)

If you want redun's call-graph DB to share a Postgres database with another application, use a `[backend] db_schema` key:

```ini
[backend]
db_uri = postgresql://redun_user@dbhost/shared_db
db_schema = redun
automigrate = False
```

Redun then sets a single-entry `search_path` on every connection, so all unqualified DDL/DML (including Alembic's `alembic_version` table) resolves into `redun.*`. A startup assertion checks `current_schema()` matches and fails loud if it doesn't.

This is a *convenience and tripwire*. The load-bearing defence is the DB-level role grant — give `redun_user` `USAGE, CREATE` on `redun` only, and `REVOKE ALL ON SCHEMA public` (plus any other application schemas in the same database).

`db_schema` is Postgres-only — it raises on SQLite backends.

---

## What was retired

`executor="apptainer"` is **retired**. The standalone `ApptainerExecutor` is now a migration stub that raises with a pointer to `executor="pueue", container=...`.

Why: the old Apptainer executor conflated "where to run" with "how to wrap" — making it impossible to combine Apptainer with pueue without a Cartesian-product `apptainer_pueue` class. The orthogonality refactor split the two concerns; the standalone Apptainer class became redundant.

```python
# Old (no longer works)
@task(executor="apptainer")
def foo(): ...

# New
@task(executor="pueue", container="image.sif")
def foo(): ...
```

---

## A complete small workflow

```python
"""tiny_pipeline.py — read a FASTQ, count reads, merge counts."""

import subprocess
from redun import task, File


REF = "/mnt/ngs_data/ref.fa"
RESULTS = "results"


@task(executor="pueue", container="samtools.sif", jobs=2)
def count_reads(bam: File) -> int:
    """Count primary mapped reads in a BAM."""
    out = subprocess.check_output(
        ["samtools", "view", "-c", "-F", "260", bam.path]
    )
    return int(out.strip())


@task(executor="inline")
def sum_counts(counts: list[int]) -> int:
    """Add counts up. Trivial Python; no container needed."""
    return sum(counts)


@task(executor="inline")
def main(bams: list[str]) -> int:
    """Workflow entrypoint."""
    counts = [count_reads(File(p)) for p in bams]
    return sum_counts(counts)
```

Invocation: `redun run tiny_pipeline.py main --bams '["a.bam", "b.bam"]'`.

The fan-out (`count_reads` calls) runs as four parallel pueue jobs each in its own samtools container; the reductions run inline.

---

## Prerequisites for running

- **Pueue daemon running.** The fork targets a [custom pueue fork](https://github.com/wjv/pueue) that adds `pueued --jobs N` for global slot limits. Start it before running any redun workflow that uses `executor="pueue"`.
- **Apptainer installed.** Tasks with `container=` shell out to `apptainer exec`.
- **Images built and accessible.** The image paths in `container=` must resolve from the pueue daemon's working directory.

---

## Testing your workflows

The fork ships layered pytest markers; see `redun/tests/README.md` for the full conventions. The short version:

- `@pytest.mark.unit` — call `your_task.func(args)` directly; pure Python, no scheduler.
- `@pytest.mark.graph` — build the expression tree with `your_workflow(args)` (no `.run()`); assert structure with helpers in `redun/tests/helpers/graph_assertions.py`.
- `@pytest.mark.smoke` — invoke the scheduler against tiny fixtures using the `redun_scheduler` fixture (in-memory SQLite, hermetic).

Recipes: `just test` (unit + graph), `just test-smoke` (+ smoke), `just test-all` (+ docker-dependent).

For pipeline-side tests, prefer `executor="inline"` in test workflows — no pueue daemon required, results are synchronous.

---

## Deferred / TODO

- **`executor="local"` + `container=`** is not implemented yet. Raises `NotImplementedError` at task-definition time. Workaround: use `executor="pueue"` (with the same `container=` value).
- **`ContainerAware` not yet applied to SGE / Slurm executors.** When you actually want containerised tasks on those, expect a small mixin-application change.

---

## Pointers

- Upstream redun design overview: <https://insitro.github.io/redun/design.html>
- Test conventions: `redun/tests/README.md`
- Executor config reference: [`config.md`](config.md)
- Executor reference (legacy + retired): [`executors.md`](executors.md)
