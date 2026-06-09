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

## Piping between commands and containers

For multi-stage tool chains — the classic `samtools view | tool_b | tool_c` shape, where each tool may live in its own container — use `script()` with a `Pipe` of `Cmd` stages instead of a single command:

```python
from redun.scripting import Cmd, Pipe, script

result = script(
    Pipe(
        Cmd(["samtools", "import", ...], container="docker://some/samtools:1.21"),
        Cmd(["evatags", "-c", "N", "-l", "9", "-o", "out.bam"], container="docker://some/evatags:0.5.5"),
    ),
    executor="pueue",
)
```

Or use the `|` overload for shell-pipe-like sugar:

```python
result = script(
    Cmd(["samtools", "import", ...], container="docker://some/samtools:1.21")
    | Cmd(["evatags", "-c", "N", "-l", "9", "-o", "out.bam"], container="docker://some/evatags:0.5.5"),
    executor="pueue",
)
```

Each stage independently declares its own `container` (or no container — bare stages run on the host shell). Stages compose freely: all-bare, all-containerised, or mixed.

The pipeline runs as **one shell process** inside a single executor submission, with kernel-level pipes between the stage processes. No intermediate files; no inter-task coordination overhead; no concerns about scheduler-side co-scheduling timing.

### What you get

- **Streaming between stages** via bash pipes (byte-clean — binary data passes through unchanged).
- **Cross-runtime portability**: the same `container="docker://..."` reference works on Apptainer hosts and Docker hosts; the per-host `container_type` config picks the runtime.
- **Per-stage stderr capture**: each stage's stderr lands in a separate `.task_error_<i>` file in scratch; the merged stderr lands in `.task_error` (same as today's single-command path).
- **First-failing exit code propagation**: if any stage fails, the first non-zero exit code becomes the headline `RETCODE`; the full PIPESTATUS array is also recorded to `.task_pipestatus` for forensic inspection.

### What stays the same

`script()`'s `inputs=`, `outputs=`, `tempdir=`, `as_mount=`, and standard task-options (`vcpus=`, `memory=`, etc.) all apply to the pipeline as a whole. Per-stage `binds` and `passthrough_env` are not currently supported (uniform across stages — file a request if this matters).

### Mixed bare and containerised stages

```python
script(
    Pipe(
        Cmd(["samtools", "view", "-h", input_bam.path], container="docker://some/samtools:1.21"),
        Cmd(["awk", "/some-filter/"]),       # bare; runs on host
        Cmd(["gzip"]),                       # bare
    ),
    executor="pueue",
    outputs=File("filtered.sam.gz"),
)
```

### Caveats

- **Pueue-only for now.** Multi-stage `Pipe` requires an executor that integrates the per-stage container-substitution path. `PueueExecutor` does; `LocalExecutor` does not. If you need pipelines under `executor="default"` or `executor="local"`, file a request — the work is small but not yet done.
- **`script_task`'s return shape changed**: see the next section ("script() return shape: stderr-on-success").
- **String argv with container needs bash in the image.** If a stage uses string argv (`Cmd("foo | bar", container="X")` rather than `Cmd(["foo", "bar"], container="X")`), the substitution wraps the command as `bash -c '…'`, which requires bash on the image's PATH. Using list-argv avoids this requirement.

---

## `script()` return shape: stderr-on-success

The return value of `script()` (when `outputs=File("-")`, the default) is a 3-element list:

```python
[exit_code, stdout_bytes, stderr_per_stage_list]
```

- `exit_code`: 0 on the success path. Failures raise `ScriptError` rather than returning a non-zero exit code.
- `stdout_bytes`: the final stage's stdout (raw bytes; binary-safe).
- `stderr_per_stage_list`: a *list* of bytes objects. Currently always a single-element list containing the merged stderr from all stages combined. Per-stage breakdown will land in a follow-up commit (the merged file is what the wrapper currently unstages to scratch; per-stage files are written but not yet unstaged).

For single-stage `script()` calls, the list still has one element (the merged stderr). The shape is uniform across single-stage and multi-stage so consumers don't have to dispatch on Pipe usage.

> **Migration note**: this is a breaking change versus the upstream `[exit_code, stdout_bytes]` 2-element shape. Code that does `result[0]` and `result[1]` keeps working; code that does `exit, stdout = result` or `result == [0, b"..."]` needs updating. `LocalExecutor` is a known exception that still returns just `bytes` — that asymmetry pre-dates this change and is tracked as a follow-up.

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

The pueue executor requires **pueue 4.0 or newer** on the host; it logs the detected client version at scheduler start and refuses to run against older releases.

### Selecting a container runtime: `container_type`

The runtime that runs your container — Apptainer or Docker — is a *host-environment* concern, not a task concern. The same `@task(container="X")` declaration runs unchanged across hosts; the host's executor config picks the local runtime via the `container_type` key:

```ini
# Mac dev box: Docker
[executors.pueue]
type = pueue
container_type = docker
scratch = .redun_scratch

# HPC server: Apptainer
[executors.pueue]
type = pueue
container_type = apptainer
scratch = /raid01/scratch/redun
```

Valid values: `apptainer` (default if the key is absent) and `docker`. Apptainer additionally honours `no_home`, `gpu_type`, and `extra_container_args` keys for runner tuning.

The runtime choice does **not** affect the cache hash. Same image string + same runtime-independent task arguments → cache hit, regardless of which host (and which container runtime) produced the result.

Container image strings work across both runtimes: `docker://registry/name:tag` references are read natively by Apptainer (which pulls and runs), and by Docker (as a normal registry ref — the `docker://` prefix is stripped before invocation); local Apptainer SIF paths (`/path/to/img.sif`) are Apptainer-only.

**Image ENTRYPOINTs are bypassed on both runtimes.** Pass the full command including the executable name (e.g. `script(["bcl2fastq", "--runfolder", ...], container="docker://mpieva/bcl2fastq:2.20")`). Apptainer's `exec` ignores ENTRYPOINTs by default; the fork injects `--entrypoint <command[0]>` on `docker run` so Docker matches that behaviour. An image declared `ENTRYPOINT=["bcl2fastq"]` therefore runs the same way on both. Users who genuinely want a different entrypoint can override per-executor via `extra_container_args = --entrypoint <X>` (Docker honours the last `--entrypoint` flag).

Legacy back-compat: the older single-image-per-executor pattern (`container_type` + `image = X.sif` set on the executor, no task-level `container=`) is still honoured.

### Image requirements for script tasks

If your task uses `script()` or `@task(script=True)`, redun wraps the command in `bash -c -o pipefail '…'`. **The container image must therefore have `bash` on its PATH** — minimal Alpine-based images that ship only `busybox`/`sh` will fail at startup with a "bash: not found" or similar. Pick a base image that includes bash (e.g. `debian:stable-slim`, `ubuntu:24.04`, or any of the standard scientific Python/R images).

Regular `@task` definitions without `script=True` don't have this requirement — they run `redun oneshot …` (Python) inside the container, not a shell wrapper.

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
