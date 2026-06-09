---
tocdepth: 3
---

# Executors

redun is able to perform task execution across various compute infrastructure using modules called Executors. For example, redun supports executors that execute jobs on threads, processes, and AWS Batch jobs. New kinds of infrastructure can be utilized by registering additional Executor modules.

Executors are instantiated by sections in the [redun configuration](./config) (`.redun/redun.ini`) following the format `[executors.{executor_name}]`, where `{executor_name}` is a user-specific name for the executor, such as `default`, `batch`, or `my_executor`. These names can then be referenced in workflows using the `executor` task option to indicate on which executor the task should execute:

```py
@task(executor="my_executor")
def task1():
    # ...
```

By default, tasks execute on the `default` executor, which is a [Local executor](#local-executor) that executes tasks on multiple threads (or processes, if configured).

In terms of executors, there is no restriction on which tasks can call which other tasks. For example, an AWS Batch task can seemingly "call" a local task, even though AWS Batch jobs don't have access to the local machine. This is possible because all task calls are lazy and are performed by the scheduler after the calling task completes.

```py
@task(executor="batch")
def task_on_batch():
    # This code runs on AWS Batch.
    return task_on_default(10)  # This appears to be a call back to a local task.

@task()  # option `executor` has default value "default"
def task_on_default(x: int):
    # This code runs on the local machine.
    return x + 1
```

Some executor options can be overridden per task using task options. For example, a user can specify a specific memory requirement (e.g. 10Gb) for a task using the `memory` task option:

```py
@task(executor="batch", memory=10)
def task2():
    # ...
```

Options can also be dynamic and overridden at call-time using the `Task.options` method:

```py
@task()
def main(data):
    memory = len(data) * gigs_per_row
    x = task_on_batch.options(memory=memory)(data)
    # ...
```

## Local executor

The **Local executor** executes tasks on the same machine as the scheduler using either multiple threads or processes (configured by the [`mode` option](config.md#local-executor)). By default, redun defines a Local executor named "default" that is used as the default executor for tasks. Users can configure the local executor using the [configuration file](config.md#local-executor).

## Alias executor
The `AliasExecutor` is a lazily-resolved alias for another executor, which allows 
executors with distinct names to resolve to the same underlying implementation.

For example, consider these tasks:

```python

@task(executor="foo_exec")
def foo() -> str:
    return f'Hello world'


@task(executor="bar_exec")
def bar(input) -> str:
    return input'

@task()
def main(greet: str = 'Hello') -> str:
    return foo(bar())
```

This example defined distinct executors for several tasks, perhaps because they need 
different environments. However, sometimes we may need them to share underlying
executors, for example, to share resources. Without aliases, consider a configuration 
like this:

```
[executors.default]
type = local
mode = process
max_workers = 1

[executors.foo_exec]
type = local
mode = process
max_workers = 1

[executors.bar_exec]
type = local
mode = process
max_workers = 1
```

Although it appears this would only use one worker at a time, it is actually creating
three distinct process pools, each with one worker, and the executors are allowed to
operate in parallel to one another. 

Using aliases, we can solve this problem. This configuration file explicitly configures 
the automatically-created `process` executor, and ensures that every task will use it. 
Since there is only one underlying process pool, with only a single worker, this achieves
the effect of running one task at a time. Note that `process` is overloaded; it is both the 
name of the `mode` for the local executor, and also the name of one of the built-in executors.

```
; Create a single-worker executor in process mode (instead of thread)
[executors.single_worker]
type = local
mode = process
max_workers = 1

; Redirect both built-in executors to use it
[executors.process]
type = alias
target = single_worker

[executors.default]
type = alias
target = single_worker

; And redirect both task-specific executors to use it
[executors.foo_exec]
type = alias
target = single_worker

[executors.bar_exec]
type = alias
target = single_worker
```

## AWS Batch executor

The **AWS Batch executor** executes tasks as jobs on [AWS Batch](https://aws.amazon.com/batch/), which is an AWS service for running [Docker-based](https://www.docker.com/) jobs on a compute cluster. AWS Batch manages job queues, compute nodes, and job assignments such that compute requirements are met (e.g. memory, vcpus, etc). redun manages the task dependency graph and will only submit a task to execute on AWS Batch once all upstream tasks are complete.

### Docker image

To use AWS Batch, users must define a Docker image that contains the necessary code for their task. If a task is a pure [script task](design.md#shell-scripting), only the commands used in the script need to be installed in the Docker image. However, if a regular task is to run on AWS Batch, then the `redun` python package must be installed in the Docker image to facilitate the execution of the task on the AWS Batch compute node. Typically, the workflow python code itself does not need to be installed inside the Docker image. Instead, redun provides automatic [code packaging](#code-packaging) as a convenience for quick iterative development.

### Configuration

To use AWS Batch, at a minimum three options must be [configured](config.md#aws-batch-executor), a Docker image for the job ([`image`](config.md#aws-batch-executor)), a AWS Batch queue to submit to ([`queue`](config.md#aws-batch-executor)), and a S3 path to store temporary files for communication between the scheduler and jobs ([`s3_scratch`](config.md#aws-batch-executor)).

### S3 scratch space

redun performs simple communication with AWS Batch jobs through a user defined S3 scratch space. Specifically, the arguments to a task are serialized as a python pickle and stored at a path such as `s3://{s3_scratch}/{eval_hash}/input`, where `eval_hash` is the hash of a task's hash and its arguments and `s3_scratch` is defined in the [configuration](config.md#aws-batch-executor). When a task completes, its output is stored similarly in a pickle file `s3://{s3_scratch}/{eval_hash}/output`. Standard output and standard error is also captured in log files within the scratch space. All of these files are temporary and can be deleted by users once a workflow is complete.


### Code packaging

When running regular (non-script) tasks on AWS Batch, redun needs access to the workflow python code itself within the Docker container at runtime. While one could install the workflow python code in the Docker image, rebuilding and pushing Docker images for each code change could be burdensome during quick iterative development. As a convenience, redun provides a mechanism, called code packaging, for copying code into the Docker container at runtime.

By default, redun copies all files matching the pattern `**/*.py` into a tar file that is copied to the s3 scratch space. This tar file is then downloaded and unzipped within the running Docker container prior to executing the task. The specific files included in the code package can be controlled using [`code_includes` and `code_excludes` configuration options](config.md#aws-batch-executor).

### Job reuniting

In certain situations, such as errors or user initiated killing, the redun scheduler might be terminated while AWS Batch jobs are running. If the redun scheduler is restarted, it will attempt to determine if a batch task has an existing AWS Batch job already running or if one has recently completed leaving an output file in s3 scratch space. If so, the redun scheduler will "reunite" with such jobs and output and avoid double submission of AWS Batch jobs. redun uses the `eval_hash` to ensure the task hash and arguments are the same since the previous job submission.

### Local debugging

During development, it may be easier to run the Docker image locally in order to debug and interactively inspect a job. Local execution of Docker-base jobs can be achieved by using the [`debug=True` option](config.md#aws-batch-executor). The S3 scratch space will still be used to transfer input and output with the Docker container.

The docker container will run in interactive mode (e.g. `docker run --interactive ...`), allowing users to place debugging breakpoints within tasks or see task output on stdout. The task option `interactive=False` can also be used to run the Docker container without interactive mode.

The task option `volume` can also be used to define volume mounts for the Docker container during debugging. Format is `volume = [(host, container), ...]`, where `host` defines a source path on the host machine, and `container` defines a destination path within the container to perform the mount.

### Multi-node

AWS Batch allows for jobs that simultaneously use multiple compute nodes. See AWS [documentation](https://docs.aws.amazon.com/batch/latest/userguide/multi-node-parallel-jobs.html)

If the executor is configured to use multiple nodes, by setting `num_nodes`, the executor will invoke the task with identical arguments on each node. Batch starts the main node first, then starts the rest of the nodes. The task implementation may inspect the AWS environment variables for details on the multi-node configuration, such as detecting if it is the main node, or determining the IPs to construct a peer network.   

Warning: For python tasks, the executor will instruct only the main node to write its outputs to storage and non-main node outputs are discarded. For script tasks, the various nodes must somehow arrange that the output is only written once, but the infrastructure does not help. 

Multi-node jobs are currently incompatible with array jobs, because this appears not to be supported by AWS.

## Docker executor

The Docker executor (`type=docker`) runs each redun job inside a local Docker container. This executor is used by AWS Batch Executor when using debug mode.

## AWS Glue Spark executor

The **AWS Glue executor** executes tasks as jobs on [AWS Glue](https://aws.amazon.com/glue/), which runs [Apache Spark](https://spark.apache.org/) jobs on a managed cluster. Spark jobs run on many CPUs and are especially useful for working with large tabular datasets such as those represented in Pandas DataFrames.

Spark jobs are essentially a mini compute cluster, with a driver that maintains a SparkContext object, and a number of workers.
Each worker can have one or more **executors**, which are the processes that run individual tasks in the Spark job. They typically
run for the life of the application and send results to the driver when complete. Executors may use multiple vCPU cores to get
their work done, depending on the configuration.

To use AWS Glue, at a minimum you must configure a temporary location in S3 where files used to communicate between the scheduler and jobs are stored. Scratch space, code packaging, and job reuniting are all done in a similar way to the AWS Batch executor.


### Loading and writing datasets

Spark datasets are typically sharded across multiple files on disk. The [`ShardedS3Dataset`](redun.file.ShardedS3Dataset) class provides an interface to
these datasets that can be tracked and recorded by redun.


### Helper functions

Spark jobs are written a bit differently than pure Python. You'll want to load large datasets to Spark DataFrames with `ShardedS3Dataset`, but frequently other operations will require the use of the Spark context that is defined when the
job is running.

The `redun.glue` module provides helper functions that can be used in glue executor jobs and can be imported into the
top level of your redun script, even when Spark isn't yet defined. The `redun.glue.get_spark_context()` and
`redun.glue.get_spark_session()` functions can be used in your tasks to retrieve the currently defined spark environment.


#### User-defined functions

You might want to define your own functions to operate on a dataset. Typically, you'd use the `pyspark.sql.functions.udf` decorator
on a function to make it a UDF, but when redun evaluates the decorator it will error out as there is no spark context available
to register the function to. The `redun.glue.udf` decorator handles this issue. See the redun examples for real-world use
of UDFs and this decorator.

### Available Python modules

AWS Glue automates management, provisioning, and deployment of Spark clusters, but only with a [pre-determined set of Python modules](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html#glue20-modules-provided).
Most functionality you may need is already available, including scipy, pandas, etc.

Additional modules that are available in the public PyPi repository can be installed with the `additional_libs` task option.
However, other modules, especially those using C/C++ compiled extensions, are not really installable at this time. 

### Task options

The following configuration options may be specified on a per-task basis in the decorator.

#### `workers`

An integer that specifies the number of workers available by default to Glue jobs. Each worker provides one or more "data processing units" (DPUs). AWS defines a  DPU as "a relative measure of processing power that consists of 4 vCPUs of compute capacity and 16GB of memory." Depending on the worker type, there will be one or more Spark executors per DPU, each with one or more spark cores. Jobs are billed by number of DPUs and time.

#### `worker_type`

Choose from:

* `Standard`: each worker will have a 50GB disk scratch space and 2 executors, each with 4 vCPU cores.
* `G.1X`: each worker maps to 1 DPU and a single executor, with 8 vCPU cores and 10 GiB of memory. AWS recommends
this worker type for memory-intensive jobs.
* `G.2X`: each worker maps to 2 DPUs and a single executor, with 16 vCPU cores and 24576 MiB of memory. AWS recommends
this worker type for memory-intensive jobs or ML transforms. Note that as this worker type provides 2 DPUs, it is twice
as expensive as the others.

#### `additional_libs`

A list of additional Python libraries that will be `pip install`'ed before the run starts. For example,
`additional_libs=["promise", "alembic==1.0.0"]` will install the promise and alembic libraries.

#### `extra_files`

A list of files that will be made available in the root directory of the run.


## Kubernetes (k8s) executor

The **k8s executor** executes tasks as jobs on a [Kubernetes](https://kubernetes.io/) cluster. This executors works similar to the [AWS Batch Executor](#aws-batch-executor) in terms of using scratch object storage to transfer task arguments, results, and code packaging. See the [configuration documentation](config.md#kubernetes-k8s-executor) for more details.

Note: The k8s executor is new executor provided as a beta release. If you experience an issues, please report them to help improve the implementation.

## Apptainer (retired — use `container=` instead)

The standalone **Apptainer executor** has been retired in the EVA fork. Containerisation is now a task-level option, orthogonal to the choice of host executor:

```py
@task(executor="pueue", container="my_image.sif")
def align_reads(sample: str, reference: File) -> File:
    return run_alignment(sample, reference)
```

The `container=` option works on any host executor that inherits `ContainerAware` (currently `pueue`; SGE and Slurm to follow). Bind mounts and environment passthrough are configured per task with `binds=[...]` and `passthrough_env=[...]`, or as executor-level defaults (`default_container`, `default_bind`, `default_passthrough_env`) in `redun.ini`.

Using `executor="apptainer"` raises an error pointing to this migration path.

## Pueue executor

The **Pueue executor** (`type = pueue`) submits tasks to a [Pueue](https://github.com/Nukesor/pueue) daemon for managed execution on a local server. Pueue is a command-line task queue that manages sequential and parallel execution of shell commands.

This executor targets a Pueue fork that adds job-slot-based resource management (`pueued --jobs N`), allowing tasks to declare how many resource slots they consume. This is useful for managing concurrent workloads on a single multi-core server without a full cluster scheduler.

**Pueue 4.0 or newer is required.** The executor logs the detected `pueue` client version at startup and refuses to start against older versions, whose `pueue status --json` output shape this code is not written to handle.

### How it works

1. The executor submits each redun job via `pueue add`, receiving a task ID.
2. A monitor thread polls `pueue status --json` to detect job completion.
3. Task arguments and results are exchanged through pickle files in a shared scratch directory.

### Configuration

At a minimum, you must configure a scratch path. See the [configuration documentation](config.md#pueue-executor) for the full list of options.

```ini
[executors.pueue]
type = pueue
scratch = /shared/scratch/redun
group = default
jobs = 1
```

### Job slots

The `jobs` option specifies how many resource slots each task consumes (default: 1). When the Pueue daemon is started with `pueued --jobs N`, it ensures that the total slot usage of running tasks never exceeds N. This can be overridden per task:

```py
@task(executor="pueue", jobs=4)
def heavy_computation(data):
    # This task requires 4 of the available job slots.
    return process(data)
```

### Container wrapping

The Pueue executor supports optional container wrapping. When `container_type` and `image` are configured, each command is wrapped in an `apptainer exec` (or `docker run`) invocation before being submitted to Pueue:

```ini
[executors.pueue]
type = pueue
scratch = /shared/scratch/redun
container_type = apptainer
image = /path/to/container.sif
```

This composability allows separating the concerns of job scheduling (Pueue manages the queue and resource slots) and execution environment (Apptainer provides the container).

## Slurm executor

The **Slurm executor** (`type = slurm`) submits tasks as batch jobs to a [Slurm](https://slurm.schedmd.com/) cluster. Jobs are submitted via `sbatch` and monitored via `sacct`. Task arguments and results are exchanged through pickle files in a shared scratch directory on a cluster-accessible filesystem (NFS, Lustre, GPFS, etc.).

### Configuration

At a minimum, you must configure a scratch path on the shared filesystem. See the [configuration documentation](config.md#slurm-executor) for the full list of options.

```ini
[executors.slurm]
type = slurm
scratch = /shared/lustre/redun_scratch
partition = compute
account = mylab
time_limit = 04:00:00
```

### Resource requests

Resource options (`vcpus`, `memory`, `gpus`) map to Slurm resource flags (`--cpus-per-task`, `--mem`, `--gres=gpu:N`). These can be set as defaults in the configuration and overridden per task:

```py
@task(executor="slurm", memory=64, vcpus=8, gpus=2)
def train_model(data: File) -> File:
    return run_training(data)
```

### Container wrapping

Like the Pueue executor, the Slurm executor supports optional container wrapping. This is the typical pattern for running containerised workloads on an HPC cluster:

```ini
[executors.slurm]
type = slurm
scratch = /shared/lustre/redun_scratch
partition = gpu
container_type = apptainer
image = /apps/containers/ml-toolkit.sif
gpus = 1
```

### Submit scripts

For each job, the executor writes a shell script to `{scratch}/jobs/{eval_hash}/submit.sh` and passes it to `sbatch`. The job name follows the pattern `redun_{eval_hash[:12]}`, making it easy to identify redun jobs in `squeue` output.

## SGE executor

The **SGE executor** (`type = sge`) submits tasks as batch jobs to a [Sun Grid Engine](https://en.wikipedia.org/wiki/Oracle_Grid_Engine) (SGE) cluster. Jobs are submitted via `qsub` and monitored via `qstat`. When a job disappears from `qstat` output, the executor reads its result from the shared scratch directory.

### Configuration

At a minimum, you must configure a scratch path on the shared filesystem. See the [configuration documentation](config.md#sge-executor) for the full list of options.

```ini
[executors.sge]
type = sge
scratch = /shared/nfs/redun_scratch
queue = all.q
parallel_environment = smp
```

### Parallel environments

When `vcpus` is greater than 1 and a `parallel_environment` is configured, the executor requests multiple slots via `qsub -pe`. This is the standard SGE mechanism for multi-threaded jobs:

```py
@task(executor="sge", vcpus=8, memory=4)
def parallel_analysis(data: File) -> File:
    # Requests 8 slots in the "smp" PE, with 4G per slot.
    return analyse(data)
```

### Container wrapping

The SGE executor supports the same container wrapping as the other HPC executors:

```ini
[executors.sge]
type = sge
scratch = /shared/nfs/redun_scratch
queue = all.q
container_type = apptainer
image = /path/to/container.sif
```

## Container wrapping (composability)

The Pueue, Slurm, and SGE executors all support optional **container wrapping** — the ability to run each task command inside a container without needing a dedicated container executor. This is configured by adding `container_type` and `image` to any scheduler executor's configuration.

Supported container types:

- `apptainer` — wraps commands with `apptainer exec` (suitable for HPC, no root required)
- `docker` — wraps commands with `docker run --rm`

This design separates two orthogonal concerns:

- **Job scheduling** — which queue/partition/group the job runs in, resource limits, job slots
- **Execution environment** — which container image provides the runtime

For example, the same Apptainer image can be used with different schedulers depending on the infrastructure:

```ini
# On a single server with Pueue
[executors.local_queue]
type = pueue
scratch = /scratch/redun
container_type = apptainer
image = /apps/containers/pipeline.sif

# On a Slurm cluster
[executors.cluster]
type = slurm
scratch = /shared/lustre/redun_scratch
partition = compute
container_type = apptainer
image = /apps/containers/pipeline.sif

# Without a container (bare execution on cluster nodes)
[executors.bare]
type = slurm
scratch = /shared/lustre/redun_scratch
partition = compute
```
