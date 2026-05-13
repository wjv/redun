"""Minimal three-task workflow used by the smoke layer.

Demonstrates the orthogonality refactor's most basic payoff:
``@task(executor="inline")`` cleanly composes in a pipeline, with the
scheduler caching downstream tasks on a re-run.
"""

from redun import task


redun_namespace = "redun.tests.fixtures.minimal_workflow"


@task(executor="inline")
def add_one(x: int) -> int:
    return x + 1


@task(executor="inline")
def double(x: int) -> int:
    return x * 2


@task(executor="inline")
def workflow(start: int) -> int:
    return double(add_one(start))
