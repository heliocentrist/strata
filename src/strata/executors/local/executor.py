from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from strata.core.operations import OperationOutput
from strata.executors.protocols import OperationInvocation
from strata.plugins.registry import get_operation


@dataclass(frozen=True)
class InlineOperationRunner:
    async def run(self, invocation: OperationInvocation) -> list[OperationOutput]:
        return get_operation(invocation.operation_name).run(
            invocation.inputs,
            invocation.config,
        )

    async def run_many(
        self,
        invocations: list[OperationInvocation],
    ) -> list[list[OperationOutput]]:
        return [await self.run(invocation) for invocation in invocations]


@dataclass(frozen=True)
class ThreadedOperationRunner:
    max_workers: int = 4

    async def run(self, invocation: OperationInvocation) -> list[OperationOutput]:
        return await asyncio.to_thread(self._run_sync, invocation)

    def _run_sync(self, invocation: OperationInvocation) -> list[OperationOutput]:
        return get_operation(invocation.operation_name).run(
            invocation.inputs,
            invocation.config,
        )

    async def run_many(
        self,
        invocations: list[OperationInvocation],
    ) -> list[list[OperationOutput]]:
        return await asyncio.to_thread(self._run_many_sync, invocations)

    def _run_many_sync(
        self,
        invocations: list[OperationInvocation],
    ) -> list[list[OperationOutput]]:
        if not invocations:
            return []
        results: list[list[OperationOutput] | None] = [None] * len(invocations)
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self._run_sync, invocation): index
                for index, invocation in enumerate(invocations)
            }
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        return [result or [] for result in results]


def register_builtin_operation_runners() -> None:
    from strata.executors.registry import register_operation_runner

    register_operation_runner(
        "local_single_thread",
        lambda config: InlineOperationRunner(),
    )
    register_operation_runner(
        "local_threaded",
        lambda config: ThreadedOperationRunner(
            max_workers=int((config or {}).get("max_workers") or 4)
        ),
    )


register_builtin_executors = register_builtin_operation_runners
