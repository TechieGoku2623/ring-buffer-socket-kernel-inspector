"""Deterministic latency harness for the local ring inspector."""

from __future__ import annotations

import asyncio
import math
import random
import statistics
import sys
import tracemalloc
from time import perf_counter_ns

from .engine import RingBufferSocketKernelInspector
from .exceptions import EngineKernelException

SEED = 20261002
ITERATIONS = 5000


def _p99(samples: list[float]) -> float:
    ordered = sorted(samples)
    count = len(ordered)
    index = math.ceil(0.99 * count) - 1
    if index < 0:
        return ordered[0]
    if index >= count:
        return ordered[-1]
    return ordered[index]


def _sample(index: int, depth: int, drops: int, sequence: int) -> dict[str, int]:
    return {
        "queue_depth": depth,
        "drops": drops,
        "rmem_alloc": 4096,
        "wmem_alloc": 2048,
        "state": 1,
        "sequence": sequence,
    }


def _edge_corrupt_length() -> bool:
    async def _run() -> None:
        engine = RingBufferSocketKernelInspector(capacity=8, stall_records=32)
        await engine.run([_sample(0, 3, 1, 1)])
        await engine.overlay_length(0, 3)
        try:
            await engine.consume()
        except EngineKernelException as exc:
            if "length" not in str(exc):
                raise AssertionError("corrupt length fault was mislabeled") from exc
        else:
            raise AssertionError("corrupted length was accepted")
        viewed = await engine.run([])
        if viewed["depth"] != 1:
            raise AssertionError("corrupt read advanced the consumer index")

    try:
        asyncio.run(_run())
    except Exception as exc:
        print(f"edge_corrupt_length failed: {exc}", file=sys.stderr)
        return False
    return True


def _edge_index_wrap() -> bool:
    async def _run() -> None:
        engine = RingBufferSocketKernelInspector(capacity=8, stall_records=1000)
        first = [_sample(index, index + 1, index, index + 1) for index in range(6)]
        await engine.run(first)
        for index in range(6):
            row = await engine.consume()
            if row["queue_depth"] != index + 1 or row["sequence"] != index + 1:
                raise AssertionError("pre-wrap record was reordered")
        second = [
            _sample(index, 100 + index, 10 + index, 100 + index) for index in range(6)
        ]
        await engine.run(second)
        for index in range(6):
            row = await engine.consume()
            if row["queue_depth"] != 100 + index or row["sequence"] != 100 + index:
                raise AssertionError("wrapped record was not intact")

    try:
        asyncio.run(_run())
    except Exception as exc:
        print(f"edge_index_wrap failed: {exc}", file=sys.stderr)
        return False
    return True


def _benchmark() -> tuple[int, float, float, int]:
    engine = RingBufferSocketKernelInspector()
    rng = random.Random(SEED)
    samples: list[float] = []

    async def _run() -> None:
        for index in range(ITERATIONS):
            sample = _sample(
                index,
                8 + rng.randrange(40),
                index % 17,
                index + 1,
            )
            started = perf_counter_ns()
            await engine.run([sample])
            samples.append((perf_counter_ns() - started) / 1000.0)
            await engine.consume()

    tracemalloc.start()
    try:
        asyncio.run(_run())
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    average = float(statistics.fmean(samples))
    return len(samples), average, _p99(samples), peak


def main() -> int:
    """Print the harness status dict and return 0 only on success."""
    failures = 0
    if not _edge_corrupt_length():
        failures += 1
    if not _edge_index_wrap():
        failures += 1
    iterations = 0
    average = 0.0
    p99 = 0.0
    peak = 0
    try:
        iterations, average, p99, peak = _benchmark()
    except Exception as exc:
        print(f"benchmark failed: {exc}", file=sys.stderr)
        failures += 1
    else:
        if iterations < ITERATIONS or peak <= 0:
            failures += 1
    status = "ok" if failures == 0 else "fail"
    print(
        {
            "status": status,
            "failures": failures,
            "latency_us": round(average, 3),
            "memory_peak_bytes": peak,
            "benchmark_iterations": iterations,
            "benchmark_avg_us": round(average, 3),
            "benchmark_p99_us": round(p99, 3),
        }
    )
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
