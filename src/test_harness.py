"""Deterministic harness for ring wrap, damaged lengths, and latency."""

from __future__ import annotations

import asyncio
import random
import sys
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

SEED = 20261001
ITERATIONS = 5000


def _percentile_index(count: int) -> int:
    index = (99 * count + 99) // 100 - 1
    if index < 0:
        return 0
    if index >= count:
        return count - 1
    return index


def _sample(depth: int, drops: int = 4) -> dict[str, int]:
    return {
        "queue_depth": depth,
        "drops": drops,
        "rmem_alloc": 4096,
        "wmem_alloc": 2048,
        "state": 1,
    }


def _edge_ring_wrap(module: object) -> str:
    engine_cls = module.SocketRingInspector

    async def _run() -> None:
        engine = engine_cls()
        engine.prime_indices(0xFFFFFFFF, 0xFFFFFFFF)
        written = await engine.observe(_sample(7, drops=1))
        if written["tail"] != 0:
            raise AssertionError("uint32 tail did not wrap to zero")
        if written["head"] != 0xFFFFFFFF:
            raise AssertionError("head moved during the wrap write")
        if written["forwarded"] is not False:
            raise AssertionError("local ring was forwarded")
        read_back = await engine.consume_one()
        if read_back["empty"] is not False:
            raise AssertionError("wrapped slot was empty")
        if read_back["queue_depth"] != 7 or read_back["head"] != 0:
            raise AssertionError("wrapped record did not survive the mask")

    try:
        asyncio.run(_run())
    except Exception as exc:
        print(f"edge_ring_wrap failed: {exc}", file=sys.stderr)
        return "FAIL"
    return "PASS"


def _edge_corrupt_length(module: object) -> str:
    engine_cls = module.SocketRingInspector
    error_cls = module.EngineKernelException

    async def _run() -> None:
        engine = engine_cls()
        await engine.observe(_sample(3, drops=0))
        engine.overlay_length(0, 1)
        try:
            await engine.consume_one()
        except error_cls as exc:
            if "length" not in str(exc):
                raise AssertionError("length fault was mislabeled") from exc
        else:
            raise AssertionError("corrupted length field was accepted")

    try:
        asyncio.run(_run())
    except Exception as exc:
        print(f"edge_corrupt_length failed: {exc}", file=sys.stderr)
        return "FAIL"
    return "PASS"


def _sockstat_roundtrip(module: object) -> str:
    engine_cls = module.SocketRingInspector

    def _run() -> None:
        engine = engine_cls()
        text = engine.render_sockstat(used=128, tcp_mem=12, udp_mem=2)
        parsed = engine.parse_sockstat(text)
        if parsed != {"used": 128, "tcp_mem": 12, "udp_mem": 2}:
            raise AssertionError("synthetic sockstat did not round-trip")
        if "CAP_NET_RAW" in text:
            raise AssertionError("rendered sockstat carried a capability token")

    try:
        _run()
    except Exception as exc:
        print(f"sockstat_roundtrip failed: {exc}", file=sys.stderr)
        return "FAIL"
    return "PASS"


def _benchmark(module: object) -> tuple[int, float, float, int]:
    engine_cls = module.SocketRingInspector
    rng = random.Random(SEED)
    depths = [8 + rng.randrange(0, 5) for _ in range(ITERATIONS)]
    engine = engine_cls()

    async def _run() -> list[float]:
        samples: list[float] = []
        for depth in depths:
            started = time.perf_counter_ns()
            await engine.observe(_sample(depth, drops=4))
            await engine.consume_one()
            elapsed_us = (time.perf_counter_ns() - started) / 1000.0
            samples.append(elapsed_us)
        return samples

    tracemalloc.start()
    try:
        samples = asyncio.run(_run())
    finally:
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    average = sum(samples) / len(samples)
    ordered = sorted(samples)
    p99 = ordered[_percentile_index(len(ordered))]
    return len(samples), average, p99, peak


def main() -> int:
    import main as engine_module

    status = {
        "edge_ring_wrap": _edge_ring_wrap(engine_module),
        "edge_corrupt_length": _edge_corrupt_length(engine_module),
        "sockstat_roundtrip": _sockstat_roundtrip(engine_module),
        "benchmark": "FAIL",
    }
    try:
        count, average, p99, peak = _benchmark(engine_module)
        print(
            f"BENCH n={count} avg_us={average:.2f} "
            f"p99_us={p99:.2f} peak_bytes={peak}"
        )
        if count >= ITERATIONS and average >= 0.0 and peak > 0:
            status["benchmark"] = "PASS"
    except Exception as exc:
        print(f"benchmark failed: {exc}", file=sys.stderr)
    print(status)
    if all(value == "PASS" for value in status.values()):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
