"""Run a realistic local occupancy batch."""

from __future__ import annotations

import asyncio
import logging
import sys

from .engine import RingBufferSocketKernelInspector
from .exceptions import EngineKernelException


async def _batch() -> dict[str, object]:
    engine = RingBufferSocketKernelInspector()
    samples: list[dict[str, int]] = []
    for index in range(48):
        samples.append(
            {
                "queue_depth": 20 + index,
                "drops": index,
                "rmem_alloc": 8192 + index * 64,
                "wmem_alloc": 4096,
                "state": 1,
                "sequence": index + 1,
            }
        )
    result = await engine.run(samples)
    if result["rejected"] != 0:
        raise EngineKernelException("short batch was rejected")
    if not result["stalled"]:
        raise EngineKernelException("stalled consumer was not detected")
    if float(result["ewma_drop"]) <= 0.0:
        raise EngineKernelException("drop EWMA did not advance")
    if int(result["depth"]) != 48:
        raise EngineKernelException("ring depth does not match the batch")
    logging.getLogger("sec.sock.inspector").info(
        "scenario complete topic=%s depth=%s stalled=%s local-only",
        engine.TOPIC,
        result["depth"],
        result["stalled"],
    )
    return result


def main() -> int:
    """Publish a local occupancy batch. Return 0."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    result = asyncio.run(_batch())
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
