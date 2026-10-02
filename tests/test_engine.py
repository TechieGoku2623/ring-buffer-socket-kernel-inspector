"""Roundtrip, happy path, and both ring edge cases."""

from __future__ import annotations

import asyncio
import json
import unittest

from ring_buffer_socket_kernel_inspector import (
    EngineKernelException,
    RingBufferSocketKernelInspector,
)
from ring_buffer_socket_kernel_inspector.wire import pack_record, unpack_record


def _sample(depth: int, drops: int, sequence: int) -> dict[str, int]:
    return {
        "queue_depth": depth,
        "drops": drops,
        "rmem_alloc": 4096,
        "wmem_alloc": 2048,
        "state": 1,
        "sequence": sequence,
    }


class RingEngineTest(unittest.TestCase):
    def test_wire_roundtrip(self) -> None:
        payload = pack_record(9, 2, 100, 50, 1, 7)
        decoded = unpack_record(payload)
        self.assertEqual(decoded["queue_depth"], 9)
        self.assertEqual(decoded["drops"], 2)
        self.assertEqual(decoded["rmem_alloc"], 100)
        self.assertEqual(decoded["wmem_alloc"], 50)
        self.assertEqual(decoded["state"], 1)
        self.assertEqual(decoded["sequence"], 7)
        self.assertEqual(unpack_record(pack_record(9, 2, 100, 50, 1, 7)), decoded)
        damaged = bytearray(payload)
        damaged[0] = 1
        damaged[1] = 0
        with self.assertRaises(EngineKernelException) as caught:
            unpack_record(bytes(damaged))
        self.assertIn("length", str(caught.exception))

    def test_happy_path(self) -> None:
        asyncio.run(self._happy_path())

    async def _happy_path(self) -> None:
        engine = RingBufferSocketKernelInspector(capacity=16, stall_records=4)
        records = [
            _sample(depth=20 + index, drops=index, sequence=index + 1)
            for index in range(15)
        ]
        result = await engine.run(records)
        json.dumps(result)
        self.assertEqual(result["depth"], 15)
        self.assertEqual(result["drops"], 14)
        self.assertGreater(float(result["ewma_drop"]), 0.0)
        self.assertTrue(result["stalled"])
        self.assertEqual(result["rejected"], 0)
        self.assertGreaterEqual(result["depth"] / engine.capacity, engine.NEAR_RATIO)
        first = await engine.consume()
        self.assertEqual(first["queue_depth"], 20)
        self.assertEqual(first["sequence"], 1)
        self.assertEqual(first["drops"], 0)

    def test_edge_corrupt_length(self) -> None:
        asyncio.run(self._edge_corrupt_length())

    async def _edge_corrupt_length(self) -> None:
        engine = RingBufferSocketKernelInspector(capacity=8, stall_records=32)
        await engine.run([_sample(3, 1, 1)])
        await engine.overlay_length(0, 3)
        with self.assertRaises(EngineKernelException) as caught:
            await engine.consume()
        self.assertIn("length", str(caught.exception))
        snapshot = await engine.run([])
        self.assertEqual(snapshot["depth"], 1)
        self.assertEqual(snapshot["rejected"], 0)

    def test_edge_index_wrap(self) -> None:
        asyncio.run(self._edge_index_wrap())

    async def _edge_index_wrap(self) -> None:
        engine = RingBufferSocketKernelInspector(capacity=8, stall_records=1000)
        first = [_sample(index + 1, index, index + 1) for index in range(6)]
        await engine.run(first)
        for index in range(6):
            row = await engine.consume()
            self.assertEqual(row["queue_depth"], index + 1)
            self.assertEqual(row["sequence"], index + 1)
        second = [_sample(100 + index, 10 + index, 100 + index) for index in range(6)]
        wrapped = await engine.run(second)
        self.assertEqual(wrapped["depth"], 6)
        self.assertEqual(wrapped["rejected"], 0)
        for index in range(6):
            row = await engine.consume()
            self.assertEqual(row["queue_depth"], 100 + index)
            self.assertEqual(row["sequence"], 100 + index)
            self.assertEqual(row["drops"], 10 + index)


if __name__ == "__main__":
    unittest.main()
