"""Power-of-two ring of local socket-buffer occupancy records.

Publish and consume advance integer indexes. The slot is ``index & mask``.
Nothing in this module opens a socket, crafts a packet, or issues a kernel call.
Kafka topic ``sec.sock.ring`` is the metric name a deployment would publish;
the bytearray itself stays in this process.
"""

from __future__ import annotations

import asyncio
import logging
import math
import statistics
import struct

from .exceptions import EngineKernelException
from .wire import RECORD, unpack_record

LOGGER = logging.getLogger("sec.sock.inspector")


class RingBufferSocketKernelInspector:
    """Track depth, drops, and a stalled consumer on a fixed local ring."""

    TOPIC = "sec.sock.ring"
    NEAR_RATIO = 0.90
    _FIELDS = (
        "queue_depth",
        "drops",
        "rmem_alloc",
        "wmem_alloc",
        "state",
        "sequence",
    )

    def __init__(
        self,
        capacity: int = 1024,
        stall_records: int = 32,
        alpha: float = 0.2,
    ) -> None:
        if capacity < 2 or math.floor(math.log2(capacity)) != math.log2(capacity):
            raise EngineKernelException("capacity must be a power of two")
        if stall_records < 1:
            raise EngineKernelException("stall window must be positive")
        if not math.isfinite(alpha) or not 0.0 < alpha <= 1.0:
            raise EngineKernelException("EWMA alpha out of range")
        if struct.calcsize(RECORD.format) != RECORD.size:
            raise EngineKernelException("occupancy record width drifted")
        self.capacity = int(capacity)
        self.stall_records = int(stall_records)
        self._mask = self.capacity - 1
        self._alpha = float(alpha)
        self._lock = asyncio.Lock()
        self._ring = bytearray(self.capacity * RECORD.size)
        self._head = 0
        self._tail = 0
        self._count = 0
        self._last_drops: int | None = None
        self._ewma_drop = 0.0
        self._increments: list[float] = []
        self._drop_streak = 0
        self._drop_latched = False
        self._prev_sequence: int | None = None
        self._stall_head = 0
        self._idle_publishes = 0
        self._stalled = False
        self._stall_latched = False
        self._near_latched = False
        self._reject_latched = False
        self._rejected = 0
        self._auto_sequence = 0
        self._drops = 0

    async def run(self, records: list[dict[str, int]]) -> dict[str, object]:
        """Publish one batch of caller-supplied occupancy samples."""
        await asyncio.sleep(0)
        async with self._lock:
            if not isinstance(records, (list, tuple)):
                raise EngineKernelException("records must be a sequence of samples")
            for sample in records:
                self._publish_unlocked(sample)
            return self._snapshot()

    async def consume(self) -> dict[str, int]:
        """Read the oldest record. A damaged length field is a hard fault."""
        await asyncio.sleep(0)
        async with self._lock:
            return self._consume_unlocked()

    async def overlay_length(self, index: int, length: int) -> None:
        """Install a length prefix so a damaged slot fails on the next read."""
        await asyncio.sleep(0)
        async with self._lock:
            if isinstance(length, bool) or not isinstance(length, int):
                raise EngineKernelException("length must be an int")
            if length < 0 or length > 0xFFFF:
                raise EngineKernelException("length exceeds record width")
            if isinstance(index, bool) or not isinstance(index, int):
                raise EngineKernelException("index must be an int")
            slot = index & self._mask
            struct.pack_into("<H", self._ring, slot * RECORD.size, length)

    def _publish_unlocked(self, sample: dict[str, int]) -> None:
        if self._count >= self.capacity:
            self._rejected += 1
            if not self._reject_latched:
                self._reject_latched = True
                LOGGER.warning(
                    "ring at capacity; sample rejected; topic=%s stays local",
                    self.TOPIC,
                )
            return
        queue_depth, drops, rmem, wmem, state, sequence = self._coerce(sample)
        slot = self._tail & self._mask
        offset = slot * RECORD.size
        RECORD.pack_into(
            self._ring,
            offset,
            RECORD.size,
            queue_depth,
            drops,
            rmem,
            wmem,
            state,
            sequence,
        )
        self._tail += 1
        self._count += 1
        self._drops = drops
        self._note_drops(drops)
        self._note_stall(sequence)
        self._note_capacity()

    def _consume_unlocked(self) -> dict[str, int]:
        if self._count == 0:
            raise EngineKernelException("ring is empty")
        slot = self._head & self._mask
        offset = slot * RECORD.size
        payload = bytes(self._ring[offset : offset + RECORD.size])
        decoded = unpack_record(payload)
        self._head += 1
        self._count -= 1
        self._stall_head = self._head
        self._idle_publishes = 0
        self._stalled = False
        self._stall_latched = False
        self._reject_latched = False
        if self._count / self.capacity < self.NEAR_RATIO:
            self._near_latched = False
        return decoded

    def _note_drops(self, drops: int) -> None:
        if self._last_drops is None:
            increment = 0.0
        else:
            increment = float(drops - self._last_drops)
        self._last_drops = drops
        self._ewma_drop = math.fsum(
            (
                (1.0 - self._alpha) * self._ewma_drop,
                self._alpha * increment,
            )
        )
        self._increments.append(increment)
        if len(self._increments) > 64:
            del self._increments[0]
        mean_increment = float(statistics.fmean(self._increments))
        if increment > 0.0:
            self._drop_streak += 1
        else:
            self._drop_streak = 0
            self._drop_latched = False
        if self._drop_streak >= 3 and not self._drop_latched:
            self._drop_latched = True
            LOGGER.warning(
                "monotonic drop increase mean_increment=%.6f ewma=%.6f topic=%s",
                mean_increment,
                self._ewma_drop,
                self.TOPIC,
            )

    def _note_stall(self, sequence: int) -> None:
        advanced = self._prev_sequence is None or sequence > self._prev_sequence
        self._prev_sequence = sequence
        if advanced and self._head == self._stall_head:
            self._idle_publishes += 1
        elif advanced:
            self._stall_head = self._head
            self._idle_publishes = 1
        if self._idle_publishes >= self.stall_records and not self._stall_latched:
            self._stalled = True
            self._stall_latched = True
            LOGGER.warning(
                "stalled consumer head=%d sequence=%d topic=%s",
                self._head,
                sequence,
                self.TOPIC,
            )

    def _note_capacity(self) -> None:
        ratio = self._count / self.capacity
        if ratio >= self.NEAR_RATIO and not self._near_latched:
            self._near_latched = True
            LOGGER.warning(
                "ring occupancy %.3f of capacity %d topic=%s local-only",
                ratio,
                self.capacity,
                self.TOPIC,
            )
        if ratio < self.NEAR_RATIO:
            self._near_latched = False

    def _coerce(self, sample: dict[str, int]) -> tuple[int, int, int, int, int, int]:
        if not isinstance(sample, dict):
            raise EngineKernelException("sample must be a dict")
        values: list[int] = []
        for key in self._FIELDS:
            if key == "sequence" and key not in sample:
                self._auto_sequence += 1
                values.append(self._auto_sequence)
                continue
            if key not in sample:
                raise EngineKernelException("sample missing fields")
            raw = sample[key]
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise EngineKernelException(f"field {key} must be an int")
            limit = 0xFFFF if key == "state" else 0xFFFFFFFF
            if raw < 0 or raw > limit:
                raise EngineKernelException(f"field {key} out of range")
            values.append(raw)
        return (values[0], values[1], values[2], values[3], values[4], values[5])

    def _snapshot(self) -> dict[str, object]:
        return {
            "depth": self._count,
            "drops": self._drops,
            "ewma_drop": self._ewma_drop,
            "stalled": self._stalled,
            "rejected": self._rejected,
        }
