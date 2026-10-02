"""Packed occupancy record for the local socket-buffer ring."""

from __future__ import annotations

import struct

from .exceptions import EngineKernelException

RECORD = struct.Struct("<HIIIIHI")


def pack_record(
    queue_depth: int,
    drops: int,
    rmem_alloc: int,
    wmem_alloc: int,
    state: int,
    sequence: int,
) -> bytes:
    """Pack one occupancy record. The length prefix equals the struct size."""
    _check("queue_depth", queue_depth, 0xFFFFFFFF)
    _check("drops", drops, 0xFFFFFFFF)
    _check("rmem_alloc", rmem_alloc, 0xFFFFFFFF)
    _check("wmem_alloc", wmem_alloc, 0xFFFFFFFF)
    _check("state", state, 0xFFFF)
    _check("sequence", sequence, 0xFFFFFFFF)
    return RECORD.pack(
        RECORD.size,
        queue_depth,
        drops,
        rmem_alloc,
        wmem_alloc,
        state,
        sequence,
    )


def unpack_record(payload: bytes) -> dict[str, int]:
    """Unpack a record produced by :func:`pack_record`."""
    if not isinstance(payload, (bytes, bytearray)) or len(payload) != RECORD.size:
        raise EngineKernelException("record length mismatch")
    (
        length,
        queue_depth,
        drops,
        rmem_alloc,
        wmem_alloc,
        state,
        sequence,
    ) = RECORD.unpack(payload)
    if length != RECORD.size:
        raise EngineKernelException(f"corrupted length {length} != {RECORD.size}")
    return {
        "queue_depth": int(queue_depth),
        "drops": int(drops),
        "rmem_alloc": int(rmem_alloc),
        "wmem_alloc": int(wmem_alloc),
        "state": int(state),
        "sequence": int(sequence),
    }


def _check(label: str, value: int, limit: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EngineKernelException(f"field {label} must be an int")
    if value < 0 or value > limit:
        raise EngineKernelException(f"field {label} out of range")
