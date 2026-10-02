"""Kernel faults for the local socket-buffer ring inspector."""

from __future__ import annotations


class EngineKernelException(Exception):
    """Raised when a telemetry record or ring slot fails validation."""
