# Ring Buffer Socket Kernel Inspector

A high-throughput, low-latency asynchronous engine engineered to resolve silent socket-buffer exhaustion by modeling rmem and wmem occupancy, drop counters, and a fixed power-of-two bytearray ring in userspace, without opening a raw socket or requesting CAP_NET_RAW.

## 🏗️ Systems Architecture & Event Topology

`SocketRingInspector` is a userspace model. Samples are packed into a `bytearray` indexed by a power-of-two mask. `observe` writes one occupancy record. `consume_one` advances the head. `prime_indices` sets head and tail for a wrap test. `overlay_length` plants a length field the reader must reject. `render_sockstat` and `parse_sockstat` round-trip a synthetic `/proc/net/sockstat` text block that this program generates. The host file is never opened.

The Kafka topic name `sec.sock.ring` is the name a deployment would publish. Nothing in this process is copied onto a socket. An `asyncio.Lock` covers the head, the tail, and the bytearray. `logging.basicConfig` timestamps every line. A corrupted length raises `EngineKernelException` on read. `run_scenario` is the coroutine `python src/main.py` awaits.

## 📊 Core Visual Walkthrough & Engine Pipeline Flow

```
synthetic occupancy sample (rmem, wmem, drops)
    |
    v
observe --> pack 20-byte record at (tail & mask) * stride
    |
    v
bytearray ring, capacity a power of two
    |
    +-- tail wraps to 0 -------- index mask, record survives
    +-- length field corrupted -- EngineKernelException, record not forwarded
    +-- drops increase ---------- warning, monotonic drop flag
    +-- head held, tail moves --- stalled consumer warning
    |
    v
render_sockstat (text this process wrote)
topic name sec.sock.ring, forwarded=False
```

Insert the structural terminal walkthrough recording at docs/assets/terminal-walkthrough.gif before publishing the release notes.

## ⚡ Low-Level OS Mechanics & Network Physics

The index mask is `capacity - 1`. A tail that passes the last slot becomes `tail & mask`, which is 0 for a power-of-two capacity. That is the wrap the harness asserts, including a uint32 tail that lands on zero. The record stride is fixed (`FRAME.size` is 20). A length that does not match that stride is a corrupted record and is rejected before the payload is interpreted as an occupancy sample.

Occupancy is a count of bytes committed in the model, not a read of `sk_rcvbuf`. Near-capacity is a comparison against the ring's own byte capacity. Drop detection is a monotonic comparison of the sample's drop counter with the previous sample. `statistics` summarizes the occupancy series. `math.log2` checks the power-of-two invariant at construction. No `AF_PACKET` socket is created, and the process never calls `capset`.

## ⚖️ Architecture Trade-offs & Pragmatic Decisions

Reading a live `sockstat` and attaching to a raw socket would show real buffers and would also require privileges this monitor must not hold. The inspector models the counters the caller already exported and keeps the ring in the process. The defensive boundary is the point: occupancy, drops, and a stall are visible without the ability to inject a frame.

The ring is a bytearray rather than a list of objects so the wrap test can speak about indices and a length field, which is the failure mode of a real record parser. The cost is manual packing. `struct` is the only packer, and the width is asserted in `__init__` so a format drift fails closed.

## 🚀 Local Installation & Benchmarking

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/main.py
python src/test_harness.py
```

```python
import asyncio

from src.main import SocketRingInspector, run_scenario


async def demo() -> None:
    inspector = SocketRingInspector()
    inspector.prime_indices(head=0, tail=0)
    report = await run_scenario()
    assert inspector.CAPACITY & (inspector.CAPACITY - 1) == 0
    return report


asyncio.run(demo())
```

The runtime is the Python 3.12 standard library. `pip install -r requirements.txt` succeeds with no third-party packages.

## 🖥️ Terminal Diagnostic Output Preview

```
WARNING sec.sock.inspector drop counter increased monotonically
WARNING sec.sock.inspector consumer stalled; head held while the producer wrote
WARNING sec.sock.inspector ring occupancy 871/1024 near capacity
ERROR sec.sock.inspector corrupted length field rejected on read
INFO sec.sock.inspector scenario complete topic=sec.sock.ring forwarded=False occupancy=880
```

`python src/main.py` exits 0. Stdout reports `forwarded` false, `tail_after_wrap` 0, and `corrupt_rejected` true.

## 📊 Empirical Benchmarking Performance Report

Measured by `python src/test_harness.py` with a deterministic seed, 5000 iterations, `time.perf_counter_ns` latency in microseconds, and `tracemalloc` peak.

| Metric | Measured |
| --- | ---: |
| Status | PASS |
| Iterations | 5000 |
| Average latency | 155.69 µs |
| Empirical P99 | 195.70 µs |
| tracemalloc peak | 172038 bytes |
| Edge: index wrap | PASS |
| Edge: corrupted length | PASS |

## 🛡️ Edge-Case Resilience & SOC2/Regulatory Compliance

Index wrap is masked onto the bytearray and the record written at the wrapped slot is readable. A corrupted length field raises `EngineKernelException` and is not forwarded. The synthetic sockstat text round-trips through `parse_sockstat` and the rendered text does not carry a capability token.

The monitor is aligned with SOC 2 availability and processing integrity for a host that must notice buffer pressure. It does not request `CAP_NET_RAW`, does not open a raw socket, does not craft packets, and does not read the host `/proc/net/sockstat`. Samples are values the caller passed in.
