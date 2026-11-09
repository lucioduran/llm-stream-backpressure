# llm-stream-backpressure

[![CI](https://github.com/lucioduran/llm-stream-backpressure/actions/workflows/ci.yml/badge.svg)](https://github.com/lucioduran/llm-stream-backpressure/actions/workflows/ci.yml)

## Problem

Slow stream consumers can hide growing queues and abandoned upstream work. Closing
a client connection does not establish that a provider has stopped producing tokens.
This project makes those boundaries observable over actual local HTTP/SSE sockets.

## Scope

A zero-runtime-dependency Python transport experiment with bounded frames and queues,
interleaved token/tool events, slow consumers, cancellation, errors and disconnects.
The upstream adapters are deterministic synthetic producers, not language models.
There is no real provider integration or inference performance claim.

## Quick start

The measured environment is CPython 3.12.11. Python 3.12 or later is required.

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pip install -e . --no-build-isolation
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python experiments/replay.py
```

## Usage

Run this with the installed package:

```python
import asyncio
from stream_backpressure import Server, Settings, SyntheticProvider, consume

async def main():
    sessions = []
    async with Server(
        lambda: SyntheticProvider(count=100, delay=0.002),
        Settings(capacity=4),
        on_session=sessions.append,
    ) as server:
        events = await consume(server.port, delay=0.01, stop_after=5)
    print(events)
    print("Completed upstream work:", sessions[0].produced)

asyncio.run(main())
```

Use `supports_cancel=False` on the synthetic adapter to exercise finite upstream work
that cannot be stopped. `fail_at=15` injects a real adapter exception after 15 events.
`consume(..., abort=True)` aborts the client transport. The fixture client collects
its finite output in memory; applications should process events incrementally.

## Architecture

Each connection owns a producer, a bounded FIFO, a drain-aware socket writer and an
EOF monitor. Events carry ordered sequence numbers. Tool events occupy the same FIFO
as token events. Failures become sanitized error events followed by a done event.

On disconnect, cancellable producer and sender tasks are cancelled before awaiting
the adapter cleanup hook. Unsupported cancellation drains remaining upstream work
into a discard consumer until its finite budget is exhausted. See the
[adapter contract and buffering design](docs/design.md).

## Reproduction

The [frozen matrix](experiments/matrix.json) contains seven evaluation cases, fixed
before measurements, with a separate unit-test development partition. Read the
[methodology](docs/methodology.md) for timestamps, thresholds and measurement limits.

Lightweight offline replay uses only Python and the repository, with no network:

```sh
PYTHONPATH=src python3.12 experiments/replay.py
PYTHONPATH=src python3.12 -m unittest discover -s tests -v
```

Replay checks source and trace hashes, recalculates per-event metrics, checks ordered
output, bounded buffering and cleanup. It establishes consistency, not authenticity.
Run without Python's `-O` option because the verifier uses assertions.

In the original coordinated workspace, all new measurements must use the shared lock:

```sh
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
python3 /Users/luchoduran/Sites/open-source-projects/.coordination/benchmark_lock.py \
.venv/bin/python experiments/run.py evidence/new-run
.venv/bin/python experiments/replay.py evidence/new-run
```

Outside that workspace, run `.venv/bin/python experiments/run.py evidence/new-run`
with exclusive local benchmark scheduling. No model downloads or API keys are needed.
The runner records exact source hashes, commit, Python, hardware, timestamps and
configuration. Existing evidence is preserved when a different output directory is used.

## Results

Actual loopback run on November 9, 2026: Intel Xeon W-3223, 16 logical CPUs, 48 GiB RAM,
macOS 26.7.1 x86_64, Python 3.12.11. One run per case with tracing enabled.

| Case | First data ms | Produced / received | Queue peak | Work after server disconnect |
| --- | ---: | ---: | ---: | ---: |
| Fast | 5.49 | 300 / 301 | 4 | 0 |
| Slow | 5.23 | 300 / 301 | 4 | 0 |
| Cancel | 4.40 | 5 / 5 | 1 | 0 |
| Unsupported cancellation | 4.68 | 100 / 5 | 2 | 95 |
| Upstream error | 2.94 | 15 / 17 | 2 | 0 |
| Client abort | 5.92 | 12 / 5 | 4 | 0 |
| Three concurrent requests | 4.78-5.02 | 80 / 81 each | 2 | 0 |

Received counts include terminal error/done events. The slow case took 1,417 ms
versus 479 ms for fast consumption. Maximum producer enqueue wait rose from 2.54 ms
to 15.35 ms. Maximum observed write buffering was 68,233 bytes, below the configured
32,768-byte high watermark plus one 65,536-byte bounded frame.

Slow-case traced Python peak was 18,947,008 bytes, including retained client output
and trace records. This is not server-only memory. Supported cancellation produced
zero additional completed events after detection in this run; unsupported cancellation
produced 95. Client abort still had seven produced-but-unreceived events at close.

[Raw per-event traces and report](evidence/report.json) support these numbers. The
[initial run](evidence/initial/report.json) is retained: it exposed one residual event
in a sender-cleanup scheduling window, also caught by the installed-wheel regression
test. The corrected implementation was measured again with the unchanged matrix.

## Testing

```sh
.venv/bin/ruff check src tests experiments
.venv/bin/ruff format --check src tests experiments
.venv/bin/mypy
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python experiments/replay.py
.venv/bin/python -m build --wheel
python3.12 -m venv .wheel-env
.wheel-env/bin/python -m pip install --no-deps dist/*.whl
.wheel-env/bin/python -m unittest discover -s tests -v
```

Tests exercise actual sockets, interleaved events, queue saturation, write timeout,
disconnect races, concurrent isolation, supported/unsupported cancellation, errors,
oversized frames, malformed requests, truncated responses and evidence tampering.
CI repeats checks and installed-wheel tests on Linux and macOS with Python 3.12.11.

## Limitations

This is a loopback research transport, not a production HTTP server. It accepts only
`GET /events HTTP/1.0`, without TLS, authentication, proxies or keepalive. Socket
buffers can absorb work before backpressure reaches the producer. No portable bound
on total process or kernel memory is established. Adapter objects and instrumentation
must have their own size limits. Unsupported adapters must finish within a finite
work budget; server context exit waits for them. An adapter that violates its contract
can hang cleanup. There is no forced termination guarantee for remote work.

Measurements are single-run observations, affected by scheduling, tracing and other
host activity. They establish neither inference speed nor statistical latency SLOs.
No compute pricing, energy use or provider cancellation behavior was measured.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Keep behavior covered by meaningful socket
tests, preserve negative results, and label synthetic work honestly.

## License

[MIT](LICENSE). Code, synthetic fixtures, raw traces and reports are all licensed
under MIT and contain no private prompts or credentials.
