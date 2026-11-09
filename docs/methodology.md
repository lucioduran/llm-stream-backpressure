# Frozen measurement protocol

The evaluation matrix was committed before measurements. Seed 0 is recorded for
provenance; the producers contain no random choices. Unit fixtures form the development
partition. Evaluation consists of seven named cases, each executed once on local
loopback with no model, remote inference or purchased compute. Thresholds were fixed
before the run. No calibration or tuning against evaluation results is performed.

Each JSONL contains request IDs and monotonic nanosecond observations from actual
running producers, socket writers and clients. First-data latency is measured from
client start, including connection setup, to the first complete received SSE event.
Client close and server disconnect detection are separate timestamps. Residual work
is the count of produced events strictly after each respective timestamp. A partially
started synthetic sleep is not completed work. Transport resets use asyncio abort;
this does not guarantee an OS-level TCP RST on every platform.

Queue occupancy is sampled after every enqueue. Socket write buffering is sampled
after each write, before drain. The encoded frame bound plus high watermark bounds
this user-space buffer for this writer. OS buffer occupancy is not measured.
tracemalloc peak is per case and includes the fixture client retaining event payloads,
raw instrumentation rows, serialization and server. RSS is cumulative process peak,
in bytes on macOS and KiB on Linux; it cannot be attributed to an individual case.
No whole-process constant-memory claim is made. Increasing total retained client
output and trace length increases process memory even when server queues are bounded.

The slow case must show producer enqueue waiting over 1 ms and a longer client
completion time than the fast case. This is an execution sanity check, not a latency
SLO or statistical performance claim. Supported cancellation permits at most one
completed event after server detection; noncancellable work must run its finite budget.

Raw source hashes, matrix hash, source commit, timestamps, Python version, CPU and
RAM identify the run. Replay verifies hashes and recomputes observations. It does not
prove authenticity or rerun a model. The deliberate upstream failure case is retained.
All fixtures, traces and reports are project-owned synthetic data under MIT.

Development observations: initial uv setup failed because default cache and managed
Python directories were not writable. Repository-local UV_CACHE_DIR and
UV_PYTHON_INSTALL_DIR resolved installation. Socket-close review identified that a
stalled peer could delay wait_closed after timeout; the disconnect path now aborts
the local transport before upstream cleanup. Regression tests exercise that path.

The initial installed-wheel regression run failed cancellation equality (5 completed
versus 4 at disconnect detection). Initial measurements likewise observed one residual
event. They are preserved in evidence/initial, with original source hashes and commit.
The source implementation now cancels producer and sender before awaiting either,
removing the scheduling window. This correction responds to a regression test failure;
the frozen evaluation matrix and allowed residual threshold remain unchanged.
Initial source hashes refer to the recorded historical commit, not current sources.

The first public CI run failed during Python setup: ubuntu-latest selected Ubuntu
26.04, which has no actions/python-versions build of pinned Python 3.12.11. CI now
pins Ubuntu 24.04 and macOS 14. No test executed in that failed Linux job. The failed
run is retained at https://github.com/lucioduran/llm-stream-backpressure/actions/runs/42692448091.
The setup-python manifest also lacks macOS Python 3.12.11 builds. CI uses pinned
uv 0.8.23 to install Python 3.12.11, matching the local installer, on both runner images.

CI then exposed a missing `editables` dependency when installing without build
isolation, and the retired macOS 14 runner label was rejected. The development
requirements now pin editables 0.5, and CI uses supported macOS 15. The failed run
is retained at https://github.com/lucioduran/llm-stream-backpressure/actions/runs/42692624826.

The macOS 15 hosted job remained queued without a runner for more than four minutes.
Release CI is limited to Ubuntu 24.04. macOS evidence is the actual local x86_64
measurement and installed-wheel tests; hosted macOS and ARM compatibility are not
claimed. The superseded queued runs were cancelled. This CI-scope adjustment does
not alter the implementation, measurement matrix, raw evidence or replay thresholds.
