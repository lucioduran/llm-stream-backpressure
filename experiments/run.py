"""Run only under the shared benchmark lock when coordinating local projects."""

import asyncio
import hashlib
import json
import os
import platform
import resource
import subprocess
import sys
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path

from stream_backpressure import Server, Settings, SyntheticProvider, consume

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(rows):
    start = next(r["time_ns"] for r in rows if r["action"] == "start")
    received = [r for r in rows if r["action"] == "received"]
    produced = [r for r in rows if r["action"] == "produced"]
    close = next(r["time_ns"] for r in rows if r["action"] == "client_close")
    disconnected = [r for r in rows if r["action"] == "disconnect"]
    disconnect = disconnected[0]["time_ns"] if disconnected else None
    return {
        "produced": len(produced),
        "received": len(received),
        "first_data_ms": (received[0]["time_ns"] - start) / 1e6 if received else None,
        "client_duration_ms": (close - start) / 1e6,
        "residual_after_client_close": sum(r["time_ns"] > close for r in produced),
        "residual_after_server_disconnect": sum(
            r["time_ns"] > disconnect for r in produced
        )
        if disconnect
        else 0,
        "disconnect_detection_ms": (disconnect - close) / 1e6 if disconnect else None,
        "queue_peak": max(
            (r["queue"] for r in rows if r["action"] == "enqueued"), default=0
        ),
        "max_enqueue_wait_ms": max(
            (r["wait_ns"] / 1e6 for r in rows if r["action"] == "enqueued"), default=0
        ),
        "max_drain_ms": max(
            (r["drain_seconds"] * 1000 for r in rows if r["action"] == "sent"),
            default=0,
        ),
        "error_events": sum(r["kind"] == "error" for r in received),
    }


async def run(output):
    matrix_path = ROOT / "experiments/matrix.json"
    matrix = json.loads(matrix_path.read_text())
    settings = Settings(
        **{
            k: v
            for k, v in matrix["thresholds"].items()
            if k != "supported_residual_max"
        }
    )
    output.mkdir(parents=True, exist_ok=True)
    report = {
        "schema": 1,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "code_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "matrix_sha256": digest(matrix_path),
        "source_sha256": {
            str(p.relative_to(ROOT)): digest(p)
            for p in sorted((ROOT / "src").rglob("*.py"))
        }
        | {"experiments/run.py": digest(Path(__file__))},
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "hardware": subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string", "hw.memsize"], text=True
        ).strip()
        if sys.platform == "darwin"
        else platform.processor(),
        "threads": {
            name: os.getenv(name)
            for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS")
        },
        "method": "One local run per frozen case; monotonic event timestamps; tracemalloc includes server, retained client payloads and instrumentation. No LLM inference. No statistical confidence claim.",
        "cases": {},
    }
    for case in matrix["cases"]:
        name = case["name"]
        raw = output / f"{name}.jsonl"
        sessions = []
        counter = 0
        rows = []
        tracemalloc.start()
        with raw.open("w") as handle:

            def trace(row):
                rows.append(row)
                handle.write(json.dumps(row, sort_keys=True) + "\n")

            def attached(session):
                nonlocal counter
                index = counter
                counter += 1
                session.trace = lambda row: trace({"request": index, **row})
                sessions.append(session)

            kwargs = {
                key: case[key]
                for key in ("count", "payload", "delay", "supports_cancel", "fail_at")
                if key in case
            }
            async with Server(
                lambda: SyntheticProvider(**kwargs), settings, attached
            ) as server:

                async def client(index):
                    trace(
                        {
                            "request": index,
                            "action": "start",
                            "time_ns": time.monotonic_ns(),
                        }
                    )
                    await consume(
                        server.port,
                        delay=case["client_delay"],
                        stop_after=case.get("stop_after"),
                        abort=case.get("abort", False),
                        trace=lambda row: trace({"request": index, **row}),
                    )

                await asyncio.wait_for(
                    asyncio.gather(
                        *(client(i) for i in range(case.get("concurrency", 1)))
                    ),
                    30,
                )
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        report["cases"][name] = {
            "raw_sha256": digest(raw),
            "python_traced_peak_bytes": peak,
            "process_peak_rss_native_units": resource.getrusage(
                resource.RUSAGE_SELF
            ).ru_maxrss,
            "requests": [
                summarize([r for r in rows if r["request"] == i])
                | {
                    "write_buffer_peak": s.write_buffer_peak,
                    "finished": s.finished.is_set(),
                }
                for i, s in enumerate(sessions)
            ],
        }
        print(name, json.dumps(report["cases"][name]), flush=True)
    report["finished_utc"] = datetime.now(timezone.utc).isoformat()
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    asyncio.run(run(Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "evidence"))
