"""Offline consistency checks, not proof of measurement authenticity."""

import json
import sys
from pathlib import Path

from run import ROOT, digest, summarize


def verify(folder):
    report = json.loads((folder / "report.json").read_text())
    matrix = json.loads((ROOT / "experiments/matrix.json").read_text())
    assert report["matrix_sha256"] == digest(ROOT / "experiments/matrix.json")
    for filename, sha in report["source_sha256"].items():
        assert digest(ROOT / filename) == sha, filename
    for case in matrix["cases"]:
        name = case["name"]
        saved = report["cases"][name]
        raw = folder / f"{name}.jsonl"
        assert saved["raw_sha256"] == digest(raw), name
        rows = [json.loads(line) for line in raw.read_text().splitlines()]
        for index, request in enumerate(saved["requests"]):
            selected = [r for r in rows if r["request"] == index]
            assert all(
                a["time_ns"] <= b["time_ns"] for a, b in zip(selected, selected[1:])
            )
            calculated = summarize(selected)
            for key, value in calculated.items():
                assert request[key] == value, (name, key)
            assert request["queue_peak"] <= matrix["thresholds"]["capacity"]
            assert (
                request["write_buffer_peak"]
                <= matrix["thresholds"]["write_high"]
                + matrix["thresholds"]["max_event_bytes"]
            )
            assert request["finished"]
            received = [r for r in selected if r["action"] == "received"]
            data = [r for r in received if r["kind"] in {"token", "tool"}]
            assert [r["sequence"] for r in data] == list(range(len(data)))
            assert all(
                r["kind"] == ("tool" if r["sequence"] % 7 == 3 else "token")
                for r in data
            )
            if "stop_after" not in case:
                assert received[-1]["kind"] == "done"
                assert len(data) == case.get("fail_at", case["count"])
            else:
                assert len(received) == case["stop_after"]
            if case.get("supports_cancel", True):
                assert (
                    request["residual_after_server_disconnect"]
                    <= matrix["thresholds"]["supported_residual_max"]
                )
            else:
                assert request["produced"] == case["count"]
                assert request["residual_after_server_disconnect"] > 0
            assert request["error_events"] == int("fail_at" in case)
        print(f"PASS {name}: hashes, raw metric replay, bounds and cleanup")
    fast = report["cases"]["fast"]["requests"][0]
    slow = report["cases"]["slow"]["requests"][0]
    assert slow["client_duration_ms"] > fast["client_duration_ms"]
    assert slow["max_enqueue_wait_ms"] > 1
    print("PASS end-to-end slow consumer propagated waiting to producer")


if __name__ == "__main__":
    verify(Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "evidence")
