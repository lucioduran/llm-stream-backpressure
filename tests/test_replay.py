import contextlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "experiments"))
from replay import ROOT, verify  # noqa: E402


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        for path in (ROOT / "evidence").glob("*.*"):
            shutil.copy(path, self.folder / path.name)

    def test_saved_evidence(self):
        with contextlib.redirect_stdout(io.StringIO()):
            verify(self.folder)

    def test_raw_tampering_rejected(self):
        with (self.folder / "fast.jsonl").open("a") as handle:
            handle.write("{}\n")
        with self.assertRaises(AssertionError):
            verify(self.folder)

    def test_summary_tampering_rejected(self):
        path = self.folder / "report.json"
        report = json.loads(path.read_text())
        report["cases"]["fast"]["requests"][0]["produced"] += 1
        path.write_text(json.dumps(report))
        with self.assertRaises(AssertionError):
            verify(self.folder)
