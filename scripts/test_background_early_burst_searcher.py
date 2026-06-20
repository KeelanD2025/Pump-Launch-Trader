#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import types
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("run_background_early_burst_searcher.py")
SPEC = importlib.util.spec_from_file_location("background_searcher", SCRIPT)
assert SPEC and SPEC.loader
background_searcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(background_searcher)


class BackgroundEarlyBurstSearcherTests(unittest.TestCase):
    def test_start_blocks_when_relay_control_preflight_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with mock.patch.object(background_searcher, "STATUS_ROOT", root):
                with mock.patch.object(background_searcher, "STATUS_PATH", root / "status.json"):
                    with mock.patch.object(background_searcher, "LIVE_SUMMARY_PATH", root / "live_summary.md"):
                        with mock.patch.object(background_searcher, "EVENTS_PATH", root / "events.ndjson"):
                            with mock.patch.object(background_searcher, "REVIEW_QUEUE_PATH", root / "review_queue.csv"):
                                with mock.patch.object(
                                    background_searcher,
                                    "run_preflight",
                                    return_value={
                                        "classification": "RELAY_CONTROL_CONFIG_BLOCK_MISSING_ENV",
                                        "ok": False,
                                        "missing_config_names": ["PUMP_RELAY_VPS_SSH_TARGET"],
                                    },
                                ):
                                    with mock.patch.object(background_searcher.subprocess, "Popen") as popen:
                                        args = types.SimpleNamespace(
                                            control_env=root / "missing.env",
                                            slices=10,
                                            duration_seconds=900,
                                            justification_id="targeted-early-burst-sample-20260620-001",
                                            max_slices=10,
                                            run_prefix="relay-r2-early-burst-targeted",
                                        )
                                        rc = background_searcher.start(args)
                                        self.assertEqual(rc, 2)
                                        self.assertFalse(popen.called)
                                        status = json.loads((root / "status.json").read_text())
                                        self.assertEqual(status["state"], "blocked")
                                        self.assertEqual(
                                            status["preflight_classification"],
                                            "RELAY_CONTROL_CONFIG_BLOCK_MISSING_ENV",
                                        )
                                        self.assertFalse(status["replay_allowed"])
                                        self.assertFalse(status["formal_backtesting_allowed"])
                                        self.assertTrue(status["launch_caps_remain_blocked"])

    def test_status_reports_not_started_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with mock.patch.object(background_searcher, "STATUS_PATH", root / "status.json"):
                with mock.patch.object(background_searcher, "LIVE_SUMMARY_PATH", root / "live_summary.md"):
                    args = types.SimpleNamespace()
                    rc = background_searcher.status(args)
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
