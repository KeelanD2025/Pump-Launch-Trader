#!/usr/bin/env python3
"""Compatibility entrypoint for R2-streaming recovery tests."""

from __future__ import annotations

import pathlib
import runpy


if __name__ == "__main__":
    runpy.run_path(str(pathlib.Path(__file__).with_name("test_r2_streaming_storage.py")), run_name="__main__")
