#!/usr/bin/env python3
from __future__ import annotations

import json
import sys


def main() -> int:
    print(json.dumps({"allowed": False, "blocker": "THRESHOLD_TUNING_DISABLED", "operator_approval_required": True}, sort_keys=True))
    return 2


if __name__ == "__main__":
    sys.exit(main())
