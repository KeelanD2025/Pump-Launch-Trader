#!/usr/bin/env python3
from __future__ import annotations

import json
import sys


def main() -> int:
    print(json.dumps({"allowed": False, "blocker": "LIVE_TRADING_DISABLED", "wallet_blocker": "WALLET_EXECUTION_DISABLED"}, sort_keys=True))
    return 2


if __name__ == "__main__":
    sys.exit(main())
