#!/usr/bin/env python3
from __future__ import annotations

import json
import sys

from strategy_pipeline.wallet import wallet_gate


def main() -> int:
    print(json.dumps(wallet_gate().to_dict(), sort_keys=True))
    return 2


if __name__ == "__main__":
    sys.exit(main())
