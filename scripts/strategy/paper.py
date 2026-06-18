from __future__ import annotations

from .trading_disabled import DisabledExecutionError


def paper_trading_stub(*_args, **_kwargs) -> None:
    raise DisabledExecutionError("PAPER_TRADING_DISABLED")

