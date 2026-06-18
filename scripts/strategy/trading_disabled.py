from __future__ import annotations


class DisabledExecutionError(RuntimeError):
    pass


def live_trading_stub(*_args, **_kwargs) -> None:
    raise DisabledExecutionError("LIVE_TRADING_DISABLED")


def wallet_execution_stub(*_args, **_kwargs) -> None:
    raise DisabledExecutionError("WALLET_EXECUTION_DISABLED")


def threshold_tuning_stub(*_args, **_kwargs) -> None:
    raise DisabledExecutionError("THRESHOLD_TUNING_DISABLED")

