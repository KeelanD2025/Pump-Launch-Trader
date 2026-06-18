"""Fail-closed trading strategy pipeline architecture.

The package intentionally contains architecture and readiness gates only. It
does not run replay, formal backtesting, threshold tuning, paper trading, live
trading, wallet execution, or real order submission.
"""

from .schemas import PIPELINE_ROOT, REPO_ROOT, STRATEGY_ARCHITECTURE_ROOT

__all__ = ["PIPELINE_ROOT", "REPO_ROOT", "STRATEGY_ARCHITECTURE_ROOT"]
