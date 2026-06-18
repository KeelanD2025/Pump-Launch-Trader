"""Research-only buy strategy architecture helpers.

This package intentionally contains no live trading or wallet execution path.
It consumes counted relay-only R2-primary artifacts and emits research datasets,
gates, reports, and fail-closed harness decisions.
"""

from .schemas import (
    ARCHITECTURE_SCHEMA_VERSION,
    ALLOWED_SIGNAL_DECISIONS,
    FORBIDDEN_SIGNAL_DECISIONS,
    HORIZONS,
    REPO_ROOT,
    STRATEGY_ARCHITECTURE_ROOT,
    STRATEGY_READINESS_ROOT,
    SignalOutput,
)

__all__ = [
    "ARCHITECTURE_SCHEMA_VERSION",
    "ALLOWED_SIGNAL_DECISIONS",
    "FORBIDDEN_SIGNAL_DECISIONS",
    "HORIZONS",
    "REPO_ROOT",
    "STRATEGY_ARCHITECTURE_ROOT",
    "STRATEGY_READINESS_ROOT",
    "SignalOutput",
]
