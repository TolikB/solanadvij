"""Custom exceptions and domain-specific error codes."""

class LiveTradingNotImplementedError(RuntimeError):
    """Raised when APP_MODE=live is used before live execution is implemented."""

    code = "LIVE_TRADING_NOT_IMPLEMENTED"

    def __init__(self, message: str = "LIVE mode is intentionally not implemented in this release"):
        super().__init__(message)


class QuoteUnavailableError(RuntimeError):
    """Raised when Jupiter quote API returns no executable route."""


class QuoteStaleError(RuntimeError):
    """Raised when cached or returned quote is expired."""


class RateLimitExceededError(RuntimeError):
    """Raised when external provider repeatedly returns 429."""


class ExecutionBlockedError(RuntimeError):
    """Raised when a request is blocked by risk manager."""
