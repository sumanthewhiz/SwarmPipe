"""Error taxonomy with stable, machine-readable error codes.

The engine decides what to do from the exception *type*: retry with backoff, defer without
penalty, pause for a human/child run, stop early, or fail permanently (and dead-letter)."""
from __future__ import annotations


class SwarmError(Exception):
    code = "INTERNAL"
    retryable = False

    def __init__(self, message: str = "", *, code: str | None = None, details: dict | None = None):
        super().__init__(message or self.__class__.__name__)
        if code:
            self.code = code
        self.details = details or {}

    def to_dict(self) -> dict:
        return {"code": self.code, "message": str(self), "retryable": self.retryable, "details": self.details}


class RetryableError(SwarmError):
    code = "RETRYABLE"
    retryable = True


class PermanentError(SwarmError):
    code = "PERMANENT"


class StepTimeout(RetryableError):
    code = "STEP_TIMEOUT"


class Deferred(SwarmError):
    """Re-schedule the run later without consuming a retry attempt (e.g. a dataset lock is busy)."""

    code = "DEFERRED"

    def __init__(self, message: str = "", delay_s: float = 1.0):
        super().__init__(message)
        self.delay_s = delay_s


class BudgetExceeded(SwarmError):
    code = "BUDGET_EXCEEDED"


class QuotaExceeded(SwarmError):
    code = "QUOTA_EXCEEDED"


class LLMUnavailable(SwarmError):
    """All models in the route chain failed (after retries, repairs and fallbacks)."""

    code = "LLM_UNAVAILABLE"


class PolicyDenied(SwarmError):
    code = "POLICY_DENIED"


class KillSwitchEngaged(SwarmError):
    code = "KILL_SWITCH"


class GuardrailViolation(SwarmError):
    code = "GUARDRAIL_VIOLATION"


class AuthorizationError(SwarmError):
    code = "FORBIDDEN"


class ToolError(SwarmError):
    code = "TOOL_ERROR"


class WaitingFor(Exception):
    """Durable interrupt: the run pauses until `waiting_on` resolves (approval:<id> | children | run:<id>)."""

    def __init__(self, waiting_on: str, reason: str = ""):
        super().__init__(reason or waiting_on)
        self.waiting_on = waiting_on
        self.reason = reason


class StopWorkflow(Exception):
    """Finish the run early with a terminal status (e.g. duplicate file -> skipped)."""

    def __init__(self, status: str, reason: str = "", output: dict | None = None):
        super().__init__(reason or status)
        self.status = status
        self.reason = reason
        self.output = output or {}
