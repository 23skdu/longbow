from typing import Optional


class LongbowError(Exception):
    """Base exception for all Longbow SDK errors."""


class LongbowConnectionError(LongbowError):
    """Raised when connection to the server fails."""


class LongbowAuthenticationError(LongbowError):
    """Raised when authentication fails (HTTP 401 / gRPC UNAUTHENTICATED).

    Examples include missing credentials, invalid/expired/revoked API keys,
    expired OIDC JWTs, invalid JWKS signatures, or replayed JWT jti values.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = 401,
        code: str = "UNAUTHENTICATED",
        operation: str = "",
        hint: str = "",
    ):
        self.status_code = status_code
        self.code = code
        self.operation = operation
        self.hint = hint
        full_msg = message
        if hint and hint not in message:
            full_msg = f"{message} (hint: {hint})"
        super().__init__(full_msg)


class LongbowAuthorizationError(LongbowAuthenticationError):
    """Raised when an authenticated caller lacks permission (HTTP 403 / gRPC PERMISSION_DENIED).

    Examples include a reader/ingest-only role attempting writes or deletes,
    cross-namespace/tenant isolation violations, or dataset allow-list denials.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = 403,
        code: str = "PERMISSION_DENIED",
        operation: str = "",
        hint: str = "",
    ):
        super().__init__(
            message,
            status_code=status_code,
            code=code,
            operation=operation,
            hint=hint,
        )


LongbowPermissionDeniedError = LongbowAuthorizationError


class LongbowQueryError(LongbowError):
    """Raised when a query fails (e.g. invalid arguments or server error)."""


class LongbowNotFoundError(LongbowQueryError):
    """Raised when a requested resource (dataset, namespace) is not found."""
