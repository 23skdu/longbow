class LongbowError(Exception):
    """Base exception for all Longbow SDK errors."""
    pass

class LongbowConnectionError(LongbowError):
    """Raised when connection to the server fails."""
    pass

class LongbowAuthenticationError(LongbowError):
    """Raised when authentication fails."""
    pass

class LongbowQueryError(LongbowError):
    """Raised when a query fails (e.g. invalid arguments or server error)."""
    pass

class LongbowNotFoundError(LongbowQueryError):
    """Raised when a requested resource (dataset, namespace) is not found."""
    pass
