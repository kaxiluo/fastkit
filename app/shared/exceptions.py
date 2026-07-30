class AppError(Exception):
    code: str = "internal_error"

    def __init__(self, message: str = "", *, code: str | None = None):
        super().__init__(message)
        if code:
            self.code = code


class NotFoundError(AppError):
    code = "not_found"


class ConflictError(AppError):
    code = "conflict"


class ValidationError(AppError):
    code = "validation_error"


class BusinessError(AppError):
    code = "business_error"
