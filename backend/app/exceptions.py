class AppException(Exception):
    status_code = 400
    detail = 'Application error'

    def __init__(self, detail: str | None = None) -> None:
        self.detail = detail or self.detail
        super().__init__(self.detail)


class NotFoundError(AppException):
    status_code = 404
    detail = 'Not found'


class ChannelNotFoundError(NotFoundError):
    detail = 'Channel is not found'


class ChannelHasNoUsernameError(AppException):
    detail = 'Channel has no username'


class ChannelEntityTypeError(AppException):
    detail = 'Entity is not a channel or group'


class EmptyChannelIdentifierError(AppException):
    detail = 'Empty channel identifier'


class UserEntityTypeError(AppException):
    detail = 'Entity is not a user'


class PromptNotFoundError(NotFoundError):
    detail = 'Prompt is not found'
