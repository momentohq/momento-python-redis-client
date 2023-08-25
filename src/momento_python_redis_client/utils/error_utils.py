from momento import errors
from momento.responses.mixins import ErrorResponseMixin
from redis import exceptions as rex


def convert_momento_to_redis_errors(err: ErrorResponseMixin) -> rex.RedisError:
    if isinstance(err.inner_exception, errors.TimeoutException):
        return rex.TimeoutError(rex.RedisError(err.inner_exception))
    elif isinstance(err.inner_exception, errors.AuthenticationException):
        return rex.AuthenticationError(rex.ConnectionError(rex.RedisError(err.inner_exception)))
    elif isinstance(err.inner_exception, errors.ServerUnavailableException):
        return rex.ConnectionError(rex.RedisError(err.inner_exception))
    else:
        return rex.RedisError(err.inner_exception)
