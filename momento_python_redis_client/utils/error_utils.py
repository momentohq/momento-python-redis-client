from momento import errors
from momento.responses.mixins import ErrorResponseMixin
from redis import exceptions as rex


def convert_momento_to_redis_errors(err: ErrorResponseMixin) -> rex.RedisError:
    match err.inner_exception:
        case errors.TimeoutException:
            return rex.TimeoutError(rex.RedisError(err.inner_exception))
        case errors.AuthenticationException:
            return rex.AuthenticationError(rex.ConnectionError(rex.RedisError(err.inner_exception)))
        case errors.ServerUnavailableException:
            return rex.ConnectionError(rex.RedisError(err.inner_exception))
        case _:
            return rex.RedisError(err.inner_exception)

