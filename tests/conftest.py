from __future__ import annotations

import uuid
from datetime import timedelta
import momento
import redis

from momento_python_redis_client.momento_redis_wrapper import MomentoRedis
import pytest


@pytest.fixture(scope="session")
def momento_redis_client():
    cache_name = f"momento-python-redis-client-test-{uuid.uuid4()}"
    with momento.CacheClient(
        momento.Configurations.Laptop.latest(),
        momento.CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
        timedelta(seconds=60)
    ) as client:
        client.create_cache(cache_name)
        try:
            yield MomentoRedis(client, cache_name)
        finally:
            client.delete_cache(cache_name)


@pytest.fixture(scope="session")
def redis_client():
    yield redis.Redis("localhost", 6379, 0)
