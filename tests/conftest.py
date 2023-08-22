from __future__ import annotations

import uuid
from datetime import timedelta

import momento
import pytest
from redis import Redis

from momento_python_redis_client.momento_redis_client import MomentoRedis


@pytest.fixture(scope="session")
def momento_redis_client():  # type: ignore
    cache_name = f"momento-python-redis-client-test-{uuid.uuid4()}"
    with momento.CacheClient(
        momento.Configurations.Laptop.latest(),
        momento.CredentialProvider.from_environment_variable("TEST_AUTH_TOKEN"),
        timedelta(seconds=60),
    ) as client:
        client.create_cache(cache_name)
        try:
            yield MomentoRedis(client, cache_name)
        finally:
            client.delete_cache(cache_name)


@pytest.fixture(scope="session")
def redis_client():  # type: ignore
    with Redis("localhost", 6379, 0) as client:
        try:
            yield client
        finally:
            client.close()
