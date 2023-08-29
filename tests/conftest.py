from __future__ import annotations

import os
import uuid
from datetime import timedelta

import momento
import pytest
from redis import Redis

from momento_redis import MomentoRedis


@pytest.fixture(scope="session")
def momento_redis_client():  # type: ignore
    cache_name = f"momento-python-redis-client-test-{uuid.uuid4()}"
    with momento.CacheClient.create(
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
    host = os.getenv("TEST_REDIS_HOST", "localhost")
    port = os.getenv("TEST_REDIS_PORT", 6379)
    with Redis(host, int(port), 0) as client:
        try:
            yield client
        finally:
            client.close()
