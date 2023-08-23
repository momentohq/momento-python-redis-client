import datetime
import os
import time
import uuid
from typing import Optional, Union

import pytest
import redis
from _pytest.fixtures import FixtureRequest

from momento_python_redis_client.momento_redis_client import MomentoRedis

TClient = Union[MomentoRedis, redis.Redis]

skip_redis = os.getenv("TEST_SKIP_REDIS", False)
skip_momento = os.getenv("TEST_SKIP_MOMENTO", False)


def _use_client(client: str) -> bool:
    if client == "momento_redis_client" and skip_momento:
        return False
    elif client == "redis_client" and skip_redis:
        return False
    return True


@pytest.mark.parametrize(
    "client,exception",
    [
        ("redis_client", redis.exceptions.DataError),
        ("momento_redis_client", redis.exceptions.RedisError),
    ],
    ids=["redis", "momento"],
)
def test_get_sad_path(client: str, exception: Exception, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    with pytest.raises(Exception) as exc_info:
        test_client.get(None)  # type: ignore
    assert exc_info.type == exception  # type: ignore


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_get_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, "bar")
    val = test_client.get(key)
    assert val == b"bar"


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_get_miss_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_bytes_key_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4()).encode("utf8")
    test_client.set(key, "bar")
    val = test_client.get(key)
    assert val == b"bar"


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_int_ex_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, "bar", ex=2)
    val = test_client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_timespan_ex_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, "bar", ex=datetime.timedelta(seconds=2))
    val = test_client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_int_px_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, "bar", px=2500)
    val = test_client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_timespan_px_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, "bar", px=datetime.timedelta(milliseconds=2500))
    val = test_client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_int_exat_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    secs_now = int(time.time())
    exat = secs_now + 2
    test_client.set(key, "bar", exat=exat)
    val = test_client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_datetime_exat_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    datetime_now = datetime.datetime.now()
    exat = datetime_now + datetime.timedelta(seconds=2)
    test_client.set(key, "bar", exat=exat)
    val = test_client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_setnx_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    resp = test_client.setnx(key, "bar")
    assert resp is True
    resp = test_client.setnx(key, "bar")
    assert resp is False


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_setnx_bytes_key_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4()).encode("utf8")
    test_client.setnx(key, "bar")
    val = test_client.get(key)
    assert val == b"bar"


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_setex_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.setex(key, datetime.timedelta(seconds=2), "bar")
    val = test_client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_delete_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, "bar")
    val = test_client.get(key)
    assert val == b"bar"
    test_client.delete(key)
    val = test_client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_delete_multi_happy_path(client: str, request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    keys = [str(uuid.uuid4()) for _ in range(0, 5)]
    keys.append(f"{uuid.uuid4()}-notakey")
    for key in keys:
        test_client.set(key, "bar")
        val = test_client.get(key)
        assert val == b"bar"
    val = test_client.delete(*keys)
    assert val == len(keys)
    for key in keys:
        val = test_client.get(key)
        assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_amount", ["101", 101], ids=["string", "integer"])
@pytest.mark.parametrize("decr_amount", [1, None], ids=["value", "no_value"])
def test_decr_happy_path(client: str, initial_amount: int, decr_amount: Optional[int], request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, initial_amount)
    if decr_amount is None:
        val = test_client.decr(key)
    else:
        val = test_client.decr(key, decr_amount)
    assert val == 100


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_amount", ["105", 105], ids=["string", "integer"])
@pytest.mark.parametrize("decr_amount,expected", [(5, 100), (None, 104)], ids=["value", "no_value"])
def test_decrby_happy_path(
    client: str, initial_amount: int, decr_amount: Optional[int], expected: int, request: FixtureRequest
) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, initial_amount)
    if decr_amount is None:
        val = test_client.decrby(key)
    else:
        val = test_client.decrby(key, decr_amount)
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_amount", ["99", 99], ids=["string", "integer"])
@pytest.mark.parametrize("incr_amount", [1, None], ids=["value", "no_value"])
def test_incr_happy_path(client: str, initial_amount: int, incr_amount: Optional[int], request: FixtureRequest) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, initial_amount)
    if incr_amount is None:
        val = test_client.incr(key)
    else:
        val = test_client.incr(key, incr_amount)
    assert val == 100


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_amount", ["95", 95], ids=["string", "integer"])
@pytest.mark.parametrize("incr_amount,expected", [(5, 100), (None, 96)], ids=["value", "no_value"])
def test_incrby_happy_path(
    client: str, initial_amount: int, incr_amount: Optional[int], expected: int, request: FixtureRequest
) -> None:
    if not _use_client(client):
        return
    test_client: TClient = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    test_client.set(key, initial_amount)
    if incr_amount is None:
        val = test_client.incrby(key)
    else:
        val = test_client.incrby(key, incr_amount)
    assert val == expected
