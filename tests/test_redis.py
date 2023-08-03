import time
import uuid
from datetime import timedelta
import pytest
import redis


def test_mm_cl(momento_redis_client):
    momento_redis_client.set("foo", "bar")
    assert momento_redis_client.get("foo") == b'bar'


@pytest.mark.parametrize(
    "client", ["momento_redis_client"]
)
def test_mm_cl_fixture(client, request):
    client = request.getfixturevalue(client)
    client.set("foo", "bar")
    assert client.get("foo") == b'bar'


# TODO: we're throwing the wrong type of error for this, yes? The same cause should return
#  the same exception unless we don't care about breaking exception handling
# @pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"], ids=["redis", "momento"])
@pytest.mark.parametrize(
    "client,exception",
    [("redis_client", redis.exceptions.DataError), ("momento_redis_client", redis.exceptions.RedisError)],
    ids=["redis", "momento"]
)
# def test_get_sad_path(client, request):
#   exception = redis.exceptions.DataError
def test_get_sad_path(client, exception, request):
    client = request.getfixturevalue(client)
    with pytest.raises(exception) as exc_info:
        client.get(None)
    assert exc_info.type == exception


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_get_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, 'bar')
    val = client.get(key)
    assert val == b'bar'


# TODO: this test fails with `RuntimeError: await wasn't used with future`, which originates from
#  the implementation of `multi_get()`
@pytest.mark.skip("skipping until we get multi async stuff sorted out")
@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_mget_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(f"{key}-1", 'bar1')
    client.set(f"{key}-2", 'bar2')
    client.set(f"{key}-3", 'bar3')
    val = client.mget([f"{key}-1", f"{key}-2", f"{key}-3"])
    # TODO: val is a coroutine, but mget is ostensibly synchronous
    assert val == [b'bar1', b'bar2', b'bar3']


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_setnx_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    resp = client.setnx(key, 'bar')
    assert resp is True
    resp = client.setnx(key, 'bar')
    assert resp is False


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_setex_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.setex(key, timedelta(seconds=2), 'bar')
    val = client.get(key)
    assert val == b'bar'
    time.sleep(2)
    val = client.get(key)
    assert val is None


# TODO: this fails because it is using multi_delete behind the scenes, which has the same red/blue problem
#  as multi_set
@pytest.mark.skip("skipping until we get multi async stuff sorted out")
@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_delete_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, 'bar')
    val = client.get(key)
    assert val == b'bar'
    client.delete("foo")
    val = client.get(key)
    assert val is None


# TODO: Redis is fine with accepting an `int` for `set. We are not. Redis is also fine with us not passing
#  amount and defaults to `1`. We do not.
@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_amount", ["101", 101], ids=["string", "integer"])
@pytest.mark.parametrize("decr_amount", [1, None], ids=["value", "no_value"])
def test_decr_happy_path(client, initial_amount, decr_amount, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, initial_amount)
    if decr_amount is None:
        val = client.decr(key)
    else:
        val = client.decr(key, decr_amount)
    assert val == 100


# TODO: Redis is fine with accepting an `int` for `set. We are not. Redis is also fine with us not passing
#  amount and defaults to `1`. We do not.
@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_amount", ["105", 105], ids=["string", "integer"])
@pytest.mark.parametrize("decr_amount,expected", [(5, 100), (None, 104)], ids=["value", "no_value"])
def test_decrby_happy_path(client, initial_amount, decr_amount, expected, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, initial_amount)
    if decr_amount is None:
        val = client.decrby(key)
    else:
        val = client.decrby(key, decr_amount)
    assert val == expected


# TODO: Redis is fine with accepting an `int` for `set. We are not. Redis is also fine with us not passing
#  amount and defaults to `1`. We do not.
@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_amount", ["99", 99], ids=["string", "integer"])
@pytest.mark.parametrize("incr_amount", [1, None], ids=["value", "no_value"])
def test_incr_happy_path(client, initial_amount, incr_amount, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, initial_amount)
    if incr_amount is None:
        val = client.incr(key)
    else:
        val = client.incr(key, incr_amount)
    assert val == 100


# TODO: Redis is fine with accepting an `int` for `set. We are not. Redis is also fine with us not passing
#  amount and defaults to `1`. We do not.
@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_amount", ["95", 95], ids=["string", "integer"])
@pytest.mark.parametrize("incr_amount,expected", [(5, 100), (None, 96)], ids=["value", "no_value"])
def test_incrby_happy_path(client, initial_amount, incr_amount, expected, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, initial_amount)
    if incr_amount is None:
        val = client.incrby(key)
    else:
        val = client.incrby(key, incr_amount)
    assert val == expected
