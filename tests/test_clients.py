import datetime
import time
import uuid

import pytest
import redis


@pytest.mark.parametrize(
    "client,exception",
    [
        ("redis_client", redis.exceptions.DataError),
        ("momento_redis_client", redis.exceptions.RedisError),
    ],
    ids=["redis", "momento"],
)
def test_get_sad_path(client, exception, request):
    client = request.getfixturevalue(client)
    with pytest.raises(exception) as exc_info:
        client.get(None)
    assert exc_info.type == exception


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_get_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, "bar")
    val = client.get(key)
    assert val == b"bar"


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_get_miss_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_bytes_key_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4()).encode("utf8")
    client.set(key, "bar")
    val = client.get(key)
    assert val == b"bar"


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_int_ex_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, "bar", ex=3)
    val = client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_timespan_ex_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, "bar", ex=datetime.timedelta(seconds=3))
    val = client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_int_px_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, "bar", px=2500)
    val = client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_timespan_px_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, "bar", px=datetime.timedelta(milliseconds=2500))
    val = client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_int_exat_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    secs_now = int(time.time())
    exat = secs_now + 3
    client.set(key, "bar", exat=exat)
    val = client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_set_with_datetime_exat_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    datetime_now = datetime.datetime.now()
    exat = datetime_now + datetime.timedelta(seconds=3)
    client.set(key, "bar", exat=exat)
    val = client.get(key)
    assert val == b"bar"
    time.sleep(3)
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_setnx_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    resp = client.setnx(key, "bar")
    assert resp is True
    resp = client.setnx(key, "bar")
    assert resp is False


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_setnx_bytes_key_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4()).encode("utf8")
    client.setnx(key, "bar")
    val = client.get(key)
    assert val == b"bar"


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_setex_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.setex(key, datetime.timedelta(seconds=2), "bar")
    val = client.get(key)
    assert val == b"bar"
    time.sleep(2)
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_delete_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    client.set(key, "bar")
    val = client.get(key)
    assert val == b"bar"
    client.delete(key)
    val = client.get(key)
    assert val is None


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_delete_multi_happy_path(client, request):
    client = request.getfixturevalue(client)
    keys = [str(uuid.uuid4()) for _ in range(0, 5)]
    keys.append(f"{uuid.uuid4()}-notakey")
    for key in keys:
        client.set(key, "bar")
        val = client.get(key)
        assert val == b"bar"
    val = client.delete(*keys)
    assert val == len(keys)
    for key in keys:
        val = client.get(key)
        assert val is None
