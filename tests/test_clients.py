import time
import uuid
from datetime import timedelta
import pytest
import redis


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


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hset_hget_keyval_happy_path(client, request):
    client = request.getfixturevalue(client)
    key = str(uuid.uuid4())
    map_name = str(uuid.uuid4())
    num_set = client.hset(map_name, key, 'bar')
    assert num_set == 1
    val = client.hget(map_name, key)
    assert val == b'bar'


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hset_hmget_mapping_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    mapping = {b"a": b"b", b"c": b"d"}
    num_set = client.hset(map_name, mapping=mapping)
    assert num_set == 2
    val = client.hmget(map_name, mapping.keys())
    assert val == list(mapping.values())


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hset_hgetall_mapping_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    mapping = {b"a": b"b", b"c": b"d"}
    num_set = client.hset(map_name, mapping=mapping)
    assert num_set == 2
    val = client.hgetall(map_name)
    assert val == mapping


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hset_hget_items_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    items = [b"a", b"b", b"c", b"d"]
    num_set = client.hset(map_name, items=items)
    assert num_set == 2
    val = client.hget(map_name, b"a")
    assert val == b"b"


# TODO: Redis accepts the multi-type inputs here without a problem. We choke on anything
#  that isn't bytes|str on the way in.
@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hset_hgetall_all_hset_params_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    items = [b"a", b"b", b"c", b"d"]
    mapping = {1: 2, 3: 4}
    key = str(uuid.uuid4())
    val = str(uuid.uuid4())
    # Redis returns the hash with all binary keys and values. Here we convert everything to binary
    # for comparison.
    expected_items = {items[i]: items[i+1] for i in range(0, len(items), 2)}
    expected_items.update({bytes(str(k), encoding='utf8'): bytes(str(v), encoding='utf8') for k, v in mapping.items()})
    expected_items.update({bytes(str(key), encoding='utf8'): bytes(str(val), encoding='utf8')})

    num_set = client.hset(map_name, key, val, mapping=mapping, items=items)
    assert num_set == 5
    val = client.hgetall(map_name)
    print(val)
    assert val == expected_items


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hmset_hget_keyval_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    mapping = {b"a": b"b", b"c": b"d"}
    val = client.hmset(map_name, mapping)
    assert val is True
    val = client.hget(map_name, b"a")
    assert val == b"b"


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hmset_hmget_mapping_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    mapping = {b"a": b"b", b"c": b"d"}
    val = client.hmset(map_name, mapping=mapping)
    assert val is True
    val = client.hmget(map_name, mapping.keys())
    assert val == list(mapping.values())


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hmset_hgetall_mapping_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    mapping = {b"a": b"b", b"c": b"d"}
    val = client.hmset(map_name, mapping)
    assert val is True
    val = client.hgetall(map_name)
    assert val == mapping


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hkeys_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    mapping = {b"a": b"b", b"c": b"d"}
    val = client.hset(map_name, mapping=mapping)
    assert val == 2
    keys = client.hkeys(map_name)
    assert keys == list(mapping.keys())


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_hdel_happy_path(client, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    mapping = {b"a": b"b", b"c": b"d"}
    val = client.hset(map_name, mapping=mapping)
    assert val == 2
    val = client.hdel(map_name, b"a")
    assert val == 1
    val = client.hgetall(map_name)
    assert val == {b"c": b"d"}


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("mapping", [{b"a": b"b", b"c": b"d"}, {"a": "b", "c": "d"}])
@pytest.mark.parametrize("to_delete", [(b"a", b"c"), ("a", "c")])
def test_hdel_multiple_keys_happy_path(client, mapping, to_delete, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    val = client.hset(map_name, mapping=mapping)
    assert val == 2
    val = client.hdel(map_name, *to_delete)
    assert val == 2
    val = client.hgetall(map_name)
    assert val == {}


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_value", [99, "99"], ids=["int", "str"])
def test_hincrby_no_amount_happy_path(client, initial_value, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    key_name = str(uuid.uuid4())
    val = client.hset(map_name, key_name, initial_value)
    assert val == 1
    val = client.hincrby(map_name, key_name)
    assert val == 100


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("initial_value", [99, "99"], ids=["int", "str"])
@pytest.mark.parametrize("incr_value", [1, "1"], ids=["int", "str"])
def test_hincrby_with_amount_happy_path(client, initial_value, incr_value, request):
    client = request.getfixturevalue(client)
    map_name = str(uuid.uuid4())
    key_name = str(uuid.uuid4())
    val = client.hset(map_name, key_name, initial_value)
    assert val == 1
    val = client.hincrby(map_name, key_name, incr_value)
    assert val == 100


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize(
    "members",
    [
        {f"{i}" for i in range(0, 10)},
        {f"{i}".encode("utf8") for i in range(0, 10)}
    ],
    ids=["string", "bytes"]
)
def test_sadd_smembers_happy_path(client, members, request):
    client = request.getfixturevalue(client)
    # we always want the set back with binary members
    expected = {f"{i}".encode("utf8") for i in range(0, 10)}
    set_name = str(uuid.uuid4())
    val = client.sadd(set_name, *members)
    assert val == len(members)
    val = client.smembers(set_name)
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_sadd_smembers_ints_happy_path(client, request):
    client = request.getfixturevalue(client)
    members = {i for i in range(0, 10)}
    expected = {str(i).encode("utf8") for i in range(0, 10)}
    set_name = str(uuid.uuid4())
    val = client.sadd(set_name, *members)
    assert val == len(members)
    val = client.smembers(set_name)
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_srem_happy_path(client, request):
    client = request.getfixturevalue(client)
    members = {f"{i}".encode("utf8") for i in range(0, 10)}
    set_name = str(uuid.uuid4())
    val = client.sadd(set_name, *members)
    assert val == len(members)
    to_remove = {f"{i}".encode("utf8") for i in range(5, 8)}
    val = client.srem(set_name, *to_remove)
    for item in to_remove:
        members.remove(item)
    assert val == len(to_remove)
    the_set = client.smembers(set_name)
    assert the_set == members


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_zadd_zrange_happy_path(client, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}": float(i) for i in range(1, 11)}
    expected = [(f"score-{i}".encode("utf8"), float(i)) for i in range(2, 5)]
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zrange(sorted_set_name, 1, 3, False, True)
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_zincrby_happy_path(client, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}": float(i) for i in range(1, 11)}
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zincrby(sorted_set_name, float(4.5), "score-1")
    assert val == 5.5
    val = client.zincrby(sorted_set_name, float(-4.5), "score-1")
    assert val == 1.0


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_zrem_happy_path(client, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}".encode("utf8"): float(i) for i in range(1, 11)}
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zrem(sorted_set_name, "score-2", "score-4", "score-6")
    assert val == 3
    mapping.pop("score-2".encode("utf8"))
    mapping.pop("score-4".encode("utf8"))
    mapping.pop("score-6".encode("utf8"))
    val = client.zrange(sorted_set_name, 0, 100)
    assert val == [k for k in mapping.keys()]


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("withscores", [
    (True, [(f"score-{i}".encode("utf8"), float(i)) for i in range(2, 5)]),
    (False, [f"score-{i}".encode("utf8") for i in range(2, 5)])
 ], ids=["withscores", "withoutscores"])
def test_zrange_withscores_happy_path(client, withscores, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}": float(i) for i in range(1, 11)}
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zrange(sorted_set_name, 1, 3, False, withscores[0])
    assert val == withscores[1]


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_zrange_desc_happy_path(client, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}": float(i) for i in range(1, 11)}
    expected = [(f"score-{i}".encode("utf8"), float(i)) for i in range(7, 10)]
    expected.reverse()
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zrange(sorted_set_name, 1, 3, True, True)
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_zrange_byscore_happy_path(client, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}": float(i*20) for i in range(1, 11)}
    expected = [(f"score-{i}".encode("utf8"), float(i*20)) for i in range(2, 5)]
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zrange(sorted_set_name, 40, 80, False, True, byscore=True)
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("withscores", [
    (True, [(f"score-{i}".encode("utf8"), float(i*20)) for i in range(2, 5)]),
    (False, [f"score-{i}".encode("utf8") for i in range(2, 5)]),
])
def test_zrangebyscore_happy_path(client, withscores, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}": float(i*20) for i in range(1, 11)}
    expected = withscores[1]
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zrangebyscore(sorted_set_name, 40, 80, withscores=withscores[0])
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("withscores", [
    (True, [(f"score-{i}".encode("utf8"), float(i*20)) for i in range(10, 0, -1)]),
    (False, [f"score-{i}".encode("utf8") for i in range(10, 0, -1)]),
])
def test_zrevrange_happy_path(client, withscores, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}": float(i*20) for i in range(1, 11)}
    expected = withscores[1]
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zrevrange(sorted_set_name, 0, 1000, withscores=withscores[0])
    assert val == withscores[1]


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
@pytest.mark.parametrize("withscores", [
    (True, [(f"score-{i}".encode("utf8"), float(i*20)) for i in range(4, 1, -1)]),
    (False, [f"score-{i}".encode("utf8") for i in range(4, 1, -1)]),
])
def test_zrevrangebyscore_happy_path(client, withscores, request):
    client = request.getfixturevalue(client)
    mapping = {f"score-{i}": float(i*20) for i in range(1, 11)}
    expected = withscores[1]
    sorted_set_name = str(uuid.uuid4())
    val = client.zadd(sorted_set_name, mapping)
    assert val == len(mapping)
    val = client.zrevrangebyscore(sorted_set_name, 80, 40, withscores=withscores[0])
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_lpush_happy_path(client, request):
    client = request.getfixturevalue(client)
    sorted_set_name = str(uuid.uuid4())
    expected = [i.encode("utf8") for i in ["one", "two", "three"]]
    val = client.lpush(sorted_set_name, *expected)
    assert val == len(expected)
    val = client.lrange(sorted_set_name, 0, 100)
    # reverse expected since we're pushing to front
    expected.reverse()
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_rpush_happy_path(client, request):
    client = request.getfixturevalue(client)
    sorted_set_name = str(uuid.uuid4())
    expected = [i.encode("utf8") for i in ["one", "two", "three"]]
    val = client.rpush(sorted_set_name, *expected)
    assert val == len(expected)
    val = client.lrange(sorted_set_name, 0, 100)
    assert val == expected


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_lpop_happy_path(client, request):
    client = request.getfixturevalue(client)
    sorted_set_name = str(uuid.uuid4())
    the_list = ["one", "two", "three"]
    val = client.rpush(sorted_set_name, *the_list)
    assert val == len(the_list)
    val = client.lpop(sorted_set_name)
    assert val == the_list[0].encode("utf8")


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_lpop_happy_path(client, request):
    client = request.getfixturevalue(client)
    sorted_set_name = str(uuid.uuid4())
    the_list = ["one", "two", "three"]
    val = client.rpush(sorted_set_name, *the_list)
    assert val == len(the_list)
    val = client.rpop(sorted_set_name)
    assert val == the_list[-1].encode("utf8")


@pytest.mark.parametrize("client", ["redis_client", "momento_redis_client"])
def test_llen_happy_path(client, request):
    client = request.getfixturevalue(client)
    sorted_set_name = str(uuid.uuid4())
    the_list = ["one", "two", "three"]
    val = client.rpush(sorted_set_name, *the_list)
    assert val == len(the_list)
    val = client.llen(sorted_set_name)
    assert val == len(the_list)

