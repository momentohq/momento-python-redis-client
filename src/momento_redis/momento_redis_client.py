"""Momento Python Redis Client."""
from __future__ import annotations

import datetime
import time
from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar, Union

from momento import CacheClient
from momento.errors import UnknownException
from momento.responses import (
    CacheDelete,
    CacheGet,
    CacheIncrement,
    CacheSet,
    CacheSetIfNotExists,
    CreateCache,
)
from redis.client import AbstractRedis
from redis.commands import CoreCommands, RedisModuleCommands, SentinelCommands
from redis.typing import AbsExpiryT, EncodableT, ExpiryT, KeyT

from .utils.error_utils import convert_momento_to_redis_errors

NOT_IMPL_ERR = (
    " is not yet implemented in MomentoRedisClient. Please drop by our Discord at "
    "https://discord.com/invite/3HkAKjUZGq , or contact us at support@momentohq.com, and let us know what "
    "APIs you need!"
)

_StrType = TypeVar("_StrType", bound=Union[str, bytes])


class MomentoRedisBase(ABC):
    @abstractmethod
    def get(self, name: KeyT) -> Optional[bytes]:
        pass

    def set(
        self,
        name: KeyT,
        value: EncodableT,
        ex: Union[ExpiryT, None] = None,
        px: Union[ExpiryT, None] = None,
        nx: bool = False,
        xx: bool = False,  # not implemented
        keepttl: bool = False,  # not implemented
        get: bool = False,  # not implemented
        exat: Union[AbsExpiryT, None] = None,
        pxat: Union[AbsExpiryT, None] = None,
    ) -> bool | None:
        pass

    def setnx(self, name: KeyT, value: EncodableT) -> bool:
        pass

    def setex(self, name: KeyT, time: ExpiryT, value: EncodableT) -> bool:
        pass

    def delete(self, *names: KeyT) -> int:
        pass

    def decrby(self, name: KeyT, amount: int = 1) -> int:
        pass

    def decr(self, name: KeyT, amount: int = 1) -> int:
        pass

    def incrby(self, name: KeyT, amount: int = 1) -> int:
        pass

    def incr(self, name: KeyT, amount: int = 1) -> int:
        pass


class MomentoRedis(
    AbstractRedis,
    RedisModuleCommands,
    CoreCommands,  # type: ignore
    SentinelCommands,
    Generic[_StrType],
    MomentoRedisBase,
):
    def __init__(self, client: CacheClient, cache_name: str):
        self._client = client
        self._cache_name = cache_name
        rsp = self._client.create_cache(cache_name)
        if isinstance(rsp, CreateCache.Error):
            raise Exception(rsp.inner_exception)

    def get(self, name: KeyT) -> Optional[bytes]:
        rsp = self._client.get(self._cache_name, name)
        if isinstance(rsp, CacheGet.Hit):
            return rsp.value_bytes
        elif isinstance(rsp, CacheGet.Miss):
            return None
        elif isinstance(rsp, CacheGet.Error):
            raise convert_momento_to_redis_errors(rsp)
        else:
            raise UnknownException(f"Unknown response type: {rsp}")

    def set(
        self,
        name: KeyT,
        value: EncodableT,
        ex: Union[ExpiryT, None] = None,
        px: Union[ExpiryT, None] = None,
        nx: bool = False,
        xx: bool = False,  # not implemented
        keepttl: bool = False,  # not implemented
        get: bool = False,  # not implemented
        exat: Union[AbsExpiryT, None] = None,
        pxat: Union[AbsExpiryT, None] = None,
    ) -> bool | None:

        if xx:
            raise NotImplementedError("SetOption XX" + NOT_IMPL_ERR)

        if get:
            raise NotImplementedError("SetOption GET" + NOT_IMPL_ERR)

        if keepttl:
            raise NotImplementedError("SetOption KEEPTTL" + NOT_IMPL_ERR)

        if isinstance(value, (float, int)):
            value = str(value)

        ttl: Optional[datetime.timedelta] = None
        if ex is not None:
            if isinstance(ex, int):
                ttl = datetime.timedelta(seconds=ex)
            elif isinstance(ex, datetime.timedelta):
                ttl = ex
            else:
                raise UnknownException(f"Unknown type for ex: {type(ex)}")
        elif px is not None:
            if isinstance(px, int):
                ttl = datetime.timedelta(seconds=int(px / 1000))
            elif isinstance(px, datetime.timedelta):
                ttl = px
        elif exat is not None:
            # TODO: is this anywhere close to correct?
            if isinstance(exat, int):
                ttl = datetime.timedelta(seconds=exat - time.time())
            elif isinstance(exat, datetime.datetime):
                ttl = exat - datetime.datetime.now()
        elif pxat is not None:
            # TODO: is this anywhere close to correct? I don't see how this could be implemented
            #  differently from exat at all?
            if isinstance(pxat, int):
                ttl = datetime.timedelta(seconds=pxat - time.time())
            else:
                ttl = pxat - datetime.datetime.now()

        if nx:
            nx_rsp = self._client.set_if_not_exists(self._cache_name, key=name, value=value, ttl=ttl)
            if isinstance(nx_rsp, CacheSetIfNotExists.Error):
                raise convert_momento_to_redis_errors(nx_rsp)
            elif isinstance(nx_rsp, CacheSetIfNotExists.NotStored):
                return False
            elif isinstance(nx_rsp, CacheSetIfNotExists.Stored):
                return True
            else:
                raise UnknownException(f"Unknown response type: {nx_rsp}")
        else:
            rsp = self._client.set(self._cache_name, name, value, ttl)
            if isinstance(rsp, CacheSet.Error):
                raise convert_momento_to_redis_errors(rsp)
            elif isinstance(rsp, CacheSet.Success):
                return True
            else:
                raise UnknownException(f"Unknown response type: {rsp}")

    def setnx(self, name: KeyT, value: EncodableT) -> bool:
        if not isinstance(value, (str, bytes)):
            value = str(value)
        rsp = self._client.set_if_not_exists(self._cache_name, key=name, value=value)
        if isinstance(rsp, CacheSetIfNotExists.Stored):
            return True
        elif isinstance(rsp, CacheSetIfNotExists.NotStored):
            return False
        elif isinstance(rsp, CacheSetIfNotExists.Error):
            raise convert_momento_to_redis_errors(rsp)
        else:
            raise UnknownException(f"Unknown response type: {rsp}")

    def setex(self, name: KeyT, time: ExpiryT, value: EncodableT) -> bool:
        if isinstance(time, int):
            time = datetime.timedelta(seconds=time)
        if not isinstance(value, (str, bytes)):
            value = str(value)
        rsp = self._client.set(self._cache_name, name, value, ttl=time)  # type: ignore
        if isinstance(rsp, CacheSet.Error):
            raise convert_momento_to_redis_errors(rsp)
        return True

    def delete(self, *names: KeyT) -> int:
        num_deleted = 0
        for name in names:
            rsp = self._client.delete(self._cache_name, name)
            if isinstance(rsp, CacheDelete.Success):
                num_deleted += 1
        return num_deleted

    def decrby(self, name: KeyT, amount: int = 1) -> int:
        rsp = self._client.increment(self._cache_name, name, -amount)
        if isinstance(rsp, CacheIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)
        else:
            raise UnknownException(f"Unknown response type: {rsp}")

    decr = decrby

    def incrby(self, name: KeyT, amount: int = 1) -> int:
        rsp = self._client.increment(self._cache_name, name, amount)
        if isinstance(rsp, CacheIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)
        else:
            raise UnknownException(f"Unknown response type: {rsp}")

    incr = incrby

    # Unimplemented methods
    def _not_implemented(self, name: str) -> None:
        raise NotImplementedError(f"{name}{NOT_IMPL_ERR}")

    def acl_cat(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_cat")

    def acl_deluser(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_deluser")

    def acl_dryrun(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_dryrun")

    def acl_genpass(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_genpass")

    def acl_getuser(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_getuser")

    def acl_help(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_help")

    def acl_list(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_list")

    def acl_load(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_load")

    def acl_log(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_log")

    def acl_log_reset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_log_reset")

    def acl_save(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_save")

    def acl_setuser(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_setuser")

    def acl_users(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_users")

    def acl_whoami(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("acl_whoami")

    def append(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("append")

    def auth(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("auth")

    def bf(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bf")

    def bgrewriteaof(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bgrewriteaof")

    def bgsave(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bgsave")

    def bitcount(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bitcount")

    def bitfield(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bitfield")

    def bitfield_ro(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bitfield_ro")

    def bitop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bitop")

    def bitpos(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bitpos")

    def blmove(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("blmove")

    def blmpop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("blmpop")

    def blpop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("blpop")

    def brpop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("brpop")

    def brpoplpush(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("brpoplpush")

    def bzmpop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bzmpop")

    def bzpopmax(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bzpopmax")

    def bzpopmin(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("bzpopmin")

    def cf(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("cf")

    def client_getname(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_getname")

    def client_getredir(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_getredir")

    def client_id(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_id")

    def client_info(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_info")

    def client_kill(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_kill")

    def client_kill_filter(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_kill_filter")

    def client_list(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_list")

    def client_no_evict(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_no_evict")

    def client_no_touch(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_no_touch")

    def client_pause(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_pause")

    def client_reply(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_reply")

    def client_setname(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_setname")

    def client_tracking(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_tracking")

    def client_tracking_off(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_tracking_off")

    def client_tracking_on(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_tracking_on")

    def client_trackinginfo(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_trackinginfo")

    def client_unblock(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_unblock")

    def client_unpause(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("client_unpause")

    def cluster(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("cluster")

    def cms(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("cms")

    def command(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("command")

    def command_count(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("command_count")

    def command_docs(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("command_docs")

    def command_getkeys(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("command_getkeys")

    def command_getkeysandflags(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("command_getkeysandflags")

    def command_info(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("command_info")

    def command_list(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("command_list")

    def config_get(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("config_get")

    def config_resetstat(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("config_resetstat")

    def config_rewrite(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("config_rewrite")

    def config_set(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("config_set")

    def copy(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("copy")

    def dbsize(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("dbsize")

    def debug_object(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("debug_object")

    def debug_segfault(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("debug_segfault")

    def dump(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("dump")

    def echo(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("echo")

    def eval(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("eval")

    def eval_ro(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("eval_ro")

    def evalsha(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("evalsha")

    def evalsha_ro(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("evalsha_ro")

    def execute_command(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("execute_command")

    def exists(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("exists")

    def expire(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("expire")

    def expireat(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("expireat")

    def expiretime(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("expiretime")

    def failover(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("failover")

    def fcall(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("fcall")

    def fcall_ro(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("fcall_ro")

    def flushall(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("flushall")

    def flushdb(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("flushdb")

    def ft(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("ft")

    def function_delete(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("function_delete")

    def function_dump(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("function_dump")

    def function_flush(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("function_flush")

    def function_kill(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("function_kill")

    def function_list(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("function_list")

    def function_load(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("function_load")

    def function_restore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("function_restore")

    def function_stats(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("function_stats")

    def geoadd(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("geoadd")

    def geodist(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("geodist")

    def geohash(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("geohash")

    def geopos(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("geopos")

    def georadius(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("georadius")

    def georadiusbymember(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("georadiusbymember")

    def geosearch(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("geosearch")

    def geosearchstore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("geosearchstore")

    def getbit(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("getbit")

    def getdel(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("getdel")

    def getex(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("getex")

    def getrange(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("getrange")

    def getset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("getset")

    def graph(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("graph")

    def hdel(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hdel")

    def hello(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hello")

    def hexists(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hexists")

    def hget(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hget")

    def hgetall(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hgetall")

    def hincrby(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hincrby")

    def hincrbyfloat(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hincrbyfloat")

    def hkeys(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hkeys")

    def hlen(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hlen")

    def hmget(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hmget")

    def hmset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hmset")

    def hrandfield(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hrandfield")

    def hscan(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hscan")

    def hscan_iter(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hscan_iter")

    def hset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hset")

    def hsetnx(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hsetnx")

    def hstrlen(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hstrlen")

    def hvals(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("hvals")

    def incrbyfloat(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("incrbyfloat")

    def info(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("info")

    def json(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("json")

    def keys(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("keys")

    def lastsave(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lastsave")

    def latency_doctor(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("latency_doctor")

    def latency_graph(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("latency_graph")

    def latency_histogram(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("latency_histogram")

    def latency_history(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("latency_history")

    def latency_latest(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("latency_latest")

    def latency_reset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("latency_reset")

    def lcs(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lcs")

    def lindex(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lindex")

    def linsert(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("linsert")

    def llen(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("llen")

    def lmove(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lmove")

    def lmpop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lmpop")

    def lolwut(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lolwut")

    def lpop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lpop")

    def lpos(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lpos")

    def lpush(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lpush")

    def lpushx(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lpushx")

    def lrange(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lrange")

    def lrem(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lrem")

    def lset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("lset")

    def ltrim(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("ltrim")

    def memory_doctor(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("memory_doctor")

    def memory_help(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("memory_help")

    def memory_malloc_stats(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("memory_malloc_stats")

    def memory_purge(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("memory_purge")

    def memory_stats(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("memory_stats")

    def memory_usage(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("memory_usage")

    def mget(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("mget")

    def migrate(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("migrate")

    def module_list(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("module_list")

    def module_load(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("module_load")

    def module_loadex(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("module_loadex")

    def module_unload(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("module_unload")

    def move(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("move")

    def mset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("mset")

    def msetnx(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("msetnx")

    def object(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("object")

    def persist(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("persist")

    def pexpire(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pexpire")

    def pexpireat(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pexpireat")

    def pexpiretime(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pexpiretime")

    def pfadd(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pfadd")

    def pfcount(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pfcount")

    def pfmerge(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pfmerge")

    def ping(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("ping")

    def psetex(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("psetex")

    def psync(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("psync")

    def pttl(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pttl")

    def publish(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("publish")

    def pubsub_channels(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pubsub_channels")

    def pubsub_numpat(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pubsub_numpat")

    def pubsub_numsub(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("pubsub_numsub")

    def quit(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("quit")

    def randomkey(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("randomkey")

    def readonly(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("readonly")

    def readwrite(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("readwrite")

    def register_script(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("register_script")

    def rename(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("rename")

    def renamenx(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("renamenx")

    def replicaof(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("replicaof")

    def reset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("reset")

    def restore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("restore")

    def role(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("role")

    def rpop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("rpop")

    def rpoplpush(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("rpoplpush")

    def rpush(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("rpush")

    def rpushx(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("rpushx")

    def sadd(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sadd")

    def save(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("save")

    def scan(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("scan")

    def scan_iter(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("scan_iter")

    def scard(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("scard")

    def script_debug(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("script_debug")

    def script_exists(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("script_exists")

    def script_flush(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("script_flush")

    def script_kill(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("script_kill")

    def script_load(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("script_load")

    def sdiff(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sdiff")

    def sdiffstore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sdiffstore")

    def select(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("select")

    def sentinel(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel")

    def sentinel_ckquorum(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_ckquorum")

    def sentinel_failover(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_failover")

    def sentinel_flushconfig(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_flushconfig")

    def sentinel_get_master_addr_by_name(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_get_master_addr_by_name")

    def sentinel_master(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_master")

    def sentinel_masters(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_masters")

    def sentinel_monitor(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_monitor")

    def sentinel_remove(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_remove")

    def sentinel_reset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_reset")

    def sentinel_sentinels(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_sentinels")

    def sentinel_set(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_set")

    def sentinel_slaves(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sentinel_slaves")

    def setbit(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("setbit")

    def setrange(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("setrange")

    def shutdown(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("shutdown")

    def sinter(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sinter")

    def sintercard(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sintercard")

    def sinterstore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sinterstore")

    def sismember(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sismember")

    def slaveof(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("slaveof")

    def slowlog_get(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("slowlog_get")

    def slowlog_len(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("slowlog_len")

    def slowlog_reset(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("slowlog_reset")

    def smembers(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("smembers")

    def smismember(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("smismember")

    def smove(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("smove")

    def sort(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sort")

    def sort_ro(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sort_ro")

    def spop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("spop")

    def srandmember(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("srandmember")

    def srem(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("srem")

    def sscan(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sscan")

    def sscan_iter(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sscan_iter")

    def stralgo(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("stralgo")

    def strlen(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("strlen")

    def substr(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("substr")

    def sunion(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sunion")

    def sunionstore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sunionstore")

    def swapdb(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("swapdb")

    def sync(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("sync")

    def tdigest(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("tdigest")

    def time(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("time")

    def topk(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("topk")

    def touch(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("touch")

    def ts(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("ts")

    def ttl(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("ttl")

    def type(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("type")

    def unlink(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("unlink")

    def unwatch(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("unwatch")

    def wait(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("wait")

    def waitaof(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("waitaof")

    def watch(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("watch")

    def xack(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xack")

    def xadd(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xadd")

    def xautoclaim(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xautoclaim")

    def xclaim(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xclaim")

    def xdel(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xdel")

    def xgroup_create(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xgroup_create")

    def xgroup_createconsumer(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xgroup_createconsumer")

    def xgroup_delconsumer(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xgroup_delconsumer")

    def xgroup_destroy(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xgroup_destroy")

    def xgroup_setid(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xgroup_setid")

    def xinfo_consumers(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xinfo_consumers")

    def xinfo_groups(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xinfo_groups")

    def xinfo_stream(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xinfo_stream")

    def xlen(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xlen")

    def xpending(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xpending")

    def xpending_range(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xpending_range")

    def xrange(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xrange")

    def xread(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xread")

    def xreadgroup(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xreadgroup")

    def xrevrange(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xrevrange")

    def xtrim(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("xtrim")

    def zadd(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zadd")

    def zcard(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zcard")

    def zcount(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zcount")

    def zdiff(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zdiff")

    def zdiffstore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zdiffstore")

    def zincrby(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zincrby")

    def zinter(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zinter")

    def zintercard(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zintercard")

    def zinterstore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zinterstore")

    def zlexcount(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zlexcount")

    def zmpop(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zmpop")

    def zmscore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zmscore")

    def zpopmax(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zpopmax")

    def zpopmin(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zpopmin")

    def zrandmember(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrandmember")

    def zrange(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrange")

    def zrangebylex(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrangebylex")

    def zrangebyscore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrangebyscore")

    def zrangestore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrangestore")

    def zrank(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrank")

    def zrem(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrem")

    def zremrangebylex(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zremrangebylex")

    def zremrangebyrank(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zremrangebyrank")

    def zremrangebyscore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zremrangebyscore")

    def zrevrange(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrevrange")

    def zrevrangebylex(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrevrangebylex")

    def zrevrangebyscore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrevrangebyscore")

    def zrevrank(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zrevrank")

    def zscan(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zscan")

    def zscan_iter(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zscan_iter")

    def zscore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zscore")

    def zunion(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zunion")

    def zunionstore(self, *args, **kwargs) -> None:  # type: ignore
        self._not_implemented("zunionstore")
