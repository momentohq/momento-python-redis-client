"""Momento Python Redis Client."""
from __future__ import annotations

import datetime
import time
from typing import Generic, Optional, TypeVar, Union

from momento import CacheClient
from momento.errors import UnknownException
from momento.responses import (
    CacheDelete,
    CacheGet,
    CacheIncrement,
    CacheSet,
    CacheSetIfNotExists,
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


class MomentoRedis(
    AbstractRedis, RedisModuleCommands, CoreCommands, SentinelCommands, Generic[_StrType]  # type: ignore
):
    def __init__(self, client: CacheClient, cache_name: str):
        self.client = client
        self.cache_name = cache_name

    def get(self, name: KeyT) -> Optional[bytes]:
        rsp = self.client.get(self.cache_name, name)
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
            nx_rsp = self.client.set_if_not_exists(self.cache_name, key=name, value=value, ttl=ttl)
            if isinstance(nx_rsp, CacheSetIfNotExists.Error):
                raise convert_momento_to_redis_errors(nx_rsp)
            elif isinstance(nx_rsp, CacheSetIfNotExists.NotStored):
                return False
            elif isinstance(nx_rsp, CacheSetIfNotExists.Stored):
                return True
            else:
                raise UnknownException(f"Unknown response type: {nx_rsp}")
        else:
            rsp = self.client.set(self.cache_name, name, value, ttl)
            if isinstance(rsp, CacheSet.Error):
                raise convert_momento_to_redis_errors(rsp)
            elif isinstance(rsp, CacheSet.Success):
                return True
            else:
                raise UnknownException(f"Unknown response type: {rsp}")

    def setnx(self, name: KeyT, value: EncodableT) -> bool:
        if not isinstance(value, (str, bytes)):
            value = str(value)
        rsp = self.client.set_if_not_exists(self.cache_name, key=name, value=value)
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
        rsp = self.client.set(self.cache_name, name, value, ttl=time)  # type: ignore
        if isinstance(rsp, CacheSet.Error):
            raise convert_momento_to_redis_errors(rsp)
        return True

    def delete(self, *names: KeyT) -> int:
        num_deleted = 0
        for name in names:
            rsp = self.client.delete(self.cache_name, name)
            if isinstance(rsp, CacheDelete.Success):
                num_deleted += 1
        return num_deleted

    def decrby(self, name: KeyT, amount: int = 1) -> int:
        rsp = self.client.increment(self.cache_name, name, -amount)
        if isinstance(rsp, CacheIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)
        else:
            raise UnknownException(f"Unknown response type: {rsp}")

    decr = decrby

    def incrby(self, name: KeyT, amount: int = 1) -> int:
        rsp = self.client.increment(self.cache_name, name, amount)
        if isinstance(rsp, CacheIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)
        else:
            raise UnknownException(f"Unknown response type: {rsp}")

    incr = incrby
