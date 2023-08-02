from __future__ import annotations

import builtins
import time
from datetime import timedelta, datetime
from typing import Generic, TypeVar, Union, Optional, Any, Iterable, Awaitable, Literal, Callable, List

from momento import CacheClient
from momento.requests import SortOrder
from momento.responses import CacheGet, CacheSetIfNotExists, CacheIncrement, CacheDictionaryGetField, \
    CacheDictionaryFetch, CacheDictionarySetField, CacheDictionarySetFields, CacheDictionaryGetFields, \
    CacheDictionaryRemoveFields, CacheDictionaryIncrement, CacheSetAddElements, CacheSetFetch, CacheSetRemoveElements, \
    CacheListPushFront, CacheListPushBack, CacheListPopFront, CacheListPopBack, CacheListLength, CacheSet, \
    CacheSortedSetPutElements, CacheSortedSetGetRank, CacheSortedSetFetch, CacheListFetch
from momento.responses.data.sorted_set.increment import CacheSortedSetIncrement
from momento.responses.data.sorted_set.remove_elements import CacheSortedSetRemoveElements
from redis.client import AbstractRedis
from redis.commands import RedisModuleCommands, CoreCommands, SentinelCommands

from utils.error_utils import convert_momento_to_redis_errors
from utils.momento_multi_utils import multi_delete, multi_get, multi_set

_StrType = TypeVar("_StrType", bound=Union[str, bytes])

NOT_IMPL_ERR = "is not yet implemented in MomentoRedisClient. Please drop by our Discord at " \
               "https://discord.com/invite/3HkAKjUZGq , or contact us at support@momentohq.com, and let us know what " \
               "APIs you need!"


class MomentoRedis(AbstractRedis, RedisModuleCommands, CoreCommands[_StrType], SentinelCommands, Generic[_StrType]):
    def __init__(self, client: CacheClient, cache_name: str):
        self.client = client
        self.cache_name = cache_name
        pass

    def get(self, name) -> Optional[_StrType]:
        rsp = self.client.get("default", name)
        if isinstance(rsp, CacheGet.Hit):
            return rsp.value_string
        elif isinstance(rsp, CacheGet.Miss):
            return None
        elif isinstance(rsp, CacheGet.Error):
            raise convert_momento_to_redis_errors(rsp)

    def mget(self, keys: _StrType | Iterable[_StrType], *args: _StrType) -> list[_StrType | None]:
        return multi_get(self.client, self.cache_name, keys)

    def set(
            self,
            name,
            value,
            ex: None | int | timedelta = ...,
            px: None | int | timedelta = ...,
            nx: bool = ...,
            xx: bool = ...,
            keepttl: bool = ...,
            get: bool = ...,
            exat: Any | None = ...,
            pxat: Any | None = ...,
    ) -> bool | None:

        ttl: int | None = None
        if ex is not None:
            ttl = ex
        elif px is not None:
            ttl = int(px / 1000)
        elif exat is not None:
            ttl = exat - int(time.time())
        elif pxat is not None:
            ttl = pxat - int(time.time() / 1000)
        elif keepttl:
            raise NotImplementedError("SetOption KEEPTTL" + NOT_IMPL_ERR)

        if nx:
            return self.setnx(name, value)
        elif xx:
            raise NotImplementedError("SetOption XX" + NOT_IMPL_ERR)

        if get:
            raise NotImplementedError("SetOption GET" + NOT_IMPL_ERR)

        rsp = self.client.set(self.cache_name, name, value, ttl)
        if isinstance(rsp, CacheSet.Error):
            raise convert_momento_to_redis_errors(rsp)

        return True

    def mset(self, mapping) -> Literal[True]:
        return multi_set(self.client, self.cache_name, mapping)

    def setnx(self, name, value) -> bool:
        rsp = self.client.set_if_not_exists(self.cache_name, key=name, value=value)
        if isinstance(rsp, CacheSetIfNotExists.Stored):
            return True
        elif isinstance(rsp, CacheSetIfNotExists.NotStored):
            return False
        elif isinstance(rsp, CacheSetIfNotExists.Error):
            raise convert_momento_to_redis_errors(rsp)

    def setex(self, name, time: int | timedelta, value) -> bool:
        rsp = self.client.set(self.cache_name, name, value, time)
        if isinstance(rsp, CacheSet.Error):
            raise convert_momento_to_redis_errors(rsp)
        return True

    def delete(self, *names) -> int:
        return len(multi_delete(self.client, self.cache_name, [i for i in names]))

    def decr(self, name, amount: int = ...) -> int:
        rsp = self.client.increment(
            self.cache_name,
            name,
            -amount
        )
        if isinstance(rsp, CacheIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)

    def decrby(self, name, amount: int = ...) -> int:
        rsp = self.client.increment(
            self.cache_name,
            name,
            -amount
        )
        if isinstance(rsp, CacheIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)

    def incr(self, name, amount: int = ...) -> int:
        rsp = self.client.increment(
            self.cache_name,
            name,
            amount
        )
        if isinstance(rsp, CacheIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)

    def incrby(self, name, amount: int = ...) -> int:
        rsp = self.client.increment(
            self.cache_name,
            name,
            amount
        )
        if isinstance(rsp, CacheIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)

    def hget(self, name, key) -> _StrType | None:
        rsp = self.client.dictionary_get_field(self.cache_name, name, key)
        if isinstance(rsp, CacheDictionaryGetField.Hit):
            return rsp.value_string
        elif isinstance(rsp, CacheDictionaryGetField.Miss):
            return None
        elif isinstance(rsp, CacheDictionaryGetField.Error):
            raise convert_momento_to_redis_errors(rsp)

    def hmget(self, name, keys: _StrType | Iterable[_StrType], *args: _StrType) -> list[_StrType | None]:
        rsp = self.client.dictionary_get_fields(self.cache_name, name, keys)
        if isinstance(rsp, CacheDictionaryGetFields.Hit):
            return list(rsp.value_dictionary_string_string.values())

    def hgetall(self, name) -> dict[_StrType, _StrType]:
        rsp = self.client.dictionary_fetch(self.cache_name, name)
        if isinstance(rsp, CacheDictionaryFetch.Hit):
            return rsp.value_dictionary_string_string
        elif isinstance(rsp, CacheDictionaryFetch.Miss):
            return {}
        elif isinstance(rsp, CacheDictionaryFetch.Error):
            raise convert_momento_to_redis_errors(rsp)

    def hset(
            self,
            name: str,
            key: Optional[str] = None,
            value: Optional[str] = None,
            mapping: Optional[dict] = None,
            items: Optional[list] = None,
    ) -> int:
        items_to_set = {}
        if key is not None:
            items_to_set.update([(key, value)])

        if mapping is not None:
            items_to_set.update(mapping)

        if items is not None:
            items_to_set.update(dict(items))

        rsp = self.client.dictionary_set_fields(self.cache_name, name, items_to_set)
        if isinstance(rsp, CacheDictionarySetFields.Success):
            return len(items_to_set)
        elif isinstance(rsp, CacheDictionarySetFields.Error):
            raise convert_momento_to_redis_errors(rsp)

    def hmset(self, name, mapping) -> bool:
        rsp = self.client.dictionary_set_fields(self.cache_name, name, mapping)
        if isinstance(rsp, CacheDictionarySetFields.Success):
            return True
        elif isinstance(rsp, CacheDictionarySetFields.Error):
            raise convert_momento_to_redis_errors(rsp)

    def hkeys(self, name) -> list[_StrType]:
        # TODO once have api to just fetch keys use that instead
        rsp = self.client.dictionary_fetch(self.cache_name, name)
        if isinstance(rsp, CacheDictionaryFetch.Hit):
            return list(rsp.value_dictionary_string_string.keys())
        elif isinstance(rsp, CacheDictionaryFetch.Miss):
            return []
        elif isinstance(rsp, CacheDictionaryFetch.Error):
            raise convert_momento_to_redis_errors(rsp)

    def hdel(self, name, *keys: List) -> int:
        # TODO: Ugh. Make sure keys are getting handled properly here.
        rsp = self.client.dictionary_remove_fields(self.cache_name, name, *keys)
        if isinstance(rsp, CacheDictionaryRemoveFields.Success):
            return len(keys)
        elif isinstance(rsp, CacheDictionaryRemoveFields.Error):
            raise convert_momento_to_redis_errors(rsp)

    def hincrby(self, name, key, amount: int = ...) -> int:
        rsp = self.client.dictionary_increment(self.cache_name, name, key, amount)
        if isinstance(rsp, CacheDictionaryIncrement.Success):
            return rsp.value
        elif isinstance(rsp, CacheDictionaryIncrement.Error):
            raise convert_momento_to_redis_errors(rsp)

    def sadd(self, name, *values) -> int:
        rsp = self.client.set_add_elements(self.cache_name, name, values)
        if isinstance(rsp, CacheSetAddElements.Success):
            return len(values)
        elif isinstance(rsp, CacheSetAddElements.Error):
            raise convert_momento_to_redis_errors(rsp)

    def smembers(self, name) -> builtins.set[_StrType]:
        rsp = self.client.set_fetch(self.cache_name, name)
        if isinstance(rsp, CacheSetFetch.Hit):
            return rsp.value_set_string
        elif isinstance(rsp, CacheSetFetch.Error):
            raise convert_momento_to_redis_errors(rsp)

    def srem(self, name, *values) -> int:
        rsp = self.client.set_remove_elements(self.cache_name, name, values)
        if isinstance(rsp, CacheSetRemoveElements.Success):
            return len([values])
        elif isinstance(rsp, CacheSetRemoveElements.Error):
            raise convert_momento_to_redis_errors(rsp)

    def zadd(
            self,
            name,
            mapping,
            nx: bool = ...,
            xx: bool = ...,
            ch: bool = ...,
            incr: bool = ...,
            gt: Any | None = ...,
            lt: Any | None = ...,
    ) -> int:
        if nx:
            raise NotImplementedError("SortedSetAddOption NX" + NOT_IMPL_ERR)
        elif xx:
            raise NotImplementedError("SortedSetAddOption XX" + NOT_IMPL_ERR)
        elif ch:
            raise NotImplementedError("SortedSetAddOption CH" + NOT_IMPL_ERR)
        elif incr is not None:
            raise NotImplementedError("SortedSetAddOption incr" + NOT_IMPL_ERR)
        elif gt is not None:
            raise NotImplementedError("SortedSetAddOption INCR" + NOT_IMPL_ERR)
        elif lt is not None:
            raise NotImplementedError("SortedSetAddOption LT" + NOT_IMPL_ERR)

        rsp = self.client.sorted_set_put_elements(self.cache_name, name, mapping)
        if isinstance(rsp, CacheSortedSetPutElements.Success):
            # Always return count of all added redis only returns count new items added
            return len(mapping)
        elif isinstance(rsp, CacheSortedSetPutElements.Error):
            raise convert_momento_to_redis_errors(rsp)

    def zincrby(self, name, amount: float, value) -> float:
        rsp = self.client.sorted_set_increment(self.cache_name, name, value, amount)
        if isinstance(rsp, CacheSortedSetIncrement.Success):
            return rsp.score
        elif isinstance(rsp, CacheSortedSetIncrement.Error):
            convert_momento_to_redis_errors(rsp)

    def zrem(self, name, *values) -> int:
        rsp = self.client.sorted_set_remove_elements(self.cache_name, name, values)
        if isinstance(rsp, CacheSortedSetRemoveElements.Success):
            return len(values)
        elif isinstance(rsp, CacheSortedSetRemoveElements.Error):
            raise convert_momento_to_redis_errors(rsp)

    def zrange(
            self,
            name,
            start: int,
            end: int,
            desc: bool,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], float | int],
            byscore: bool = ...,
            bylex: bool = ...,
            offset: int | None = ...,
            num: int | None = ...,
    ) -> list[tuple[_StrType, float | int]] | list[_StrType]:

        sort_order = SortOrder.ASCENDING
        if desc:
            sort_order = sort_order.DESCENDING

        if byscore:
            rsp = self.client.sorted_set_fetch_by_score(
                cache_name=self.cache_name,
                sorted_set_name=name,
                min_score=start,
                max_score=end,
                sort_order=SortOrder.DESCENDING,
                offset=start,
                count=num
            )
        else:
            rsp = self.client.sorted_set_fetch_by_rank(
                cache_name=self.cache_name,
                sorted_set_name=name,
                start_rank=start,
                end_rank=end,
                sort_order=sort_order
            )

        if isinstance(rsp, CacheSortedSetFetch.Hit):
            if withscores:
                return rsp.value_list_string
            else:
                return [v[0] for v in rsp.value_list_string]
        elif isinstance(rsp, CacheSortedSetFetch.Miss):
            return []
        elif isinstance(rsp, CacheSortedSetFetch.Error):
            raise convert_momento_to_redis_errors(rsp)

    def zrangebyscore(
            self,
            name,
            min,
            max,
            start: int | None = ...,
            num: int | None = ...,
            *,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], float | int],
    ) -> list[tuple[_StrType, float | int]] | list[_StrType]:
        rsp = self.client.sorted_set_fetch_by_score(
            cache_name=self.cache_name,
            sorted_set_name=name,
            min_score=min,
            max_score=max,
            sort_order=SortOrder.DESCENDING,
            offset=start,
            count=num
        )
        if isinstance(rsp, CacheSortedSetFetch.Hit):
            if withscores:
                return rsp.value_list_string
            else:
                return [v[0] for v in rsp.value_list_string]
        elif isinstance(rsp, CacheSortedSetFetch.Miss):
            return []
        elif isinstance(rsp, CacheSortedSetFetch.Error):
            raise convert_momento_to_redis_errors(rsp)

    def zrevrange(
            self,
            name,
            start: int,
            end: int,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], float | int],
    ) -> list[tuple[_StrType, float | int]] | list[_StrType]:
        rsp = self.client.sorted_set_fetch_by_rank(
            self.cache_name,
            name,
            start_rank=start,
            end_rank=end,
            sort_order=SortOrder.ASCENDING
        )
        if isinstance(rsp, CacheSortedSetFetch.Hit):
            if withscores:
                return rsp.value_list_string
            else:
                return [v[0] for v in rsp.value_list_string]
        elif isinstance(rsp, CacheSortedSetFetch.Miss):
            return []
        elif isinstance(rsp, CacheSortedSetFetch.Error):
            raise convert_momento_to_redis_errors(rsp)

    def zrevrangebyscore(
            self,
            name,
            max,
            min,
            start: int | None = ...,
            num: int | None = ...,
            *,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], float | int],
    ) -> list[tuple[_StrType, float | int]] | list[_StrType]:
        rsp = self.client.sorted_set_fetch_by_score(
            cache_name=self.cache_name,
            sorted_set_name=name,
            min_score=min,
            max_score=max,
            sort_order=SortOrder.ASCENDING,
            offset=start,
            count=num
        )
        if isinstance(rsp, CacheSortedSetFetch.Hit):
            if withscores:
                return rsp.value_list_string
            else:
                return [v[0] for v in rsp.value_list_string]
        elif isinstance(rsp, CacheSortedSetFetch.Miss):
            return []
        elif isinstance(rsp, CacheSortedSetFetch.Error):
            raise convert_momento_to_redis_errors(rsp)

    def lpush(self, name, *values) -> int:
        rsp = self.client.list_push_front(self.cache_name, name, *values)
        if isinstance(rsp, CacheListPushFront.Success):
            return len(values)
        elif isinstance(rsp, CacheListPushFront.Error):
            raise convert_momento_to_redis_errors(rsp)

    def rpush(self, name, *values) -> int:
        rsp = self.client.list_push_back(self.cache_name, name, *values)
        if isinstance(rsp, CacheListPushBack.Success):
            return len(values)
        elif isinstance(rsp, CacheListPushBack.Error):
            raise convert_momento_to_redis_errors(rsp)

    def lpop(self, name, count: int | None = ...):
        if count is not None:
            # FIXME our API only supports popping single item off list for now
            raise NotImplementedError("Cannot pop more then 1 element off list for now")

        rsp = self.client.list_pop_front(self.cache_name, name)
        if isinstance(rsp, CacheListPopFront.Hit):
            return rsp.value_string
        elif isinstance(rsp, CacheListPopFront.Miss):
            return None
        elif isinstance(rsp, CacheListPopFront.Error):
            raise convert_momento_to_redis_errors(rsp)

    def rpop(self, name, count: int | None = ...):
        if count is not None:
            # FIXME our API only supports popping single item off list for now
            raise NotImplementedError("Cannot pop more then 1 element off list for now")

        rsp = self.client.list_pop_back(self.cache_name, name)
        if isinstance(rsp, CacheListPopBack.Hit):
            return rsp.value_string
        elif isinstance(rsp, CacheListPopBack.Miss):
            return None
        elif isinstance(rsp, CacheListPopBack.Error):
            raise convert_momento_to_redis_errors(rsp)

    def llen(self, name) -> int:
        rsp = self.client.list_length(self.cache_name, name)
        if isinstance(rsp, CacheListLength.Hit):
            return rsp.length
        elif isinstance(rsp, CacheListLength.Miss):
            return 0
        elif isinstance(rsp, CacheListLength.Error):
            raise convert_momento_to_redis_errors(rsp)

    def lrange(self, name, start: int, end: int) -> list[_StrType]:
        if start:
            raise NotImplementedError("ListRangeOption start" + NOT_IMPL_ERR)
        elif end:
            raise NotImplementedError("ListRangeOption end" + NOT_IMPL_ERR)

        rsp = self.client.list_fetch(self.cache_name, name)
        if isinstance(rsp, CacheListFetch.Hit):
            return rsp.value_list_string
        elif isinstance(rsp, CacheListFetch.Miss):
            return []
        elif isinstance(rsp, CacheListFetch.Error):
            raise convert_momento_to_redis_errors(rsp)
