import asyncio
from typing import List

from momento import CacheClient
from momento.responses import CacheDelete, CacheGet, CacheSet
from momento.typing import TDictionaryItems

from .error_utils import convert_momento_to_redis_errors


# TODO: ????
def multi_get(client: CacheClient, cache_name: str, keys: List[str]):
    promises = []
    for k in keys:
        promises.append(aio_get_wrapper(client, cache_name, k))

    responses = asyncio.gather(*promises, return_exceptions=True)

    values = []
    for rsp in responses:
        if isinstance(rsp, CacheGet.Hit):
            values.append(rsp.value_bytes)
        elif isinstance(rsp, CacheGet.Miss):
            values.append(None)
        elif isinstance(rsp, CacheGet.Error):
            raise convert_momento_to_redis_errors(rsp)

    return values


async def aio_get_wrapper(client: CacheClient, cache_name: str, key: str):
    return client.get(cache_name, key)


def multi_set(client: CacheClient, cache_name: str, mapping: TDictionaryItems):
    promises = []
    for k, v in mapping:
        promises.append(aio_set_wrapper(client=client, cache_name=cache_name, key=k, val=v))

    responses = asyncio.gather(*promises, return_exceptions=True)

    for rsp in responses:
        if isinstance(rsp, CacheSet.Error):
            raise convert_momento_to_redis_errors(rsp)
    return True


async def aio_set_wrapper(client: CacheClient, cache_name: str, key: str, val):
    return client.set(cache_name, key, val)


def multi_delete(client: CacheClient, cache_name: str, keys: List[str]):
    promises = []
    for k in keys:
        promises.append(aio_delete_wrapper(client=client, cache_name=cache_name, key=k))

    responses = asyncio.gather(*promises, return_exceptions=True)

    values = []
    for rsp in responses:
        if isinstance(rsp, CacheDelete.Success):
            values.append(True)
        elif isinstance(rsp, CacheDelete.Error):
            raise convert_momento_to_redis_errors(rsp)
    return values


async def aio_delete_wrapper(client: CacheClient, cache_name: str, key: str):
    return client.delete(cache_name, key)
