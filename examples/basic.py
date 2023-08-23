import datetime

import momento

from momento_python_redis_client import MomentoRedis, MomentoRedisBase

_CACHE_NAME = "my-cache"
# Initialize Momento client.
print(f"using momento-python-redis-client with cache {_CACHE_NAME}")
redis_client: MomentoRedisBase = MomentoRedis(
    momento.CacheClient(
        momento.Configurations.Laptop.latest(),
        momento.CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
        datetime.timedelta(seconds=60)
    ),
    _CACHE_NAME
)

print("Issuing a 'get' for 'key1', which we have not yet set.")
result = redis_client.get("foo")
print(f"result: {result}")
print("Issuing a 'set' for 'key1', with value 'value1'.")
result = redis_client.set("foo", "bar")
print(f"result: {result}")
print("Issuing another 'get' for 'key1'.")
result = redis_client.get("foo")
print(f"result: {result}")
print("done")
