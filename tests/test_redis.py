from datetime import timedelta
import momento
from momento.config import Configurations
import redis
from momento_python_redis_client.momento_redis_wrapper import MomentoRedis

CACHE_NAME = "cache"


def test_redis_set_get():
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.set('foo', 'bar')
    res = r.get('foo')
    assert res == b'bar'


def test_momento_redis_get_set():
    momento_credential_provider = momento.CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
    momento_client = momento.CacheClient(
        Configurations.Laptop.latest(), momento_credential_provider, timedelta(seconds=60)
    )
    c = MomentoRedis(momento_client, CACHE_NAME)
    c.set('foo', 'bar')
    res = c.get('foo')
    assert res == 'bar'
