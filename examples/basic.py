import datetime
import getopt
import sys

import momento
from redis import Redis

from momento_redis import MomentoRedis

_CACHE_NAME = "my-cache"


def get_momento_client():
    print("Using momento redis client")
    return MomentoRedis(
        momento.CacheClient(
            momento.Configurations.Laptop.latest(),
            momento.CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
            datetime.timedelta(seconds=60)
        ),
        _CACHE_NAME
    )


def get_redis_client(host, port):
    print(f"Using redis client on port {port} of host {host}")
    return Redis(
        host, port, 0
    )


def parse_command_line(argv):
    client_to_use = "momento"
    host = "localhost"
    port = 6379
    opts, args = getopt.getopt(argv, "rh:p:")
    for opt, arg in opts:
        if opt == '-r':
            client_to_use = "redis"
        elif opt == '-h':
            host = arg
        elif opt == '-p':
            port = arg
    return client_to_use, host, port


def get_client(argv):
    client_to_use, host, port = parse_command_line(argv)
    if client_to_use == "redis":
        return get_redis_client(host, port)
    else:
        return get_momento_client()


def main(argv):
    key = "key1"
    val = "value1"
    client = get_client(argv)
    print("Issuing a 'get' for 'key1', which we have not yet set.")
    result = client.get(key)
    print(f"result: {result}")
    print("Issuing a 'set' for 'key1', with value 'value1'.")
    result = client.set(key, val)
    print(f"result: {result}")
    print("Issuing another 'get' for 'key1'.")
    result = client.get(key)
    print(f"result: {result}")
    print("done")


if __name__ == "__main__":
    main(sys.argv[1:])
