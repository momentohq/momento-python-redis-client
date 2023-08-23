{{ ossHeader }}

# Momento Python Redis compatibility client

## What and why?

This project provides a Momento-backed implementation of [redis/redis-py](https://github.com/redis/redis-py)
The goal is to provide a drop-in replacement for [redis/redis-py](https://github.com/redis/redis-py) so that you can
use the same code with either a Redis server or with the Momento Cache service!

## Usage

To switch your existing `redis/redis-py` application to use Momento, you only need to change the code where you construct
your client object:

### With redis-py client

```python
# Import the redis module
from redis import Redis
# Replace these values with your Redis server's details
_REDIS_HOST = 'my.redis-server.com';
_REDIS_PORT = 6379;
_REDIS_DB = 0
_REDIS_PASSWORD = 'mypasswd';
# Create a Redis client
redis_client = Redis(host=_REDIS_HOST, port=_REDIS_PORT, db=_REDIS_DB, password=_REDIS_PASSWORD)
```

### With Momento's Redis compatibility client

```python
import datetime
# Import the Momento redis compatibility client.
import momento
from momento_python_redis_client.momento_redis_client import MomentoRedis

_CACHE_NAME = "my-cache"
# Initialize Momento client.
redis_client = MomentoRedis(
    momento.CacheClient(
        momento.Configurations.Laptop.latest(),
        momento.CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
        datetime.timedelta(seconds=60)
    ),
    _CACHE_NAME
)
```

**NOTE**: The Momento `redis/redis-py` implementation currently supports simple key/value pairs (`GET`, `SET`, `DELETE`) 
as well as `INCR/INCRBY` and `DECR/DECRBY`. We will continue to add support for additional Redis APIs in the future; 
for more information see the [current Redis API support](#current-redis-api-support) section later in this doc.

## Installation

The Momento Python Redis compatibility client is [available on PyPi](https://pypi.org/project/momento-redis-client/).
You can install it via:

```bash
poetry add momento-redis-client
```

## Examples

### Prerequisites

To run these examples, you will need a Momento auth token. You can generate one using the [Momento Console](https://console.gomomento.com).

The examples will utilize your auth token via the environment variable `MOMENTO_AUTH_TOKEN` you set.

### Basic example

In the [`examples/`](./examples/) directory, you will find a simple CLI app that does some basic sets and gets
on string. You can pass the `--momento` flag to run against Momento, or the `--redis` flag to run against
a local Redis (127.0.0.0:6379).

Here's an example run against Momento Cache:

```bash
cd examples/
export MOMENTO_AUTH_TOKEN=<your momento auth token goes here>
python basic.py --momento
```

And the output should look something like this:

```bash

using momento-node-redis with cache 'cache'
[2023-04-10T23:16:45.893Z] INFO (Momento: CacheClient): Creating Momento CacheClient
[2023-04-10T23:16:45.901Z] INFO (Momento: ControlClient): Creating cache: cache

Issuing a 'get' for key 'key1', which we have not yet set.
result 'null'

Issuing a 'set' for key 'key1', with value 'value1'.
result: OK

Issuing another 'get' for key key1.
result: 'value1' (String)

done
```

## Current Redis API Support

This library supports the most popular Redis APIs, but does not yet support all Redis APIs. We currently support the most
common APIs related to string values (GET, SET, DELETE, INCR, DECR). We will be adding support for additional
APIs in the future. If there is a particular API that you need support for, please drop by our [Discord](https://discord.com/invite/3HkAKjUZGq)
or e-mail us at [support@momentohq.com](mailto:support@momentohq.com) and let us know!

{{ ossFooter }}
