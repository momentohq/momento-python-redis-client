<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

[![project status](https://momentohq.github.io/standards-and-practices/badges/project-status-official.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)
[![project stability](https://momentohq.github.io/standards-and-practices/badges/project-stability-alpha.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md) 


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
from momento_redis import MomentoRedis

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

The Momento Python Redis compatibility client is [available on PyPi](https://pypi.org/project/momento-redis/).
You can install it via:

```bash
poetry add momento-redis
```

## Examples

### Prerequisites

To run these examples, you will need a Momento auth token. You can generate one using the [Momento Console](https://console.gomomento.com).

The examples will utilize the auth token via an environment variable `MOMENTO_AUTH_TOKEN` that you set.

### Basic example

In the [`examples/`](./examples/) directory, you will find a simple CLI app, `basic.py`, that does some basic sets and 
gets on strings. It uses the Momento Redis client by default, but you can also pass a '-r' flag on the command line 
to use a Redis client instead to verify that the two clients are functioning identically. You may also pass a 
'-h <hostname>' flag and/or a '-p <port>' flag to specify a specific host and port for the Redis client. By 
default, `localhost` and `6379` are used.

Here's an example run against Momento Cache:

```bash
cd examples/
export MOMENTO_AUTH_TOKEN=<your momento auth token goes here>
python basic.py
```

And the output should look like this:

```bash
Issuing a 'get' for 'key1', which we have not yet set.
result: None
Issuing a 'set' for 'key1', with value 'value1'.
result: True
Issuing another 'get' for 'key1'.
result: b'bar'
done
```

Running the script using Redis (`python basic.py -r`) should produce identical output.

## Current Redis API Support

This library supports the most popular Redis APIs, but does not yet support all Redis APIs. We currently support the most
common APIs related to string values (GET, SET, DELETE, INCR, DECR). We will be adding support for additional
APIs in the future. If there is a particular API that you need support for, please drop by our [Discord](https://discord.com/invite/3HkAKjUZGq)
or e-mail us at [support@momentohq.com](mailto:support@momentohq.com) and let us know!

### Type Checking

To allow the use of tools such as `mypy` and in-IDE type checking to tell you if you're using any APIs that we 
don't support yet, we provide our own `MomentoRedisBase` abstract base class which explicitly lists out 
the APIs we currently support. Simply use the class as a type annotation for your client:

```python
from momento_redis import MomentoRedis, MomentoRedisBase
redis_client: MomentoRedisBase = MomentoRedis(...)
```

Once the client is typed using the abstract base class, static analysis tools will allow you to find 
calls to as yet unsupported APIs.

----------------------------------------------------------------------------------------
For more info, visit our website at [https://gomomento.com](https://gomomento.com)!
