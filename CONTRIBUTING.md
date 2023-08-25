# Welcome to the momento-python-redis-client contributing guide :wave:

Thank you for taking your time to contribute to our Momento redis/redis-py wrapper!
<br/>
This guide will provide you information to start your own development and testing.
<br/>
Happy coding :dancer:
<br/>

## Requirements :coffee:

- Python version [3.7](https://www.python.org/downloads/) is required
- A Momento Auth Token is required, you can generate one using the [Momento Console](https://console.gomomento.com)

<br/>

## First-time setup :wrench:

### Install dependencies

```
poetry install
```

<br />

## Linting and Formatting :flashlight:

```
make format
make lint
```

<br/>

## Tests :zap:

### Run integration tests against Momento and/or Redis

You may set environment variables to specify which clients, Momento and/or Redis, to test against. Additionally,
if you are testing using a Redis server, you may specify the host (`localhost` by default) and port (`6379` by default) 
your Redis server is running on.

The `TEST_SKIP_REDIS` and `TEST_SKIP_MOMENTO` variables are used to skip testing one or the other of the clients. The 
`TEST_REDIS_HOST` and `TEST_REDIS_PORT` variables are available to configure your Redis host. So, for example, to run 
the tests against the Momento Redis client but **not** the Redis client:

```shell
TEST_SKIP_REDIS=true TEST_AUTH_TOKEN=<YOUR_AUTH_TOKEN> make test
```

To test **only** the Redis client on a host and port other than the default:

```shell
TEST_SKIP_MOMENTO=true TEST_REDIS_HOST="my.redis.host" TEST_REDIS_PORT=1234 make test
```

If you choose to test against Redis, first run Redis either natively or in a Docker container: 

```shell
docker run -it -p 6379:6379 redis
```

The following will run the integration tests against both Momento and Redis and assumes the Redis server is running on `localhost:6379`.

```shell
TEST_AUTH_TOKEN=<YOUR_AUTH_TOKEN> make test
```
