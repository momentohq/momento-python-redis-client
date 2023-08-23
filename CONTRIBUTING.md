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

### Run integration tests against Momento and Redis

First run Redis either natively or in a Docker container: 

```
docker run -it -p 6379:6379 redis
```

Then run the tests:

```
TEST_AUTH_TOKEN=<YOUR_AUTH_TOKEN> make test
```

This will run the integration tests against both Momento and Redis and assumes the Redis server is running on `localhost:6379`. If using a different host and port, modify the above command as follows:

```
TEST_REDIS_HOST=<HOST> TEST_REDIS_PORT=<PORT> TEST_AUTH_TOKEN=<YOUR_AUTH_TOKEN> make test
```
