# Momento Python Redis Client Examples

### Prerequisites

To run these examples, you will need a Momento auth token. You can generate one using the [Momento Console](https://console.gomomento.com).

The examples will utilize the auth token via an environment variable `MOMENTO_AUTH_TOKEN` that you set.

## Installation

```shell
poetry install
```

## Basic Example

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
