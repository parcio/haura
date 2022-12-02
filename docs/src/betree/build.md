# Build & Test

To build the storage stack on its own navigate to `betree/` and execute:

```sh
$ cargo build
```

This should build the storage stack in _Debug_ after a few minutes.

### Tests

We perform a number of tests which partially take some time to successfully
complete on your system. 

#### Internal

Navigate to `betree/` and execute:

```sh
$ cargo test
```

> Some of the unit tests take a considerable amount of time, which is still
> under investigation.

#### Integration

Due to the implementation of tests a large amount of memory is taken up during
the integration tests affecting the remaining system considerably, please be
aware that the tests will consume several GiB of memory of available space

Navigate to `betree/tests/` and execute:

```sh
$ ./scripts/test.sh
```

> The provided script limits the usage of test threads proportionally to the
> available system memory with each test thread at maximum requiring 4 GiB in
> memory.

Additionally to the memory usage two files will be created with 2 GiB in size
each. They allow us to test some persistency guarantees our storage stack gives.

