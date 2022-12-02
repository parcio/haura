# Build & Test

To build the storage stack on its own navigate to `betree/` and execute:

```sh
$ cargo build
```

This should build the storage stack after a few minutes.

> When executing `cargo build` normally the library will be built without any
> optimizations in debug mode. This is helpful when developing as you get
> information such as backtraces with understandable names. If you are planning
> to test for performance or need fast execution always use `cargo build
> --release`.

### Tests

We perform a number of tests as *unit* and *intergration* tests. Some of them require a considerable amount of time as they are run multiple times with different input values.

#### Unit

Navigate to `betree/` and execute:

```sh
$ cargo test
```

#### Integration

Due to the implementation of tests a large amount of memory is taken up during
the integration tests affecting the remaining system considerably, please be
aware that the tests will consume several GiB of memory and 4 GiB of storage space as some temporary files will be created in the `betree/tests` directory.

Navigate to `betree/tests/` and execute:
    
```sh
$ ./scripts/test.sh
```

> The provided script limits the usage of test threads proportionally to the
> available system memory with each test thread at maximum requiring 4 GiB in
> memory.

Additionally to the memory usage two files will be created with 2 GiB in size
each. They allow us to test some persistency guarantees our storage stack gives.

