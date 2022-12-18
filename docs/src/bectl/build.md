# Build & Test


Building `bectl` is relatively simple but does require all dependencies of
`betree_storage_stack` to be available. If you have not done this yet refer to
the [Building chapter](../build.md).

Given all prerequisites are fulfilled simply run

```sh
$ cargo build
```

from the `bectl` directory to build `bectl` on its own.

To avoid specifiyng the location of the binary or running `bectl` with `cargo
run` on each invocation you can also install the app via `cargo` to your local
user.

```sh
$ cargo install --path .
```

--- 

There are not yet any tests provided for the `bectl` as the functionality is a
rather simple mapping to `betree_storage_stack` functions. If we want to expand
this in the future we might want to ensure that `bectl` behaves correctly
internally too.
