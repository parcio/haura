# Build

Most of the build process should work without any intervention, you just need to
point the generation to the JULEA headers.

Importantly you have to provide the `JULEA_INCLUDE` environment variable which
points to the `include` directory of the JULEA repository.
Also ensure that you have already installed all depending all depending libraries, for reference check [Chapter Building](../build.md).

```sh
$ git clone https://github.com/parcio/julea.git
$ export JULEA_INCLUDE=$PWD/julea/include
$ export BINDGEN_EXTRA_CLANG_ARGS="$(pkg-config --cflags glib-2.0) $(pkg-config --cflags libbson-1.0)"
```

## Documentation

You can open similar to the other packages a basic documentation of the crate with `cargo`.

```sh
$ cargo doc --open
```
