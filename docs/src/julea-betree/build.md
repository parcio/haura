# Build

To build you will need the generated bindings to JULEA, check the [Chapter
Building](../build.md). Furthermore ensure that all dependencies are installed.

Once you are done navigate to the `julea-betree` directory.

```sh
$ cargo build
```

The build process produces a shared library object which we further use with JULEA.

> TODO
>
> ---
>
> All this is not automated at the moment, although we would like to. We may
> simply evaluate the JULEA environment, if set, and copy our shared object there, so
> that we can actually use JULEA in the process.
