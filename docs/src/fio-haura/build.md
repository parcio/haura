# Build

Building Haura's fio ioengine requires some internal files which are not available in most provided fio packages. The Makefile automatically downloads and prepares the required files when building the module; to build the module run:

```sh
$ make
```

On build errors consult the `configure.log` in the fio directory.

You'll find the ioengine in the `src/` directory.
