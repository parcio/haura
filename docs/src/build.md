# Building

To build the complete project including all included crates requires some
configuration beforehand.

## Install Dependencies

First install the Rust compiler to actually compile the project.  Use your
package manager or [set up in your local user home](https://rustup.rs/).

```sh
# # For rpm based distributions (Fedora, CentOS, Rocky, ...)
# dnf install cargo
#
# # For apt based distributions (Debian, Ubuntu, ...)
# apt update
# apt install cargo
#
# # For Arch Linux and its derivatives
# pacman -Sy cargo
```

You'll need atleast version 1.61. Most package manager should deliver this or
more recent version - rustup will always deliver the most up-to-date version.

Then install the required system dependencies for _bindgen_ and the _JULEA_
bindings.

### Fedora/RHEL

```sh
# dnf install glib2 glib2-devel libbson libbson-devel clang make pkgconf
```

### Ubuntu/Debian/...

```sh
# apt update
# apt install libglib2.0-0 libglib2.0-dev libbson-1.0-0 libbson-dev clang make pkg-config
```

### Arch Linux+

```sh
# pacman -Sy glib2 clang make libbson pkgconf
```


## Prepare the environment

> This step can be skipped if you do not need to use the JULEA interface. See [betree](./betree.md).

To compile the bindings you'll need JULEA present and specify it's headers in your environemnt.

```sh
$ # git clone https://github.com/julea-io/julea.git
$ # Support for seamless integration is not quite there yet in JULEA, we require a special fork
$ git clone -b modules-conditional-unload https://github.com/tilpner/julea.git
$ git clone https://github.com/julea-io/haura.git
```

To build the complete _Haura_ project from this state, execute:

```sh
$ export JULEA_INCLUDE=$PWD/julea/include
$ export BINDGEN_EXTRA_CLANG_ARGS="$(pkg-config --cflags glib-2.0) $(pkg-config --cflags libbson-1.0)"
$ cd haura
$ cargo build
```
