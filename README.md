# Be-Tree Storage Stack

[![CI](https://github.com/julea-io/haura/workflows/CI/badge.svg)](https://github.com/julea-io/haura/actions)
[![Pages](https://github.com/julea-io/haura/workflows/Pages/badge.svg)](https://github.com/julea-io/haura/actions)

A storage library offering key-value and object interfaces by managing B^ε-trees on block storage devices.

## Dependencies

We advise you to use always the latest version of Rust *Stable*. For compatability we provide the minimum rust version also in the Cargo.toml of each crate in this project.

```
git clone https://github.com/julea-io/haura
cd haura/betree
cargo build
cd tests
./scripts/test.sh
```

## Documentation

You can find an in-depth documentation and developer guide under
https://julea-io.github.io/haura or you may build it yourself locally.  For
building the documentation [`mdbook`](https://rust-lang.github.io/mdBook/) is
required. You can find install directions in their documentation under
https://rust-lang.github.io/mdBook/.

Also you'll require [mdbook-grapviz](https://lib.rs/crates/mdbook-graphviz) to
render graphs within the documentation. This crate also needs graphviz to be
installed on your system.

The documentation is automatically built and published on GitHub Pages - the workflow for
which you can find under `.github/workflows/pages.yml`.

### julea-sys

`julea-sys` generates limited Rust bindings from the JULEA headers, by using rust-bindgen, which uses libclang.
The location of the JULEA headers must be provided by passing the `JULEA_INCLUDE` environment variable.

bindgen needs to access included headers of any libraries JULEA is referencing. The search path can be provided as shown below,
though the specifics may change depending on future JULEA versions and cross-compilation.
See the [bindgen documentation](https://github.com/rust-lang/rust-bindgen#environment-variables) for options
of providing the necessary include paths.

```
export BINDGEN_EXTRA_CLANG_ARGS="$(pkg-config --cflags glib-2.0) $(pkg-config --cflags libbson-1.0)"
```

## Attribution

This code was originally written as a part of Felix Wiedemanns ([@Nilix007](https://github.com/Nilix007)) [master's thesis](https://wr.informatik.uni-hamburg.de/_media/research:theses:felix_wiedemann_modern_storage_stack_with_key_value_store_interface_and_snapshots_based_on_copy_on_write_b%CE%B5_trees.pdf).

## License

Licensed under either of

 * Apache License, Version 2.0
    ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
    ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
    at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
