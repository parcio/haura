# PMap 

This diretory contains an implementation of an transactional hashmap for
persistent memory.

Besides the system dependencies an object file is build which is required as it
contains the actual logic of the hashmap implemented in C. This crate provides
an abstraction over this called `PMap` which handles all unsafe calls. Due to
this, this crate is nothing more than a thin layer above unsafe. Buyer beware.

``` rust
fn main() -> Result<()> {
    let pmap = PMap::create("/my/expensive/device", 128 * TO_MEBIBYTE)?;
    let key = b"Hello from 🦀!";
    pmap.insert(key, &[72, 101, 119, 111]);
    assert_eq!(pmap.get(key), [72, 101, 119, 111])
}
```

## System Dependencies

- `libpmemobj`
- `libpmemobj-devel`

## Tests

Auto-generated tests by `bindgen` are provided as well as tests with the actual
abstractions and unsafe code. No special hardware is required to run the tests.

``` sh
$ cargo test
```

