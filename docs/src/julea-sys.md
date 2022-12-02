# julea-sys

`julea-sys` contains generated bindings to the JULEA headers via
[bindgen](https://lib.rs/crates/bindgen). They are used internally by the
`julea-betree` crate to provide the *Haura* JULEA backend. You'll likely do not
need to modify the code itself (which is not a lot) but take care of the
building process which we will explain here shortly.
