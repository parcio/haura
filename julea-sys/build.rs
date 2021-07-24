extern crate bindgen;

use std::{env, path::PathBuf};

fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=bindings.h");

    let julea = env!("JULEA_INCLUDE");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("bindings.h")
        .clang_args(&["-I", julea])
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .anon_fields_prefix("anon")
        .disable_nested_struct_naming()
        .allowlist_type("JBackend")
        .allowlist_function("j_trace.*")
        .rustified_non_exhaustive_enum("JTrace.*")
        .constified_enum_module("JBackend.*")
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
