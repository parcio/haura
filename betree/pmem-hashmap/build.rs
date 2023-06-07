use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Compile and Link to the hashmap impl
    if !Command::new("make")
        .current_dir("./c_impl")
        .output()
        .expect("Could not compile hashmap_tx")
        .status
        .success()
    {
        panic!("hashmap_tx: Compilation failed");
    }

    // Linking Info
    println!("cargo:rustc-link-lib=hashmap_tx");
    println!("cargo:rustc-link-lib=pmemobj");
    let path = PathBuf::from("./c_impl")
        .canonicalize()
        .expect("Could not determine path to c_impl");
    println!("cargo:rustc-link-search={}", path.to_str().unwrap());

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
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
