with (import <nixpkgs> {}).unstable;

let
  inherit (nur.repos.mozilla) rustChannelOf;

  rustStable = rustChannelOf {
    channel = "stable";
    date = "2021-02-11";
  };

  rustNightly = rustChannelOf {
    channel = "nightly";
    date = "2021-02-17";
  };

  rust = rustNightly.rust.override {
  #rust = rustStable.rust.override {
    extensions = [ "rust-src" "llvm-tools-preview" ];
  };

  llvmPackages = llvmPackages_latest;
in (mkShell.override { inherit (llvmPackages) stdenv; }) rec {
  RUST_BACKTRACE = "full";
  RUSTFLAGS = "-C link-arg=-fuse-ld=lld -C target-cpu=native -C prefer-dynamic";
  QUICKCHECK_TESTS = 1;

  JULEA_PREFIX = "${toString ./.}/julea-install";
  JULEA_TRACE = "echo";
  G_DEBUG = "resident-modules";
  G_SLICE = "debug-blocks";

  LIBCLANG_PATH="${llvmPackages.libclang.lib}/lib/libclang.so";
  JULEA_INCLUDE = "${toString ./.}/julea/include";

  BINDGEN_EXTRA_CLANG_ARGS = [
    "-I${glib.dev}/include/glib-2.0"
    "-I${glib.out}/lib/glib-2.0/include"
    "-I${libbson.out}/include/libbson-1.0"
  ];

  nativeBuildInputs = [
    rustNightly.rustfmt-preview
    rust

    cargo-outdated
    cargo-bloat
    rust-bindgen

    pkgconfig

    heaptrack
    psrecord

    # julea
    meson
    ninja
    doxygen

    glib
  ] ++ (with llvmPackages; [
    libcxxClang
    lldClang
    libclang.lib
  ]);

  buildInputs = [
    # julea
    glib libbson
    fuse
  ];

  shellHook = ''
    export PKG_CONFIG_PATH="$PKG_CONFIG_PATH:${toString ./.}/betree"
    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${JULEA_PREFIX}/lib"
  '';
}
