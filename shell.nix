with import <nixpkgs> {};

let
  rustOverlay = fetchFromGitHub {
    owner = "oxalica";
    repo = "rust-overlay";
    rev = "8d79f4f49aef96b35de8229c9a059a2bdab2b1ec";
    sha256 = "067gwa19qzxds50lj2s2gk5ww8grv76hzybabf00r6nz1zc3wyxb";
  };

  rustPkgs = let self = pkgs // (import rustOverlay self pkgs); in self;

  extensions = [ "rust-src" "llvm-tools-preview" ];
  rustStable = rustPkgs.rust-bin.nightly.latest.default.override {
    inherit extensions;
  };
  rustNightly = rustPkgs.rust-bin.selectLatestNightlyWith
    (toolchain: toolchain.default.override { inherit extensions; });

  rust = rustNightly;

  llvmPackages = llvmPackages_latest;
in (mkShell.override { inherit (llvmPackages) stdenv; }) rec {
  RUST_BACKTRACE = "full";
  RUSTFLAGS = lib.concatStringsSep " " [
    # "-C link-arg=-fuse-ld=lld"
    # "-C prefer-dynamic"
    "-C target-cpu=native"
    # "-C target-feature=+avx"
  ];
  # pretend we have nightly to use cargo-fuzz
  # RUSTC_BOOTSTRAP = "1";
  QUICKCHECK_TESTS = 1000;

  JULEA_PREFIX = "${toString ./.}/julea-install";
  # JULEA_TRACE = "echo";
  # G_DEBUG = "resident-modules";
  # G_SLICE = "debug-blocks";

  LIBCLANG_PATH="${llvmPackages.libclang.lib}/lib/libclang.so";
  JULEA_INCLUDE = "${toString ./.}/julea/include";

  BINDGEN_EXTRA_CLANG_ARGS = [
    "-I${glib.dev}/include/glib-2.0"
    "-I${glib.out}/lib/glib-2.0/include"
    "-I${libbson.out}/include/libbson-1.0"
  ];

  nativeBuildInputs = [
    # rustNightly.rustfmt-preview
    rust

    cargo-outdated
    cargo-bloat
    rust-bindgen
    rust-cbindgen

    pkgconfig

    perf-tools
    flamegraph
    heaptrack
    hotspot
    psrecord

    trace-cmd

    # julea
    meson
    ninja
    doxygen

    glib
  ] ++ (with llvmPackages; [
    libcxxClang
    bintools
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
    TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | ${jq}/bin/jq -r .target_directory)"
    export PATH="$PATH:$TARGET_DIR/release"
  '';
}
