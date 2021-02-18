with (import <nixpkgs> {}).unstable;

let
  inherit (nur.repos.mozilla) rustChannelOf;

  rustStable = rustChannelOf {
    channel = "stable";
    date = "2021-02-11";
  };

  rustNightly = rustChannelOf {
    channel = "nightly";
    date = "2020-02-17";
  };

  rust = rustStable.rust.override {
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

  nativeBuildInputs = [
    rustNightly.rustfmt-preview
    rust

    cargo-outdated
    cargo-bloat

    pkgconfig

    heaptrack
    psrecord

    # julea
    meson
    ninja
    doxygen
  ] ++ (with llvmPackages; [
    libcxxClang
    lldClang
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
