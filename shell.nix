with import <nixpkgs> {};

let
  inherit (nur.repos.mozilla) rustChannelOf;

  rustStable = rustChannelOf {
    channel = "stable";
    date = "2020-10-08";
  };

  rustNightly = rustChannelOf {
    channel = "nightly";
    date = "2020-10-08";
  };

  rust = rustStable.rust.override {
    extensions = [ "rust-src" "llvm-tools-preview" ];
  };

  llvmPackages = llvmPackages_latest;
in (mkShell.override { inherit (llvmPackages) stdenv; }) {
  RUSTFLAGS = "-C link-arg=-fuse-ld=lld";
  QUICKCHECK_TESTS = 1;

  nativeBuildInputs = [
    rustNightly.rustfmt-preview
    rust

    cargo-outdated

    pkgconfig
  ] ++ (with llvmPackages; [
    libcxxClang
    lldClang
  ]);
}
