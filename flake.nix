{
  description = "Fractalbits";

  inputs = {
    # We want to stay as up to date as possible but need to be careful that the
    # glibc versions used by our dependencies from Nix are compatible with the
    # system glibc that the user is building for.
    nixpkgs.url = "https://channels.nixos.org/nixos-25.05/nixexprs.tar.xz";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" ];
        };

        zigToolchain = pkgs.zig_0_13;

        buildInputs = with pkgs; [
          # Rust toolchain
          rustToolchain

          # Zig toolchain
          zigToolchain

          # Build tools
          pkg-config
          protobuf

          # System libraries
          openssl
          zlib

          # AWS/Cloud tools
          awscli2

          # Node.js for UI
          nodejs
          nodePackages.npm

          # Java runtime for DynamoDB Local
          jre

          # Certificate tools
          mkcert

          # System utilities
          netcat
          gawk
          coreutils
          findutils
          gnugrep
          gnused
          git

          # MinIO server for S3-compatible storage
          minio

          # Development tools
          direnv

          # Debugging tools
          gdb

          # Performance tools for benchmarking
          linuxPackages.perf

          # Formatting tools
          nixpkgs-fmt

          # Command runner
          just
        ];

        nativeBuildInputs = with pkgs; [
          pkg-config
        ];

      in
      {
        devShells.default = pkgs.mkShell {
          inherit buildInputs nativeBuildInputs;

          shellHook = ''
            echo "Fractalbits development environment"
            echo "Rust version: $(rustc --version)"
            echo "Zig version: $(zig version)"
            echo ""
            echo "Available commands:"
            echo "  cargo xtask build    - Build the project"
            echo "  cargo xtask service  - Manage services"
            echo ""
            echo "Nix environment ready!"
          '';

          # Environment variables
          RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";
          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";

          # Disable hardening that causes jemalloc build issues
          hardeningDisable = [ "fortify" ];
        };

        # Build the project using Nix
        packages.default = pkgs.stdenv.mkDerivation {
          pname = "fractalbits-dev";
          version = "0.1.0";
          src = ./.;

          nativeBuildInputs = [ rustToolchain zigToolchain ] ++ nativeBuildInputs;
          inherit buildInputs;

          buildPhase = ''
            export CARGO_HOME=$(mktemp -d)
            cargo xtask build
          '';

          installPhase = ''
            mkdir -p $out/bin
            # Copy built binaries (adjust paths as needed)
            find target/release -maxdepth 1 -type f -executable -exec cp {} $out/bin/ \; 2>/dev/null || true
            find zig-out/bin -maxdepth 1 -type f -executable -exec cp {} $out/bin/ \; 2>/dev/null || true
          '';

          # Set environment variables for build
          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
        };

        # Formatter for the flake
        formatter = pkgs.nixpkgs-fmt;
      });
}
