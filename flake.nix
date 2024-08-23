{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    naersk.url = "github:nix-community/naersk";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    fenix,
    naersk,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = (import nixpkgs) {
          inherit system;
          overlays = [
            (import ./nix/overlays/hdf5.nix)
          ];
        };

        toolchain = fenix.packages.${system}.toolchainOf {
          channel = "1.75";
          date = "2023-12-28";
          sha256 = "SXRtAuO4IqNOQq+nLbrsDFbVk+3aVA8NNpSZsKlVH/8=";
        };

        naersk' = pkgs.callPackage naersk {
          cargo = toolchain.rust;
          rustc = toolchain.rust;
        };

        workspaceCargo = builtins.fromTOML (builtins.readFile ./Cargo.toml);
        version = workspaceCargo.workspace.package.version;
        gitRevision = self.shortRev or self.dirtyShortRev;

        hdf5-joined = pkgs.symlinkJoin {
          name = "hdf5";
          paths = with pkgs; [hdf5 hdf5.dev];
        };
        nativeBuildInputs = with pkgs; [cmake flatbuffers hdf5-joined perl tcl pkg-config];
        buildInputs = with pkgs; [openssl cyrus_sasl hdf5-joined];

        lintingRustFlags = "-D unused-crate-dependencies";
      in {
        devShell = pkgs.mkShell {
          nativeBuildInputs = nativeBuildInputs ++ [toolchain.toolchain];
          buildInputs = buildInputs;

          packages = with pkgs; [
            # Code formatting tools
            alejandra
            treefmt
            mdl

            # Container image management
            skopeo

            # Documentation tools
            adrs
          ];

          RUSTFLAGS = lintingRustFlags;
          HDF5_DIR = "${hdf5-joined}";
        };

        packages =
          import ./diagnostics {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs;}
          // import ./digitiser-aggregator {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs;}
          // import ./nexus-writer {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs hdf5-joined;}
          // import ./simulator {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs;}
          // import ./trace-archiver-hdf5 {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs hdf5-joined;}
          // import ./trace-archiver-tdengine {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs;}
          // import ./trace-reader {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs;}
          // import ./trace-telemetry-exporter {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs;}
          // import ./trace-to-events {inherit pkgs naersk' version gitRevision nativeBuildInputs buildInputs;};
      }
    );
}
