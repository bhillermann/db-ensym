{
  description = "db-nvrmap flake";

  inputs = { nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable"; };

  outputs = { nixpkgs, ... }:
    let
      configFilePath =
        "./config.json";
      secretPath =
        "$HOME/.config/opnix/secrets/postgisPassword"; # location of file containing the database password
      supportedSystems =
        [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];

      forAllSystems = f:
        nixpkgs.lib.genAttrs supportedSystems (system:
          let
            pkgs = nixpkgs.legacyPackages.${system};

            pythonEnv = pkgs.python3.withPackages (p:
              with p; [
                numpy
                pandas
                geopandas
                sqlalchemy
                geoalchemy2
                psycopg2
                openpyxl
                fiona
              ]);

          in f { inherit pkgs pythonEnv system; });

    in {
      packages = forAllSystems ({ pkgs, pythonEnv, ... }: {
        default = pkgs.stdenv.mkDerivation {
          pname = "db-ensym";
          version = "1.1";

          src = ./.;

          buildInputs = [ pythonEnv ];
          dontBuild = true;

          installPhase = ''
            mkdir -p $out/bin
            cp $src/db-nvrmap.py $out/bin/db-nvrmap
            chmod +x $out/bin/db-nvrmap
          '';
        };
      });

      devShells = forAllSystems ({ pkgs, pythonEnv, ... }: {
        default = pkgs.mkShell {
          buildInputs = [ pythonEnv pkgs.python3Packages.pytest ];
          shellHook = ''
                export NVRMAP_DB_PASSWORD=`${pkgs.coreutils}/bin/cat ${secretPath}`
            		export NVRMAP_CONFIG=${configFilePath}
            		${pkgs.coreutils}/bin/echo "Environment variables set!"
          '';
        };
      });
    };
}
