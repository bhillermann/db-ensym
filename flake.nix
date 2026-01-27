{
  description = "db-nvrmap flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  };

  outputs = { nixpkgs, ... }:
    let
      supportedSystems = [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];

      forAllSystems = f:
	nixpkgs.lib.genAttrs supportedSystems (system:
	  let
	    pkgs = nixpkgs.legacyPackages.${system};

	    pythonEnv = pkgs.python3.withPackages (p: with p; [
	      numpy
	      pandas
	      geopandas
	      sqlalchemy
	      geoalchemy2
	      psycopg2
	      openpyxl
	      fiona
	      flask
	    ]);

	  in

	    f { inherit pkgs pythonEnv system; }
	);

    in {
      packages = forAllSystems ({ pkgs, pythonEnv, ... }: {
	default = pkgs.stdenv.mkDerivation {
	  pname = "db-ensym";
	  version = "1.2";

	  src = ./.;

	  buildInputs = [ pythonEnv ];
	  dontBuild = true;

          installPhase = ''
            mkdir -p $out/bin
            mkdir -p $out/lib/python3/db_nvrmap/templates

            # Copy the package
            cp $src/db_nvrmap/__init__.py $out/lib/python3/db_nvrmap/
            cp $src/db_nvrmap/core.py $out/lib/python3/db_nvrmap/
            cp $src/db_nvrmap/cli.py $out/lib/python3/db_nvrmap/
            cp $src/db_nvrmap/web.py $out/lib/python3/db_nvrmap/
            cp $src/db_nvrmap/templates/index.html $out/lib/python3/db_nvrmap/templates/

            # Create wrapper script with correct Python interpreter
            cat > $out/bin/db-nvrmap << EOF
#!${pythonEnv}/bin/python3
import sys
sys.path.insert(0, "$out/lib/python3")
from db_nvrmap.cli import main
sys.exit(main())
EOF
            chmod +x $out/bin/db-nvrmap
          '';
        };
      });

      devShells = forAllSystems ({ pkgs, pythonEnv, ... }: {
	default = pkgs.mkShell {
	  buildInputs = [ pythonEnv ];
	};
      });
    };
}
