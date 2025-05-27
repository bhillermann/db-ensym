{
  description = "db-nvrmap flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  };

  outputs = { nixpkgs, ... }:
    let
      supportedSystems = [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      pkgs = forAllSystems (system: nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.python3.withPackages (pypkgs:
            with pypkgs; [
              numpy
              pandas
              geopandas
              sqlalchemy
              psycopg2
              openpyxl
            ]);
        }
      );

      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.mkShell {
            buildInputs = [
              (pkgs.python3.withPackages (pypkgs:
                with pypkgs; [
                  numpy
                  pandas
                  geopandas
                  sqlalchemy
                  psycopg2
                  openpyxl
                ]))
            ];
          };
        }
      );
    };
}
