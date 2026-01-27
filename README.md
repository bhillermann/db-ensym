# db-ensym

Use PostGIS database with VicData shapefiles to generate EnSym and NVRMap compatible shapefiles.

## Overview

This tool queries a PostGIS database containing Victorian spatial data to generate shapefiles in either EnSym or NVRMap format. It processes parcel or property view PFIs (Parcel Feature Identifiers) and produces output shapefiles with vegetation and bioregion data.

**Requires a PostGIS instance** with the following tables:
- `parcel_view`
- `nv1750_evc`
- `bioregions`
- `parcel_property`
- `parcel_detail`
- `property_detail`

All source data files are available from [data.vic.gov.au](https://data.vic.gov.au).

## Command Line Usage

```bash
db-nvrmap [OPTIONS] N [N ...]
```

### Positional Arguments

| Argument | Description |
|----------|-------------|
| `N` | One or more PFI (Parcel Feature Identifier) values of the Parcel View. Multiple PFIs can be specified separated by spaces. |

### Options

| Option | Long Form | Description |
|--------|-----------|-------------|
| `-h` | `--help` | Show help message and exit |
| `-s NAME` | `--shapefile NAME` | Name of the shapefile/directory to write. Default is `nvrmap`. |
| `-g VALUE` | `--gainscore VALUE` | Override the default gain score value (float) |
| `-p` | `--property` | Use Property View PFIs instead of Parcel View PFIs |
| `-e` | `--ensym` | Output in EnSym 2017 format |
| `-b` | `--sbeu` | Output in EnSym 2013 SBEU format |
| | `--web` | Start the web interface instead of processing PFIs |
| | `--port PORT` | Port for web server (default: 5000) |
| | `--host HOST` | Host for web server (default: 127.0.0.1) |

### Examples

```bash
# Process a single parcel view PFI with default NVRMap output
db-nvrmap 12345678

# Process multiple parcel view PFIs
db-nvrmap 12345678 87654321 11223344

# Output to a custom shapefile name
db-nvrmap -s my_output 12345678

# Use property view PFIs instead of parcel view PFIs
db-nvrmap -p 98765432

# Output in EnSym 2017 format
db-nvrmap -e 12345678

# Output in EnSym 2013 SBEU format
db-nvrmap -b 12345678

# Override the gain score
db-nvrmap -g 0.35 12345678

# Combine options: EnSym format with custom shapefile and gain score
db-nvrmap -e -s ensym_output -g 0.30 12345678 87654321
```

## Web Interface

The tool includes an optional web interface for users who prefer a graphical form over the command line.

### Starting the Web Server

```bash
# Start on localhost:5000 (default)
db-nvrmap --web

# Start on a custom port
db-nvrmap --web --port 8080

# Make accessible on the network
db-nvrmap --web --host 0.0.0.0

# Custom host and port
db-nvrmap --web --host 0.0.0.0 --port 8080
```

### Web Interface Features

- **PFI Input**: Paste PFI numbers in a textarea (supports comma, space, or newline separation)
- **View Type Toggle**: Switch between Parcel View and Property View PFIs
- **Output Format Selection**: Choose from NVRMap, EnSym 2017, or EnSym 2013 SBEU formats
- **Optional Gain Score Override**: Specify a custom gain score value
- **Custom Filename**: Set the output filename (defaults to "output")
- **ZIP Download**: Downloads all shapefile components as a single ZIP file

### Security Notes

When running with `--host 0.0.0.0`, the web interface will be accessible from other machines on the network. Only use this in trusted network environments. The default `127.0.0.1` restricts access to localhost only.

For production deployments, consider placing the Flask app behind a reverse proxy (nginx, Apache) with proper authentication.

## Configuration File

The application requires a JSON configuration file located at `$NVRMAP_CONFIG/config.json`.

### Configuration Structure

```json
{
    "db_connection": {
        "db_type": "postgresql+psycopg2",
        "username": "your_username",
        "password": "your_password",
        "host": "localhost",
        "database": "gisdb"
    },
    "evc_data": "/path/to/EVC benchmark data - external use.xlsx",
    "attribute_table": {
        "project": "Project Name",
        "collector": "Collector Name",
        "default_gain_score": 0.22,
        "default_habitat_score": 0.4
    }
}
```

### Configuration Options

#### `db_connection`

| Key | Required | Description |
|-----|----------|-------------|
| `db_type` | Yes | Database connection type. Use `postgresql+psycopg2` for PostgreSQL. |
| `username` | Yes | Database username |
| `password` | Yes | Database password |
| `host` | Yes | Database host address |
| `database` | Yes | Database name |

#### `evc_data`

Path to the EVC (Ecological Vegetation Class) benchmark data Excel file. This file is used to look up BCS (Bioregional Conservation Status) values.

#### `attribute_table`

| Key | Description |
|-----|-------------|
| `project` | Project identifier used in `HH_PAI` (EnSym) or `prop_id` (NVRMap) fields |
| `collector` | Collector/contact person name used in `HH_CP` (EnSym) or `cp` (NVRMap) fields |
| `default_gain_score` | Default gain score value (can be overridden with `-g` flag) |
| `default_habitat_score` | Default habitat/condition score value |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `NVRMAP_CONFIG` | Yes | Path to the directory containing the `config.json` file. The application will look for `$NVRMAP_CONFIG/config.json`. |

### Setting the Environment Variable

```bash
# Bash/Zsh
export NVRMAP_CONFIG=/path/to/config/directory

# Or set it inline when running the command
NVRMAP_CONFIG=/path/to/config db-nvrmap 12345678
```

For permanent configuration, add the export statement to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.).

## Output Formats

### NVRMap Format (Default)

The default output format with the following schema:

| Field | Type | Description |
|-------|------|-------------|
| `site_id` | int | Site identifier (increments for multiple parcels) |
| `zone_id` | str | Zone identifier (alphabetic: A-Z, then AA, BB, etc.) |
| `prop_id` | str | Property/project identifier |
| `vlot` | int | Virtual lot number |
| `lot` | int | Lot number |
| `recruits` | int | Number of recruits |
| `type` | str | Type indicator (default: "p") |
| `cp` | str | Collector/contact person |
| `veg_codes` | str | Vegetation codes (bioregion + EVC) |
| `lt_count` | int | Large tree count |
| `cond_score` | float | Condition/habitat score |
| `gain_score` | float | Gain score |
| `surv_date` | int | Survey date (YYYYMMDD format) |

### EnSym 2017 Format (`-e` flag)

| Field | Type | Description |
|-------|------|-------------|
| `HH_PAI` | str | Project area identifier |
| `HH_D` | date | Survey date |
| `HH_CP` | str | Contact person |
| `HH_SI` | int | Site index |
| `HH_ZI` | str | Zone index |
| `HH_VAC` | str | Vegetation assessment category |
| `HH_EVC` | str | EVC code |
| `BCS` | str | Bioregional conservation status |
| `LT_CNT` | int | Large tree count |
| `HH_H_S` | float | Habitat score |
| `G_S` | float | Gain score |
| `HH_A` | float | Area in hectares |

### EnSym 2013 SBEU Format (`-b` flag)

| Field | Type | Description |
|-------|------|-------------|
| `HH_PAI` | str | Project area identifier |
| `HH_SI` | int | Site index |
| `HH_ZI` | str | Zone index |
| `HH_VAC` | str | Vegetation assessment category |
| `HH_CP` | str | Contact person |
| `HH_D` | date | Survey date |
| `HH_H_S` | float | Habitat score |
| `G_HA` | float | Gain per hectare |
| `HH_A` | float | Area in hectares |

## Nix Flake Usage

This project provides a Nix flake for reproducible builds and development environments.

### Using as a Flake Input

To use `db-ensym` as an input in your own Nix flake:

```nix
{
  description = "My flake using db-ensym";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    db-ensym = {
      url = "github:your-username/db-ensym";  # Or path:/path/to/db-ensym
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, db-ensym, ... }:
    let
      system = "x86_64-linux";  # Or your target system
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      # Use the package in your derivation
      packages.${system}.default = pkgs.stdenv.mkDerivation {
        pname = "my-package";
        version = "1.0";
        buildInputs = [ db-ensym.packages.${system}.default ];
        # ...
      };

      # Or include it in a dev shell
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [
          db-ensym.packages.${system}.default
        ];
      };
    };
}
```

### Supported Systems

The flake supports the following systems:
- `x86_64-linux`
- `x86_64-darwin`
- `aarch64-linux`
- `aarch64-darwin`

### Running Directly

```bash
# Run directly without installing
nix run github:your-username/db-ensym -- 12345678

# Run from a local checkout
nix run . -- 12345678
```

### Development Shell

Enter a development shell with all dependencies:

```bash
# From the project directory
nix develop

# Or from anywhere
nix develop github:your-username/db-ensym
```

The development shell includes Python 3 with the following packages:
- numpy
- pandas
- geopandas
- sqlalchemy
- geoalchemy2
- psycopg2
- openpyxl
- fiona
- flask

### Building the Package

```bash
# Build the package
nix build

# The output will be in ./result/bin/db-nvrmap
./result/bin/db-nvrmap --help
```

### Installing to Profile

```bash
# Install to your user profile
nix profile install github:your-username/db-ensym

# Or from local checkout
nix profile install .
```

## Dependencies

### Python Packages
- numpy
- pandas
- geopandas
- sqlalchemy
- geoalchemy2
- psycopg2
- openpyxl
- fiona
- flask (for web interface)

### External Requirements
- PostGIS-enabled PostgreSQL database
- EVC benchmark data Excel file
