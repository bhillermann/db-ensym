# CLAUDE.md - Project Context for Claude Code

## Project Overview

**db-ensym** is a Python CLI tool that extracts and transforms Victorian cadastral and ecological vegetation data from a PostGIS database into standardized shapefile formats for environmental assessment workflows.

## Primary Script

`db-nvrmap.py` - Main application (~600 lines)

## What It Does

1. Accepts Parcel View PFI(s) as input
2. Queries a PostGIS database for spatial data (parcels, EVCs, bioregions)
3. Performs spatial intersections (parcel geometry with EVC and bioregion boundaries)
4. Outputs shapefiles in one of three formats:
   - **NVRMap** (default) - Native Vegetation Removal Map format
   - **EnSym 2017** - Environmental Symposium format
   - **EnSym 2013** - Legacy SBEU format

## CLI Usage

```bash
# Basic usage (NVRMap format)
./db-nvrmap.py <view_pfi> -s output.shp

# EnSym 2017 format
./db-nvrmap.py <view_pfi> -s output.shp -e

# EnSym 2013 format
./db-nvrmap.py <view_pfi> -s output.shp -b

# Property View PFI (converts to parcel PFIs)
./db-nvrmap.py <property_view_pfi> -s output.shp -p

# Override gain score
./db-nvrmap.py <view_pfi> -s output.shp -g 0.5
```

## Configuration

### Environment Variables (preferred for credentials)
```bash
# Database connection
NVRMAP_DB_TYPE=postgresql+psycopg2
NVRMAP_DB_USER=gisuser
NVRMAP_DB_PASSWORD=secret
NVRMAP_DB_HOST=localhost
NVRMAP_DB_NAME=gisdb

# EVC data path
NVRMAP_EVC_DATA=/path/to/evc_data.xlsx

# Attribute table settings
NVRMAP_PROJECT=MyProject
NVRMAP_COLLECTOR=CollectorName
NVRMAP_DEFAULT_GAIN_SCORE=0.22
NVRMAP_DEFAULT_HABITAT_SCORE=0.5
```

### Config File (fallback)
Set `NVRMAP_CONFIG` environment variable to config directory path.
Config file: `$NVRMAP_CONFIG/config.json`

```json
{
    "db_connection": {
        "db_type": "postgresql+psycopg2",
        "username": "gisuser",
        "password": "password",
        "host": "localhost",
        "database": "gisdb"
    },
    "attribute_table": {
        "project": "PROJECT_ID",
        "collector": "Collector Name",
        "default_habitat_score": 0.5,
        "default_gain_score": 0.22
    },
    "evc_data": "~/path/to/evc_data.xlsx"
}
```

**Priority:** Environment variables override config file values.

## Database Requirements

PostGIS database with these tables:
- `parcel_view` - Parcel geometry and PFI
- `parcel_detail` - Parcel details including view_pfi
- `parcel_property` - Links parcels to properties
- `property_detail` - Property details including view_pfi
- `nv1750_evc` - Ecological Vegetation Class polygons
- `bioregions` - Bioregion boundaries and codes

## Key Dependencies

- **geopandas** - Spatial data manipulation
- **sqlalchemy** + **geoalchemy2** - Database ORM with PostGIS support
- **psycopg2** - PostgreSQL adapter
- **fiona** - Shapefile I/O
- **pandas** - Data manipulation
- **openpyxl** - Excel file reading (for EVC data)

## Build System

Uses **Nix flakes** for reproducible builds:
```bash
nix build    # Build the package
nix develop  # Enter dev shell with all dependencies
```

## Code Architecture

### Constants
- `DEFAULT_CRS = 'epsg:7899'` - Victorian CRS
- `PARCEL_BUFFER_METERS = -6` - Inward buffer for parcel geometry
- `SQ_METERS_PER_HECTARE = 10000` - Area conversion factor
- Schema definitions for each output format

### Key Functions
- `load_config()` / `load_db_config_from_env()` / `load_config_from_env()` - Configuration loading
- `connect_db()` - Database connection and table reflection
- `process_view_pfis()` - PFI conversion (property to parcel)
- `build_query()` - Spatial SQL query construction
- `build_ensym_gdf()` / `build_nvrmap_gdf()` - Output DataFrame builders
- `write_gdf()` - Shapefile output with schema

### Utility Functions
- `format_bioevc()` - Format bioregion/EVC codes
- `calculate_site_id()` - Calculate site ID from PFI list
- `generate_zone_id()` - Convert count to alphabetic zone ID (A-Z, AA-AZ, etc.)
- `lookup_bcs_value()` - BCS conservation status lookup
- `move_column_to_end()` - DataFrame column reordering
- `get_attribute()` - Safe config value accessor

## Common Tasks

### Adding a new output format
1. Define schema constant (like `ENSYM_2017_SCHEMA`)
2. Create builder function (like `build_ensym_gdf()`)
3. Add CLI argument in `parse_args()`
4. Update `select_output_gdf()` to handle new format
5. Update `write_gdf()` to select correct schema

### Modifying spatial query
Edit `build_query()` - uses SQLAlchemy ORM with PostGIS functions (ST_Buffer, ST_Intersection, ST_Dump, etc.)

### Adding new config options
1. Add to config file structure
2. Use `get_attribute(config, 'key')` to access values

## Testing

For local testing, set `NVRMAP_CONFIG` to the project directory:
```bash
export NVRMAP_CONFIG='/home/brendon/Development/db-ensym'
export NVRMAP_DB_PASSWORD='your_password'  # Or configure in flake.nix
```

Run with a known PFI to verify output:
```bash
./db-nvrmap.py 378176 -s test_output.shp
```

## Important Notes

- The `Geometry` import from geoalchemy2 is required for SQLAlchemy to recognize PostGIS geometry types during table reflection
- ST_Dump results must be cast to Geometry type: `cast(func.ST_Dump(...).geom, Geometry)`
- The `-6` meter buffer shrinks parcel geometry inward to avoid edge artifacts in spatial intersections
