# CLAUDE.md - Project Context for Claude Code

## Project Overview

**db-ensym** is a Python CLI tool that extracts and transforms Victorian cadastral and ecological vegetation data from a PostGIS database into standardized shapefile formats for environmental assessment workflows.

## Primary Script

`db-nvrmap.py` - Main application (~600 lines)

## What It Does

1. Accepts one of two input modes:
   - **Parcel View PFI(s)** - Database-driven workflow (traditional mode)
   - **Custom shapefile** - User-provided boundary polygons (new feature)
2. Queries a PostGIS database for spatial data (parcels, EVCs, bioregions)
3. Performs spatial intersections (parcel geometry with EVC and bioregion boundaries)
4. Outputs shapefiles in one of three formats:
   - **NVRMap** (default) - Native Vegetation Removal Map format
   - **EnSym 2017** - Environmental Symposium format
   - **EnSym 2013** - Legacy SBEU format

## CLI Usage

### PFI Mode (Traditional)
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

# Multiple PFIs
./db-nvrmap.py 12345 67890 -s output.shp
```

### Shapefile Input Mode (New Feature)
```bash
# Basic shapefile input (NVRMap format)
./db-nvrmap.py -i boundary.shp -s output.shp

# Shapefile input with EnSym 2017 format
./db-nvrmap.py -i boundary.shp -s output.shp -e

# Shapefile input with EnSym 2013 format
./db-nvrmap.py -i boundary.shp -s output.shp -b

# Shapefile input with custom gain score
./db-nvrmap.py -i boundary.shp -s output.shp -g 0.3

# Site ID field (reserved for future grouping functionality)
./db-nvrmap.py -i boundary.shp -s output.shp --site-id-field "SITE_ID"
```

### Web Interface
```bash
# Start web interface (localhost only)
./db-nvrmap.py --web

# Start on custom port
./db-nvrmap.py --web --port 8080

# Start on all network interfaces
./db-nvrmap.py --web --host 0.0.0.0

# Production mode with Gunicorn
./db-nvrmap.py --web --production --workers 4
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
- `parcel_view` - Parcel geometry and PFI (required for PFI mode)
- `parcel_detail` - Parcel details including view_pfi (required for PFI mode)
- `parcel_property` - Links parcels to properties (required for property PFI mode)
- `property_detail` - Property details including view_pfi (required for property PFI mode)
- `nv1750_evc` - Ecological Vegetation Class polygons (required for all modes)
- `bioregions` - Bioregion boundaries and codes (required for all modes)

**Note:** Shapefile input mode only requires `nv1750_evc` and `bioregions` tables, as it bypasses the parcel lookup workflow.

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

**Configuration and Database:**
- `load_config()` / `load_db_config_from_env()` / `load_config_from_env()` - Configuration loading
- `connect_db()` - Database connection and table reflection

**PFI Mode (Traditional):**
- `process_view_pfis()` - PFI conversion (property to parcel)
- `build_query()` - Spatial SQL query construction for PFI input
- `run_orchestrator()` - Main orchestration for PFI mode

**Shapefile Input Mode (New):**
- `load_input_shapefile()` - Load and validate input shapefile (polygon geometries only)
- `build_query_from_geometry()` - Spatial SQL query construction for custom geometry
- `process_shapefile_input()` - Process each polygon against EVC/bioregion data
- `run_orchestrator_shapefile()` - Main orchestration for shapefile mode

**Output Generation:**
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
- **PFI mode:** Edit `build_query()` - uses SQLAlchemy ORM with PostGIS functions (ST_Buffer, ST_Intersection, ST_Dump, etc.)
- **Shapefile mode:** Edit `build_query_from_geometry()` - constructs query from geometry instead of PFI

### Adding new config options
1. Add to config file structure
2. Use `get_attribute(config, 'key')` to access values

### Working with shapefile input
1. Input shapefiles must contain polygon geometries (Polygon or MultiPolygon)
2. System automatically reprojects to EPSG:7899 if needed
3. MultiPolygons are automatically exploded to individual Polygons
4. Each polygon is processed independently against EVC/bioregion data
5. The `--site-id-field` parameter is reserved for future grouping functionality

## Testing

For local testing, set `NVRMAP_CONFIG` to the project directory:
```bash
export NVRMAP_CONFIG='/home/brendon/Development/db-ensym'
export NVRMAP_DB_PASSWORD='your_password'  # Or configure in flake.nix
```

### Test PFI mode
```bash
./db-nvrmap.py 378176 -s test_output.shp
```

### Test shapefile input mode
```bash
# Create a test shapefile with your GIS software, then:
./db-nvrmap.py -i test_boundary.shp -s test_output.shp
```

### Run automated tests
```bash
# Run all tests
pytest

# Run shapefile input tests specifically
pytest tests/test_shapefile_input.py -v

# Run with coverage
pytest --cov=db_nvrmap
```

## Important Notes

- The `Geometry` import from geoalchemy2 is required for SQLAlchemy to recognize PostGIS geometry types during table reflection
- ST_Dump results must be cast to Geometry type: `cast(func.ST_Dump(...).geom, Geometry)`
- The `-6` meter buffer shrinks parcel geometry inward to avoid edge artifacts in spatial intersections
- **Shapefile input mode:**
  - Input shapefiles must be in a recognized CRS (automatic reprojection to EPSG:7899)
  - Only polygon geometries are supported (Polygon or MultiPolygon)
  - Line and point geometries are rejected with clear error messages
  - Each intersecting parcel from parcel_view is buffered by -6m first, then unioned together, and the custom polygon is clipped to this result. This preserves gaps between adjacent parcels while avoiding cadastral edge artifacts
  - If no cadastral parcels intersect the input boundary, a clear error message is returned
  - The web interface supports drag-and-drop .zip files containing complete shapefile components

## Web Interface

The web interface provides a user-friendly way to process both PFI and shapefile inputs:

### Features
- **PFI Mode:** Enter PFI values in a text area (comma or space-separated)
- **Shapefile Mode:** Upload shapefile as .zip file (drag-and-drop or click to select)
- **Output Format Selection:** Choose between NVRMap, EnSym 2017, or EnSym 2013
- **Custom Gain Score:** Optional override for default gain score
- **Property PFI Mode:** Toggle for property view PFI conversion
- **Progress Feedback:** Real-time processing status and error messages
- **Download Results:** Automatic .zip download containing output shapefiles

### Starting the Web Interface
```bash
# Development mode (Flask dev server)
./db-nvrmap.py --web

# Production mode (Gunicorn)
./db-nvrmap.py --web --production --workers 4 --host 0.0.0.0 --port 8080
```

### Shapefile Upload Requirements
- Must be a .zip file containing all shapefile components (.shp, .shx, .dbf, .prj recommended)
- Only polygon geometries are accepted
- CRS must be defined (automatic reprojection if needed)
- Maximum file size: 100MB (configurable in web.py)
