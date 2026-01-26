# db-ensym Project Memory

## Project Overview
- Python CLI tool for extracting Victorian cadastral and ecological vegetation data from PostGIS
- Main script: `db-nvrmap.py` (~600 lines)
- Outputs shapefiles in three formats: NVRMap (default), EnSym 2017, EnSym 2013

## Test Suite (January 2026)
Created comprehensive pytest unit tests in `tests/` directory:

- `test_config_functions.py` - Tests for load_db_config_from_env, load_config, get_attribute
- `test_utility_functions.py` - Tests for generate_zone_id, format_bioevc, calculate_site_id, lookup_bcs_value, move_column_to_end
- `test_processing_functions.py` - Tests for process_nvrmap_rows, process_ensym_rows, build_ensym_gdf, build_nvrmap_gdf, select_output_gdf
- `test_db_io_functions.py` - Tests for connect_db, write_gdf, load_evc_data, load_geo_dataframe

### Running Tests
```bash
pytest tests/ -v
```

### Key Testing Notes
- Module import uses `importlib.util` due to hyphenated filename `db-nvrmap.py`
- Geometry column is `geom` not `geometry`
- CRS is lowercase `epsg:7899`
- `lookup_bcs_value()` uses substring matching via `.str.contains()`
- Zone IDs: A-Z for 1-26, AA-AZ for 27-52, etc.

## Configuration
- Environment variables: NVRMAP_DB_TYPE, NVRMAP_DB_USER, NVRMAP_DB_PASSWORD, NVRMAP_DB_HOST, NVRMAP_DB_NAME
- Config file: `$NVRMAP_CONFIG/config.json`
- Env vars override config file values

## Build System
Uses Nix flakes: `nix develop` for dev shell
