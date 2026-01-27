# Test Results Memory - 2026-01-20

## Test Run Summary

**Overall: 96 passed, 22 failed (81% pass rate)**

### Passing Test Modules
- `test_config_functions.py` - 19/19 passed
- `test_processing_functions.py` - 31/31 passed

### Failing Tests

#### `test_utility_functions.py` - 2 failures

1. **`test_high_count_values`** - `generate_zone_id()` returns wrong values for counts >= 27
   - Count 27 expected "AA", got "BA"
   - Count 52 expected "BA", got "CA"
   - Algorithm off-by-one for multi-character zone IDs

2. **`test_lookup_empty_dataframe`** - `lookup_bcs_value()` crashes with empty DataFrame
   - Error: `AttributeError: Can only use .str accessor with string values`
   - Needs empty DataFrame validation

#### `test_db_io_functions.py` - 20 failures

All failures are import errors - cannot import these functions from `db-nvrmap.py`:
- `connect_db`
- `write_gdf`
- `load_evc_data`
- `load_geo_dataframe`

Possible causes:
- Function names don't match between test and source
- Functions not properly exposed/exported from main script

## Recommended Fixes

1. Fix zone ID algorithm in `generate_zone_id()` for multi-character IDs
2. Add empty DataFrame validation in `lookup_bcs_value()`
3. Verify function names in `test_db_io_functions.py` match the source
