# db-ensym Refactoring Plan

## Overview

This document outlines the refactoring plan for `db-nvrmap.py` based on codebase analysis.

**Status:** ✅ **COMPLETED** (2026-01-16)

**Original State:** Single-file Python application (396 lines) that transforms Victorian cadastral and ecological data from PostGIS into shapefile formats (NVRMap, EnSym 2017, EnSym 2013).

**Final State:** Refactored application (~600 lines) with improved modularity, error handling, and configuration options.

**Branch:** `refactor/cleanup-and-env-vars`

---

## Phase 1: Environment Variable Support for Database Connection

### Objective
Add support for environment variables to configure PostGIS database connection, with fallback to config file.

### Environment Variables
| Variable | Maps to | Example |
|----------|---------|---------|
| `NVRMAP_DB_TYPE` | db_type | `postgresql+psycopg2` |
| `NVRMAP_DB_USER` | username | `gisuser` |
| `NVRMAP_DB_PASSWORD` | password | `secret` |
| `NVRMAP_DB_HOST` | host | `localhost` |
| `NVRMAP_DB_NAME` | database | `gisdb` |

### Behavior
1. Check for environment variables first
2. Fall back to config file (`$NVRMAP_CONFIG/config.json`) if env vars not set
3. Allow partial override (e.g., password from env var, rest from config)
4. Env vars take precedence over config file values

### Implementation
- Modify `load_config()` to merge env vars with config file
- Add new function `load_db_config_from_env()` to read env vars
- Update docstrings to document new behavior

---

## Phase 2: Extract Utility Functions

### 2.1 `format_bioevc(bioregcode, evc) -> str`
**Issue:** Duplicate logic at lines 221-223 and 235-238

**Current code (duplicated):**
```python
# In process_nvrmap_rows()
bioevc = (f"{row['bioregcode']}_{str(int(row['evc'])).zfill(4)}"
          if len(str(row["bioregcode"])) <= 3
          else f"{row['bioregcode']}{str(int(row['evc'])).zfill(4)}")

# In process_ensym_rows()
if len(str(row["bioregcode"])) <= 3:
    bioevc = row["bioregcode"] + "_" + str(int(row["evc"])).zfill(4)
if len(str(row["bioregcode"])) == 4:
    bioevc = row["bioregcode"] + str(int(row["evc"])).zfill(4)
```

**Refactored:**
```python
def format_bioevc(bioregcode: str, evc: int) -> str:
    """Format bioregion code and EVC into combined identifier.

    Adds underscore separator for bioregcodes <= 3 chars.
    """
    evc_padded = str(int(evc)).zfill(4)
    if len(str(bioregcode)) <= 3:
        return f"{bioregcode}_{evc_padded}"
    return f"{bioregcode}{evc_padded}"
```

### 2.2 `calculate_site_id(view_pfi_list, row_view_pfi) -> int`
**Issue:** Duplicate logic at lines 216-217 and 256-257

**Current code (duplicated):**
```python
si = (view_pfi_list.index(row['view_pfi'])
      + 1 if len(view_pfi_list) > 1 else 1)
```

**Refactored:**
```python
def calculate_site_id(view_pfi_list: List[str], view_pfi: str) -> int:
    """Calculate site ID based on position in view PFI list."""
    if len(view_pfi_list) > 1:
        return view_pfi_list.index(view_pfi) + 1
    return 1
```

---

## Phase 3: Simplify Row Processors

### 3.1 Refactor `process_ensym_rows()` (lines 228-265)
**Issues:**
- 38 lines with multiple nested conditions
- Duplicated site_id/zone_id logic
- Complex BCS value validation

**Actions:**
- Extract `lookup_bcs_value(bioevc, evc_df) -> str`
- Use shared `format_bioevc()` and `calculate_site_id()`
- Simplify BCS validation logic

### 3.2 Refactor `process_nvrmap_rows()` (lines 211-224)
**Actions:**
- Use shared `format_bioevc()` and `calculate_site_id()`

---

## Phase 4: Refactor GDF Builders

### 4.1 Extract column reordering logic (lines 297-300)
**Issue:** Unclear column rotation logic

**Current code:**
```python
cols = ensym_gdf.columns.tolist()
cols = cols[+1:] + cols[:+1]  # Moves first column to end
ensym_gdf = ensym_gdf[cols]
```

**Refactored:**
```python
def move_column_to_end(gdf: gpd.GeoDataFrame, column: str) -> gpd.GeoDataFrame:
    """Move specified column to end of DataFrame."""
    cols = [c for c in gdf.columns if c != column] + [column]
    return gdf[cols]
```

### 4.2 Extract config value access
**Issue:** Repeated `config['attribute_table'].get('key')` pattern (8 occurrences)

**Refactored:**
```python
def get_attribute(config: Dict, key: str, default=None):
    """Get value from attribute_table config section."""
    return config.get('attribute_table', {}).get(key, default)
```

---

## Phase 5: Improve Error Handling

### 5.1 Replace bare exception (lines 376-379)
**Current:**
```python
try:
    output_gdf.to_file(args.shapefile, schema=schema, engine='fiona')
except Exception as e:
    print(f"Failed to write to {args.shapefile}: {e}")
```

**Refactored:**
```python
try:
    output_gdf.to_file(args.shapefile, schema=schema, engine='fiona')
except (IOError, OSError) as e:
    logging.error(f"Failed to write shapefile {args.shapefile}: {e}")
    raise
```

### 5.2 Add logging for silent EVC lookup failures (lines 240-244)
**Current:**
```python
try:
    bcs_value = evc_df[evc_df['BIOEVCCODE'].str.contains(bioevc)].iloc[0, 5]
except IndexError:
    bcs_value = 'LC'
```

**Refactored:**
```python
try:
    bcs_value = evc_df[evc_df['BIOEVCCODE'].str.contains(bioevc)]['BCS_CATEGORY'].iloc[0]
except IndexError:
    logging.warning(f"BCS value not found for {bioevc}, defaulting to 'LC'")
    bcs_value = 'LC'
```

---

## Phase 6: Add Constants

### Magic Numbers to Extract
| Value | Location | Constant Name | Purpose |
|-------|----------|---------------|---------|
| `-6` | Line 156 | `PARCEL_BUFFER_METERS` | Buffer distance for parcel geometry |
| `10000` | Line 293 | `SQ_METERS_PER_HECTARE` | Conversion factor for area |

```python
# Constants
PARCEL_BUFFER_METERS = -6  # Negative buffer to shrink parcel geometry
SQ_METERS_PER_HECTARE = 10000
```

---

## Phase 7: Cleanup

### 7.1 GeoAlchemy2 Import
- **Note:** The `from geoalchemy2 import Geometry` import was initially removed as "unused" but was restored because it's required for SQLAlchemy to recognize PostGIS geometry types during table reflection.
- Also added `cast` import to properly handle ST_Dump composite type results.

### 7.2 Standardize code style
- Use single `#` for comments (currently mixed `#` and `##`)
- Standardize on f-strings (currently mixed with string concatenation)
- Consistent quote style for dictionary access

---

## Implementation Order

1. Create git branch `refactor/cleanup-and-env-vars`
2. Phase 1: Environment variable support
3. Phase 2: Extract utility functions
4. Phase 3: Simplify row processors
5. Phase 4: Refactor GDF builders
6. Phase 5: Improve error handling
7. Phase 6: Add constants
8. Phase 7: Cleanup

---

## Files Affected

- `db-nvrmap.py` - Main application (all changes)
- `.envrc` - May need updates for new env var documentation
- `README.md` - Document new environment variable options

---

## Testing Checklist

- [x] Environment variables override config file
- [x] Partial env var override works (e.g., password only)
- [x] Falls back to config file when env vars not set
- [x] All three output formats still work (NVRMap, EnSym 2017, EnSym 2013)
- [x] Property view PFI conversion still works
- [x] Error messages are logged correctly
- [x] Script runs successfully with test data (verified 2026-01-16)
