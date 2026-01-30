# Implementation Plan: Custom Shapefile Input Support

## Overview

This document details the implementation plan for adding custom shapefile input support to db-ensym. The feature allows users to provide their own boundary shapefile instead of using parcel/property view_pfi values. The system will intersect the user's geometry with EVC and bioregion data from the PostGIS database.

**Primary Approach**: Boundary-only shapefile - User provides site boundary geometry, system queries DB for EVC/bioregion intersections.

---

## 1. ProcessingOptions Changes (core.py)

The `ProcessingOptions` dataclass at line 81-98 needs to be extended to support shapefile input.

### Current Structure

```python
@dataclass
class ProcessingOptions:
    """Options for shapefile processing."""
    view_pfi: List[int]
    shapefile: str = "nvrmap"
    gainscore: Optional[float] = None
    property_view: bool = False
    output_format: OutputFormat = OutputFormat.NVRMAP
```

### Required Changes

- Add `input_shapefile: Optional[str] = None` - Path to user-provided shapefile
- Make `view_pfi: List[int]` optional (allow empty list when using shapefile input)
- Add validation logic: either `view_pfi` must be provided OR `input_shapefile` must be provided (not both, not neither)
- Add property to check input mode: `@property def uses_shapefile_input(self) -> bool`

### New Fields

```python
input_shapefile: Optional[str] = None  # Path to input shapefile
site_id_field: Optional[str] = None    # Optional: field name in shapefile to use for site_id grouping
```

### Validation Method

Add a `__post_init__` method to validate that exactly one input mode is specified:

```python
def __post_init__(self):
    has_pfi = self.view_pfi and len(self.view_pfi) > 0
    has_shapefile = self.input_shapefile is not None
    if not has_pfi and not has_shapefile:
        raise ValueError("Either view_pfi or input_shapefile must be provided")
    if has_pfi and has_shapefile:
        raise ValueError("Cannot specify both view_pfi and input_shapefile")

@property
def uses_shapefile_input(self) -> bool:
    return self.input_shapefile is not None
```

---

## 2. CLI Changes (cli.py)

### New Arguments to Add

```python
parser.add_argument(
    "-i", "--input",
    type=str,
    dest="input_shapefile",
    help="Path to input shapefile (boundary only). System will intersect with EVC/bioregion data."
)

parser.add_argument(
    "--site-id-field",
    type=str,
    help="Field name in input shapefile to use for site_id grouping (optional)"
)
```

### Updates to `parse_args()` (lines 10-89)

- Make `view_pfi` truly optional by updating its nargs handling
- Add the new arguments above

### Updates to `args_to_options()` (lines 92-107)

- Handle the new input_shapefile argument
- Pass site_id_field if provided

### Updates to `run_cli()` (lines 110-122)

- Update validation: require either PFI values OR input shapefile (not both)
- Current check `if not args.view_pfi` needs to change to check for either input mode

### Updated Validation Logic

```python
def run_cli(args: argparse.Namespace) -> int:
    """Run the CLI processing mode."""
    has_pfi = args.view_pfi and len(args.view_pfi) > 0
    has_shapefile = args.input_shapefile is not None

    if not has_pfi and not has_shapefile:
        print("Error: Either PFI values or --input shapefile is required.", file=sys.stderr)
        return 1

    if has_pfi and has_shapefile:
        print("Error: Cannot specify both PFI values and --input shapefile.", file=sys.stderr)
        return 1
    # ... rest of function
```

---

## 3. Core Processing Changes (core.py)

This is the most significant set of changes. The key insight is that `build_query()` (lines 167-204) constructs a spatial query that:

1. Gets parcel geometry from DB based on PFI
2. Buffers the geometry by -6 meters
3. Intersects with EVC data
4. Intersects with bioregion data

For shapefile input, we need an alternative path that:

1. Loads geometry from user shapefile
2. Uses that geometry for intersection with EVC/bioregion (skip buffer step since user provides exact boundary)

### 3.1 New Function: `load_input_shapefile()`

```python
def load_input_shapefile(path: str, site_id_field: Optional[str] = None) -> gpd.GeoDataFrame:
    """
    Load user-provided input shapefile.

    Args:
        path: Path to the shapefile
        site_id_field: Optional field to use for site_id grouping

    Returns:
        GeoDataFrame with geometry and optional site_id
    """
    gdf = gpd.read_file(path)

    # Validate CRS - must be EPSG:7899 (Victorian) or reproject
    if gdf.crs is None:
        raise ValueError("Input shapefile has no CRS defined. Expected EPSG:7899.")

    if gdf.crs.to_epsg() != 7899:
        logging.info(f"Reprojecting from {gdf.crs} to EPSG:7899")
        gdf = gdf.to_crs(DEFAULT_CRS)

    # Validate geometry type - must be Polygon or MultiPolygon
    geom_types = gdf.geometry.geom_type.unique()
    valid_types = {'Polygon', 'MultiPolygon'}
    if not set(geom_types).issubset(valid_types):
        raise ValueError(f"Input shapefile must contain Polygon geometries. Found: {geom_types}")

    # Explode MultiPolygons to Polygons for consistent processing
    gdf = gdf.explode(index_parts=False).reset_index(drop=True)

    return gdf
```

### 3.2 New Function: `build_query_from_geometry()`

This function builds a spatial query using user-provided geometry instead of parcel PFI lookup.

```python
def build_query_from_geometry(
    nv1750_evc,
    bioregions,
    geometry_wkt: str,
    srid: int = 7899
) -> Any:
    """
    Construct SQL query for spatial data extraction using provided geometry.

    Args:
        nv1750_evc: SQLAlchemy table for EVC data
        bioregions: SQLAlchemy table for bioregion data
        geometry_wkt: WKT representation of input geometry
        srid: Spatial Reference ID (default 7899 for Victoria)

    Returns:
        SQLAlchemy select statement
    """
    # Convert WKT to PostGIS geometry
    input_geom = func.ST_GeomFromText(geometry_wkt, srid)

    # Intersect with EVC - no buffer since user provides exact boundary
    clipped_geom = func.ST_Dump(
        func.ST_Intersection(input_geom, nv1750_evc.c.geom)
    ).geom

    clipped_subq = (
        select(
            nv1750_evc.c.evc,
            nv1750_evc.c.x_evcname,
            clipped_geom.label("geom")
        )
        .where(func.ST_Intersects(input_geom, nv1750_evc.c.geom))
        .subquery("clipped")
    )

    # Intersect with bioregions
    outer_geom = func.ST_Dump(
        func.ST_Intersection(clipped_subq.c.geom, bioregions.c.geom)
    ).geom

    bio_clipped_subq = (
        select(
            clipped_subq.c.evc,
            clipped_subq.c.x_evcname,
            bioregions.c.bioregcode,
            bioregions.c.bioregion,
            outer_geom.label("geom")
        )
        .join(bioregions, func.ST_Intersects(clipped_subq.c.geom, bioregions.c.geom))
        .where(func.ST_Dimension(clipped_subq.c.geom) == 2)
        .subquery("bio_clipped")
    )

    return select(bio_clipped_subq).order_by(bio_clipped_subq.c.bioregcode)
```

### 3.3 New Function: `process_shapefile_input()`

This orchestrates the shapefile-based processing:

```python
def process_shapefile_input(
    opts: ProcessingOptions,
    engine: Any,
    tables: Dict[str, Any]
) -> gpd.GeoDataFrame:
    """
    Process user-provided shapefile through EVC/bioregion intersection.

    Args:
        opts: Processing options with input_shapefile path
        engine: SQLAlchemy database engine
        tables: Dictionary of reflected database tables

    Returns:
        GeoDataFrame with intersected results
    """
    # Load input shapefile
    input_gdf = load_input_shapefile(opts.input_shapefile, opts.site_id_field)

    results = []

    # Process each polygon separately and track source
    for idx, row in input_gdf.iterrows():
        geom_wkt = row.geometry.wkt

        query = build_query_from_geometry(
            tables["nv1750_evc"],
            tables["bioregions"],
            geom_wkt
        )

        try:
            result_gdf = load_geo_dataframe(engine, query)
            # Add source polygon index for site_id tracking
            result_gdf['source_idx'] = idx
            results.append(result_gdf)
        except ValueError:
            # No intersection found for this polygon
            logging.warning(f"No EVC/bioregion data found for polygon {idx}")
            continue

    if not results:
        raise ValueError("No EVC/bioregion data found for any input polygons.")

    # Combine all results
    combined_gdf = pd.concat(results, ignore_index=True)
    return combined_gdf
```

### 3.4 Updates to `generate_shapefile()` (lines 393-415)

The main orchestrator function needs to branch based on input mode:

```python
def generate_shapefile(opts: ProcessingOptions) -> gpd.GeoDataFrame:
    """Generate a shapefile from PFI values or input shapefile."""
    config = load_config()
    engine, tables = connect_db(config["db_connection"])

    if opts.uses_shapefile_input:
        # Shapefile input mode
        input_gdf = process_shapefile_input(opts, engine, tables)
        # Create synthetic view_pfis list from source_idx for compatibility
        view_pfis = input_gdf['source_idx'].unique().tolist()
        # Map source_idx to view_pfi column for downstream processing
        input_gdf['view_pfi'] = input_gdf['source_idx'].astype(str)
    else:
        # PFI input mode (existing logic)
        view_pfis = process_view_pfis(opts, engine, tables["parcel_property"],
                                      tables["parcel_detail"], tables["property_detail"])
        query = build_query(tables["parcel_view"], tables["nv1750_evc"],
                           tables["bioregions"], view_pfis)
        input_gdf = load_geo_dataframe(engine, query)

    evc_df = load_evc_data(config["evc_data"])
    output_gdf = select_output_gdf(opts, input_gdf, evc_df, view_pfis, config)
    write_shapefile(output_gdf, opts.output_format, opts.shapefile)
    return output_gdf
```

Similar updates needed for `generate_shapefile_to_gdf()` (lines 418-438).

---

## 4. Web Interface Changes (web.py)

### 4.1 Template Changes (index.html)

Add a file upload section and input mode toggle. Insert after the PFI textarea:

```html
<div class="form-group">
    <label>Input Method</label>
    <div class="radio-group">
        <label class="radio-option">
            <input type="radio" name="input_method" value="pfi" checked onchange="toggleInputMethod()">
            PFI Numbers
        </label>
        <label class="radio-option">
            <input type="radio" name="input_method" value="shapefile" onchange="toggleInputMethod()">
            Upload Shapefile
        </label>
    </div>
</div>

<div class="form-group" id="pfi-input">
    <label for="pfis">
        PFI Numbers
        <span class="label-hint">(one per line, or comma/space separated)</span>
    </label>
    <textarea id="pfis" name="pfis" rows="6" placeholder="12345678&#10;87654321"></textarea>
</div>

<div class="form-group" id="shapefile-input" style="display: none;">
    <label for="shapefile_upload">
        Shapefile Upload
        <span class="label-hint">(ZIP containing .shp, .shx, .dbf, .prj files)</span>
    </label>
    <input type="file" id="shapefile_upload" name="shapefile_upload" accept=".zip">
    <p class="help-text">Upload a ZIP file containing your shapefile components</p>
</div>

<script>
function toggleInputMethod() {
    const method = document.querySelector('input[name="input_method"]:checked').value;
    document.getElementById('pfi-input').style.display = method === 'pfi' ? 'block' : 'none';
    document.getElementById('shapefile-input').style.display = method === 'shapefile' ? 'block' : 'none';
}
</script>
```

### 4.2 Flask Route Changes (web.py)

Update the `/generate` route (lines 32-123) to handle file uploads:

```python
@app.route("/generate", methods=["POST"])
def generate():
    """Process PFIs or uploaded shapefile and return ZIP download."""
    input_method = request.form.get("input_method", "pfi")

    if input_method == "shapefile":
        # Handle shapefile upload
        if 'shapefile_upload' not in request.files:
            flash("Please upload a shapefile.", "error")
            return redirect(url_for("index"))

        file = request.files['shapefile_upload']
        if file.filename == '':
            flash("No file selected.", "error")
            return redirect(url_for("index"))

        if not file.filename.endswith('.zip'):
            flash("Please upload a ZIP file containing shapefile components.", "error")
            return redirect(url_for("index"))

        # Process will extract and validate shapefile in temp directory
        # ... (extraction logic below)

        opts = ProcessingOptions(
            view_pfi=[],  # Empty for shapefile mode
            input_shapefile=extracted_shapefile_path,
            shapefile=filename,
            gainscore=gainscore,
            property_view=False,
            output_format=output_format,
        )
    else:
        # Existing PFI processing logic (lines 36-86)
        # ...
        opts = ProcessingOptions(
            view_pfi=pfis,
            shapefile=filename,
            gainscore=gainscore,
            property_view=(view_type == "property"),
            output_format=output_format,
        )
```

### 4.3 New Helper Function for ZIP Extraction

Add to web.py:

```python
def extract_shapefile_from_zip(zip_file, temp_dir: str) -> str:
    """
    Extract shapefile from uploaded ZIP and return path to .shp file.

    Args:
        zip_file: Werkzeug FileStorage object
        temp_dir: Temporary directory path

    Returns:
        Path to extracted .shp file

    Raises:
        ValueError: If ZIP doesn't contain valid shapefile components
    """
    extract_path = Path(temp_dir) / "input"
    extract_path.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_file, 'r') as zf:
        zf.extractall(extract_path)

    # Find .shp file
    shp_files = list(extract_path.glob("**/*.shp"))
    if not shp_files:
        raise ValueError("No .shp file found in uploaded ZIP")
    if len(shp_files) > 1:
        raise ValueError("Multiple .shp files found in ZIP. Please upload only one shapefile.")

    shp_path = shp_files[0]

    # Validate required components exist
    required_extensions = ['.shp', '.shx', '.dbf']
    for ext in required_extensions:
        if not (shp_path.parent / (shp_path.stem + ext)).exists():
            raise ValueError(f"Missing required shapefile component: {shp_path.stem}{ext}")

    return str(shp_path)
```

### 4.4 Update Form Encoding

Change form tag to support file upload:

```html
<form action="{{ url_for('generate') }}" method="POST" enctype="multipart/form-data">
```

---

## 5. Testing Considerations

### 5.1 New Test File: `tests/test_shapefile_input.py`

```python
"""Tests for custom shapefile input functionality."""

import pytest
import geopandas as gpd
from shapely.geometry import Polygon
from pathlib import Path
import tempfile

from db_nvrmap.core import (
    ProcessingOptions,
    load_input_shapefile,
    process_shapefile_input,
)


class TestProcessingOptionsValidation:
    """Tests for ProcessingOptions validation with shapefile input."""

    def test_requires_either_pfi_or_shapefile(self):
        """Test that validation fails when neither is provided."""
        with pytest.raises(ValueError, match="Either view_pfi or input_shapefile"):
            ProcessingOptions(view_pfi=[], shapefile="output")

    def test_rejects_both_pfi_and_shapefile(self):
        """Test that validation fails when both are provided."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            ProcessingOptions(
                view_pfi=[12345],
                input_shapefile="/path/to/file.shp",
                shapefile="output"
            )

    def test_accepts_pfi_only(self):
        """Test that PFI-only mode works."""
        opts = ProcessingOptions(view_pfi=[12345], shapefile="output")
        assert not opts.uses_shapefile_input

    def test_accepts_shapefile_only(self):
        """Test that shapefile-only mode works."""
        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile="/path/to/file.shp",
            shapefile="output"
        )
        assert opts.uses_shapefile_input


class TestLoadInputShapefile:
    """Tests for load_input_shapefile function."""

    @pytest.fixture
    def sample_shapefile(self, tmp_path):
        """Create a sample shapefile for testing."""
        polygon = Polygon([(0, 0), (100, 0), (100, 100), (0, 100)])
        gdf = gpd.GeoDataFrame(
            {'name': ['Site A']},
            geometry=[polygon],
            crs='epsg:7899'
        )
        shp_path = tmp_path / "test.shp"
        gdf.to_file(shp_path)
        return str(shp_path)

    def test_loads_valid_shapefile(self, sample_shapefile):
        """Test loading a valid shapefile."""
        gdf = load_input_shapefile(sample_shapefile)
        assert len(gdf) == 1
        assert gdf.crs.to_epsg() == 7899

    def test_reprojects_different_crs(self, tmp_path):
        """Test that shapefiles in different CRS are reprojected."""
        polygon = Polygon([(144.9, -37.8), (145.0, -37.8), (145.0, -37.7), (144.9, -37.7)])
        gdf = gpd.GeoDataFrame(
            {'name': ['Site A']},
            geometry=[polygon],
            crs='epsg:4326'  # WGS84
        )
        shp_path = tmp_path / "wgs84.shp"
        gdf.to_file(shp_path)

        result = load_input_shapefile(str(shp_path))
        assert result.crs.to_epsg() == 7899

    def test_rejects_no_crs(self, tmp_path):
        """Test that shapefiles without CRS are rejected."""
        polygon = Polygon([(0, 0), (100, 0), (100, 100), (0, 100)])
        gdf = gpd.GeoDataFrame({'name': ['Site A']}, geometry=[polygon])
        # Manually write without .prj file
        shp_path = tmp_path / "no_crs.shp"
        gdf.to_file(shp_path)
        (tmp_path / "no_crs.prj").unlink()  # Remove .prj file

        with pytest.raises(ValueError, match="no CRS defined"):
            load_input_shapefile(str(shp_path))

    def test_explodes_multipolygons(self, tmp_path):
        """Test that MultiPolygons are exploded to Polygons."""
        from shapely.geometry import MultiPolygon
        poly1 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        poly2 = Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])
        multi = MultiPolygon([poly1, poly2])

        gdf = gpd.GeoDataFrame({'name': ['Multi']}, geometry=[multi], crs='epsg:7899')
        shp_path = tmp_path / "multi.shp"
        gdf.to_file(shp_path)

        result = load_input_shapefile(str(shp_path))
        assert len(result) == 2
        assert all(result.geometry.geom_type == 'Polygon')
```

### 5.2 Integration Tests

Add integration tests that test the full pipeline with mock database:
- Test shapefile input with mocked DB queries
- Test that results have correct structure for all output formats
- Test error handling when no intersections found

### 5.3 CLI Tests

Update `tests/test_cli.py` (if exists) or add new tests:
- Test `--input` flag parsing
- Test mutual exclusivity of PFI and shapefile input
- Test `--site-id-field` parsing

---

## 6. Error Handling and Validation

### 6.1 Input Shapefile Validation

Implement in `load_input_shapefile()`:
- **CRS validation**: Must have CRS defined; auto-reproject if not EPSG:7899
- **Geometry type**: Must be Polygon or MultiPolygon
- **Empty geometries**: Skip with warning
- **File existence**: Clear error message if file not found
- **Required components**: Check .shp, .shx, .dbf files exist

### 6.2 Processing Errors

Implement in `process_shapefile_input()`:
- **No intersections**: Warning per polygon, error if all fail
- **Database errors**: Propagate with context
- **Memory concerns**: Consider processing large shapefiles in chunks

### 6.3 Web Upload Errors

Implement in `/generate` route:
- **File size limits**: Add max file size check (e.g., 50MB)
- **File type validation**: Only accept .zip
- **ZIP content validation**: Must contain valid shapefile components
- **Temporary file cleanup**: Ensure cleanup on error

---

## 7. Implementation Sequence

### Phase 1: Core Infrastructure
1. Update `ProcessingOptions` dataclass with new fields and validation
2. Implement `load_input_shapefile()` function
3. Implement `build_query_from_geometry()` function
4. Add unit tests for new functions

### Phase 2: Processing Integration
5. Implement `process_shapefile_input()` function
6. Update `generate_shapefile()` to handle both modes
7. Update `generate_shapefile_to_gdf()` similarly
8. Add integration tests

### Phase 3: CLI Integration
9. Add new CLI arguments
10. Update `args_to_options()` and validation
11. Add CLI tests

### Phase 4: Web Interface
12. Update HTML template with file upload
13. Update Flask route to handle uploads
14. Add ZIP extraction helper
15. Test web interface manually

### Phase 5: Polish
16. Add logging throughout
17. Improve error messages
18. Update documentation/CLAUDE.md
19. Final testing

---

## 8. Files to Modify

| File | Changes |
|------|---------|
| `db_nvrmap/core.py` | ProcessingOptions, 3 new functions, update orchestrator |
| `db_nvrmap/cli.py` | New arguments, validation updates |
| `db_nvrmap/web.py` | File upload handling, ZIP extraction |
| `db_nvrmap/templates/index.html` | File upload UI, JavaScript toggle |
| `tests/test_shapefile_input.py` | New test file for shapefile input |

---

## 9. Example Usage (After Implementation)

### CLI

```bash
# Use custom shapefile
./db-nvrmap.py -i my_boundary.shp -s output.shp

# With custom site ID field
./db-nvrmap.py -i my_boundary.shp -s output.shp --site-id-field LOT_ID

# EnSym format with custom shapefile
./db-nvrmap.py -i my_boundary.shp -s output.shp -e

# EnSym 2013 format
./db-nvrmap.py -i my_boundary.shp -s output.shp -b

# With gain score override
./db-nvrmap.py -i my_boundary.shp -s output.shp -g 0.5
```

### Web Interface

Users will see a radio toggle to choose between:
- **PFI Numbers** (existing functionality)
- **Upload Shapefile** (new functionality)

With appropriate form inputs for each mode. The output format, gain score, and filename options remain the same for both input methods.
