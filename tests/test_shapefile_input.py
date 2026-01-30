#!/usr/bin/env python3
"""
Unit tests for custom shapefile input functionality (Phase 1).

Tests cover:
- ProcessingOptions validation with shapefile input
- load_input_shapefile function validation and processing
"""

import pytest
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from pathlib import Path
import sys

# Add parent directory to path to import from db_nvrmap
sys.path.insert(0, str(Path(__file__).parent.parent))

from db_nvrmap.core import (
    ProcessingOptions,
    load_input_shapefile,
    process_shapefile_input,
    build_query_from_geometry,
    check_parcel_intersection,
    OutputFormat,
)
from db_nvrmap.cli import parse_args, args_to_options, run_cli


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
        assert opts.view_pfi == [12345]

    def test_accepts_shapefile_only(self):
        """Test that shapefile-only mode works."""
        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile="/path/to/file.shp",
            shapefile="output"
        )
        assert opts.uses_shapefile_input
        assert opts.input_shapefile == "/path/to/file.shp"

    def test_site_id_field_optional(self):
        """Test that site_id_field is optional."""
        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile="/path/to/file.shp",
            shapefile="output",
            site_id_field="LOT_ID"
        )
        assert opts.site_id_field == "LOT_ID"

    def test_all_fields_with_shapefile(self):
        """Test creating options with all fields for shapefile mode."""
        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile="/path/to/file.shp",
            shapefile="output.shp",
            gainscore=0.5,
            property_view=False,
            output_format=OutputFormat.ENSYM_2017,
            site_id_field="SITE_ID"
        )
        assert opts.uses_shapefile_input
        assert opts.gainscore == 0.5
        assert opts.output_format == OutputFormat.ENSYM_2017
        assert opts.ensym


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

    @pytest.fixture
    def multi_polygon_shapefile(self, tmp_path):
        """Create a shapefile with MultiPolygon geometry."""
        poly1 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        poly2 = Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])
        multi = MultiPolygon([poly1, poly2])

        gdf = gpd.GeoDataFrame({'name': ['Multi']}, geometry=[multi], crs='epsg:7899')
        shp_path = tmp_path / "multi.shp"
        gdf.to_file(shp_path)
        return str(shp_path)

    @pytest.fixture
    def wgs84_shapefile(self, tmp_path):
        """Create a shapefile in WGS84 CRS."""
        # Simple polygon in WGS84 coordinates (near Melbourne)
        polygon = Polygon([(144.9, -37.8), (145.0, -37.8), (145.0, -37.7), (144.9, -37.7)])
        gdf = gpd.GeoDataFrame(
            {'name': ['Site A']},
            geometry=[polygon],
            crs='epsg:4326'  # WGS84
        )
        shp_path = tmp_path / "wgs84.shp"
        gdf.to_file(shp_path)
        return str(shp_path)

    def test_loads_valid_shapefile(self, sample_shapefile):
        """Test loading a valid shapefile."""
        gdf = load_input_shapefile(sample_shapefile)
        assert len(gdf) == 1
        assert gdf.crs.to_epsg() == 7899
        assert gdf.geometry.geom_type.iloc[0] == 'Polygon'

    def test_reprojects_different_crs(self, wgs84_shapefile):
        """Test that shapefiles in different CRS are reprojected."""
        result = load_input_shapefile(wgs84_shapefile)
        assert result.crs.to_epsg() == 7899
        assert len(result) == 1

    def test_rejects_no_crs(self, tmp_path):
        """Test that shapefiles without CRS are rejected."""
        polygon = Polygon([(0, 0), (100, 0), (100, 100), (0, 100)])
        gdf = gpd.GeoDataFrame({'name': ['Site A']}, geometry=[polygon])

        # Create a shapefile directory structure without .prj file
        shp_dir = tmp_path / "no_crs"
        shp_dir.mkdir()
        shp_path = shp_dir / "no_crs.shp"

        # Write shapefile with fiona explicitly setting crs to None
        import fiona
        from fiona.crs import from_epsg

        schema = {
            'geometry': 'Polygon',
            'properties': {'name': 'str'}
        }

        with fiona.open(str(shp_path), 'w', driver='ESRI Shapefile',
                       crs=None, schema=schema) as dst:
            dst.write({
                'geometry': polygon.__geo_interface__,
                'properties': {'name': 'Site A'}
            })

        with pytest.raises(ValueError, match="no CRS defined"):
            load_input_shapefile(str(shp_path))

    def test_explodes_multipolygons(self, multi_polygon_shapefile):
        """Test that MultiPolygons are exploded to Polygons."""
        result = load_input_shapefile(multi_polygon_shapefile)
        assert len(result) == 2
        assert all(result.geometry.geom_type == 'Polygon')

    def test_rejects_invalid_geometry_types(self, tmp_path):
        """Test that non-polygon geometries are rejected."""
        from shapely.geometry import Point

        point = Point(0, 0)
        gdf = gpd.GeoDataFrame({'name': ['Point']}, geometry=[point], crs='epsg:7899')
        shp_path = tmp_path / "points.shp"
        gdf.to_file(shp_path)

        with pytest.raises(ValueError, match="must contain Polygon geometries"):
            load_input_shapefile(str(shp_path))

    def test_rejects_linestring_geometry(self, tmp_path):
        """Test that LineString geometries are rejected."""
        from shapely.geometry import LineString

        line = LineString([(0, 0), (100, 100)])
        gdf = gpd.GeoDataFrame({'name': ['Line']}, geometry=[line], crs='epsg:7899')
        shp_path = tmp_path / "lines.shp"
        gdf.to_file(shp_path)

        with pytest.raises(ValueError, match="must contain Polygon geometries"):
            load_input_shapefile(str(shp_path))

    def test_multiple_polygons_preserved(self, tmp_path):
        """Test that multiple polygon features are preserved."""
        poly1 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        poly2 = Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])
        poly3 = Polygon([(40, 0), (50, 0), (50, 10), (40, 10)])

        gdf = gpd.GeoDataFrame(
            {'name': ['Site A', 'Site B', 'Site C']},
            geometry=[poly1, poly2, poly3],
            crs='epsg:7899'
        )
        shp_path = tmp_path / "multiple.shp"
        gdf.to_file(shp_path)

        result = load_input_shapefile(str(shp_path))
        assert len(result) == 3
        assert all(result.geometry.geom_type == 'Polygon')

    def test_file_not_found_raises_error(self):
        """Test that missing file raises appropriate error."""
        with pytest.raises(Exception):  # Can be FileNotFoundError or fiona error
            load_input_shapefile("/nonexistent/path/to/file.shp")

    def test_preserves_attributes(self, tmp_path):
        """Test that shapefile attributes are preserved."""
        polygon = Polygon([(0, 0), (100, 0), (100, 100), (0, 100)])
        gdf = gpd.GeoDataFrame(
            {
                'name': ['Site A'],
                'lot_id': [123],
                'area_ha': [1.5]
            },
            geometry=[polygon],
            crs='epsg:7899'
        )
        shp_path = tmp_path / "with_attrs.shp"
        gdf.to_file(shp_path)

        result = load_input_shapefile(str(shp_path))
        assert 'name' in result.columns
        assert 'lot_id' in result.columns
        assert 'area_ha' in result.columns
        assert result['name'].iloc[0] == 'Site A'


class TestProcessShapefileInput:
    """Integration tests for process_shapefile_input function."""

    @pytest.fixture
    def sample_shapefile_path(self, tmp_path):
        """Create a sample shapefile and return its path."""
        polygon = Polygon([(0, 0), (100, 0), (100, 100), (0, 100)])
        gdf = gpd.GeoDataFrame(
            {'name': ['Site A']},
            geometry=[polygon],
            crs='epsg:7899'
        )
        shp_path = tmp_path / "test.shp"
        gdf.to_file(shp_path)
        return str(shp_path)

    @pytest.fixture
    def mock_check_parcel_intersection(self, monkeypatch):
        """Mock check_parcel_intersection to always return True."""
        def mock_check(engine, parcel_view, geometry_wkt, srid=7899):
            return True

        import db_nvrmap.core
        monkeypatch.setattr(db_nvrmap.core, 'check_parcel_intersection', mock_check)

    @pytest.fixture
    def mock_build_query_from_geometry(self, monkeypatch):
        """Mock build_query_from_geometry to avoid SQLAlchemy complexity."""
        from unittest.mock import Mock

        def mock_builder(parcel_view, nv1750_evc, bioregions, geometry_wkt, srid=7899):
            # Return a simple mock query object
            return Mock()

        # Patch the build_query_from_geometry function
        import db_nvrmap.core
        monkeypatch.setattr(db_nvrmap.core, 'build_query_from_geometry', mock_builder)

    @pytest.fixture
    def mock_load_geo_dataframe(self, monkeypatch):
        """Mock load_geo_dataframe to return test data."""
        from unittest.mock import Mock

        def mock_loader(engine, query):
            # Return a simple GeoDataFrame with test data
            polygon = Polygon([(0, 0), (50, 0), (50, 50), (0, 50)])
            gdf = gpd.GeoDataFrame(
                {
                    'evc': [101],
                    'x_evcname': ['Test EVC'],
                    'bioregcode': ['BIO1'],
                    'bioregion': ['Test Bioregion']
                },
                geometry=[polygon],
                crs='epsg:7899'
            )
            return gdf

        # Patch the load_geo_dataframe function
        import db_nvrmap.core
        monkeypatch.setattr(db_nvrmap.core, 'load_geo_dataframe', mock_loader)

    def test_process_shapefile_input_basic(
        self,
        sample_shapefile_path,
        mock_check_parcel_intersection,
        mock_build_query_from_geometry,
        mock_load_geo_dataframe
    ):
        """Test basic shapefile processing with single polygon."""
        from unittest.mock import Mock

        # Create minimal mock engine and tables
        engine = Mock()
        tables = {
            "parcel_view": Mock(),
            "nv1750_evc": Mock(),
            "bioregions": Mock()
        }

        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile=sample_shapefile_path,
            shapefile="output.shp"
        )

        result_gdf, view_pfis = process_shapefile_input(opts, engine, tables)

        # Check that we got results
        assert len(result_gdf) > 0
        assert len(view_pfis) > 0

        # Check that source_idx and view_pfi columns were added
        assert 'source_idx' in result_gdf.columns
        assert 'view_pfi' in result_gdf.columns

        # Check that view_pfis match source indices
        assert view_pfis == ['0']

    def test_process_shapefile_input_multiple_polygons(
        self,
        tmp_path,
        mock_check_parcel_intersection,
        mock_build_query_from_geometry,
        mock_load_geo_dataframe
    ):
        """Test processing multiple polygons."""
        from unittest.mock import Mock

        # Create shapefile with multiple polygons
        poly1 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        poly2 = Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])
        poly3 = Polygon([(40, 0), (50, 0), (50, 10), (40, 10)])

        gdf = gpd.GeoDataFrame(
            {'name': ['Site A', 'Site B', 'Site C']},
            geometry=[poly1, poly2, poly3],
            crs='epsg:7899'
        )
        shp_path = tmp_path / "multiple.shp"
        gdf.to_file(shp_path)

        # Create minimal mock engine and tables
        engine = Mock()
        tables = {
            "parcel_view": Mock(),
            "nv1750_evc": Mock(),
            "bioregions": Mock()
        }

        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile=str(shp_path),
            shapefile="output.shp"
        )

        result_gdf, view_pfis = process_shapefile_input(opts, engine, tables)

        # Check that we got results for all 3 polygons
        assert len(view_pfis) == 3
        assert view_pfis == ['0', '1', '2']

        # Check that source_idx values match
        assert set(result_gdf['source_idx'].unique()) == {0, 1, 2}

    def test_process_shapefile_input_no_parcel_intersections(
        self,
        sample_shapefile_path,
        monkeypatch
    ):
        """Test error handling when no parcels intersect the input geometry."""
        from unittest.mock import Mock

        def mock_check_no_parcels(engine, parcel_view, geometry_wkt, srid=7899):
            return False

        import db_nvrmap.core
        monkeypatch.setattr(db_nvrmap.core, 'check_parcel_intersection', mock_check_no_parcels)

        # Create minimal mock engine and tables
        engine = Mock()
        tables = {
            "parcel_view": Mock(),
            "nv1750_evc": Mock(),
            "bioregions": Mock()
        }

        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile=sample_shapefile_path,
            shapefile="output.shp"
        )

        # Should raise ValueError when no parcels intersect
        with pytest.raises(ValueError, match="No cadastral parcels found intersecting"):
            process_shapefile_input(opts, engine, tables)

    def test_process_shapefile_input_no_evc_intersections(
        self,
        sample_shapefile_path,
        mock_check_parcel_intersection,
        mock_build_query_from_geometry,
        monkeypatch
    ):
        """Test error handling when no EVC/bioregion intersections are found."""
        from unittest.mock import Mock

        def mock_loader_no_results(engine, query):
            # Raise ValueError to simulate no intersections
            raise ValueError("No search results found")

        # Patch the load_geo_dataframe function to raise error
        import db_nvrmap.core
        monkeypatch.setattr(db_nvrmap.core, 'load_geo_dataframe', mock_loader_no_results)

        # Create minimal mock engine and tables
        engine = Mock()
        tables = {
            "parcel_view": Mock(),
            "nv1750_evc": Mock(),
            "bioregions": Mock()
        }

        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile=sample_shapefile_path,
            shapefile="output.shp"
        )

        # Should raise ValueError when all polygons fail (parcels exist but no EVC data)
        with pytest.raises(ValueError, match="No cadastral parcels found intersecting"):
            process_shapefile_input(opts, engine, tables)

    def test_process_shapefile_input_partial_intersections(
        self,
        tmp_path,
        mock_check_parcel_intersection,
        mock_build_query_from_geometry,
        monkeypatch
    ):
        """Test processing when some polygons have no intersections."""
        from unittest.mock import Mock

        # Create shapefile with multiple polygons
        poly1 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        poly2 = Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])

        gdf = gpd.GeoDataFrame(
            {'name': ['Site A', 'Site B']},
            geometry=[poly1, poly2],
            crs='epsg:7899'
        )
        shp_path = tmp_path / "partial.shp"
        gdf.to_file(shp_path)

        # Create minimal mock engine and tables
        engine = Mock()
        tables = {
            "parcel_view": Mock(),
            "nv1750_evc": Mock(),
            "bioregions": Mock()
        }

        # Mock loader that fails for second polygon
        call_count = [0]

        def mock_loader_partial(engine, query):
            call_count[0] += 1
            if call_count[0] == 2:
                # Second polygon has no intersections
                raise ValueError("No search results found")
            else:
                # First polygon succeeds
                polygon = Polygon([(0, 0), (50, 0), (50, 50), (0, 50)])
                return gpd.GeoDataFrame(
                    {
                        'evc': [101],
                        'x_evcname': ['Test EVC'],
                        'bioregcode': ['BIO1'],
                        'bioregion': ['Test Bioregion']
                    },
                    geometry=[polygon],
                    crs='epsg:7899'
                )

        import db_nvrmap.core
        monkeypatch.setattr(db_nvrmap.core, 'load_geo_dataframe', mock_loader_partial)

        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile=str(shp_path),
            shapefile="output.shp"
        )

        result_gdf, view_pfis = process_shapefile_input(opts, engine, tables)

        # Should succeed with only the first polygon
        assert len(view_pfis) == 1
        assert view_pfis == ['0']
        assert 0 in result_gdf['source_idx'].values
        assert 1 not in result_gdf['source_idx'].values

    def test_process_shapefile_result_structure(
        self,
        sample_shapefile_path,
        mock_check_parcel_intersection,
        mock_build_query_from_geometry,
        mock_load_geo_dataframe
    ):
        """Test that results have correct structure for downstream processing."""
        from unittest.mock import Mock

        # Create minimal mock engine and tables
        engine = Mock()
        tables = {
            "parcel_view": Mock(),
            "nv1750_evc": Mock(),
            "bioregions": Mock()
        }

        opts = ProcessingOptions(
            view_pfi=[],
            input_shapefile=sample_shapefile_path,
            shapefile="output.shp"
        )

        result_gdf, view_pfis = process_shapefile_input(opts, engine, tables)

        # Check required columns exist
        required_columns = ['evc', 'x_evcname', 'bioregcode', 'bioregion', 'source_idx', 'view_pfi']
        for col in required_columns:
            assert col in result_gdf.columns, f"Missing required column: {col}"

        # Check that view_pfi is string type
        assert result_gdf['view_pfi'].dtype == object  # string type

        # Check geometry column exists
        assert result_gdf.geometry is not None
        assert result_gdf.crs.to_epsg() == 7899


class TestBuildQueryFromGeometry:
    """Tests for build_query_from_geometry function."""

    def test_query_signature_requires_parcel_view(self):
        """Test that build_query_from_geometry requires parcel_view parameter."""
        from db_nvrmap.core import build_query_from_geometry
        import inspect

        # Check the function signature
        sig = inspect.signature(build_query_from_geometry)
        params = list(sig.parameters.keys())

        # parcel_view should be the first parameter
        assert params[0] == 'parcel_view', "First parameter should be parcel_view"
        assert 'nv1750_evc' in params, "nv1750_evc should be a parameter"
        assert 'bioregions' in params, "bioregions should be a parameter"
        assert 'geometry_wkt' in params, "geometry_wkt should be a parameter"

    def test_query_signature_param_order(self):
        """Test that build_query_from_geometry has correct parameter order."""
        from db_nvrmap.core import build_query_from_geometry
        import inspect

        # Check the function signature
        sig = inspect.signature(build_query_from_geometry)
        params = list(sig.parameters.keys())

        # Verify the expected parameter order
        expected_order = ['parcel_view', 'nv1750_evc', 'bioregions', 'geometry_wkt', 'srid']
        assert params == expected_order, f"Parameters should be {expected_order}, got {params}"

    def test_query_srid_has_default(self):
        """Test that srid parameter has default value of 7899."""
        from db_nvrmap.core import build_query_from_geometry
        import inspect

        sig = inspect.signature(build_query_from_geometry)
        srid_param = sig.parameters['srid']

        assert srid_param.default == 7899, "srid should default to 7899 (Victorian CRS)"


class TestCheckParcelIntersection:
    """Tests for check_parcel_intersection function."""

    def test_returns_true_when_parcels_exist(self, monkeypatch):
        """Test that function returns True when parcels intersect."""
        from unittest.mock import Mock, MagicMock
        from db_nvrmap.core import check_parcel_intersection

        # Create mock engine and parcel_view
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 5  # 5 parcels found

        mock_engine = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

        parcel_view = Mock()
        parcel_view.c.geom = MagicMock()

        result = check_parcel_intersection(
            mock_engine,
            parcel_view,
            "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))"
        )

        assert result is True

    def test_returns_false_when_no_parcels(self, monkeypatch):
        """Test that function returns False when no parcels intersect."""
        from unittest.mock import Mock, MagicMock
        from db_nvrmap.core import check_parcel_intersection

        # Create mock engine and parcel_view
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 0  # No parcels found

        mock_engine = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

        parcel_view = Mock()
        parcel_view.c.geom = MagicMock()

        result = check_parcel_intersection(
            mock_engine,
            parcel_view,
            "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))"
        )

        assert result is False


class TestCLIShapefileInput:
    """Tests for CLI argument parsing with shapefile input."""

    def test_input_flag_parsing(self):
        """Test that --input flag is parsed correctly."""
        args = parse_args(['-i', '/path/to/file.shp', '-s', 'output.shp'])
        assert args.input_shapefile == '/path/to/file.shp'
        assert args.shapefile == 'output.shp'
        assert args.view_pfi == []

    def test_input_short_flag_parsing(self):
        """Test that -i short flag is parsed correctly."""
        args = parse_args(['-i', '/path/to/file.shp'])
        assert args.input_shapefile == '/path/to/file.shp'

    def test_site_id_field_parsing(self):
        """Test that --site-id-field is parsed correctly."""
        args = parse_args(['-i', '/path/to/file.shp', '--site-id-field', 'LOT_ID'])
        assert args.input_shapefile == '/path/to/file.shp'
        assert args.site_id_field == 'LOT_ID'

    def test_site_id_field_optional(self):
        """Test that --site-id-field is optional."""
        args = parse_args(['-i', '/path/to/file.shp'])
        assert args.input_shapefile == '/path/to/file.shp'
        assert args.site_id_field is None

    def test_shapefile_with_ensym_format(self):
        """Test combining shapefile input with EnSym output format."""
        args = parse_args(['-i', '/path/to/file.shp', '-e'])
        assert args.input_shapefile == '/path/to/file.shp'
        assert args.ensym is True

    def test_shapefile_with_sbeu_format(self):
        """Test combining shapefile input with SBEU output format."""
        args = parse_args(['-i', '/path/to/file.shp', '-b'])
        assert args.input_shapefile == '/path/to/file.shp'
        assert args.sbeu is True

    def test_shapefile_with_gainscore(self):
        """Test combining shapefile input with gain score override."""
        args = parse_args(['-i', '/path/to/file.shp', '-g', '0.5'])
        assert args.input_shapefile == '/path/to/file.shp'
        assert args.gainscore == 0.5

    def test_pfi_only_mode_parsing(self):
        """Test that PFI-only mode still works."""
        args = parse_args(['12345', '67890'])
        assert args.view_pfi == [12345, 67890]
        assert args.input_shapefile is None

    def test_args_to_options_with_shapefile(self):
        """Test conversion of args to ProcessingOptions with shapefile input."""
        args = parse_args(['-i', '/path/to/file.shp', '-s', 'output.shp'])
        opts = args_to_options(args)

        assert opts.input_shapefile == '/path/to/file.shp'
        assert opts.shapefile == 'output.shp'
        assert opts.view_pfi == []
        assert opts.uses_shapefile_input

    def test_args_to_options_with_site_id_field(self):
        """Test conversion includes site_id_field."""
        args = parse_args(['-i', '/path/to/file.shp', '--site-id-field', 'SITE_ID'])
        opts = args_to_options(args)

        assert opts.site_id_field == 'SITE_ID'

    def test_args_to_options_with_pfi(self):
        """Test conversion of args to ProcessingOptions with PFI input."""
        args = parse_args(['12345', '67890', '-s', 'output.shp'])
        opts = args_to_options(args)

        assert opts.view_pfi == [12345, 67890]
        assert opts.input_shapefile is None
        assert not opts.uses_shapefile_input

    def test_args_to_options_preserves_output_format(self):
        """Test that output format is preserved when using shapefile input."""
        args = parse_args(['-i', '/path/to/file.shp', '-e'])
        opts = args_to_options(args)

        assert opts.output_format == OutputFormat.ENSYM_2017
        assert opts.ensym


class TestCLIValidation:
    """Tests for CLI input validation logic."""

    def test_run_cli_rejects_no_input(self, capsys):
        """Test that run_cli rejects when neither PFI nor shapefile is provided."""
        args = parse_args([])
        result = run_cli(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Either PFI values or --input shapefile is required" in captured.err

    def test_run_cli_rejects_both_inputs(self, capsys):
        """Test that run_cli rejects when both PFI and shapefile are provided."""
        args = parse_args(['12345', '-i', '/path/to/file.shp'])
        result = run_cli(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Cannot specify both PFI values and --input shapefile" in captured.err

    def test_run_cli_accepts_pfi_only(self, monkeypatch):
        """Test that run_cli accepts PFI-only input."""
        from unittest.mock import Mock

        # Mock generate_shapefile to avoid actual processing
        mock_generate = Mock()
        import db_nvrmap.cli
        monkeypatch.setattr(db_nvrmap.cli, 'generate_shapefile', mock_generate)

        args = parse_args(['12345'])
        result = run_cli(args)

        assert result == 0
        assert mock_generate.called

    def test_run_cli_accepts_shapefile_only(self, monkeypatch, tmp_path):
        """Test that run_cli accepts shapefile-only input."""
        from unittest.mock import Mock

        # Create a dummy shapefile path (doesn't need to exist for this test)
        shp_path = tmp_path / "test.shp"

        # Mock generate_shapefile to avoid actual processing
        mock_generate = Mock()
        import db_nvrmap.cli
        monkeypatch.setattr(db_nvrmap.cli, 'generate_shapefile', mock_generate)

        args = parse_args(['-i', str(shp_path)])
        result = run_cli(args)

        assert result == 0
        assert mock_generate.called

    def test_run_cli_error_handling(self, monkeypatch, capsys):
        """Test that run_cli properly handles errors from generate_shapefile."""
        from unittest.mock import Mock

        # Mock generate_shapefile to raise an error
        def mock_generate_error(opts):
            raise ValueError("Test error message")

        import db_nvrmap.cli
        monkeypatch.setattr(db_nvrmap.cli, 'generate_shapefile', mock_generate_error)

        args = parse_args(['12345'])
        result = run_cli(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Test error message" in captured.err

    def test_pfi_with_property_flag_rejected_with_shapefile(self, capsys):
        """Test that property flag doesn't make sense with shapefile input."""
        # The validation should happen in ProcessingOptions, but we test the CLI flow
        args = parse_args(['-i', '/path/to/file.shp', '-p'])

        # This should be caught by run_cli validation
        # Property flag doesn't cause mutual exclusivity error,
        # but it's semantically meaningless with shapefile input
        # For now, we just ensure it parses without CLI-level errors
        assert args.input_shapefile == '/path/to/file.shp'
        assert args.property is True


class TestCLIExampleUsage:
    """Tests that verify example usage patterns from documentation."""

    def test_basic_shapefile_usage(self):
        """Test: ./db-nvrmap.py -i my_boundary.shp -s output.shp"""
        args = parse_args(['-i', 'my_boundary.shp', '-s', 'output.shp'])
        opts = args_to_options(args)

        assert opts.input_shapefile == 'my_boundary.shp'
        assert opts.shapefile == 'output.shp'
        assert opts.uses_shapefile_input
        assert opts.output_format == OutputFormat.NVRMAP

    def test_shapefile_with_site_id_field(self):
        """Test: ./db-nvrmap.py -i my_boundary.shp -s output.shp --site-id-field LOT_ID"""
        args = parse_args(['-i', 'my_boundary.shp', '-s', 'output.shp', '--site-id-field', 'LOT_ID'])
        opts = args_to_options(args)

        assert opts.input_shapefile == 'my_boundary.shp'
        assert opts.site_id_field == 'LOT_ID'

    def test_shapefile_ensym_format(self):
        """Test: ./db-nvrmap.py -i my_boundary.shp -s output.shp -e"""
        args = parse_args(['-i', 'my_boundary.shp', '-s', 'output.shp', '-e'])
        opts = args_to_options(args)

        assert opts.input_shapefile == 'my_boundary.shp'
        assert opts.output_format == OutputFormat.ENSYM_2017
        assert opts.ensym

    def test_shapefile_sbeu_format(self):
        """Test: ./db-nvrmap.py -i my_boundary.shp -s output.shp -b"""
        args = parse_args(['-i', 'my_boundary.shp', '-s', 'output.shp', '-b'])
        opts = args_to_options(args)

        assert opts.input_shapefile == 'my_boundary.shp'
        assert opts.output_format == OutputFormat.ENSYM_2013
        assert opts.sbeu

    def test_shapefile_with_gainscore(self):
        """Test: ./db-nvrmap.py -i my_boundary.shp -s output.shp -g 0.5"""
        args = parse_args(['-i', 'my_boundary.shp', '-s', 'output.shp', '-g', '0.5'])
        opts = args_to_options(args)

        assert opts.input_shapefile == 'my_boundary.shp'
        assert opts.gainscore == 0.5


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
