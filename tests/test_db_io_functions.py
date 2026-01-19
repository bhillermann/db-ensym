#!/usr/bin/env python3
"""
Comprehensive unit tests for database and IO functions in db-nvrmap.py.

Tests cover:
- connect_db: Database connection and table reflection
- write_gdf: Writing GeoDataFrame to shapefile
- load_evc_data: Loading EVC data from Excel
- load_geo_dataframe: Loading spatial data from database
"""

import pytest
import pandas as pd
import geopandas as gpd
import argparse
from shapely.geometry import Polygon
from unittest.mock import patch, MagicMock, Mock
from typing import Dict
import sys
from pathlib import Path

# Add parent directory to path to import from db-nvrmap.py
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import using module name without hyphen
import importlib.util
spec = importlib.util.spec_from_file_location("db_nvrmap", Path(__file__).parent.parent / "db-nvrmap.py")
db_nvrmap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_nvrmap)

connect_db = db_nvrmap.connect_db
write_gdf = db_nvrmap.write_gdf
load_evc_data = db_nvrmap.load_evc_data
load_geo_dataframe = db_nvrmap.load_geo_dataframe
NVRMAP_SCHEMA = db_nvrmap.NVRMAP_SCHEMA
ENSYM_2017_SCHEMA = db_nvrmap.ENSYM_2017_SCHEMA
ENSYM_2013_SCHEMA = db_nvrmap.ENSYM_2013_SCHEMA


@pytest.fixture
def valid_db_config() -> Dict[str, str]:
    """Create a valid database configuration dictionary."""
    return {
        "db_type": "postgresql+psycopg2",
        "username": "test_user",
        "password": "test_password",
        "host": "localhost",
        "database": "test_db"
    }


@pytest.fixture
def sample_geodataframe() -> gpd.GeoDataFrame:
    """Create a sample GeoDataFrame for testing."""
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

    data = {
        'site_id': [1],
        'zone_id': ['A'],
        'prop_id': ['TEST_PROJ'],
        'vlot': [0],
        'lot': [0],
        'recruits': [0],
        'type': ['p'],
        'cp': ['Test Collector'],
        'veg_codes': ['VVP_0055'],
        'lt_count': [0],
        'cond_score': [0.5],
        'gain_score': [0.22],
        'surv_date': ['20260119'],
        'geometry': [polygon]
    }

    return gpd.GeoDataFrame(data, crs='epsg:7899')


@pytest.fixture
def sample_ensym_2017_geodataframe() -> gpd.GeoDataFrame:
    """Create a sample GeoDataFrame in EnSym 2017 format."""
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

    data = {
        'HH_PAI': ['TEST_PROJ'],
        'HH_D': ['2026-01-19'],
        'HH_CP': ['Test Collector'],
        'HH_SI': [1],
        'HH_ZI': ['A'],
        'HH_VAC': ['P'],
        'HH_EVC': ['VVP_0055'],
        'BCS': ['E'],
        'LT_CNT': [0],
        'HH_H_S': [0.5],
        'G_S': [0.22],
        'HH_A': [1.5],
        'geometry': [polygon]
    }

    return gpd.GeoDataFrame(data, crs='epsg:7899')


@pytest.fixture
def sample_ensym_2013_geodataframe() -> gpd.GeoDataFrame:
    """Create a sample GeoDataFrame in EnSym 2013 format."""
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

    data = {
        'HH_PAI': ['TEST_PROJ'],
        'HH_SI': [1],
        'HH_ZI': ['A'],
        'HH_VAC': ['P'],
        'HH_CP': ['Test Collector'],
        'HH_D': ['2026-01-19'],
        'HH_H_S': [0.5],
        'G_HA': [0.22],
        'HH_A': [1.5],
        'geometry': [polygon]
    }

    return gpd.GeoDataFrame(data, crs='epsg:7899')


@pytest.fixture
def mock_args_nvrmap() -> argparse.Namespace:
    """Create mock arguments for NVRMAP format."""
    return argparse.Namespace(
        shapefile='test_output.shp',
        ensym=False,
        sbeu=False,
        gainscore=None
    )


@pytest.fixture
def mock_args_ensym_2017() -> argparse.Namespace:
    """Create mock arguments for EnSym 2017 format."""
    return argparse.Namespace(
        shapefile='test_output.shp',
        ensym=True,
        sbeu=False,
        gainscore=None
    )


@pytest.fixture
def mock_args_ensym_2013() -> argparse.Namespace:
    """Create mock arguments for EnSym 2013 format."""
    return argparse.Namespace(
        shapefile='test_output.shp',
        ensym=False,
        sbeu=True,
        gainscore=None
    )


class TestConnectDb:
    """Test suite for connect_db() function."""

    def test_connect_db_missing_keys(self):
        """Test that connect_db raises KeyError when required keys are missing."""
        # Missing 'database' key
        incomplete_config = {
            "db_type": "postgresql+psycopg2",
            "username": "test_user",
            "password": "test_password",
            "host": "localhost"
        }

        with pytest.raises(KeyError) as exc_info:
            connect_db(incomplete_config)

        assert "Missing DB config keys" in str(exc_info.value)
        assert "database" in str(exc_info.value)

    def test_connect_db_multiple_missing_keys(self):
        """Test connect_db with multiple missing keys."""
        minimal_config = {
            "db_type": "postgresql+psycopg2"
        }

        with pytest.raises(KeyError) as exc_info:
            connect_db(minimal_config)

        error_msg = str(exc_info.value)
        assert "Missing DB config keys" in error_msg
        # Check that all missing keys are mentioned
        for key in ["username", "password", "host", "database"]:
            assert key in error_msg

    def test_connect_db_empty_value(self):
        """Test that connect_db raises ValueError for empty string values."""
        config_with_empty = {
            "db_type": "postgresql+psycopg2",
            "username": "test_user",
            "password": "",  # Empty password
            "host": "localhost",
            "database": "test_db"
        }

        with pytest.raises(ValueError) as exc_info:
            connect_db(config_with_empty)

        assert "Database config values cannot be empty" in str(exc_info.value)
        assert "password" in str(exc_info.value)

    def test_connect_db_whitespace_value(self):
        """Test that connect_db raises ValueError for whitespace-only values."""
        config_with_whitespace = {
            "db_type": "postgresql+psycopg2",
            "username": "   ",  # Whitespace only
            "password": "test_password",
            "host": "localhost",
            "database": "test_db"
        }

        with pytest.raises(ValueError) as exc_info:
            connect_db(config_with_whitespace)

        assert "Database config values cannot be empty" in str(exc_info.value)
        assert "username" in str(exc_info.value)

    def test_connect_db_multiple_empty_values(self):
        """Test connect_db with multiple empty/whitespace values."""
        config_with_multiple_empty = {
            "db_type": "",
            "username": "test_user",
            "password": "  ",
            "host": "localhost",
            "database": ""
        }

        with pytest.raises(ValueError) as exc_info:
            connect_db(config_with_multiple_empty)

        error_msg = str(exc_info.value)
        assert "Database config values cannot be empty" in error_msg

    @patch('db_nvrmap.create_engine')
    @patch('db_nvrmap.MetaData')
    @patch('db_nvrmap.URL.create')
    def test_connect_db_valid_config(
        self,
        mock_url_create: Mock,
        mock_metadata_class: Mock,
        mock_create_engine: Mock,
        valid_db_config: Dict[str, str]
    ):
        """Test connect_db with valid configuration successfully creates engine and reflects tables."""
        # Setup mocks
        mock_url = MagicMock()
        mock_url_create.return_value = mock_url

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_metadata = MagicMock()
        mock_metadata.tables = {
            'parcel_view': MagicMock(),
            'nv1750_evc': MagicMock(),
            'bioregions': MagicMock(),
            'parcel_property': MagicMock(),
            'parcel_detail': MagicMock(),
            'property_detail': MagicMock()
        }
        mock_metadata_class.return_value = mock_metadata

        # Execute
        engine, tables = connect_db(valid_db_config)

        # Verify URL.create was called with correct parameters
        mock_url_create.assert_called_once_with(
            "postgresql+psycopg2",
            username="test_user",
            password="test_password",
            host="localhost",
            database="test_db"
        )

        # Verify engine was created with the URL
        mock_create_engine.assert_called_once_with(mock_url)

        # Verify metadata reflection was called with correct tables
        mock_metadata.reflect.assert_called_once()
        call_kwargs = mock_metadata.reflect.call_args[1]
        assert 'only' in call_kwargs
        assert set(call_kwargs['only']) == {
            "parcel_view", "nv1750_evc", "bioregions",
            "parcel_property", "parcel_detail", "property_detail"
        }
        assert call_kwargs['bind'] == mock_engine

        # Verify return values
        assert engine == mock_engine
        assert tables == mock_metadata.tables
        assert len(tables) == 6


class TestWriteGdf:
    """Test suite for write_gdf() function."""

    @patch('db_nvrmap.logging')
    def test_write_gdf_nvrmap_schema(
        self,
        mock_logging: Mock,
        sample_geodataframe: gpd.GeoDataFrame,
        mock_args_nvrmap: argparse.Namespace
    ):
        """Test write_gdf uses NVRMAP schema for default format."""
        # Mock the to_file method
        with patch.object(sample_geodataframe, 'to_file') as mock_to_file:
            write_gdf(sample_geodataframe, mock_args_nvrmap)

            # Verify to_file was called with NVRMAP schema
            mock_to_file.assert_called_once_with(
                'test_output.shp',
                schema=NVRMAP_SCHEMA,
                engine='fiona'
            )

    @patch('db_nvrmap.logging')
    def test_write_gdf_ensym_2017_schema(
        self,
        mock_logging: Mock,
        sample_ensym_2017_geodataframe: gpd.GeoDataFrame,
        mock_args_ensym_2017: argparse.Namespace
    ):
        """Test write_gdf uses ENSYM_2017 schema when --ensym flag is set."""
        with patch.object(sample_ensym_2017_geodataframe, 'to_file') as mock_to_file:
            write_gdf(sample_ensym_2017_geodataframe, mock_args_ensym_2017)

            mock_to_file.assert_called_once_with(
                'test_output.shp',
                schema=ENSYM_2017_SCHEMA,
                engine='fiona'
            )

    @patch('db_nvrmap.logging')
    def test_write_gdf_ensym_2013_schema(
        self,
        mock_logging: Mock,
        sample_ensym_2013_geodataframe: gpd.GeoDataFrame,
        mock_args_ensym_2013: argparse.Namespace
    ):
        """Test write_gdf uses ENSYM_2013 schema when --sbeu flag is set."""
        with patch.object(sample_ensym_2013_geodataframe, 'to_file') as mock_to_file:
            write_gdf(sample_ensym_2013_geodataframe, mock_args_ensym_2013)

            mock_to_file.assert_called_once_with(
                'test_output.shp',
                schema=ENSYM_2013_SCHEMA,
                engine='fiona'
            )

    @patch('db_nvrmap.logging')
    def test_write_gdf_ioerror_handling(
        self,
        mock_logging: Mock,
        sample_geodataframe: gpd.GeoDataFrame,
        mock_args_nvrmap: argparse.Namespace
    ):
        """Test write_gdf handles IOError appropriately."""
        # Mock to_file to raise IOError
        with patch.object(
            sample_geodataframe,
            'to_file',
            side_effect=IOError("Permission denied")
        ):
            with pytest.raises(IOError) as exc_info:
                write_gdf(sample_geodataframe, mock_args_nvrmap)

            assert "Permission denied" in str(exc_info.value)

            # Verify error was logged
            mock_logging.error.assert_called_once()
            log_message = mock_logging.error.call_args[0][0]
            assert "Failed to write shapefile" in log_message
            assert "test_output.shp" in log_message

    @patch('db_nvrmap.logging')
    def test_write_gdf_oserror_handling(
        self,
        mock_logging: Mock,
        sample_geodataframe: gpd.GeoDataFrame,
        mock_args_nvrmap: argparse.Namespace
    ):
        """Test write_gdf handles OSError appropriately."""
        with patch.object(
            sample_geodataframe,
            'to_file',
            side_effect=OSError("Disk full")
        ):
            with pytest.raises(OSError) as exc_info:
                write_gdf(sample_geodataframe, mock_args_nvrmap)

            assert "Disk full" in str(exc_info.value)

            # Verify error was logged
            mock_logging.error.assert_called_once()
            log_message = mock_logging.error.call_args[0][0]
            assert "Failed to write shapefile" in log_message

    @patch('db_nvrmap.logging')
    def test_write_gdf_logs_dataframe_info(
        self,
        mock_logging: Mock,
        sample_geodataframe: gpd.GeoDataFrame,
        mock_args_nvrmap: argparse.Namespace
    ):
        """Test write_gdf logs DataFrame information before writing."""
        with patch.object(sample_geodataframe, 'to_file'):
            write_gdf(sample_geodataframe, mock_args_nvrmap)

            # Verify logging calls
            assert mock_logging.info.call_count >= 3

            # Check that dataframe, columns, and filename were logged
            log_calls = [call[0][0] for call in mock_logging.info.call_args_list]
            assert any("Final DataFrame" in str(call) for call in log_calls)
            assert any("Current columns" in str(call) for call in log_calls)
            assert any("Writing shapefile" in str(call) for call in log_calls)


class TestLoadEvcData:
    """Test suite for load_evc_data() function."""

    @patch('db_nvrmap.pd.read_excel')
    def test_load_evc_data_with_absolute_path(self, mock_read_excel: Mock):
        """Test load_evc_data with an absolute file path."""
        # Setup mock
        mock_df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055', 'STIF_0135'],
            'BCS_CATEGORY': ['E', 'V']
        })
        mock_read_excel.return_value = mock_df

        # Execute
        result = load_evc_data('/absolute/path/to/evc_data.xlsx')

        # Verify
        mock_read_excel.assert_called_once()
        called_path = mock_read_excel.call_args[0][0]
        assert isinstance(called_path, Path)
        assert str(called_path) == '/absolute/path/to/evc_data.xlsx'
        assert result.equals(mock_df)

    @patch('db_nvrmap.pd.read_excel')
    def test_load_evc_data_with_tilde_expansion(self, mock_read_excel: Mock):
        """Test load_evc_data expands tilde in path."""
        # Setup mock
        mock_df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055'],
            'BCS_CATEGORY': ['E']
        })
        mock_read_excel.return_value = mock_df

        # Execute
        result = load_evc_data('~/data/evc_data.xlsx')

        # Verify path expansion occurred
        mock_read_excel.assert_called_once()
        called_path = mock_read_excel.call_args[0][0]
        assert isinstance(called_path, Path)
        # Path should be expanded (not contain ~)
        assert '~' not in str(called_path)
        assert result.equals(mock_df)

    @patch('db_nvrmap.pd.read_excel')
    def test_load_evc_data_with_relative_path(self, mock_read_excel: Mock):
        """Test load_evc_data with relative file path."""
        mock_df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055'],
            'BCS_CATEGORY': ['E']
        })
        mock_read_excel.return_value = mock_df

        result = load_evc_data('data/evc_data.xlsx')

        mock_read_excel.assert_called_once()
        called_path = mock_read_excel.call_args[0][0]
        assert isinstance(called_path, Path)
        assert result.equals(mock_df)

    @patch('db_nvrmap.pd.read_excel')
    def test_load_evc_data_returns_dataframe(self, mock_read_excel: Mock):
        """Test load_evc_data returns a pandas DataFrame."""
        expected_df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055', 'STIF_0135', 'VVP_0132'],
            'BCS_CATEGORY': ['E', 'V', 'D'],
            'EVC_NAME': ['Coastal Alkaline Scrub', 'Heathy Woodland', 'Valley Grassy Forest']
        })
        mock_read_excel.return_value = expected_df

        result = load_evc_data('/path/to/evc.xlsx')

        assert isinstance(result, pd.DataFrame)
        assert result.equals(expected_df)
        assert list(result.columns) == ['BIOEVCCODE', 'BCS_CATEGORY', 'EVC_NAME']

    @patch('db_nvrmap.pd.read_excel')
    def test_load_evc_data_file_not_found(self, mock_read_excel: Mock):
        """Test load_evc_data propagates FileNotFoundError."""
        mock_read_excel.side_effect = FileNotFoundError("File not found")

        with pytest.raises(FileNotFoundError):
            load_evc_data('/nonexistent/file.xlsx')

    @patch('db_nvrmap.pd.read_excel')
    def test_load_evc_data_invalid_excel(self, mock_read_excel: Mock):
        """Test load_evc_data propagates Excel read errors."""
        mock_read_excel.side_effect = Exception("Invalid Excel file")

        with pytest.raises(Exception) as exc_info:
            load_evc_data('/path/to/invalid.xlsx')

        assert "Invalid Excel file" in str(exc_info.value)


class TestLoadGeoDataFrame:
    """Test suite for load_geo_dataframe() function."""

    @patch('db_nvrmap.gpd.GeoDataFrame.from_postgis')
    def test_load_geo_dataframe_empty_result_raises_error(
        self,
        mock_from_postgis: Mock
    ):
        """Test load_geo_dataframe raises ValueError when query returns no results."""
        # Create an empty GeoDataFrame
        empty_gdf = gpd.GeoDataFrame()
        mock_from_postgis.return_value = empty_gdf

        mock_engine = MagicMock()
        mock_query = MagicMock()

        with pytest.raises(ValueError) as exc_info:
            load_geo_dataframe(mock_engine, mock_query)

        assert "No search results found" in str(exc_info.value)
        assert "Check your View PFI values" in str(exc_info.value)

    @patch('db_nvrmap.gpd.GeoDataFrame.from_postgis')
    def test_load_geo_dataframe_valid_result(self, mock_from_postgis: Mock):
        """Test load_geo_dataframe successfully loads and sets CRS."""
        # Create a sample GeoDataFrame without CRS
        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        sample_data = {
            'view_pfi': ['12345'],
            'evc': [55],
            'bioregcode': ['VVP'],
            'geometry': [polygon]
        }
        sample_gdf = gpd.GeoDataFrame(sample_data)

        # Mock from_postgis to return our sample GeoDataFrame
        mock_from_postgis.return_value = sample_gdf

        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_query = MagicMock()

        # Execute
        result = load_geo_dataframe(mock_engine, mock_query)

        # Verify from_postgis was called correctly
        mock_from_postgis.assert_called_once_with(
            mock_query,
            con=mock_connection,
            geom_col="geom"
        )

        # Verify CRS was set
        assert result.crs is not None
        assert result.crs.to_string().upper() == 'EPSG:7899'

        # Verify data is present
        assert len(result) == 1
        assert 'view_pfi' in result.columns
        assert result['view_pfi'].iloc[0] == '12345'

    @patch('db_nvrmap.gpd.GeoDataFrame.from_postgis')
    def test_load_geo_dataframe_multiple_rows(self, mock_from_postgis: Mock):
        """Test load_geo_dataframe with multiple result rows."""
        polygon1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        polygon2 = Polygon([(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)])

        sample_data = {
            'view_pfi': ['12345', '67890'],
            'evc': [55, 135],
            'bioregcode': ['VVP', 'STIF'],
            'geometry': [polygon1, polygon2]
        }
        sample_gdf = gpd.GeoDataFrame(sample_data)
        mock_from_postgis.return_value = sample_gdf

        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_query = MagicMock()

        result = load_geo_dataframe(mock_engine, mock_query)

        assert len(result) == 2
        assert result.crs.to_string().upper() == 'EPSG:7899'
        assert list(result['view_pfi']) == ['12345', '67890']
        assert list(result['evc']) == [55, 135]

    @patch('db_nvrmap.gpd.GeoDataFrame.from_postgis')
    def test_load_geo_dataframe_preserves_columns(self, mock_from_postgis: Mock):
        """Test load_geo_dataframe preserves all columns from query."""
        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

        expected_columns = [
            'view_pfi', 'evc', 'x_evcname', 'bioregcode', 'bioregion', 'geom'
        ]
        sample_data = {
            'view_pfi': ['12345'],
            'evc': [55],
            'x_evcname': ['Coastal Alkaline Scrub'],
            'bioregcode': ['VVP'],
            'bioregion': ['Victorian Volcanic Plain'],
            'geometry': [polygon]
        }
        sample_gdf = gpd.GeoDataFrame(sample_data)
        mock_from_postgis.return_value = sample_gdf

        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_query = MagicMock()

        result = load_geo_dataframe(mock_engine, mock_query)

        assert set(result.columns) == set(expected_columns)
        assert result['x_evcname'].iloc[0] == 'Coastal Alkaline Scrub'
        assert result['bioregion'].iloc[0] == 'Victorian Volcanic Plain'

    @patch('db_nvrmap.gpd.GeoDataFrame.from_postgis')
    def test_load_geo_dataframe_engine_connection_called(
        self,
        mock_from_postgis: Mock
    ):
        """Test load_geo_dataframe calls engine.connect()."""
        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        sample_gdf = gpd.GeoDataFrame({
            'view_pfi': ['12345'],
            'geometry': [polygon]
        })
        mock_from_postgis.return_value = sample_gdf

        mock_engine = MagicMock()
        mock_query = MagicMock()

        load_geo_dataframe(mock_engine, mock_query)

        # Verify engine.connect() was called
        mock_engine.connect.assert_called_once()


class TestIntegration:
    """Integration-style tests that verify function interactions."""

    @patch('db_nvrmap.create_engine')
    @patch('db_nvrmap.MetaData')
    @patch('db_nvrmap.URL.create')
    def test_connect_db_returns_usable_objects(
        self,
        mock_url_create: Mock,
        mock_metadata_class: Mock,
        mock_create_engine: Mock,
        valid_db_config: Dict[str, str]
    ):
        """Test that connect_db returns objects that can be used for queries."""
        # Setup comprehensive mock tables
        mock_tables = {}
        for table_name in ["parcel_view", "nv1750_evc", "bioregions",
                          "parcel_property", "parcel_detail", "property_detail"]:
            mock_table = MagicMock()
            mock_table.c = MagicMock()  # Mock columns attribute
            mock_tables[table_name] = mock_table

        mock_metadata = MagicMock()
        mock_metadata.tables = mock_tables
        mock_metadata_class.return_value = mock_metadata

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        engine, tables = connect_db(valid_db_config)

        # Verify we can access table objects
        assert 'parcel_view' in tables
        assert 'nv1750_evc' in tables
        assert 'bioregions' in tables

        # Verify tables have the column attribute (needed for queries)
        assert hasattr(tables['parcel_view'], 'c')

    @patch('db_nvrmap.logging')
    @patch('db_nvrmap.pd.read_excel')
    def test_load_evc_data_integration_with_lookup(
        self,
        mock_read_excel: Mock,
        mock_logging: Mock
    ):
        """Test load_evc_data produces data usable by lookup_bcs_value function."""
        from db_nvrmap import lookup_bcs_value

        # Create realistic EVC data
        evc_data = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055', 'STIF_0135', 'VVP_0132'],
            'BCS_CATEGORY': ['Endangered', 'Vulnerable', 'Depleted'],
            'EVC_NAME': ['Coastal Alkaline Scrub', 'Heathy Woodland', 'Valley Grassy Forest']
        })
        mock_read_excel.return_value = evc_data

        # Load the data
        evc_df = load_evc_data('~/test_data.xlsx')

        # Verify it can be used with lookup_bcs_value
        bcs = lookup_bcs_value('VVP_0055', evc_df)
        assert bcs == 'E'  # First character of 'Endangered'

        bcs = lookup_bcs_value('STIF_0135', evc_df)
        assert bcs == 'V'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
