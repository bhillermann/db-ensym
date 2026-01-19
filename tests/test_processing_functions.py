#!/usr/bin/env python3
"""
Comprehensive unit tests for processing and DataFrame builder functions in db-nvrmap.py.

Tests cover:
- process_nvrmap_rows: Generate site_id, zone_id, veg_codes
- process_ensym_rows: Create HH_EVC values with BCS lookup
- build_ensym_gdf: Build EnSym output GeoDataFrame
- build_nvrmap_gdf: Build NVRMap output GeoDataFrame
- select_output_gdf: Select correct output format based on args
"""

import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from datetime import datetime
from unittest.mock import Mock
import sys
from pathlib import Path

# Add parent directory to path to import from db-nvrmap.py
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import using module name without hyphen
import importlib.util
spec = importlib.util.spec_from_file_location("db_nvrmap", Path(__file__).parent.parent / "db-nvrmap.py")
db_nvrmap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_nvrmap)

process_nvrmap_rows = db_nvrmap.process_nvrmap_rows
process_ensym_rows = db_nvrmap.process_ensym_rows
build_ensym_gdf = db_nvrmap.build_ensym_gdf
build_nvrmap_gdf = db_nvrmap.build_nvrmap_gdf
select_output_gdf = db_nvrmap.select_output_gdf
format_bioevc = db_nvrmap.format_bioevc
calculate_site_id = db_nvrmap.calculate_site_id
generate_zone_id = db_nvrmap.generate_zone_id
lookup_bcs_value = db_nvrmap.lookup_bcs_value


@pytest.fixture
def sample_polygon():
    """Create a sample polygon for testing (100m x 100m = 10000 sq meters = 1 hectare)."""
    return Polygon([(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)])


@pytest.fixture
def sample_config():
    """Create a sample configuration dictionary."""
    return {
        'attribute_table': {
            'project': 'TEST_PROJECT',
            'collector': 'Test Collector',
            'default_habitat_score': 0.5,
            'default_gain_score': 0.22
        }
    }


@pytest.fixture
def sample_args():
    """Create a sample argparse.Namespace object."""
    args = Mock()
    args.gainscore = None
    args.ensym = False
    args.sbeu = False
    return args


@pytest.fixture
def sample_args_with_gainscore():
    """Create args with custom gainscore."""
    args = Mock()
    args.gainscore = 0.75
    args.ensym = False
    args.sbeu = False
    return args


@pytest.fixture
def sample_args_ensym():
    """Create args for EnSym format."""
    args = Mock()
    args.gainscore = None
    args.ensym = True
    args.sbeu = False
    return args


@pytest.fixture
def sample_args_sbeu():
    """Create args for SBEU format."""
    args = Mock()
    args.gainscore = None
    args.ensym = False
    args.sbeu = True
    return args


@pytest.fixture
def sample_evc_df():
    """Create a sample EVC DataFrame."""
    return pd.DataFrame({
        'BIOEVCCODE': ['VVP_0055', 'STIF0132', 'GGP_0175', 'NCU_0823'],
        'BCS_CATEGORY': ['Endangered', 'Vulnerable', 'Depleted', 'LC']
    })


@pytest.fixture
def sample_input_gdf(sample_polygon):
    """Create a sample input GeoDataFrame."""
    data = {
        'geom': [sample_polygon, sample_polygon, sample_polygon],
        'bioregcode': ['VVP', 'STIF', 'GGP'],
        'evc': [55, 132, 175],
        'view_pfi': ['123456', '123456', '789012'],
        'x_evcname': ['Test EVC 1', 'Test EVC 2', 'Test EVC 3'],
        'bioregion': ['Victorian Volcanic Plain', 'Strzelecki Ranges', 'Gippsland Plain']
    }
    gdf = gpd.GeoDataFrame(data, geometry='geom', crs='epsg:7899')
    return gdf


@pytest.fixture
def sample_input_gdf_single(sample_polygon):
    """Create a sample input GeoDataFrame with single parcel."""
    data = {
        'geom': [sample_polygon, sample_polygon],
        'bioregcode': ['VVP', 'STIF'],
        'evc': [55, 132],
        'view_pfi': ['123456', '123456'],
        'x_evcname': ['Test EVC 1', 'Test EVC 2'],
        'bioregion': ['Victorian Volcanic Plain', 'Strzelecki Ranges']
    }
    gdf = gpd.GeoDataFrame(data, geometry='geom', crs='epsg:7899')
    return gdf


class TestProcessNvrmapRows:
    """Tests for process_nvrmap_rows function."""

    def test_single_parcel_first_zone(self):
        """Test processing first row for a single parcel."""
        row = pd.Series({
            'view_pfi': '123456',
            'bioregcode': 'VVP',
            'evc': 55
        })
        view_pfi_list = ['123456']
        count = [0]

        si, zi, bioevc = process_nvrmap_rows(row, view_pfi_list, count)

        assert si == 1
        assert zi == 'A'
        assert bioevc == 'VVP_0055'
        assert count[0] == 1

    def test_single_parcel_multiple_zones(self):
        """Test multiple zones for same parcel (count incrementing)."""
        row1 = pd.Series({'view_pfi': '123456', 'bioregcode': 'VVP', 'evc': 55})
        row2 = pd.Series({'view_pfi': '123456', 'bioregcode': 'STIF', 'evc': 132})
        row3 = pd.Series({'view_pfi': '123456', 'bioregcode': 'GGP', 'evc': 175})

        view_pfi_list = ['123456']
        count = [0]

        si1, zi1, _ = process_nvrmap_rows(row1, view_pfi_list, count)
        si2, zi2, _ = process_nvrmap_rows(row2, view_pfi_list, count)
        si3, zi3, _ = process_nvrmap_rows(row3, view_pfi_list, count)

        assert si1 == si2 == si3 == 1
        assert zi1 == 'A'
        assert zi2 == 'B'
        assert zi3 == 'C'
        assert count[0] == 3

    def test_multiple_parcels(self):
        """Test processing rows from multiple parcels."""
        row1 = pd.Series({'view_pfi': '123456', 'bioregcode': 'VVP', 'evc': 55})
        row2 = pd.Series({'view_pfi': '789012', 'bioregcode': 'STIF', 'evc': 132})
        row3 = pd.Series({'view_pfi': '123456', 'bioregcode': 'GGP', 'evc': 175})

        view_pfi_list = ['123456', '789012']
        count = [0, 0]

        si1, zi1, bioevc1 = process_nvrmap_rows(row1, view_pfi_list, count)
        si2, zi2, bioevc2 = process_nvrmap_rows(row2, view_pfi_list, count)
        si3, zi3, bioevc3 = process_nvrmap_rows(row3, view_pfi_list, count)

        assert si1 == 1
        assert si2 == 2
        assert si3 == 1
        assert zi1 == 'A'
        assert zi2 == 'A'
        assert zi3 == 'B'
        assert count[0] == 2
        assert count[1] == 1
        assert bioevc1 == 'VVP_0055'
        assert bioevc2 == 'STIF0132'
        assert bioevc3 == 'GGP_0175'

    def test_zone_id_z_boundary(self):
        """Test zone ID generation at Z boundary (count 25 -> 26 = zone 'Z')."""
        row = pd.Series({'view_pfi': '123456', 'bioregcode': 'VVP', 'evc': 55})
        view_pfi_list = ['123456']
        count = [25]  # Start at 25, will increment to 26

        si, zi, bioevc = process_nvrmap_rows(row, view_pfi_list, count)

        assert si == 1
        assert zi == 'Z'
        assert bioevc == 'VVP_0055'
        assert count[0] == 26

    def test_zone_id_beyond_26(self):
        """Test zone ID generation beyond 26 zones (AA, AB, etc.)."""
        row = pd.Series({'view_pfi': '123456', 'bioregcode': 'VVP', 'evc': 55})
        view_pfi_list = ['123456']
        count = [26]  # Start at 26

        si1, zi1, _ = process_nvrmap_rows(row, view_pfi_list, count)
        assert zi1 == 'AA'
        assert count[0] == 27

        si2, zi2, _ = process_nvrmap_rows(row, view_pfi_list, count)
        assert zi2 == 'AB'
        assert count[0] == 28

    def test_bioevc_formatting_short_code(self):
        """Test bioregion/EVC formatting for short codes (<=3 chars)."""
        row = pd.Series({'view_pfi': '123456', 'bioregcode': 'VVP', 'evc': 55})
        view_pfi_list = ['123456']
        count = [0]

        _, _, bioevc = process_nvrmap_rows(row, view_pfi_list, count)
        assert bioevc == 'VVP_0055'

    def test_bioevc_formatting_long_code(self):
        """Test bioregion/EVC formatting for long codes (>3 chars)."""
        row = pd.Series({'view_pfi': '123456', 'bioregcode': 'STIF', 'evc': 132})
        view_pfi_list = ['123456']
        count = [0]

        _, _, bioevc = process_nvrmap_rows(row, view_pfi_list, count)
        assert bioevc == 'STIF0132'


class TestProcessEnsymRows:
    """Tests for process_ensym_rows function."""

    def test_single_parcel_with_bcs_lookup(self, sample_evc_df):
        """Test processing with BCS lookup for single parcel."""
        row = pd.Series({
            'view_pfi': '123456',
            'bioregcode': 'VVP',
            'evc': 55
        })
        view_pfi_list = ['123456']
        count = [0]

        si, zi, bioevc, bcs = process_ensym_rows(row, sample_evc_df, view_pfi_list, count)

        assert si == 1
        assert zi == 'A'
        assert bioevc == 'VVP_0055'
        assert bcs == 'E'
        assert count[0] == 1

    def test_bcs_vulnerable_category(self, sample_evc_df):
        """Test BCS lookup for Vulnerable category (should return 'V')."""
        row = pd.Series({
            'view_pfi': '123456',
            'bioregcode': 'STIF',
            'evc': 132
        })
        view_pfi_list = ['123456']
        count = [0]

        si, zi, bioevc, bcs = process_ensym_rows(row, sample_evc_df, view_pfi_list, count)

        assert bcs == 'V'

    def test_bcs_depleted_category(self, sample_evc_df):
        """Test BCS lookup for Depleted category (should return 'D')."""
        row = pd.Series({
            'view_pfi': '123456',
            'bioregcode': 'GGP',
            'evc': 175
        })
        view_pfi_list = ['123456']
        count = [0]

        si, zi, bioevc, bcs = process_ensym_rows(row, sample_evc_df, view_pfi_list, count)

        assert bcs == 'D'

    def test_bcs_least_concern_category(self, sample_evc_df):
        """Test BCS lookup for Least Concern category (should return 'LC')."""
        row = pd.Series({
            'view_pfi': '123456',
            'bioregcode': 'NCU',
            'evc': 823
        })
        view_pfi_list = ['123456']
        count = [0]

        si, zi, bioevc, bcs = process_ensym_rows(row, sample_evc_df, view_pfi_list, count)

        assert bcs == 'LC'

    def test_bcs_not_found_defaults_to_lc(self, sample_evc_df):
        """Test BCS lookup defaults to 'LC' when not found."""
        row = pd.Series({
            'view_pfi': '123456',
            'bioregcode': 'UNKNOWN',
            'evc': 999
        })
        view_pfi_list = ['123456']
        count = [0]

        si, zi, bioevc, bcs = process_ensym_rows(row, sample_evc_df, view_pfi_list, count)

        assert bcs == 'LC'

    def test_multiple_parcels(self, sample_evc_df):
        """Test processing rows from multiple parcels."""
        row1 = pd.Series({'view_pfi': '123456', 'bioregcode': 'VVP', 'evc': 55})
        row2 = pd.Series({'view_pfi': '789012', 'bioregcode': 'STIF', 'evc': 132})

        view_pfi_list = ['123456', '789012']
        count = [0, 0]

        si1, zi1, bioevc1, bcs1 = process_ensym_rows(row1, sample_evc_df, view_pfi_list, count)
        si2, zi2, bioevc2, bcs2 = process_ensym_rows(row2, sample_evc_df, view_pfi_list, count)

        assert si1 == 1
        assert si2 == 2
        assert zi1 == 'A'
        assert zi2 == 'A'
        assert bcs1 == 'E'
        assert bcs2 == 'V'

    def test_count_incrementing(self, sample_evc_df):
        """Test that count increments correctly across multiple calls."""
        row = pd.Series({'view_pfi': '123456', 'bioregcode': 'VVP', 'evc': 55})
        view_pfi_list = ['123456']
        count = [0]

        process_ensym_rows(row, sample_evc_df, view_pfi_list, count)
        assert count[0] == 1

        process_ensym_rows(row, sample_evc_df, view_pfi_list, count)
        assert count[0] == 2

        process_ensym_rows(row, sample_evc_df, view_pfi_list, count)
        assert count[0] == 3


class TestBuildEnsymGdf:
    """Tests for build_ensym_gdf function."""

    def test_standard_ensym_format(self, sample_input_gdf, sample_evc_df, sample_config, sample_args):
        """Test building EnSym 2017 format GeoDataFrame."""
        view_pfi_list = ['123456', '789012']

        result_gdf = build_ensym_gdf(sample_input_gdf, sample_evc_df, view_pfi_list, sample_config, sample_args)

        # Check columns exist
        expected_columns = ['HH_PAI', 'HH_D', 'HH_CP', 'HH_SI', 'HH_ZI', 'HH_VAC',
                          'HH_EVC', 'BCS', 'LT_CNT', 'HH_H_S', 'G_S', 'HH_A', 'geom']
        assert list(result_gdf.columns) == expected_columns

        # Check number of rows
        assert len(result_gdf) == 3

        # Check data types and values
        assert result_gdf['HH_PAI'].iloc[0] == 'TEST_PROJECT'
        assert result_gdf['HH_CP'].iloc[0] == 'Test Collector'
        assert result_gdf['HH_VAC'].iloc[0] == 'P'
        assert result_gdf['LT_CNT'].iloc[0] == 0
        assert result_gdf['HH_H_S'].iloc[0] == 0.5
        assert result_gdf['G_S'].iloc[0] == 0.22

        # Check date format
        today_str = datetime.today().strftime("%Y-%m-%d")
        assert result_gdf['HH_D'].iloc[0] == today_str

        # Check geometry is last column
        assert result_gdf.columns[-1] == 'geom'

    def test_sbeu_2013_format(self, sample_input_gdf, sample_evc_df, sample_config, sample_args_sbeu):
        """Test building EnSym 2013 (SBEU) format GeoDataFrame."""
        view_pfi_list = ['123456', '789012']

        result_gdf = build_ensym_gdf(sample_input_gdf, sample_evc_df, view_pfi_list, sample_config, sample_args_sbeu)

        # Check columns exist (should not have HH_EVC, BCS, LT_CNT)
        expected_columns = ['HH_PAI', 'HH_SI', 'HH_ZI', 'HH_VAC', 'HH_CP',
                          'HH_D', 'HH_H_S', 'G_HA', 'HH_A', 'geom']
        assert list(result_gdf.columns) == expected_columns

        # Check G_S renamed to G_HA
        assert 'G_HA' in result_gdf.columns
        assert 'G_S' not in result_gdf.columns

        # Check removed columns
        assert 'HH_EVC' not in result_gdf.columns
        assert 'BCS' not in result_gdf.columns
        assert 'LT_CNT' not in result_gdf.columns

    def test_custom_gainscore(self, sample_input_gdf_single, sample_evc_df, sample_config, sample_args_with_gainscore):
        """Test building GeoDataFrame with custom gainscore."""
        view_pfi_list = ['123456']

        result_gdf = build_ensym_gdf(sample_input_gdf_single, sample_evc_df, view_pfi_list,
                                     sample_config, sample_args_with_gainscore)

        # Check custom gainscore applied
        assert result_gdf['G_S'].iloc[0] == 0.75
        assert result_gdf['G_S'].iloc[1] == 0.75

    def test_single_parcel_multiple_zones(self, sample_input_gdf_single, sample_evc_df, sample_config, sample_args):
        """Test zone ID incrementing for single parcel with multiple zones."""
        view_pfi_list = ['123456']

        result_gdf = build_ensym_gdf(sample_input_gdf_single, sample_evc_df, view_pfi_list, sample_config, sample_args)

        # All rows should have site_id = 1
        assert all(result_gdf['HH_SI'] == 1)

        # Zone IDs should increment
        assert result_gdf['HH_ZI'].iloc[0] == 'A'
        assert result_gdf['HH_ZI'].iloc[1] == 'B'


class TestBuildNvrmapGdf:
    """Tests for build_nvrmap_gdf function."""

    def test_standard_nvrmap_format(self, sample_input_gdf, sample_config, sample_args):
        """Test building NVRMap format GeoDataFrame."""
        view_pfi_list = ['123456', '789012']

        result_gdf = build_nvrmap_gdf(sample_input_gdf, view_pfi_list, sample_config, sample_args)

        # Check columns in correct order
        expected_columns = ['site_id', 'zone_id', 'prop_id', 'vlot', 'lot', 'recruits',
                          'type', 'cp', 'veg_codes', 'lt_count', 'cond_score',
                          'gain_score', 'surv_date', 'geom']
        assert list(result_gdf.columns) == expected_columns

        # Check number of rows
        assert len(result_gdf) == 3

        # Check static values
        assert result_gdf['prop_id'].iloc[0] == 'TEST_PROJECT'
        assert result_gdf['cp'].iloc[0] == 'Test Collector'
        assert result_gdf['vlot'].iloc[0] == 0
        assert result_gdf['lot'].iloc[0] == 0
        assert result_gdf['recruits'].iloc[0] == 0
        assert result_gdf['type'].iloc[0] == 'p'
        assert result_gdf['lt_count'].iloc[0] == 0
        assert result_gdf['cond_score'].iloc[0] == 0.5
        assert result_gdf['gain_score'].iloc[0] == 0.22

        # Check date format (YYYYMMDD)
        today_str = datetime.today().strftime('%Y%m%d')
        assert result_gdf['surv_date'].iloc[0] == today_str

        # Check veg_codes format
        assert result_gdf['veg_codes'].iloc[0] == 'VVP_0055'
        assert result_gdf['veg_codes'].iloc[1] == 'STIF0132'
        assert result_gdf['veg_codes'].iloc[2] == 'GGP_0175'

    def test_custom_gainscore(self, sample_input_gdf_single, sample_config, sample_args_with_gainscore):
        """Test building GeoDataFrame with custom gainscore."""
        view_pfi_list = ['123456']

        result_gdf = build_nvrmap_gdf(sample_input_gdf_single, view_pfi_list,
                                      sample_config, sample_args_with_gainscore)

        # Check custom gainscore applied
        assert result_gdf['gain_score'].iloc[0] == 0.75
        assert result_gdf['gain_score'].iloc[1] == 0.75

    def test_single_parcel_multiple_zones(self, sample_input_gdf_single, sample_config, sample_args):
        """Test zone ID incrementing for single parcel with multiple zones."""
        view_pfi_list = ['123456']

        result_gdf = build_nvrmap_gdf(sample_input_gdf_single, view_pfi_list, sample_config, sample_args)

        # All rows should have site_id = 1
        assert all(result_gdf['site_id'] == 1)

        # Zone IDs should increment
        assert result_gdf['zone_id'].iloc[0] == 'A'
        assert result_gdf['zone_id'].iloc[1] == 'B'

    def test_geometry_column_last(self, sample_input_gdf, sample_config, sample_args):
        """Test that geometry column is last."""
        view_pfi_list = ['123456', '789012']

        result_gdf = build_nvrmap_gdf(sample_input_gdf, view_pfi_list, sample_config, sample_args)

        # Check geometry is last column
        assert result_gdf.columns[-1] == 'geom'


class TestSelectOutputGdf:
    """Tests for select_output_gdf function."""

    def test_default_nvrmap_output(self, sample_input_gdf, sample_evc_df, sample_config, sample_args):
        """Test that default output is NVRMap format."""
        view_pfi_list = ['123456', '789012']

        result_gdf = select_output_gdf(sample_args, sample_input_gdf, sample_evc_df, view_pfi_list, sample_config)

        # Check it returns NVRMap columns
        expected_columns = ['site_id', 'zone_id', 'prop_id', 'vlot', 'lot', 'recruits',
                          'type', 'cp', 'veg_codes', 'lt_count', 'cond_score',
                          'gain_score', 'surv_date', 'geom']
        assert list(result_gdf.columns) == expected_columns

    def test_ensym_flag(self, sample_input_gdf, sample_evc_df, sample_config, sample_args_ensym):
        """Test that ensym flag selects EnSym 2017 format."""
        view_pfi_list = ['123456', '789012']

        result_gdf = select_output_gdf(sample_args_ensym, sample_input_gdf, sample_evc_df, view_pfi_list, sample_config)

        # Check it returns EnSym 2017 columns
        assert 'HH_EVC' in result_gdf.columns
        assert 'BCS' in result_gdf.columns
        assert 'LT_CNT' in result_gdf.columns
        assert 'G_S' in result_gdf.columns

    def test_sbeu_flag(self, sample_input_gdf, sample_evc_df, sample_config, sample_args_sbeu):
        """Test that sbeu flag selects EnSym 2013 format."""
        view_pfi_list = ['123456', '789012']

        result_gdf = select_output_gdf(sample_args_sbeu, sample_input_gdf, sample_evc_df, view_pfi_list, sample_config)

        # Check it returns EnSym 2013 columns
        assert 'HH_EVC' not in result_gdf.columns
        assert 'BCS' not in result_gdf.columns
        assert 'LT_CNT' not in result_gdf.columns
        assert 'G_HA' in result_gdf.columns  # Renamed from G_S

    def test_both_flags_sbeu_takes_precedence(self, sample_input_gdf, sample_evc_df, sample_config):
        """Test that when both flags are set, sbeu is processed."""
        args = Mock()
        args.gainscore = None
        args.ensym = True
        args.sbeu = True

        view_pfi_list = ['123456', '789012']

        result_gdf = select_output_gdf(args, sample_input_gdf, sample_evc_df, view_pfi_list, sample_config)

        # Both sbeu and ensym trigger ensym output, but sbeu modifies columns
        assert 'G_HA' in result_gdf.columns  # sbeu format


class TestIntegration:
    """Integration tests for complete workflows."""

    def test_nvrmap_complete_workflow(self, sample_input_gdf, sample_evc_df, sample_config, sample_args):
        """Test complete NVRMap workflow from input to output."""
        view_pfi_list = ['123456', '789012']

        # Select output format
        result_gdf = select_output_gdf(sample_args, sample_input_gdf, sample_evc_df, view_pfi_list, sample_config)

        # Verify output
        assert isinstance(result_gdf, gpd.GeoDataFrame)
        assert len(result_gdf) == 3
        assert result_gdf.crs.to_string() == 'EPSG:7899'
        assert 'veg_codes' in result_gdf.columns
        assert result_gdf['site_id'].iloc[0] == 1
        assert result_gdf['zone_id'].iloc[0] == 'A'

    def test_ensym_complete_workflow(self, sample_input_gdf, sample_evc_df, sample_config, sample_args_ensym):
        """Test complete EnSym workflow from input to output."""
        view_pfi_list = ['123456', '789012']

        # Select output format
        result_gdf = select_output_gdf(sample_args_ensym, sample_input_gdf, sample_evc_df, view_pfi_list, sample_config)

        # Verify output
        assert isinstance(result_gdf, gpd.GeoDataFrame)
        assert len(result_gdf) == 3
        assert result_gdf.crs.to_string() == 'EPSG:7899'
        assert 'HH_EVC' in result_gdf.columns
        assert 'BCS' in result_gdf.columns
        assert result_gdf['HH_SI'].iloc[0] == 1
        assert result_gdf['HH_ZI'].iloc[0] == 'A'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
