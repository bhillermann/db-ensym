#!/usr/bin/env python3
"""
Comprehensive unit tests for utility functions in db-nvrmap.py.

Tests cover:
- generate_zone_id: Zone ID generation (A-Z, AA, AB, etc.)
- format_bioevc: Bioregion/EVC code formatting
- calculate_site_id: Site ID calculation from PFI list
- lookup_bcs_value: BCS value lookup from DataFrame
- move_column_to_end: DataFrame column reordering
"""

import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import sys
from pathlib import Path

# Add parent directory to path to import from db-nvrmap.py
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import using module name without hyphen
import importlib.util
spec = importlib.util.spec_from_file_location("db_nvrmap", Path(__file__).parent.parent / "db-nvrmap.py")
db_nvrmap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_nvrmap)

generate_zone_id = db_nvrmap.generate_zone_id
format_bioevc = db_nvrmap.format_bioevc
calculate_site_id = db_nvrmap.calculate_site_id
lookup_bcs_value = db_nvrmap.lookup_bcs_value
move_column_to_end = db_nvrmap.move_column_to_end


class TestGenerateZoneId:
    """Test suite for generate_zone_id function.

    This function converts a count to alphabetic zone IDs:
    - 1-26 -> A-Z
    - 27 -> AA
    - 28 -> AB
    - etc.
    """

    def test_single_letter_zones_start(self):
        """Test zone IDs for counts 1-3 (A, B, C)."""
        count = [1, 0, 0]
        assert generate_zone_id(count, 1) == 'A'

        count = [2, 0, 0]
        assert generate_zone_id(count, 1) == 'B'

        count = [3, 0, 0]
        assert generate_zone_id(count, 1) == 'C'

    def test_single_letter_zones_end(self):
        """Test zone IDs for counts 24-26 (X, Y, Z)."""
        count = [24, 0, 0]
        assert generate_zone_id(count, 1) == 'X'

        count = [25, 0, 0]
        assert generate_zone_id(count, 1) == 'Y'

        count = [26, 0, 0]
        assert generate_zone_id(count, 1) == 'Z'

    def test_double_letter_zones_start(self):
        """Test zone IDs for counts 27-29 (AA, AB, AC)."""
        count = [27, 0, 0]
        assert generate_zone_id(count, 1) == 'AA'

        count = [28, 0, 0]
        assert generate_zone_id(count, 1) == 'AB'

        count = [29, 0, 0]
        assert generate_zone_id(count, 1) == 'AC'

    def test_double_letter_zones_transition(self):
        """Test zone IDs at Z->AA boundary and AZ->BA transition."""
        count = [26, 0, 0]
        assert generate_zone_id(count, 1) == 'Z'

        count = [27, 0, 0]
        assert generate_zone_id(count, 1) == 'AA'

        count = [52, 0, 0]  # 27 + 25 = 52 should be AZ
        assert generate_zone_id(count, 1) == 'AZ'

        count = [53, 0, 0]  # 27 + 26 = 53 should be BA
        assert generate_zone_id(count, 1) == 'BA'

    def test_multiple_sites(self):
        """Test zone ID generation with multiple site indices."""
        count = [5, 10, 15]

        # Site 1
        assert generate_zone_id(count, 1) == 'E'

        # Site 2
        assert generate_zone_id(count, 2) == 'J'

        # Site 3
        assert generate_zone_id(count, 3) == 'O'

    def test_high_count_values(self):
        """Test zone IDs for high count values (100+)."""
        count = [78, 0, 0]  # Should be CA
        assert generate_zone_id(count, 1) == 'CA'

        count = [104, 0, 0]  # Should be DA
        assert generate_zone_id(count, 1) == 'DA'


class TestFormatBioevc:
    """Test suite for format_bioevc function.

    This function formats bioregion code and EVC:
    - bioregcodes <= 3 chars: add underscore (e.g., 'VVP_0055')
    - bioregcodes > 3 chars: no underscore (e.g., 'STIF0055')
    """

    def test_short_bioregcode_single_char(self):
        """Test single character bioregion code."""
        result = format_bioevc('V', 55)
        assert result == 'V_0055'

    def test_short_bioregcode_two_chars(self):
        """Test two character bioregion code."""
        result = format_bioevc('VP', 55)
        assert result == 'VP_0055'

    def test_short_bioregcode_three_chars(self):
        """Test three character bioregion code (boundary case)."""
        result = format_bioevc('VVP', 55)
        assert result == 'VVP_0055'

    def test_long_bioregcode_four_chars(self):
        """Test four character bioregion code (no underscore)."""
        result = format_bioevc('STIF', 55)
        assert result == 'STIF0055'

    def test_long_bioregcode_five_chars(self):
        """Test five character bioregion code."""
        result = format_bioevc('STIFX', 55)
        assert result == 'STIFX0055'

    def test_evc_padding_single_digit(self):
        """Test EVC number padding for single digit."""
        result = format_bioevc('VVP', 5)
        assert result == 'VVP_0005'

    def test_evc_padding_two_digits(self):
        """Test EVC number padding for two digits."""
        result = format_bioevc('VVP', 55)
        assert result == 'VVP_0055'

    def test_evc_padding_three_digits(self):
        """Test EVC number padding for three digits."""
        result = format_bioevc('VVP', 555)
        assert result == 'VVP_0555'

    def test_evc_no_padding_four_digits(self):
        """Test EVC number with four digits (no padding needed)."""
        result = format_bioevc('VVP', 5555)
        assert result == 'VVP_5555'

    def test_evc_zero(self):
        """Test EVC number zero."""
        result = format_bioevc('VVP', 0)
        assert result == 'VVP_0000'


class TestCalculateSiteId:
    """Test suite for calculate_site_id function.

    This function returns the 1-based index of a PFI in the list:
    - Single item list: always returns 1
    - Multiple items: returns position + 1
    - Item not found: returns 1 (with warning)
    """

    def test_single_pfi_list(self):
        """Test with single PFI in list."""
        view_pfi_list = ['12345']
        result = calculate_site_id(view_pfi_list, '12345')
        assert result == 1

    def test_multiple_pfis_first_position(self):
        """Test with multiple PFIs, finding first one."""
        view_pfi_list = ['12345', '67890', '11111']
        result = calculate_site_id(view_pfi_list, '12345')
        assert result == 1

    def test_multiple_pfis_middle_position(self):
        """Test with multiple PFIs, finding middle one."""
        view_pfi_list = ['12345', '67890', '11111']
        result = calculate_site_id(view_pfi_list, '67890')
        assert result == 2

    def test_multiple_pfis_last_position(self):
        """Test with multiple PFIs, finding last one."""
        view_pfi_list = ['12345', '67890', '11111']
        result = calculate_site_id(view_pfi_list, '11111')
        assert result == 3

    def test_pfi_not_found(self):
        """Test with PFI not in list (should return 1 with warning)."""
        view_pfi_list = ['12345', '67890']
        result = calculate_site_id(view_pfi_list, '99999')
        assert result == 1

    def test_large_list(self):
        """Test with larger list of PFIs."""
        view_pfi_list = [str(i * 1000) for i in range(1, 11)]

        # First
        assert calculate_site_id(view_pfi_list, '1000') == 1

        # Middle
        assert calculate_site_id(view_pfi_list, '5000') == 5

        # Last
        assert calculate_site_id(view_pfi_list, '10000') == 10

    def test_single_pfi_different_value(self):
        """Test single PFI list always returns 1."""
        view_pfi_list = ['99999']
        result = calculate_site_id(view_pfi_list, '99999')
        assert result == 1


class TestLookupBcsValue:
    """Test suite for lookup_bcs_value function.

    This function looks up BCS (Bioregional Conservation Status) values:
    - Found value: returns first character (except 'LC' which is returned as-is)
    - Not found: returns 'LC' with warning
    - Invalid values: returns 'LC'
    - 'TBC' value: returns 'LC'
    """

    @pytest.fixture
    def sample_evc_df(self):
        """Create sample EVC DataFrame for testing."""
        data = {
            'BIOEVCCODE': [
                'VVP_0055',
                'STIF0055',
                'GGP_0132',
                'VVP_0001',
                'VVP_0002',
                'VVP_0003',
                'VVP_0004'
            ],
            'BCS_CATEGORY': [
                'Endangered',
                'Vulnerable',
                'Depleted',
                'Rare',
                'LC',
                'TBC',
                None
            ]
        }
        return pd.DataFrame(data)

    def test_lookup_endangered(self, sample_evc_df):
        """Test lookup of Endangered status (returns 'E')."""
        result = lookup_bcs_value('VVP_0055', sample_evc_df)
        assert result == 'E'

    def test_lookup_vulnerable(self, sample_evc_df):
        """Test lookup of Vulnerable status (returns 'V')."""
        result = lookup_bcs_value('STIF0055', sample_evc_df)
        assert result == 'V'

    def test_lookup_depleted(self, sample_evc_df):
        """Test lookup of Depleted status (returns 'D')."""
        result = lookup_bcs_value('GGP_0132', sample_evc_df)
        assert result == 'D'

    def test_lookup_rare(self, sample_evc_df):
        """Test lookup of Rare status (returns 'R')."""
        result = lookup_bcs_value('VVP_0001', sample_evc_df)
        assert result == 'R'

    def test_lookup_least_concern(self, sample_evc_df):
        """Test lookup of Least Concern status (returns 'LC')."""
        result = lookup_bcs_value('VVP_0002', sample_evc_df)
        assert result == 'LC'

    def test_lookup_tbc_value(self, sample_evc_df):
        """Test lookup of 'TBC' value (returns 'LC')."""
        result = lookup_bcs_value('VVP_0003', sample_evc_df)
        assert result == 'LC'

    def test_lookup_none_value(self, sample_evc_df):
        """Test lookup of None value (returns 'LC')."""
        result = lookup_bcs_value('VVP_0004', sample_evc_df)
        assert result == 'LC'

    def test_lookup_not_found(self, sample_evc_df):
        """Test lookup of non-existent bioevc (returns 'LC')."""
        result = lookup_bcs_value('XXX_9999', sample_evc_df)
        assert result == 'LC'

    def test_lookup_empty_dataframe(self):
        """Test lookup with empty DataFrame (returns 'LC')."""
        empty_df = pd.DataFrame({'BIOEVCCODE': [], 'BCS_CATEGORY': []})
        result = lookup_bcs_value('VVP_0055', empty_df)
        assert result == 'LC'

    def test_lookup_whitespace_value(self):
        """Test lookup with whitespace-only BCS value (returns 'LC')."""
        df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055'],
            'BCS_CATEGORY': ['   ']
        })
        result = lookup_bcs_value('VVP_0055', df)
        assert result == 'LC'

    def test_lookup_empty_string(self):
        """Test lookup with empty string BCS value (returns 'LC')."""
        df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055'],
            'BCS_CATEGORY': ['']
        })
        result = lookup_bcs_value('VVP_0055', df)
        assert result == 'LC'

    def test_lookup_substring_matching_behavior(self):
        """Test that lookup_bcs_value uses substring matching, not exact matching.

        IMPORTANT: This function uses str.contains() which performs substring matching.
        This means:
        - Searching for 'VVP_005' will match both 'VVP_0055' and 'VVP_00551'
        - The function returns the FIRST match found
        - Users should be aware of this behavior to avoid unintended matches

        This test documents the actual implementation behavior.
        """
        df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055', 'VVP_00551', 'GGP_0132'],
            'BCS_CATEGORY': ['Endangered', 'Vulnerable', 'Depleted']
        })

        # Substring 'VVP_005' matches both 'VVP_0055' and 'VVP_00551'
        # Should return first match ('VVP_0055' -> 'E')
        result = lookup_bcs_value('VVP_005', df)
        assert result == 'E'

        # Exact match still works
        result_exact = lookup_bcs_value('VVP_00551', df)
        assert result_exact == 'V'

        # Partial match at end
        result_partial = lookup_bcs_value('0055', df)
        assert result_partial == 'E'  # Matches first occurrence (VVP_0055)


class TestMoveColumnToEnd:
    """Test suite for move_column_to_end function.

    This function moves a specified column to the end of a GeoDataFrame.
    """

    @pytest.fixture
    def sample_gdf(self):
        """Create sample GeoDataFrame for testing."""
        data = {
            'col_a': [1, 2, 3],
            'col_b': [4, 5, 6],
            'col_c': [7, 8, 9],
            'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)]
        }
        return gpd.GeoDataFrame(data, geometry='geometry')

    def test_move_first_column_to_end(self, sample_gdf):
        """Test moving first column to end."""
        result = move_column_to_end(sample_gdf, 'col_a')
        expected_order = ['col_b', 'col_c', 'geometry', 'col_a']
        assert result.columns.tolist() == expected_order

    def test_move_middle_column_to_end(self, sample_gdf):
        """Test moving middle column to end."""
        result = move_column_to_end(sample_gdf, 'col_b')
        expected_order = ['col_a', 'col_c', 'geometry', 'col_b']
        assert result.columns.tolist() == expected_order

    def test_move_geometry_to_end(self, sample_gdf):
        """Test moving geometry column to end."""
        result = move_column_to_end(sample_gdf, 'geometry')
        expected_order = ['col_a', 'col_b', 'col_c', 'geometry']
        assert result.columns.tolist() == expected_order

    def test_move_invalid_column_raises_keyerror(self, sample_gdf):
        """Test that moving non-existent column raises KeyError."""
        with pytest.raises(KeyError, match="Column 'invalid_col' not found"):
            move_column_to_end(sample_gdf, 'invalid_col')

    def test_move_column_preserves_data(self, sample_gdf):
        """Test that moving column preserves all data."""
        result = move_column_to_end(sample_gdf, 'col_a')

        # Check data is preserved
        assert result['col_a'].tolist() == [1, 2, 3]
        assert result['col_b'].tolist() == [4, 5, 6]
        assert result['col_c'].tolist() == [7, 8, 9]

        # Check geometry is preserved
        assert len(result) == 3
        assert all(result.geometry.geom_type == 'Point')

    def test_move_column_returns_geodataframe(self, sample_gdf):
        """Test that function returns a GeoDataFrame."""
        result = move_column_to_end(sample_gdf, 'col_a')
        assert isinstance(result, gpd.GeoDataFrame)


class TestEdgeCasesAndIntegration:
    """Additional tests for edge cases and integration scenarios."""

    def test_generate_zone_id_with_zero_count(self):
        """Test that zero count is handled (though shouldn't occur in practice)."""
        # This is an edge case - in practice count should be >= 1
        # The function uses chr(ord('@') + counter) so 0 would give '@'
        count = [0, 0, 0]
        result = generate_zone_id(count, 1)
        assert result == '@'  # chr(64) = '@'

    def test_format_bioevc_with_float_evc(self):
        """Test format_bioevc with float EVC (should convert to int)."""
        result = format_bioevc('VVP', 55.7)
        assert result == 'VVP_0055'

    def test_calculate_site_id_with_empty_list(self):
        """Test calculate_site_id with empty list."""
        view_pfi_list = []
        result = calculate_site_id(view_pfi_list, '12345')
        # Empty list means len() == 0, so it goes to else branch -> return 1
        assert result == 1

    def test_lookup_bcs_with_numeric_bcs_value(self):
        """Test lookup_bcs_value when BCS_CATEGORY is numeric."""
        df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055'],
            'BCS_CATEGORY': [123]  # Numeric value
        })
        result = lookup_bcs_value('VVP_0055', df)
        # isinstance(123, str) is False, so should return 'LC'
        assert result == 'LC'

    def test_multiple_functions_integration(self):
        """Integration test using multiple utility functions together."""
        # Simulate processing a row
        bioregcode = 'VVP'
        evc = 55
        view_pfi_list = ['12345', '67890', '11111']
        count = [0, 0, 0]

        # Calculate site_id
        si = calculate_site_id(view_pfi_list, '67890')
        assert si == 2

        # Increment count for this site
        count[si - 1] += 1

        # Generate zone_id
        zi = generate_zone_id(count, si)
        assert zi == 'A'

        # Format bioevc
        bioevc = format_bioevc(bioregcode, evc)
        assert bioevc == 'VVP_0055'

        # Create sample EVC DataFrame and lookup BCS
        evc_df = pd.DataFrame({
            'BIOEVCCODE': ['VVP_0055'],
            'BCS_CATEGORY': ['Endangered']
        })
        bcs = lookup_bcs_value(bioevc, evc_df)
        assert bcs == 'E'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
