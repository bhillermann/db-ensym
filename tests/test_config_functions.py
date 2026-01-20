#!/usr/bin/env python3
"""
Comprehensive unit tests for configuration functions in db-nvrmap.py.

Tests cover:
- load_db_config_from_env: Loading DB config from environment variables
- load_config: Loading config from env vars and/or config file
- get_attribute: Getting values from attribute_table section
"""

import pytest
import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

# Add parent directory to path to import from db-nvrmap.py
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import using module name without hyphen
import importlib.util
spec = importlib.util.spec_from_file_location("db_nvrmap", Path(__file__).parent.parent / "db-nvrmap.py")
db_nvrmap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_nvrmap)

load_db_config_from_env = db_nvrmap.load_db_config_from_env
load_config = db_nvrmap.load_config
get_attribute = db_nvrmap.get_attribute


class TestLoadDbConfigFromEnv:
    """Test suite for load_db_config_from_env function.

    This function reads NVRMAP_DB_* environment variables and returns
    a dictionary with database configuration.
    """

    def test_all_vars_set(self):
        """Test with all environment variables set."""
        env_vars = {
            'NVRMAP_DB_TYPE': 'postgresql+psycopg2',
            'NVRMAP_DB_USER': 'testuser',
            'NVRMAP_DB_PASSWORD': 'testpass',
            'NVRMAP_DB_HOST': 'localhost',
            'NVRMAP_DB_NAME': 'testdb'
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_db_config_from_env()

        assert result == {
            'db_type': 'postgresql+psycopg2',
            'username': 'testuser',
            'password': 'testpass',
            'host': 'localhost',
            'database': 'testdb'
        }

    def test_some_vars_set(self):
        """Test with only some environment variables set."""
        env_vars = {
            'NVRMAP_DB_TYPE': 'postgresql+psycopg2',
            'NVRMAP_DB_USER': 'testuser'
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_db_config_from_env()

        assert result == {
            'db_type': 'postgresql+psycopg2',
            'username': 'testuser'
        }

    def test_no_vars_set(self):
        """Test with no environment variables set."""
        with patch.dict(os.environ, {}, clear=True):
            result = load_db_config_from_env()

        assert result == {}

    def test_vars_with_whitespace(self):
        """Test that whitespace-only values are ignored."""
        env_vars = {
            'NVRMAP_DB_TYPE': '  postgresql+psycopg2  ',  # Has surrounding whitespace
            'NVRMAP_DB_USER': '   ',  # Only whitespace - should be ignored
            'NVRMAP_DB_PASSWORD': 'testpass',
            'NVRMAP_DB_HOST': '',  # Empty - should be ignored
            'NVRMAP_DB_NAME': 'testdb'
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_db_config_from_env()

        # Note: The function strips whitespace, so 'postgresql+psycopg2' is kept
        # but '   ' (whitespace only) should be ignored
        assert result == {
            'db_type': 'postgresql+psycopg2',
            'password': 'testpass',
            'database': 'testdb'
        }

    def test_empty_string_vars(self):
        """Test that empty string values are ignored."""
        env_vars = {
            'NVRMAP_DB_TYPE': '',
            'NVRMAP_DB_USER': '',
            'NVRMAP_DB_PASSWORD': '',
            'NVRMAP_DB_HOST': '',
            'NVRMAP_DB_NAME': ''
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_db_config_from_env()

        assert result == {}


class TestLoadConfig:
    """Test suite for load_config function.

    Configuration priority (highest to lowest):
    1. Environment variables (NVRMAP_DB_*)
    2. Config file ($NVRMAP_CONFIG/config.json)
    """

    @pytest.fixture
    def sample_config_file(self, tmp_path):
        """Create a sample config file for testing."""
        config_data = {
            'db_connection': {
                'db_type': 'postgresql+psycopg2',
                'username': 'fileuser',
                'password': 'filepass',
                'host': 'filehost',
                'database': 'filedb'
            },
            'attribute_table': {
                'project': 'TEST_PROJECT',
                'collector': 'Test Collector',
                'default_habitat_score': 0.5,
                'default_gain_score': 0.22
            },
            'evc_data': '~/evc_data.xlsx'
        }
        config_file = tmp_path / 'config.json'
        config_file.write_text(json.dumps(config_data))
        return tmp_path, config_data

    def test_all_env_vars_provided(self):
        """Test loading config when all DB env vars are provided."""
        env_vars = {
            'NVRMAP_DB_TYPE': 'postgresql+psycopg2',
            'NVRMAP_DB_USER': 'envuser',
            'NVRMAP_DB_PASSWORD': 'envpass',
            'NVRMAP_DB_HOST': 'envhost',
            'NVRMAP_DB_NAME': 'envdb'
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_config()

        assert result['db_connection'] == {
            'db_type': 'postgresql+psycopg2',
            'username': 'envuser',
            'password': 'envpass',
            'host': 'envhost',
            'database': 'envdb'
        }

    def test_config_file_only(self, sample_config_file):
        """Test loading config from file only (no env vars)."""
        config_dir, expected_config = sample_config_file

        env_vars = {
            'NVRMAP_CONFIG': str(config_dir)
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_config()

        assert result['db_connection'] == expected_config['db_connection']
        assert result['attribute_table'] == expected_config['attribute_table']
        assert result['evc_data'] == expected_config['evc_data']

    def test_env_vars_override_config_file(self, sample_config_file):
        """Test that env vars override config file values."""
        config_dir, _ = sample_config_file

        env_vars = {
            'NVRMAP_CONFIG': str(config_dir),
            'NVRMAP_DB_TYPE': 'postgresql+psycopg2',
            'NVRMAP_DB_USER': 'override_user',  # Override from file
            'NVRMAP_DB_PASSWORD': 'override_pass'  # Override from file
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_config()

        # These should be overridden by env vars
        assert result['db_connection']['username'] == 'override_user'
        assert result['db_connection']['password'] == 'override_pass'
        # These should remain from file
        assert result['db_connection']['host'] == 'filehost'
        assert result['db_connection']['database'] == 'filedb'

    def test_missing_required_config_raises_error(self):
        """Test that missing required config raises EnvironmentError."""
        # No env vars and no config file
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError):
                load_config()

    def test_partial_env_vars_without_config_file_raises_error(self):
        """Test partial env vars without config file raises error."""
        # Only some DB env vars set, no config file
        env_vars = {
            'NVRMAP_DB_TYPE': 'postgresql+psycopg2',
            'NVRMAP_DB_USER': 'testuser'
            # Missing password, host, database
        }
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(EnvironmentError):
                load_config()

    def test_config_file_not_found(self, tmp_path):
        """Test handling when NVRMAP_CONFIG points to non-existent directory."""
        nonexistent_path = tmp_path / 'nonexistent'

        env_vars = {
            'NVRMAP_CONFIG': str(nonexistent_path)
        }
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(EnvironmentError):
                load_config()

    def test_config_dir_exists_but_file_missing(self, tmp_path):
        """Test when NVRMAP_CONFIG points to existing directory without config.json.

        When all DB env vars are provided, it should still work using just the env vars.
        """
        # Create an empty directory (no config.json)
        empty_dir = tmp_path / 'empty_config_dir'
        empty_dir.mkdir()

        env_vars = {
            'NVRMAP_CONFIG': str(empty_dir),
            'NVRMAP_DB_TYPE': 'postgresql+psycopg2',
            'NVRMAP_DB_USER': 'testuser',
            'NVRMAP_DB_PASSWORD': 'testpass',
            'NVRMAP_DB_HOST': 'testhost',
            'NVRMAP_DB_NAME': 'testdb'
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_config()

        # Should work with env vars only
        assert result['db_connection'] == {
            'db_type': 'postgresql+psycopg2',
            'username': 'testuser',
            'password': 'testpass',
            'host': 'testhost',
            'database': 'testdb'
        }

    def test_config_preserves_other_sections(self, sample_config_file):
        """Test that other config sections are preserved."""
        config_dir, expected_config = sample_config_file

        env_vars = {
            'NVRMAP_CONFIG': str(config_dir)
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = load_config()

        # attribute_table and evc_data should be preserved
        assert 'attribute_table' in result
        assert result['attribute_table']['project'] == 'TEST_PROJECT'
        assert 'evc_data' in result


class TestGetAttribute:
    """Test suite for get_attribute function.

    This function retrieves values from the 'attribute_table' section of config.
    """

    def test_key_exists(self):
        """Test getting a key that exists."""
        config = {
            'attribute_table': {
                'project': 'MY_PROJECT',
                'collector': 'John Doe',
                'default_habitat_score': 0.5
            }
        }

        assert get_attribute(config, 'project') == 'MY_PROJECT'
        assert get_attribute(config, 'collector') == 'John Doe'
        assert get_attribute(config, 'default_habitat_score') == 0.5

    def test_key_not_exists_returns_default(self):
        """Test getting a key that doesn't exist returns default."""
        config = {
            'attribute_table': {
                'project': 'MY_PROJECT'
            }
        }

        # Without explicit default, returns None
        assert get_attribute(config, 'nonexistent') is None

        # With explicit default
        assert get_attribute(config, 'nonexistent', 'default_value') == 'default_value'
        assert get_attribute(config, 'nonexistent', 0) == 0

    def test_no_attribute_table_section(self):
        """Test config without attribute_table section."""
        config = {
            'db_connection': {
                'host': 'localhost'
            }
        }

        # Should return None (no attribute_table section)
        assert get_attribute(config, 'project') is None
        assert get_attribute(config, 'project', 'default') == 'default'

    def test_empty_config(self):
        """Test with empty config dictionary."""
        config = {}

        assert get_attribute(config, 'project') is None
        assert get_attribute(config, 'project', 'fallback') == 'fallback'

    def test_various_value_types(self):
        """Test retrieving various value types."""
        config = {
            'attribute_table': {
                'string_val': 'text',
                'int_val': 42,
                'float_val': 3.14,
                'bool_val': True,
                'list_val': [1, 2, 3],
                'none_val': None
            }
        }

        assert get_attribute(config, 'string_val') == 'text'
        assert get_attribute(config, 'int_val') == 42
        assert get_attribute(config, 'float_val') == 3.14
        assert get_attribute(config, 'bool_val') is True
        assert get_attribute(config, 'list_val') == [1, 2, 3]
        assert get_attribute(config, 'none_val') is None


class TestConfigIntegration:
    """Integration tests for configuration functions."""

    def test_full_config_workflow(self, tmp_path):
        """Test complete configuration workflow."""
        # Create config file
        config_data = {
            'db_connection': {
                'db_type': 'postgresql+psycopg2',
                'username': 'fileuser',
                'password': 'filepass',
                'host': 'filehost',
                'database': 'filedb'
            },
            'attribute_table': {
                'project': 'TEST_PROJECT',
                'collector': 'Test Collector'
            }
        }
        config_file = tmp_path / 'config.json'
        config_file.write_text(json.dumps(config_data))

        # Set env vars to override some values
        env_vars = {
            'NVRMAP_CONFIG': str(tmp_path),
            'NVRMAP_DB_USER': 'env_override_user'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config()

            # Verify env override worked
            assert config['db_connection']['username'] == 'env_override_user'

            # Verify file values retained
            assert config['db_connection']['password'] == 'filepass'

            # Verify get_attribute works
            assert get_attribute(config, 'project') == 'TEST_PROJECT'
            assert get_attribute(config, 'collector') == 'Test Collector'
            assert get_attribute(config, 'missing', 'default') == 'default'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
