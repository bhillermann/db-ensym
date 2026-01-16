#!/usr/bin/env python3

import os
import json
import argparse
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Any
from pathlib import Path

import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, select, func, MetaData, cast
from sqlalchemy.engine.url import URL
from geoalchemy2 import Geometry

# Constants
DEFAULT_CRS = 'epsg:7899'
PARCEL_BUFFER_METERS = -6  # Negative buffer to shrink parcel geometry inward
SQ_METERS_PER_HECTARE = 10000  # Conversion factor for area calculations
ENSYM_2013_SCHEMA = {
    'geometry': 'Polygon',
    'properties': {
        'HH_PAI': 'str:11',
        'HH_SI': 'int:2',
        'HH_ZI': 'str:2',
        'HH_VAC': 'str:2',
        'HH_CP' : 'str:50',
        'HH_D': 'date',
        'HH_H_S': 'float:3.2',
        'G_HA': 'float:5.4',
        'HH_A': 'float:10.4'
    }
}
ENSYM_2017_SCHEMA = {
    'geometry': 'Polygon',
    'properties': {
        'HH_PAI': 'str:11',
        'HH_D': 'date',
        'HH_CP' : 'str:50',
        'HH_SI': 'int:2',
        'HH_ZI': 'str:2',
        'HH_VAC': 'str:2',
        'HH_EVC': 'str:10',
        'BCS': 'str:2',
        'LT_CNT': 'int:5',
        'HH_H_S': 'float:3.2',
        'G_S': 'float:5.4',
        'HH_A': 'float:10.4'
    }
}
NVRMAP_SCHEMA = {
    'geometry': 'Polygon',
    'properties': {
        'site_id': 'int:2',
        'zone_id': 'str:2',
        'prop_id': 'str:50',
        'vlot': 'int',
        'lot': 'int',
        'recruits': 'int',
        'type': 'str:2',
        'cp' : 'str:50',
        'veg_codes': 'str:10',
        'lt_count': 'int',
        'cond_score': 'float:3.2',
        'gain_score': 'float:5.4',
        'surv_date': 'int'
    }
}



# Configure logging
logging.basicConfig(level=logging.INFO)

def load_db_config_from_env() -> Dict[str, str]:
    """Load database configuration from environment variables.

    Reads the following environment variables:
    - NVRMAP_DB_TYPE: Database type (e.g., 'postgresql+psycopg2')
    - NVRMAP_DB_USER: Database username
    - NVRMAP_DB_PASSWORD: Database password
    - NVRMAP_DB_HOST: Database host
    - NVRMAP_DB_NAME: Database name

    Returns:
        Dict[str, str]: Dictionary with database configuration keys that were found
                       in environment variables. Keys map to config.json format:
                       db_type, username, password, host, database
    """
    env_to_config_map = {
        'NVRMAP_DB_TYPE': 'db_type',
        'NVRMAP_DB_USER': 'username',
        'NVRMAP_DB_PASSWORD': 'password',
        'NVRMAP_DB_HOST': 'host',
        'NVRMAP_DB_NAME': 'database'
    }

    db_config = {}
    for env_var, config_key in env_to_config_map.items():
        value = os.environ.get(env_var, '').strip()
        if value:
            db_config[config_key] = value

    return db_config

def load_config() -> Dict[str, Any]:
    """Load configuration from environment variables and config file.

    Configuration priority (highest to lowest):
    1. Environment variables (NVRMAP_DB_* for database connection)
    2. Config file ($NVRMAP_CONFIG/config.json)

    Database environment variables:
    - NVRMAP_DB_TYPE: Database type (e.g., 'postgresql+psycopg2')
    - NVRMAP_DB_USER: Database username
    - NVRMAP_DB_PASSWORD: Database password
    - NVRMAP_DB_HOST: Database host
    - NVRMAP_DB_NAME: Database name

    The config file is optional if all database environment variables are provided.
    Environment variables can partially override config file values.

    Returns:
        Dict[str, Any]: Complete configuration dictionary

    Raises:
        EnvironmentError: If required configuration is missing from both sources
    """
    # Load database config from environment variables first
    db_config_from_env = load_db_config_from_env()

    # Try to load config file
    config_dir = os.environ.get("NVRMAP_CONFIG")
    config_from_file = {}

    if config_dir:
        config_path = Path(config_dir) / "config.json"
        if config_path.exists():
            with config_path.open("r") as f:
                config_from_file = json.load(f)

    # Check if we have enough configuration
    # We need either: all DB env vars OR a config file OR a combination
    required_db_keys = ["db_type", "username", "password", "host", "database"]

    # Merge configurations: start with file config, override with env vars
    # Note: Environment variables take precedence over config file values
    if config_from_file:
        final_config = config_from_file.copy()
        # Override db_connection values with environment variables
        if 'db_connection' in final_config:
            final_config['db_connection'].update(db_config_from_env)
        else:
            final_config['db_connection'] = db_config_from_env
    else:
        # No config file, must have all DB vars from environment
        if all(key in db_config_from_env for key in required_db_keys):
            final_config = {'db_connection': db_config_from_env}
        else:
            raise EnvironmentError(
                "NVRMAP_CONFIG environment variable is not set or config file not found, "
                "and not all database environment variables are provided. "
                "Either provide a config file or set all of: "
                "NVRMAP_DB_TYPE, NVRMAP_DB_USER, NVRMAP_DB_PASSWORD, NVRMAP_DB_HOST, NVRMAP_DB_NAME"
            )

    # Validate that we have all required database connection keys
    if 'db_connection' not in final_config:
        raise EnvironmentError("Database connection configuration is missing")

    missing_keys = [k for k in required_db_keys if k not in final_config['db_connection']]
    if missing_keys:
        raise EnvironmentError(
            f"Missing required database configuration keys: {missing_keys}. "
            "Provide them via environment variables (NVRMAP_DB_*) or config file."
        )

    return final_config

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Process View PFIs to an Ensym shapefile.")
    parser.add_argument('view_pfi', metavar='N', type=int, nargs='+', help="PFI of the Parcel View")
    parser.add_argument("-s", "--shapefile", default='nvrmap', help="Name of the shapefile/directory to write. Default is 'nvrmap'.")
    parser.add_argument("-g", "--gainscore", type=float, help="Override gainscore value")
    parser.add_argument("-p", "--property", action='store_true', help="Use Property View PFIs")
    parser.add_argument("-e", "--ensym", action='store_true', help="Output in EnSym format")
    parser.add_argument("-b", "--sbeu", action='store_true', help="Output in 2013 SBEU format")
    return parser.parse_args()
    

def connect_db(db_config: Dict[str, str]) -> Tuple[Any, Dict[str, Any]]:
    """Connect to the database and reflect required tables.

    Args:
        db_config: Dictionary containing database connection parameters

    Returns:
        Tuple of (SQLAlchemy engine, dictionary of reflected tables)

    Raises:
        KeyError: If required keys are missing from db_config
        ValueError: If any required config value is empty or whitespace
    """
    required_keys = ["db_type", "username", "password", "host", "database"]

    # Check for missing keys
    missing_keys = [k for k in required_keys if k not in db_config]
    if missing_keys:
        raise KeyError(f"Missing DB config keys: {missing_keys}")

    # Check for empty/whitespace values
    empty_keys = [k for k in required_keys if not str(db_config[k]).strip()]
    if empty_keys:
        raise ValueError(
            f"Database config values cannot be empty: {empty_keys}. "
            "Provide valid values via environment variables (NVRMAP_DB_*) or config file."
        )

    url = URL.create(
        db_config["db_type"],
        username=db_config["username"],
        password=db_config["password"],
        host=db_config["host"],
        database=db_config["database"]
    )
    engine = create_engine(url)
    metadata = MetaData()
    metadata.reflect(only=["parcel_view", "nv1750_evc", "bioregions",
                           "parcel_property", "parcel_detail", "property_detail"], bind=engine)
    return engine, metadata.tables

def process_view_pfis(args: argparse.Namespace, engine: Any, parcel_property, parcel_detail, property_detail) -> List : 
    """Convert parcel view pfis to list of strings or convert property view pfis to parcels pfis"""
    if args.property:

        # Step 1: CTE for property_pfi
        property_pfi_cte = (
            select(property_detail.c.pfi.label('pr_pfi'))
            .where(property_detail.c.view_pfi.in_(list(map(str, args.view_pfi))))
            .cte('property_pfi')
        )

        # Step 2: CTE for parcel_pfi
        parcel_pfi_cte = (
            select(parcel_property.c.parcel_pfi)
            .select_from(parcel_property.join(property_pfi_cte, parcel_property.c.pr_pfi == property_pfi_cte.c.pr_pfi))
            .cte('parcel_pfi')
        )

        # Step 3: Final select
        parc_view_pfi = (
            select(parcel_detail.c.view_pfi)
            .select_from(parcel_detail.join(parcel_pfi_cte, parcel_detail.c.pfi == parcel_pfi_cte.c.parcel_pfi))
        )


        with engine.connect() as conn:
            result = conn.execute(parc_view_pfi)

        return [r[0] for r in result]
    else:
        return list(map(str, args.view_pfi))


def build_query(parcel_view, nv1750_evc, bioregions, pfi_values: List[str]) -> Any:
    """Construct SQL query for spatial data extraction.

    This function builds a complex spatial query that:
    1. Buffers parcel geometries inward by PARCEL_BUFFER_METERS
    2. Intersects buffered parcels with EVC (Ecological Vegetation Class) layer
    3. Uses ST_Dump to convert any multi-part geometries to single parts
    4. Further intersects with bioregions layer for classification

    Args:
        parcel_view: SQLAlchemy table for parcel_view
        nv1750_evc: SQLAlchemy table for nv1750_evc (vegetation)
        bioregions: SQLAlchemy table for bioregions
        pfi_values: List of parcel PFI (Property Feature Identifier) values to process

    Returns:
        SQLAlchemy select statement ready for execution
    """
    # ST_Dump returns a composite type (geometry_dump) with a geom field
    # We extract the geometry using cast to convert the composite field access
    clipped_geom = cast(
        func.ST_Dump(
            func.ST_Intersection(
                func.ST_Buffer(parcel_view.c.geom, PARCEL_BUFFER_METERS),
                nv1750_evc.c.geom
            )
        ).geom,
        Geometry
    )

    clipped_subq = (
        select(
            nv1750_evc.c.evc,
            nv1750_evc.c.x_evcname,
            parcel_view.c.pfi.label("view_pfi"),
            clipped_geom.label("geom")
        )
        .join(nv1750_evc, func.ST_Intersects(parcel_view.c.geom, nv1750_evc.c.geom))
        .where(parcel_view.c.pfi.in_(pfi_values))
        .subquery("clipped")
    )

    # Second ST_Dump for intersection with bioregions
    outer_geom = cast(
        func.ST_Dump(
            func.ST_Intersection(clipped_subq.c.geom, bioregions.c.geom)
        ).geom,
        Geometry
    )

    bio_clipped_subq = (
        select(
            clipped_subq.c.evc,
            clipped_subq.c.x_evcname,
            clipped_subq.c.view_pfi,
            bioregions.c.bioregcode,
            bioregions.c.bioregion,
            outer_geom.label("geom")
        )
        .join(bioregions, func.ST_Intersects(clipped_subq.c.geom, bioregions.c.geom))
        .where(func.ST_Dimension(clipped_subq.c.geom) == 2)
        .subquery("bio_clipped")
    )

    return select(bio_clipped_subq).order_by(bio_clipped_subq.c.bioregcode)

def load_geo_dataframe(engine, query: Any) -> gpd.GeoDataFrame:
    """Load spatial data into a GeoDataFrame."""
    gdf = gpd.GeoDataFrame.from_postgis(query, con=engine.connect(), geom_col="geom")
    if gdf.empty:
        raise ValueError("No search results found. Check your View PFI values.")
    return gdf.set_crs(DEFAULT_CRS)

def load_evc_data(path: str) -> pd.DataFrame:
    """Load EVC data from Excel file."""
    return pd.read_excel(Path(path).expanduser())


def generate_zone_id(count: List[int], si: int) -> str:
    """Generate Zone IDs by converting count to letters (A-Z, then AA, AB, etc.)."""
    counter = count[si - 1]
    if counter <= 26:
        return chr(ord('@') + counter)
    else:
        # Base-26 conversion for counts > 26
        counter -= 1  # Make 0-indexed for calculation
        first_letter = chr(ord('A') + (counter // 26) - 1)
        second_letter = chr(ord('A') + (counter % 26))
        return first_letter + second_letter


def format_bioevc(bioregcode: str, evc: int) -> str:
    """Format bioregion code and EVC into combined identifier.

    Adds underscore separator for bioregcodes <= 3 chars.

    Args:
        bioregcode: Bioregion code (e.g., 'VVP', 'STIF')
        evc: Ecological Vegetation Class number

    Returns:
        str: Formatted bioregion-EVC identifier (e.g., 'VVP_0055' or 'STIF0055')
    """
    evc_padded = str(int(evc)).zfill(4)
    if len(bioregcode) <= 3:
        return f"{bioregcode}_{evc_padded}"
    return f"{bioregcode}{evc_padded}"


def calculate_site_id(view_pfi_list: List[str], view_pfi: str) -> int:
    """Calculate site ID based on position in view PFI list.

    Args:
        view_pfi_list: List of all view PFI values being processed
        view_pfi: Current view PFI to find in the list

    Returns:
        int: Site ID (1-based index), or 1 if only one PFI in list
    """
    if len(view_pfi_list) > 1:
        try:
            return view_pfi_list.index(view_pfi) + 1
        except ValueError:
            logging.warning(f"View PFI {view_pfi} not found in list, defaulting to 1")
            return 1
    return 1


def lookup_bcs_value(bioevc: str, evc_df: pd.DataFrame) -> str:
    """Look up BCS (Bioregional Conservation Status) value for a bioevc code.

    Searches for the bioevc code in the EVC DataFrame and returns the BCS category.
    Handles missing values, invalid data types, and placeholder values ('TBC').

    Args:
        bioevc: Combined bioregion/EVC code (e.g., 'VVP_0055')
        evc_df: DataFrame containing EVC data with BIOEVCCODE column

    Returns:
        str: BCS value ('E', 'V', 'D', 'R', 'N', 'LC') or 'LC' if not found/invalid
             For values other than 'LC', returns only the first character.
    """
    try:
        bcs_value = evc_df[evc_df['BIOEVCCODE'].str.contains(bioevc)]['BCS_CATEGORY'].iloc[0]
    except IndexError:
        logging.warning(f"BCS value not found for {bioevc}, defaulting to 'LC'")
        return 'LC'

    # Handle invalid BCS values (not string, empty, or 'TBC')
    if not isinstance(bcs_value, str) or not bcs_value.strip() or bcs_value.strip() == 'TBC':
        return 'LC'
    elif bcs_value != 'LC':
        return bcs_value[0]  # Return first character only
    return bcs_value


def move_column_to_end(gdf: gpd.GeoDataFrame, column: str) -> gpd.GeoDataFrame:
    """Move specified column to end of DataFrame.

    Args:
        gdf: GeoDataFrame to reorder
        column: Name of column to move to end

    Returns:
        GeoDataFrame with column moved to end

    Raises:
        KeyError: If column does not exist in GeoDataFrame
    """
    if column not in gdf.columns:
        raise KeyError(f"Column '{column}' not found in GeoDataFrame")
    cols = [c for c in gdf.columns if c != column] + [column]
    return gdf[cols]


def get_attribute(config: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Get value from attribute_table config section.

    Args:
        config: Full configuration dictionary
        key: Key to retrieve from attribute_table
        default: Default value if key not found

    Returns:
        Value from config or default
    """
    return config.get('attribute_table', {}).get(key, default)


def process_nvrmap_rows(row: pd.Series,
                        view_pfi_list: List[str],
                        count: List[int]
                        ) -> Tuple[int, str, str]:
    """Generate site_id, zone_id, and veg_codes for a row."""
    si = calculate_site_id(view_pfi_list, row['view_pfi'])
    count[si - 1] += 1
    # Change the Zone ID from an integer to alpha
    zi = generate_zone_id(count, si)
    bioevc = format_bioevc(row['bioregcode'], row['evc'])
    return si, zi, bioevc


# Define the function to generate ensym data
def process_ensym_rows(row: pd.Series,
                       evc_df:pd.DataFrame,
                       view_pfi_list: List[str],
                       count: List[int]
                       ) -> Tuple[int, str, str, str]:
    """Create the `HH_EVC` values from bioregcod and evc, with padding"""
    bioevc = format_bioevc(row['bioregcode'], row['evc'])
    bcs_value = lookup_bcs_value(bioevc, evc_df)

    # Set the correct Site ID if there are multiple parcels
    si = calculate_site_id(view_pfi_list, row['view_pfi'])

    # Update the count list
    count[si - 1] += 1

    # Change the Zone ID from an integer to alpha
    zi = generate_zone_id(count, si)

    return si, zi, bioevc, bcs_value

def build_ensym_gdf(input_gdf: gpd.GeoDataFrame,
                    evc_df: pd.DataFrame,
                    view_pfi_list: List[str],
                    config: Dict[str, Any],
                    args: argparse.Namespace
                    ) -> gpd.GeoDataFrame:
    """Build the final GeoDataFrame for EnSym output."""
    count = [0] * len(view_pfi_list)
    ensym_gdf = input_gdf.loc[:, ['geom', 'bioregcode', 'evc', 'view_pfi']]
    ensym_gdf['HH_PAI'] = get_attribute(config, 'project')
    ensym_gdf['HH_D'] = datetime.today().strftime("%Y-%m-%d")
    ensym_gdf['HH_CP'] = get_attribute(config, 'collector')
    ensym_gdf['HH_SI'] = 1
    ensym_gdf['HH_ZI'] = ensym_gdf.index + 1
    ensym_gdf['HH_VAC'] = "P"
    # Apply the function to each row
    ensym_gdf[['HH_SI', 'HH_ZI', 'HH_EVC', 'BCS']] = \
        ensym_gdf.apply(lambda row: process_ensym_rows(row, evc_df, view_pfi_list, count), axis=1, result_type="expand")
    ensym_gdf['LT_CNT'] = 0
    ensym_gdf['HH_H_S'] = get_attribute(config, 'default_habitat_score')
    # Set the gainscore if specified, otherwise default to 0.22
    if args.gainscore:
        ensym_gdf['G_S'] = args.gainscore
    else:
        ensym_gdf['G_S'] = get_attribute(config, 'default_gain_score')
    # Calculate the area in hectares
    ensym_gdf['HH_A'] = ensym_gdf['geom'].area / SQ_METERS_PER_HECTARE
    # Drop the extra columns
    ensym_gdf = ensym_gdf.drop(['bioregcode', 'evc', 'view_pfi'], axis=1)
    # Move 'geom' column to end
    ensym_gdf = move_column_to_end(ensym_gdf, 'geom')

    # Change columns for 2013 Ensym
    if args.sbeu:
        logging.info('Changing to EnSym 2013 format.')
        ensym_gdf = ensym_gdf.drop(['HH_EVC', 'BCS', 'LT_CNT'], axis=1)
        ensym_gdf = ensym_gdf.rename(columns={'G_S': 'G_HA'})
        ensym_gdf = ensym_gdf[['HH_PAI', 'HH_SI', 'HH_ZI', 'HH_VAC', 'HH_CP', 
                            'HH_D', 'HH_H_S', 'G_HA', 'HH_A', 'geom']]

    return ensym_gdf

def build_nvrmap_gdf(input_gdf: gpd.GeoDataFrame,
                     view_pfi_list: List[str],
                     config: Dict[str, Any],
                     args: argparse.Namespace
                     ) -> gpd.GeoDataFrame:
    """Build the final GeoDataFrame for NVRMap output."""
    count = [0] * len(view_pfi_list)
    gdf = input_gdf.loc[:, ['geom', 'bioregcode', 'evc', 'view_pfi']]
    gdf['site_id'] = 1
    gdf['zone_id'] = gdf.index + 1
    gdf['prop_id'] = get_attribute(config, 'project')
    gdf['vlot'] = 0
    gdf['lot'] = 0
    gdf['recruits'] = 0
    gdf['type'] = "p"
    gdf['cp'] = get_attribute(config, 'collector')
    gdf[['site_id', 'zone_id', 'veg_codes']] = gdf.apply(
        lambda row: process_nvrmap_rows(row, view_pfi_list, count), axis=1, result_type="expand"
    )
    gdf['lt_count'] = 0
    gdf['cond_score'] = get_attribute(config, 'default_habitat_score')
    gdf['gain_score'] = (args.gainscore
                         if args.gainscore
                         else get_attribute(config, 'default_gain_score')
                         )
    gdf['surv_date'] = datetime.today().strftime('%Y%m%d')
    gdf = gdf.drop(['bioregcode', 'evc', 'view_pfi'], axis=1)
    # Arrange the columns to the schema specification
    gdf = gdf[['site_id', 'zone_id', 'prop_id', 'vlot', 'lot', 'recruits', 'type', 
               'cp', 'veg_codes', 'lt_count', 'cond_score', 'gain_score', 'surv_date', 'geom']]
    logging.info(f'NVRMAP Dataframe: \n\n {gdf}')
    return gdf


def select_output_gdf(args: argparse.Namespace,
                      input_gdf: gpd.GeoDataFrame,
                      evc_df: pd.DataFrame,
                      view_pfis: List,
                      config: Dict[str, Any],
                      ) -> gpd.GeoDataFrame:
    """Select the correct output format from options"""
    if args.sbeu or args.ensym:
        logging.info('EnSym output format selected')
        output_gdf = build_ensym_gdf(input_gdf, evc_df, view_pfis, config, args)
    else:
        logging.info('NVRMap output selected')
        output_gdf = build_nvrmap_gdf(input_gdf, view_pfis, config, args)
    
    return output_gdf

def write_gdf(output_gdf: gpd.GeoDataFrame,
              args: argparse.Namespace
              ) -> None:
    """Write GeoDataFrame to shapefile with appropriate schema.

    Args:
        output_gdf: GeoDataFrame to write
        args: Command-line arguments containing shapefile path and format flags

    Raises:
        IOError: If unable to write shapefile to specified path
        OSError: If file system error occurs during write
    """
    logging.info("Final DataFrame:\n\n %s", output_gdf)
    logging.info(f'Current columns: {output_gdf.columns.tolist()}')
    logging.info("Writing shapefile: %s", args.shapefile)

    # Set the appropriate schema
    if args.sbeu:
        schema = ENSYM_2013_SCHEMA
    elif args.ensym:
        schema = ENSYM_2017_SCHEMA
    else:
        schema = NVRMAP_SCHEMA

    try:
        output_gdf.to_file(args.shapefile, schema=schema, engine='fiona')
    except (IOError, OSError) as e:
        logging.error(f"Failed to write shapefile {args.shapefile}: {e}")
        raise


def main() -> None:
    """Main execution function."""
    args = parse_args()
    config = load_config()
    engine, tables = connect_db(config["db_connection"])
    view_pfis = process_view_pfis(args, engine, tables["parcel_property"], 
                                  tables["parcel_detail"], tables["property_detail"])
    query = build_query(tables["parcel_view"], tables["nv1750_evc"], tables["bioregions"], view_pfis)
    input_gdf = load_geo_dataframe(engine, query)
    evc_df = load_evc_data(config["evc_data"])
    output_gdf = select_output_gdf(args, input_gdf, evc_df, view_pfis, config)
    write_gdf(output_gdf, args)

if __name__ == "__main__":
    main()
