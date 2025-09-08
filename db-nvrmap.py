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
from sqlalchemy import create_engine, select, func, MetaData
from sqlalchemy.engine.url import URL
from geoalchemy2 import Geometry

# Constants
DEFAULT_CRS = 'epsg:7899'
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

def load_config() -> Dict[str, Any]:
    """Load configuration from the NVRMAP_CONFIG environment variable."""
    config_dir = os.environ.get("NVRMAP_CONFIG")
    if not config_dir:
        raise EnvironmentError("NVRMAP_CONFIG environment variable is not set.")
    
    config_path = Path(config_dir) / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    with config_path.open("r") as f:
        return json.load(f)

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
    """Connect to the database and reflect required tables."""
    required_keys = ["db_type", "username", "password", "host", "database"]
    missing_keys = [k for k in required_keys if k not in db_config]
    if missing_keys:
        raise KeyError(f"Missing DB config keys: {missing_keys}")
    
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
    """Construct SQL query for spatial data extraction."""
    clipped_geom = func.ST_Dump(
        func.ST_Intersection(
            func.ST_Buffer(parcel_view.c.geom, -6),
            nv1750_evc.c.geom
        )
    ).geom

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

    outer_geom = func.ST_Dump(func.ST_Intersection(clipped_subq.c.geom, bioregions.c.geom)).geom

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
    """Generate the Zone IDs by changing the number to characters"""
    if count[si - 1] <= 26:
        return chr(ord('@') + count[si - 1])
    else:
        return chr(ord('@') + count[si - 1] - 26) * 2


def process_nvrmap_rows(row: pd.Series, 
                        view_pfi_list: List[int], 
                        count: List[int]
                        ) -> Tuple[int, str, str]:
    """Generate site_id, zone_id, and veg_codes for a row."""
    si = (view_pfi_list.index(row['view_pfi']) 
          + 1 if len(view_pfi_list) > 1 else 1)
    count[si - 1] += 1
    ## Change the Zone ID from an integer to alpha
    zi = generate_zone_id(count, si)
    bioevc = (f"{row['bioregcode']}_{str(int(row['evc'])).zfill(4)}" 
              if len(str(row["bioregcode"])) <= 3 
              else f"{row['bioregcode']}{str(int(row['evc'])).zfill(4)}")
    return si, zi, bioevc


# Define the function to generate ensym data
def process_ensym_rows(row: pd.Series, 
                       evc_df:pd.DataFrame, 
                       view_pfi_list: List[int], 
                       count: List[int]
                       ) -> Tuple[int, str, str, str]:
    """Create the `HH_EVC` values from bioregcod and evc, with padding"""
    bioevc = ""
    if len(str(row["bioregcode"])) <= 3:
        bioevc = row["bioregcode"] + "_" + str(int(row["evc"])).zfill(4)
    if len(str(row["bioregcode"])) == 4:
        bioevc = row["bioregcode"] + str(int(row["evc"])).zfill(4)
    # Search for `bioevc` as sometimes there are sub evcs. Choose the 1st one
    try:
        bcs_value = evc_df[evc_df['BIOEVCCODE'].str.contains(
            bioevc)].iloc[0, 5]
    except IndexError:
        bcs_value = 'LC'

    # Step through conditions as the BCS value isn't present for mosaic EVCs,
    # sub EVCs (like VVP_0055_61) or they are TBC

    if not isinstance(bcs_value, str) or not bcs_value or bcs_value == 'TBC':
        bcs_value = 'LC'
    elif bcs_value != 'LC':
        bcs_value = bcs_value[0]
    
    # Set the correct Site ID if there are multiple parcels
    ## Find the index of the current 'row['view_pfi']' within 'view_pfi_list'
    si = (view_pfi_list.index(row['view_pfi'])
          + 1 if len(view_pfi_list) > 1 else 1)

    ## Update the count list
    count[si - 1] += 1

    ## Change the Zone ID from an integer to alpha
    zi = generate_zone_id(count, si)

    return si, zi, bioevc, bcs_value

def build_ensym_gdf(input_gdf: gpd.GeoDataFrame, 
                    evc_df: pd.DataFrame, 
                    view_pfi_list: List[int], 
                    config: Dict[str, Any], 
                    args: argparse.Namespace
                    ) -> gpd.GeoDataFrame:
    """Build the final GeoDataFrame for EnSym output."""
    count = [0] * len(view_pfi_list)
    ensym_gdf = input_gdf.loc[:, ['geom', 'bioregcode', 'evc', 'view_pfi']]
    ensym_gdf['HH_PAI'] = config['attribute_table'].get('project')
    ensym_gdf['HH_D'] = datetime.today().strftime("%Y-%m-%d")
    ensym_gdf['HH_CP'] = config['attribute_table'].get('collector')
    ensym_gdf['HH_SI'] = 1
    ensym_gdf['HH_ZI'] = ensym_gdf.index + 1
    ensym_gdf['HH_VAC'] = "P"
    # Apply the function to each row
    ensym_gdf[['HH_SI', 'HH_ZI', 'HH_EVC', 'BCS']] = \
        ensym_gdf.apply(lambda row: process_ensym_rows(row, evc_df, view_pfi_list, count), axis=1, result_type="expand")
    ensym_gdf['LT_CNT'] = 0
    ensym_gdf['HH_H_S'] = config['attribute_table'].get('default_habitat_score')
    # Set the gainscore if specified, otherwise default to 0.22
    if args.gainscore:
        ensym_gdf['G_S'] = args.gainscore
    else:
        ensym_gdf['G_S'] = config['attribute_table'].get('default_gain_score')
    # Calculate the area in hecatres
    ensym_gdf['HH_A'] = ensym_gdf['geom'].area / 10000
    # Drop the extra columns
    ensym_gdf = ensym_gdf.drop(['bioregcode', 'evc', 'view_pfi'], axis=1)
    # Sort the columns to put 'geom' last
    cols = ensym_gdf.columns.tolist()
    cols = cols[+1:] + cols[:+1]
    # Apply the new column order
    ensym_gdf = ensym_gdf[cols]

    # Change columns for 2013 Ensym
    if args.sbeu:
        logging.info('Changing to EnSym 2013 format.')
        ensym_gdf = ensym_gdf.drop(['HH_EVC', 'BCS', 'LT_CNT'], axis=1)
        ensym_gdf = ensym_gdf.rename(columns={'G_S': 'G_HA'})
        ensym_gdf = ensym_gdf[['HH_PAI', 'HH_SI', 'HH_ZI', 'HH_VAC', 'HH_CP', 
                            'HH_D', 'HH_H_S', 'G_HA', 'HH_A', 'geom']]

    return ensym_gdf

def build_nvrmap_gdf(input_gdf: gpd.GeoDataFrame, 
                     view_pfi_list: List[int], 
                     config: Dict[str, Any], 
                     args: argparse.Namespace
                     ) -> gpd.GeoDataFrame:
    """Build the final GeoDataFrame for NVRMap output."""
    count = [0] * len(view_pfi_list)
    gdf = input_gdf.loc[:, ['geom', 'bioregcode', 'evc', 'view_pfi']]
    gdf['site_id'] = 1
    gdf['zone_id'] = gdf.index + 1
    gdf['prop_id'] = config['attribute_table'].get('project')
    gdf['vlot'] = 0
    gdf['lot'] = 0
    gdf['recruits'] = 0
    gdf['type'] = "p"
    gdf['cp'] = config['attribute_table'].get('collector')
    gdf[['site_id', 'zone_id', 'veg_codes']] = gdf.apply(
        lambda row: process_nvrmap_rows(row, view_pfi_list, count), axis=1, result_type="expand"
    )
    gdf['lt_count'] = 0
    gdf['cond_score'] = config['attribute_table'].get('default_habitat_score')
    gdf['gain_score'] = (args.gainscore 
                         if args.gainscore 
                         else config['attribute_table'].get('default_gain_score')
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
    except Exception as e:
        print(f"Failed to write to {args.shapefile}: {e}")


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
