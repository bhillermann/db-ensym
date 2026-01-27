"""Core business logic for db-nvrmap shapefile generation."""

import os
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Dict, Any, Optional
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


class OutputFormat(Enum):
    """Output format for shapefile generation."""
    NVRMAP = "nvrmap"
    ENSYM_2017 = "ensym_2017"
    ENSYM_2013 = "ensym_2013"


@dataclass
class ProcessingOptions:
    """Options for shapefile processing."""
    view_pfi: List[int]
    shapefile: str = "nvrmap"
    gainscore: Optional[float] = None
    property_view: bool = False
    output_format: OutputFormat = OutputFormat.NVRMAP

    @property
    def ensym(self) -> bool:
        """Check if output format is EnSym 2017."""
        return self.output_format == OutputFormat.ENSYM_2017

    @property
    def sbeu(self) -> bool:
        """Check if output format is EnSym 2013 SBEU."""
        return self.output_format == OutputFormat.ENSYM_2013


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


def process_view_pfis(opts: ProcessingOptions, engine: Any, parcel_property, parcel_detail, property_detail) -> List:
    """Convert parcel view pfis to list of strings or convert property view pfis to parcels pfis."""
    if opts.property_view:
        # Step 1: CTE for property_pfi
        property_pfi_cte = (
            select(property_detail.c.pfi.label('pr_pfi'))
            .where(property_detail.c.view_pfi.in_(list(map(str, opts.view_pfi))))
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
        return list(map(str, opts.view_pfi))


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
    """Generate the Zone IDs by changing the number to characters."""
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
    zi = generate_zone_id(count, si)
    bioevc = (f"{row['bioregcode']}_{str(int(row['evc'])).zfill(4)}"
              if len(str(row["bioregcode"])) <= 3
              else f"{row['bioregcode']}{str(int(row['evc'])).zfill(4)}")
    return si, zi, bioevc


def process_ensym_rows(row: pd.Series,
                       evc_df: pd.DataFrame,
                       view_pfi_list: List[int],
                       count: List[int]
                       ) -> Tuple[int, str, str, str]:
    """Create the `HH_EVC` values from bioregcod and evc, with padding."""
    bioevc = ""
    if len(str(row["bioregcode"])) <= 3:
        bioevc = row["bioregcode"] + "_" + str(int(row["evc"])).zfill(4)
    if len(str(row["bioregcode"])) == 4:
        bioevc = row["bioregcode"] + str(int(row["evc"])).zfill(4)

    try:
        bcs_value = evc_df[evc_df['BIOEVCCODE'].str.contains(
            bioevc)].iloc[0, 5]
    except IndexError:
        bcs_value = 'LC'

    if not isinstance(bcs_value, str) or not bcs_value or bcs_value == 'TBC':
        bcs_value = 'LC'
    elif bcs_value != 'LC':
        bcs_value = bcs_value[0]

    si = (view_pfi_list.index(row['view_pfi'])
          + 1 if len(view_pfi_list) > 1 else 1)
    count[si - 1] += 1
    zi = generate_zone_id(count, si)

    return si, zi, bioevc, bcs_value


def build_ensym_gdf(input_gdf: gpd.GeoDataFrame,
                    evc_df: pd.DataFrame,
                    view_pfi_list: List[int],
                    config: Dict[str, Any],
                    opts: ProcessingOptions
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

    ensym_gdf[['HH_SI', 'HH_ZI', 'HH_EVC', 'BCS']] = \
        ensym_gdf.apply(lambda row: process_ensym_rows(row, evc_df, view_pfi_list, count), axis=1, result_type="expand")
    ensym_gdf['LT_CNT'] = 0
    ensym_gdf['HH_H_S'] = config['attribute_table'].get('default_habitat_score')

    if opts.gainscore:
        ensym_gdf['G_S'] = opts.gainscore
    else:
        ensym_gdf['G_S'] = config['attribute_table'].get('default_gain_score')

    ensym_gdf['HH_A'] = ensym_gdf['geom'].area / 10000
    ensym_gdf = ensym_gdf.drop(['bioregcode', 'evc', 'view_pfi'], axis=1)

    cols = ensym_gdf.columns.tolist()
    cols = cols[+1:] + cols[:+1]
    ensym_gdf = ensym_gdf[cols]

    if opts.sbeu:
        logging.info('Changing to EnSym 2013 format.')
        ensym_gdf = ensym_gdf.drop(['HH_EVC', 'BCS', 'LT_CNT'], axis=1)
        ensym_gdf = ensym_gdf.rename(columns={'G_S': 'G_HA'})
        ensym_gdf = ensym_gdf[['HH_PAI', 'HH_SI', 'HH_ZI', 'HH_VAC', 'HH_CP',
                            'HH_D', 'HH_H_S', 'G_HA', 'HH_A', 'geom']]

    return ensym_gdf


def build_nvrmap_gdf(input_gdf: gpd.GeoDataFrame,
                     view_pfi_list: List[int],
                     config: Dict[str, Any],
                     opts: ProcessingOptions
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
    gdf['gain_score'] = (opts.gainscore
                         if opts.gainscore
                         else config['attribute_table'].get('default_gain_score')
                         )
    gdf['surv_date'] = datetime.today().strftime('%Y%m%d')
    gdf = gdf.drop(['bioregcode', 'evc', 'view_pfi'], axis=1)
    gdf = gdf[['site_id', 'zone_id', 'prop_id', 'vlot', 'lot', 'recruits', 'type',
               'cp', 'veg_codes', 'lt_count', 'cond_score', 'gain_score', 'surv_date', 'geom']]
    logging.info(f'NVRMAP Dataframe: \n\n {gdf}')
    return gdf


def select_output_gdf(opts: ProcessingOptions,
                      input_gdf: gpd.GeoDataFrame,
                      evc_df: pd.DataFrame,
                      view_pfis: List,
                      config: Dict[str, Any],
                      ) -> gpd.GeoDataFrame:
    """Select the correct output format from options."""
    if opts.sbeu or opts.ensym:
        logging.info('EnSym output format selected')
        output_gdf = build_ensym_gdf(input_gdf, evc_df, view_pfis, config, opts)
    else:
        logging.info('NVRMap output selected')
        output_gdf = build_nvrmap_gdf(input_gdf, view_pfis, config, opts)

    return output_gdf


def get_schema_for_format(output_format: OutputFormat) -> dict:
    """Get the appropriate schema for the output format."""
    if output_format == OutputFormat.ENSYM_2013:
        return ENSYM_2013_SCHEMA
    elif output_format == OutputFormat.ENSYM_2017:
        return ENSYM_2017_SCHEMA
    else:
        return NVRMAP_SCHEMA


def write_shapefile(output_gdf: gpd.GeoDataFrame,
                    output_format: OutputFormat,
                    path: str) -> None:
    """Write GeoDataFrame to shapefile with appropriate schema."""
    logging.info("Final DataFrame:\n\n %s", output_gdf)
    logging.info(f'Current columns: {output_gdf.columns.tolist()}')
    logging.info("Writing shapefile: %s", path)

    schema = get_schema_for_format(output_format)

    try:
        output_gdf.to_file(path, schema=schema, engine='fiona')
    except Exception as e:
        raise RuntimeError(f"Failed to write to {path}: {e}")


def generate_shapefile(opts: ProcessingOptions) -> gpd.GeoDataFrame:
    """
    Generate a shapefile from PFI values.

    This is the main orchestrator function that coordinates the entire
    shapefile generation process.

    Args:
        opts: ProcessingOptions containing all configuration for the operation.

    Returns:
        The generated GeoDataFrame (also writes to disk at opts.shapefile).
    """
    config = load_config()
    engine, tables = connect_db(config["db_connection"])
    view_pfis = process_view_pfis(opts, engine, tables["parcel_property"],
                                  tables["parcel_detail"], tables["property_detail"])
    query = build_query(tables["parcel_view"], tables["nv1750_evc"], tables["bioregions"], view_pfis)
    input_gdf = load_geo_dataframe(engine, query)
    evc_df = load_evc_data(config["evc_data"])
    output_gdf = select_output_gdf(opts, input_gdf, evc_df, view_pfis, config)
    write_shapefile(output_gdf, opts.output_format, opts.shapefile)
    return output_gdf


def generate_shapefile_to_gdf(opts: ProcessingOptions) -> gpd.GeoDataFrame:
    """
    Generate a GeoDataFrame from PFI values without writing to disk.

    This function is useful for the web interface where we want to
    generate the data in memory first.

    Args:
        opts: ProcessingOptions containing all configuration for the operation.

    Returns:
        The generated GeoDataFrame.
    """
    config = load_config()
    engine, tables = connect_db(config["db_connection"])
    view_pfis = process_view_pfis(opts, engine, tables["parcel_property"],
                                  tables["parcel_detail"], tables["property_detail"])
    query = build_query(tables["parcel_view"], tables["nv1750_evc"], tables["bioregions"], view_pfis)
    input_gdf = load_geo_dataframe(engine, query)
    evc_df = load_evc_data(config["evc_data"])
    return select_output_gdf(opts, input_gdf, evc_df, view_pfis, config)
