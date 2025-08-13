#!/usr/bin/env python

import os
import sys
import argparse
import json
from datetime import datetime
from sqlalchemy import create_engine, select, func, MetaData
from sqlalchemy.engine.url import URL
import pandas as pd
import geopandas as gpd
from geoalchemy2 import Geometry

# ---------------------------
# Load configuration
# ---------------------------
config_dir = os.environ.get("NVRMAP_CONFIG")
if not config_dir:
    print("Error: NVRMAP_CONFIG environment variable is not set.")
    sys.exit(1)

config_path = os.path.join(config_dir, "config.json")
if not os.path.exists(config_path):
    print(f"Error: Config file not found at {config_path}")
    sys.exit(1)

try:
    with open(config_path, "r") as f:
        config = json.load(f)
except json.JSONDecodeError as e:
    print(f"Error: Failed to parse {config_path}: {e}")
    sys.exit(1)

# Extract config
db_connection = config.get("db_connection", {})
evc_data_path = config.get("evc_data")
ensym_cfg = config.get("ensym", {})

project = ensym_cfg.get("project", "Python")
collector = ensym_cfg.get("collector", "Desktop")
default_gain_score = ensym_cfg.get("default_gain_score", 0.22)
default_habitat_score = ensym_cfg.get("default_habitat_score", 0.4)

required_db_keys = ["db_type", "username", "password", "host", "database"]
missing_db_keys = [k for k in required_db_keys if k not in db_connection]
if missing_db_keys:
    print(f"Error: Missing DB config keys in {config_path}: {missing_db_keys}")
    sys.exit(1)

if not evc_data_path or not os.path.exists(os.path.expanduser(evc_data_path)):
    print(f"Error: EVC data file not found: {evc_data_path}")
    sys.exit(1)

# ---------------------------
# CLI args
# ---------------------------
parser = argparse.ArgumentParser(description='Process View PFIs to an Ensym shapefile.')
parser.add_argument('view_pfi', metavar='N', type=int, nargs='+', help='PFI of the Parcel View')
parser.add_argument("-s", "--shapefile", default='ensym', help="Name of the shapefile/directory to write")
parser.add_argument("-g", "--gainscore", type=float, help="Override gainscore value")
args = parser.parse_args()

# ---------------------------
# Connect to DB
# ---------------------------
url_object = URL.create(
    db_connection['db_type'],
    username=db_connection['username'],
    password=db_connection['password'],
    host=db_connection['host'],
    database=db_connection['database']
)

engine = create_engine(url_object)
metadata = MetaData()

# Reflect only the tables we need
metadata.reflect(only=["parcel_view", "nv1750_evc", "bioregions"], bind=engine)

parcel_view = metadata.tables["parcel_view"]
nv1750_evc = metadata.tables["nv1750_evc"]
bioregions = metadata.tables["bioregions"]

# ---------------------------
# Build SQLAlchemy query
# ---------------------------
pfi_values = list(map(str, args.view_pfi))

# Inner "clipped" query
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

# Outer query with bioregions
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

# Final query
final_query = select(bio_clipped_subq).order_by(bio_clipped_subq.c.bioregcode)

# ---------------------------
# Load data into GeoDataFrame
# ---------------------------
count = [0] * len(args.view_pfi)
bioevc_gdf = gpd.GeoDataFrame.from_postgis(final_query, con=engine.connect(), geom_col="geom")

if bioevc_gdf.empty:
    print("No search results found. Please check your \"View PFI\" values")
    sys.exit()

bioevc_gdf = bioevc_gdf.set_crs('epsg:7899')

# ---------------------------
# Load EVC benchmark data
# ---------------------------
try:
    evc_df = pd.read_excel(os.path.expanduser(evc_data_path))
except FileNotFoundError as e:
    print("Excel file not found:", e)
    sys.exit()
except Exception as e:
    print("Error reading EVC data:", e)
    sys.exit()

# ---------------------------
# Helper function
# ---------------------------
def find_hh_evc(row):
    if len(str(row["bioregcode"])) <= 3:
        bioevc = row["bioregcode"] + "_" + str(int(row["evc"])).zfill(4)
    else:
        bioevc = row["bioregcode"] + str(int(row["evc"])).zfill(4)

    global count
    si = args.view_pfi.index(int(row['view_pfi'])) + 1 if len(args.view_pfi) > 1 else 1
    count[si - 1] += 1

    if count[si - 1] <= 26:
        zi = chr(ord('@') + count[si - 1])
    else:
        zi = chr(ord('@') + (count[si - 1]) - 26) + chr(ord('@') + (count[si - 1]) - 26)
    return si, zi, bioevc

# ---------------------------
# Build Ensym GeoDataFrame
# ---------------------------
ensym_gdf = bioevc_gdf.loc[:, ['geom', 'bioregcode', 'evc', 'view_pfi']]
ensym_gdf['site_id'] = 1
ensym_gdf['zone_id'] = ensym_gdf.index + 1
ensym_gdf['prop_id'] = project
ensym_gdf['vlot'] = 0
ensym_gdf['lot'] = 0
ensym_gdf['recruits'] = 0
ensym_gdf['type'] = "p"
ensym_gdf['cp'] = collector

ensym_gdf[['site_id', 'zone_id', 'veg_codes']] = \
    ensym_gdf.apply(lambda row: find_hh_evc(row), axis=1, result_type="expand")

ensym_gdf['lt_count'] = 0
ensym_gdf['cond_score'] = default_habitat_score
ensym_gdf['gain_score'] = args.gainscore if args.gainscore else default_gain_score
ensym_gdf['surv_date'] = datetime.today().strftime('%Y%m%d')

ensym_gdf = ensym_gdf.drop(['bioregcode', 'evc', 'view_pfi'], axis=1)
cols = ensym_gdf.columns.tolist()
cols = cols[+1:] + cols[:+1]
ensym_gdf = ensym_gdf[cols]

print("=====Final Dataframe====\n\n", ensym_gdf)

# ---------------------------
# Write output
# ---------------------------
print("\n\nWriting shapefile:", args.shapefile)
ensym_gdf.to_file(args.shapefile)
