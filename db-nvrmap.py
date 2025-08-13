#!/usr/bin/env python

import os
import sys 
import argparse
import json
from datetime import datetime

from sqlalchemy import create_engine, text, URL
import pandas as pd
import geopandas as gpd


# Load config directory from env var
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

# Extract sections
db_connection = config.get("db_connection", {})
evc_data_path = config.get("evc_data")
ensym_cfg = config.get("ensym", {})

# Ensym constants
project = ensym_cfg.get("project", "Python")
collector = ensym_cfg.get("collector", "Desktop")
default_gain_score = ensym_cfg.get("default_gain_score", 0.22)
default_habitat_score = ensym_cfg.get("default_habitat_score", 0.4)

# Validate required config
required_db_keys = ["db_type", "username", "password", "host", "database"]
missing_db_keys = [k for k in required_db_keys if k not in db_connection]
if missing_db_keys:
    print(f"Error: Missing DB config keys in {config_path}: {missing_db_keys}")
    sys.exit(1)

if not evc_data_path or not os.path.exists(os.path.expanduser(evc_data_path)):
    print(f"Error: EVC data file not found: {evc_data_path}")
    sys.exit(1)

# CLI args
parser = argparse.ArgumentParser(description='Process View PFIs to an Ensym shapefile.')
parser.add_argument('view_pfi', metavar='N', type=int, nargs='+', help='PFI of the Parcel View')
parser.add_argument("-s", "--shapefile", default='ensym', help="Name of shapefile/directory to write")
parser.add_argument("-g", "--gainscore", type=float, help="Override gainscore value")
args = parser.parse_args()

# DB connection
url_object = URL.create(
    db_connection['db_type'],
    username=db_connection['username'],
    password=db_connection['password'],
    host=db_connection['host'],
    database=db_connection['database']
)

engine = create_engine(url_object)

# Define the SQL query
sql_query = f"""
SELECT bio_clipped.*
FROM (
    SELECT clipped.evc, clipped.x_evcname, clipped.view_pfi, bio.bioregcode,
        bio.bioregion, (ST_Dump(ST_Intersection(clipped.geom, bio.geom))).geom\
              geom
    FROM (
        SELECT evc.evc, evc.x_evcname, parcel.pfi AS view_pfi,
            (ST_Dump(ST_Intersection(ST_Buffer(parcel.geom, -6),\
                  evc.geom))).geom geom
        FROM parcel_view parcel
        INNER JOIN nv1750_evc evc
        ON ST_Intersects(parcel.geom, evc.geom)
        WHERE parcel.pfi IN ({', '.join([f"'{pfi}'" for pfi in args.view_pfi])})
    ) AS clipped
    INNER JOIN bioregions bio
    ON ST_Intersects(clipped.geom, bio.geom)
    WHERE ST_Dimension(clipped.geom) = 2
) AS bio_clipped
ORDER BY bioregcode
"""

# define a counter for the different PFIs so we can assign proper zone IDs
count = [0] * len(args.view_pfi)

# Retrieve query resuls as GeoDataFrame substituting the sql query with
# view_pfi arguments we collected
bioevc_gdf = gpd.GeoDataFrame.from_postgis(sql=text(sql_query),\
                                           con=engine.connect())
if bioevc_gdf.empty:
    print("No search results found. Please check your \"View PFI\" values")
    sys.exit()

# Set the CRS for the GeoDataFrame
bioevc_gdf = bioevc_gdf.set_crs('epsg:7899')

# Import the EVC benchmark data with pandas
# Open the Excel file. Quit if not FileNotFoundError
try:
    evc_df = pd.read_excel(evc_data_path)
except FileNotFoundError as e:
    print("Excel file not found: ", e)
    sys.exit()


# Define the function to generate ensym data
def find_hh_evc(row):
    # Create the `HH_EVC` values from bioregcod and evc, with padding
    if len(str(row["bioregcode"])) <= 3:
        bioevc = row["bioregcode"] + "_" + str(int(row["evc"])).zfill(4)
    if len(str(row["bioregcode"])) == 4:
        bioevc = row["bioregcode"] + str(int(row["evc"])).zfill(4)
   
    # Set the correct Site ID if there are multiple parcels
    global count

    # Find the index of the current 'row['view_pfi']' within 'args.view_pfi'
    si = args.view_pfi.index(int(row['view_pfi']))\
          + 1 if len(args.view_pfi) > 1 else 1

    # Update the count list
    count[si - 1] += 1

    # Change the Zone ID from an integer to alpha
    if count[si - 1] <= 26:
        zi = chr(ord('@') + count[si - 1])
    else:
        zi = chr(ord('@') + (count[si - 1]) - 26)\
            + chr(ord('@') + (count[si - 1]) - 26)
    return si, zi, bioevc


# Create a new dataframe from evc_df and bioevc_gdf
ensym_gdf = bioevc_gdf.loc[:, ['geom', 'bioregcode', 'evc', 'view_pfi']]
ensym_gdf['site_id'] = 1
ensym_gdf['zone_id'] = ensym_gdf.index + 1
ensym_gdf['prop_id'] = 'python'
ensym_gdf['vlot'] = 0
ensym_gdf['lot'] = 0
ensym_gdf['recruits'] = 0
ensym_gdf['type'] = "p"
ensym_gdf['cp'] = collector

# Apply the function to each row
ensym_gdf[['site_id', 'zone_id', 'veg_codes']] = \
    ensym_gdf.apply(lambda row: find_hh_evc(row), axis=1, result_type="expand")

ensym_gdf['lt_count'] = 0
ensym_gdf['cond_score'] = default_habitat_score
# Set the gainscore if specified, otherwise default to 0.22
if args.gainscore:
    ensym_gdf['gain_score'] = args.gainscore
else:
    ensym_gdf['gain_score'] = default_gain_score
# Calculate the area in hecatres
ensym_gdf['surv_date'] = datetime.today().strftime('%Y%m%d')

# Drop the extra columns
ensym_gdf = ensym_gdf.drop(['bioregcode', 'evc', 'view_pfi'], axis=1)
# Sort the columns to put 'geom' last
cols = ensym_gdf.columns.tolist()
cols = cols[+1:] + cols[:+1]
# Apply the new column order
ensym_gdf = ensym_gdf[cols]


# schema = gpd.io.file.infer_schema(ensym_gdf)
# schema['properties']['surv_date'] = 'date'
# schema['properties']['zone_id'] = 'str'

print("=====Final Dataframe====\n\n", ensym_gdf)

# Write the Ensym shapefile
print("\n\nWriting shapefile:", args.shapefile)
#ensym_gdf.to_file(args.shapefile, schema=schema)
try:
    ensym_gdf.to_file(args.shapefile)
except Exception as e:
    print("Shapefile not writable: ", e)
    sys.exit()    
