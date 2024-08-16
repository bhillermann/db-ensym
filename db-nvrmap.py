#!/usr/bin/env python

from sqlalchemy import create_engine, text, URL
from datetime import datetime
import pandas as pd
import geopandas as gpd
import argparse
import json

# Define constants
db_config = ('/home/bhillermann/Documents/Development/Python/db-ensym/'
             'db_config.json')

# Define the Excel file to import
evc_data = ('/home/bhillermann/Documents/GIS/Ensym/'
            'EVC benchmark data - external use.xlsx')

# define the Ensym constants
project = "Python"
collector = "Desktop"
default_gain_score = 0.22
default_habitat_score = 0.4

# Call argparse and define the arguments
parser = argparse.ArgumentParser(description=r'Process View PFIs to an Ensym'
                                 r' shapefile.')
parser.add_argument('view_pfi', metavar='N', type=int, nargs='+',
                    help='PFI of the Parcel View')
parser.add_argument("-s", "--shapefile", default='ensym',
                    help="Name of the shapefile to write to.\n If no extention\
                    is specified then shapefiles will be written to a \
                    directory instead. If not used, the default folder \
                    \"ensym\" will be used")
parser.add_argument("-g", "--gainscore", type=float,
                    help="Set the value of the gainscore. Default is \"0.22\"")

args = parser.parse_args()


# Load the configuration from the JSON file
with open(db_config, 'r') as config_file:
    config = json.load(config_file)

db_connection = config.get('db_connection')

# Connect to your postgres DB
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
            (ST_Dump(ST_Intersection(ST_Buffer(ST_Transform(parcel.geom, 3111), -6),\
                  evc.geom))).geom geom
        FROM parcel_view parcel
        INNER JOIN nv1750_evc_gda94 evc
        ON ST_Intersects(ST_Transform(parcel.geom, 3111), evc.geom)
        WHERE parcel.pfi IN ({', '.join([f"'{pfi}'" for pfi in args.view_pfi])})
    ) AS clipped
    INNER JOIN bioregions_gda94 bio
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
    exit()

# Set the CRS for the GeoDataFrame
bioevc_gdf = bioevc_gdf.set_crs('epsg:3111')
bioevc_gdf = bioevc_gdf.to_crs('epsg:7899')

# Import the EVC benchmark data with pandas
# Open the Excel file. Quit if not FileNotFoundError
try:
    evc_df = pd.read_excel(evc_data)
except FileNotFoundError as e:
    print("Excel file not found: ", e)
    exit()


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
ensym_gdf.to_file(args.shapefile)
