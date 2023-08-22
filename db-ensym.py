#!/usr/bin/env python

from sqlalchemy import create_engine, text
from datetime import date
import pandas as pd
import geopandas as gpd
import argparse

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


# define the Ensym constants
project = "Python"
collector = "Desktop"
# declare the variable for the SQL substitution
sub_string = ''


# Connect to your postgres DB
# conn = psycopg2.connect(dbname="gisdb", user="gisuser", password="#!gis^")
engine = create_engine('postgresql://gisuser:#!gis^@'
                       'Brendon-Lenovo.local/gisdb'\
                        pool_size=20, max_overflow=20)


# Define the SQL query
sql_query = """
select bio_clipped.* from
(
select clipped.evc, clipped.x_evcname, clipped.view_pfi, bio.bioregcode,
   bio.bioregion, (st_dump(st_intersection(clipped.geom, bio.geom))).geom geom
   from (
       select evc.evc, evc.x_evcname, parcel.pfi as view_pfi,
       (st_dump(st_intersection(st_buffer(parcel.geom,-6),
           evc.geom))).geom geom
       from parcel_view_gda94 parcel
           inner join nv1750_evc_gda94 evc
               on st_intersects(parcel.geom, evc.geom)
                   where
                       ==SUBS_TXT==
    ) as clipped
    inner join bioregions_gda94 bio
    on st_intersects(clipped.geom, bio.geom)
    where ST_Dimension(clipped.geom) = 2
) as bio_clipped order by bioregcode
"""

# Process the arguments into the correct SQL `WHERE` statements
if args.view_pfi[0]:  # First statment doesn't start with OR
    sub_string += "parcel.pfi = \'" + str(args.view_pfi[0]) + "\'"
if len(args.view_pfi) > 1:  # Needs an OR from second statement on
    for i in args.view_pfi[1:]:
        sub_string += "\n or \nparcel.pfi = \'" + str(i) + "\'\n"

# define a counter for the different PFIs so we can assign proper zone IDs
count = [0] * len(args.view_pfi)

# Retrieve query resuls as GeoDataFrame substituting the sql query with
# view_pfi arguments we collected
sql_query = sql_query.replace("==SUBS_TXT==", sub_string)
bioevc_gdf = gpd.GeoDataFrame.from_postgis(sql=text(sql_query),\
                                           con=engine.connect())
if bioevc_gdf.empty:
    print("No search results found. Please check your \"View PFI\" values")
    exit()

# Set the CRS for the GeoDataFrame
bioevc_gdf = bioevc_gdf.set_crs('epsg:3111')


# Import the EVC benchmark data with pandas
# Define the Excel file to import
evc_data = (r"~/Documents/GIS/Ensym/"
            r"EVC benchmark data - external use.xlsx")
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
    # Search for `bioevc` as sometimes there are sub evcs. Choose the 1st one
    try:
        bcs_value = evc_df[evc_df['BIOEVCCODE'].str.contains(
            bioevc)].iloc[0, 5]
    except IndexError:
        bcs_value = 'LC'
    # Step through conditions as the BCS value isn't present for mosaic EVCs,
    # sub EVCs (like VVP_0055_61) or they are TBC
    if not bcs_value:
        bcs_value = 'LC'
    elif bcs_value == 'TBC':
        bcs_value = 'LC'
    elif bcs_value != 'LC':  # All but LC are only 1 letter codes
        bcs_value = bcs_value[0]
    # Set the correct Site ID if there are multiple parcels
    global count
    if len(args.view_pfi) > 1:
        for id in range(len(args.view_pfi)):
            if int(args.view_pfi[id]) == int(row['view_pfi']):
                si = (id + 1)
                count[id] = count[id] + 1
    else:
        si = 1
        count[si - 1] = count[si - 1] + 1
    # Change the Zone ID from an integer to alpha
    if count[si - 1] <= 26:
        zi = chr(ord('@') + count[si - 1])
    else:
        zi = chr(ord('@') + (count[si - 1]) - 26)\
            + chr(ord('@') + (count[si - 1]) - 26)
    return si, zi, bioevc, bcs_value


# Create a new dataframe from evc_df and bioevc_gdf
ensym_gdf = bioevc_gdf.loc[:, ['geom', 'bioregcode', 'evc', 'view_pfi']]
ensym_gdf['HH_PAI'] = project
ensym_gdf['HH_D'] = date.today()
ensym_gdf['HH_CP'] = collector
ensym_gdf['HH_SI'] = 1
ensym_gdf['HH_ZI'] = ensym_gdf.index + 1
ensym_gdf['HH_VAC'] = "P"
# Apply the function to each row
ensym_gdf[['HH_SI', 'HH_ZI', 'HH_EVC', 'BCS']] = \
    ensym_gdf.apply(lambda row: find_hh_evc(row), axis=1, result_type="expand")
ensym_gdf['LT_CNT'] = 0
ensym_gdf['HH_H_S'] = 0.4
# Set the gainscore if specified, otherwise default to 0.22
if args.gainscore:
    ensym_gdf['G_S'] = args.gainscore
else:
    ensym_gdf['G_S'] = 0.22
# Calculate the area in hecatres
ensym_gdf['HH_A'] = ensym_gdf['geom'].area / 10000
# Drop the extra columns
ensym_gdf = ensym_gdf.drop(['bioregcode', 'evc', 'view_pfi'], axis=1)
# Sort the columns to put 'geom' last
cols = ensym_gdf.columns.tolist()
cols = cols[+1:] + cols[:+1]
# Apply the new column order
ensym_gdf = ensym_gdf[cols]


schema = gpd.io.file.infer_schema(ensym_gdf)
schema['properties']['HH_D'] = 'date'
schema['properties']['HH_ZI'] = 'str'

print("=====Final Dataframe====\n\n", ensym_gdf)

# Write the Ensym shapefile
print("\n\nWriting shapefile:", args.shapefile)
ensym_gdf.to_file(args.shapefile, schema=schema)
