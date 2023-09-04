# db-ensym
Use PostGIS database with VicData shapefiles to generate Ensym compatible shapefiles

**Requires a PostGIS instance**
Need to create a docker image with the required database components
All files are available from data.vic.gov.au

**Create a database configuration file**
{
    "db_connection": {
        "db_type": "postgresql+psycopg2",
        "username": "username",
        "password": "password",
        "host": "host",
        "database": "database"
    }
}
