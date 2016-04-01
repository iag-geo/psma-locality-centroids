# psma-locality-centroids
A postgres/postgis script to create centroids for PSMA localities which are based on actual population concentration, rather than geometric centroids. Uses new open PSMA GNAF and PSMA Administrative Boundaries datasets. Assumes you are using the GNAF Loader and Cleaned Admin Boundaries

## locality-clean

This process takes ~30-45 mins.

![image1.png](https://github.com/iag-geo/psma-locality-centroids/blob/master/image1.png "original vs new centroids")

### Important

The cleaned localities are not well suited to data processing as they have been deliberately thinned to improve display performance.

A better dataset for processing is the admin_bdys.locality_bdy_analysis table that gets created in the [gnaf-loader](https://github.com/minus34/gnaf-loader) process

### I Just Want the Data!

You can run the script to get the result or just download the data from here:
- [Shapefile](https://github.com/iag-geo/psma-admin-bdys/releases/download/v1.0/locality_bdys_display_shapefile.zip) (~40Mb) 
- [GeoJSON](https://github.com/iag-geo/psma-admin-bdys/releases/download/v1.0/locality_bdys_display_geojson.zip) (~25Mb) 

#### Data License

Incorporates or developed using Administrative Boundaries ©PSMA Australia Limited licensed by the Commonwealth of Australia under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

### Script Pre-requisites

- You will need to run the [gnaf-loader](https://github.com/minus34/gnaf-loader) script to load the required Admin Bdy tables into Postgres
- Postgres 9.x (tested on 9.3, 9.4 & 9.5 on Windows and 9.5 on OSX)
- PostGIS 2.1+
- Python 2.7 with Psycopg2 2.6

### Missing localities
Trimming the boundaries to the coastline removes a small number of bay or estuary based localities.  These have very few G-NAF addresses.
