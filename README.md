# psma-locality-centroids
A postgres/postgis script to create centroids for PSMA localities which are based on actual population concentration, rather than geometric centroids. 

This process and resulting files are useful for web mapping applications for a quick rapid lookup set of usable starting points to centre your map on, or also as an alternative, updated reference set of coordinates for the locality centroids provided in GNAF

Uses new open PSMA GNAF and PSMA Administrative Boundaries datasets. Assumes you are using the GNAF Loader and Cleaned Admin Boundaries

## creation of centroids.

The basic principle I have employed here is that of clustering of sets of address points, the assignment of these clusters to their localities and then the identification of the largest cluster's centroid to act as the updated centroid coordinates.

This process takes ~30-45 mins.

### clusters visualised

![image3.png](https://github.com/iag-geo/psma-locality-centroids/blob/master/image3.png "visual example of the created clusters")

### original vs new centroids

![image1.png](https://github.com/iag-geo/psma-locality-centroids/blob/master/image1.png "original vs new centroids")

![image2.png](https://github.com/iag-geo/psma-locality-centroids/blob/master/image2.png "original vs new centroids")

### Important

This is never a perfect process! whilst every effort has been made to make a more usable centroid dataset than what currently exists for PSMA locality boundaries, this is but one methodology, it has it's flaws and doesn't work perfectly for all situations. It's up here for any suggestions or improvements. But note, the process needs to be data driven and repeatable 4 times per year!

### I Just Want the Data!

You can run the script to get the result or just download the data from here:
- [Shapefile](https://github.com/iag-geo/psma-locality-centroids/releases/download/v1.0/psma_locality_centroids_shapefile.zip) (~40Mb) 
- [GeoJSON](https://github.com/iag-geo/psma-locality-centroids/releases/download/v1.0/psma_locality_centroids_geojson.zip) (~25Mb) 

#### Data License
Incorporates or developed using G-NAF ©PSMA Australia Limited licensed by the Commonwealth of Australia under the [Open Geo-coded National Address File (G-NAF) End User Licence Agreement](http://data.gov.au/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/09f74802-08b1-4214-a6ea-3591b2753d30/download/20160226---EULA---Open-G-NAF.pdf).

Incorporates or developed using Administrative Boundaries ©PSMA Australia Limited licensed by the Commonwealth of Australia under [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

### Script Pre-requisites

- You will need to run the [gnaf-loader](https://github.com/minus34/gnaf-loader) script to load the required Admin Bdy and GNAF tables into Postgres
- Will assume you have the gnaf and admin-bdys schemas populated with data as per above. This data will be created as a new table in the admin-bdys schema
- Postgres 9.x (tested on 9.3, 9.4 & 9.5 on Windows and 9.5 on OSX)
- PostGIS 2.1+

### Missing localities
Trimming the boundaries to the coastline removes a small number of bay or estuary based localities.  These have very few G-NAF addresses.

As an alternative you could perform this operation on the raw PSMA localities file instead.
