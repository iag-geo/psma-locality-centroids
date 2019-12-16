--********************************************************************************************************
-- locality reference point creation using GNAF and PSMA Boundaries
--
-- author: Ed Haverkamp
-- email: ed.haverkamp@iag.com.au
-- twitter: @EdHaverkamp
--
--********************************************************************************************************


-- STAGE 1 ~ 30 secs
-- Create on surface geometric centroids for the PSMA locality boundaries

drop table if exists admin_bdys_201911.temp_locality_centroid;
create table admin_bdys_201911.temp_locality_centroid as
select locality_pid,
       locality_name,
       locality_class,
       postcode,
       state,
       ST_X(ST_PointOnSurface(geom))::numeric(9,6) as on_surface_x,
       ST_Y(ST_PointOnSurface(geom))::numeric(8,6) as on_surface_y,
       null::numeric(9,6) as new_x,
       null::numeric(8,6) as new_y,
       null::text as centroid_type,
       null::geometry(POINT,4283) as geom -- will be populated with the new weighted centroid geometry
from admin_bdys_201911.locality_bdys_display;

ALTER TABLE admin_bdys_201911.temp_locality_centroid OWNER to postgres;
ALTER TABLE admin_bdys_201911.temp_locality_centroid ADD CONSTRAINT temp_locality_centroid_codes_pkey PRIMARY KEY (locality_pid);

ANALYSE admin_bdys_201911.temp_locality_centroid;

-- STAGE 2
-- cluster GNAF addresses by locality_pid
-- assumes you have a copy of the flattened address principals table created by the GNAF Loader process
-- TO DO investigate parallel processing using python for this step - will take 30-45 minutes

drop table if exists temp_addresses_clustering;
create temporary table temp_addresses_clustering as
select locality_pid,
       unnest(ST_ClusterWithin(geom, 0.002)) as gc -- tolerances have been tried at varying levels to obtain 'best fit' for entire state based datasets
from gnaf_201911.addresses
group by locality_pid;

ANALYSE temp_addresses_clustering;

drop table if exists admin_bdys_201911.temp_cluster_all_addresses;
create table admin_bdys_201911.temp_cluster_all_addresses as
select row_number() over () AS id,
  ST_NumGeometries(gc) as num_geometries,
  locality_pid,
  ST_MinimumBoundingCircle(gc) AS geom,
  false::boolean as primary_cluster
from temp_addresses_clustering;

ANALYSE admin_bdys_201911.temp_cluster_all_addresses;

-- select the cluster created for each locality that has the most number of GNAF points in it, this approach is better than using largest radius
drop table if exists admin_bdys_201911.temp_cluster_largest;
create table admin_bdys_201911.temp_cluster_largest as
SELECT DISTINCT first_value("id") OVER (PARTITION BY locality_pid ORDER BY num_geometries DESC) as first_value
FROM admin_bdys_201911.temp_cluster_all_addresses
ORDER BY 1;

ANALYSE admin_bdys_201911.temp_cluster_largest;

-- flag the primary cluster
update admin_bdys_201911.temp_cluster_all_addresses a
set primary_cluster = true
from admin_bdys_201911.temp_cluster_largest b
where a.id = b.first_value;

ANALYSE admin_bdys_201911.temp_cluster_all_addresses;


-- STAGE 3
-- create a centroid table for the primary clusters

drop table if exists admin_bdys_201911.temp_cluster_centroid;
create table admin_bdys_201911.temp_cluster_centroid as
select locality_pid,
       null::text as pip_locality_pid,
       null::text as centroid_type,
       ST_X(ST_PointOnSurface(geom))::numeric (9,6) as on_surface_x,
       ST_Y(ST_PointOnSurface(geom))::numeric (8,6) as on_surface_y,
       ST_PointOnSurface(geom) as geom
from admin_bdys_201911.temp_cluster_all_addresses
where primary_cluster;

-- add in a new column for the point in polygon(pip) locality pid to check the validity of the cluster centroid  - due to shape or concentration of address points
-- this can actually end up outside the polygon...NOT what we want.

UPDATE admin_bdys_201911.temp_cluster_centroid AS pnts
  SET pip_locality_pid = bdy.locality_pid
FROM admin_bdys_201911.locality_bdys_display AS bdy
WHERE ST_Intersects(bdy.geom, pnts.geom);

-- set cluster centroids
update admin_bdys_201911.temp_cluster_centroid
    set centroid_type = 'CLUSTER_CENTROID'
where locality_pid = pip_locality_pid;

-- set the geometric centroids for those where the cluster centroids fall outside the polygon due to odd shapes, or incorrect cluster size for the locality
update admin_bdys_201911.temp_cluster_centroid
set centroid_type = 'GEOMETRIC_CENTROID - CLUSTER CENTROID NOT PIP'
where locality_pid <> pip_locality_pid;

update admin_bdys_201911.temp_cluster_centroid a
set on_surface_x  = b.on_surface_x,
    on_surface_y = b.on_surface_y
from admin_bdys_201911.temp_locality_centroid b
where a.locality_pid = b.locality_pid and a.centroid_type = 'GEOMETRIC_CENTROID - CLUSTER CENTROID NOT PIP';

-- update the main table with the new coordinates where they have been able to be assigned....
-- for those that can't, retain their original on surface XY and update the centroid type to be geometric_centroid

update admin_bdys_201911.temp_locality_centroid a
    set new_x = b.on_surface_x,
        new_y = b.on_surface_y,
        centroid_type = b.centroid_type,
        geom = ST_SetSRID(ST_MakePoint(b.on_surface_x, b.on_surface_y), 4283)
from admin_bdys_201911.temp_cluster_centroid b
where a.locality_pid = b.locality_pid;

update admin_bdys_201911.temp_locality_centroid
    set new_x = on_surface_x,
        new_y = on_surface_y,
        centroid_type = 'GEOMETRIC_CENTROID - NO CLUSTER CREATED FOR TOLERANCE',
        geom = ST_SetSRID(ST_MakePoint(on_surface_x, on_surface_y), 4283)
where new_x is null
  and new_y is null
  and centroid_type is null;

ANALYSE admin_bdys_201911.temp_locality_centroid;

-- -- add new geometry column GDA94 and index
-- UPDATE admin_bdys_201911.locality_centroid
--     SET geom = ST_SetSRID(ST_MakePoint(new_x, new_y), 4283);
-- CREATE INDEX idx_locality_centroid_geom ON admin_bdys_201911.locality_centroid USING GIST(geom);

-- create a new file with the updated centroid coordinates and geom with the centroid type field
drop table if exists admin_bdys_201911.locality_centroid;
create table admin_bdys_201911.locality_centroid as
select locality_pid,
       locality_name,
       locality_class,
       postcode,
       state,
       new_x as longitude,
       new_y as latitude,
       centroid_type,
       geom
from admin_bdys_201911.temp_locality_centroid;

CREATE INDEX locality_centroid_geom_idx ON admin_bdys_201911.locality_centroid USING GIST (geom);
ALTER TABLE admin_bdys_201911.locality_centroid CLUSTER ON locality_centroid_geom_idx;


-- output as json file
-- trimmed to save space in the file here...you can choose to allow full detail on coordinates and import all fields if you want
-- COPY (select array_to_json(array_agg(row_to_json(t)))
-- from (
--    select locality_name as locality_name,postcode,state,longitude::numeric(7,4),latitude::numeric(8,4) from admin_bdys_201911.locality_centroid where longitude is not NULL and latitude is not NULL
--    ) t) TO 'c:\temp\PSMA_locality_centroids.json';


-- drop unnecessary tables
drop table admin_bdys_201911.temp_cluster_all_addresses;
drop table admin_bdys_201911.temp_cluster_centroid;
drop table admin_bdys_201911.temp_cluster_largest;
drop table admin_bdys_201911.temp_locality_centroid;
drop table temp_addresses_clustering;

-- TODO
-- shapefile output - need to pythonise this code to allow pgsql2shp
-- parallel processing - again pythonise
