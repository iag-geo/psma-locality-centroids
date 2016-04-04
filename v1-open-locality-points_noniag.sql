--********************************************************************************************************
-- locality reference point creation using GNAF and PSMA Boundaries
--
-- author: Ed Haverkamp
-- email: ed.haverkamp@iag.com.au
-- twitter: @EdHaverkamp
--
--********************************************************************************************************


-- STAGE 1
-- Create OnSurface geometric centroids for the PSMA locality boundaries

drop table if exists public.locality_centroid;
create table public.locality_centroid as 
select loc_pid,ST_PointOnSurface(geom) as the_geom
from public.locality_bdys_display; --assumes you already have this processing done

create index idx_locality_bdys_display_loc_pid ON public.locality_bdys_display (loc_pid);

-- add columns for X and Y

alter table public.locality_centroid
add column OnSurface_X numeric (11,8), add column OnSurface_Y numeric (12,8);

update public.locality_centroid
set OnSurface_X = ST_x(the_geom),OnSurface_Y = ST_y(the_geom);

-- STAGE 2
-- assumes you have a copy of the flattened address table created by the GNAF Loader process
-- run each state seperately as it's too slow to run the whole country at this stage
-- TO DO investigate parallel processing using python for this step


--*****************************************
-- Create clusters of address points in NT
--*****************************************

drop table if exists public.cluster_all_addresses_nt;
create table public.cluster_all_addresses_nt as 
SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  f.locality_pid,
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / 2*(pi())) AS radius --creates pretty circles
FROM (
  SELECT locality_pid,unnest(ST_ClusterWithin(geom, 0.002)) gc -- tolerances have been tried at varying levels to obtain 'best fit' for entire state based datasets 
  FROM gnaf.addresses
  where locality_pid like 'NT%'
  group by locality_pid
) f;

-- select the cluster created for each locality that has the most number of GNAF points in it, have found this approach better than using largest radius

drop table if exists public.cluster_largest_nt;
create table public.cluster_largest_nt as
SELECT DISTINCT
  first_value("id") OVER (PARTITION BY "locality_pid" ORDER BY "st_numgeometries" DESC) 
FROM public.cluster_all_addresses_nt
ORDER BY 1;

-- alter the cluster table by adding an indicator to flag the primary cluster from above

alter table public.cluster_all_addresses_nt
add column primary_cluster character (1);

update public.cluster_all_addresses_nt a
set primary_cluster = 'Y'
from public.cluster_largest_nt b
where a.id = b.first_value;

--*****************************************
-- Create clusters of address points in QLD
--*****************************************

drop table if exists public.cluster_all_addresses_qld;
create table public.cluster_all_addresses_qld as 
SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  f.locality_pid,
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / 2*(pi())) AS radius --creates pretty circles
FROM (
  SELECT locality_pid,unnest(ST_ClusterWithin(geom, 0.002)) gc -- tolerances have been tried at varying levels to obtain 'best fit' for entire state based datasets 
  FROM gnaf.addresses
  where locality_pid like 'QLD%'
  group by locality_pid
) f;

-- select the cluster created for each locality that has the most number of GNAF points in it, have found this approach better than using largest radius

drop table if exists public.cluster_largest_qld;
create table public.cluster_largest_qld as
SELECT DISTINCT
  first_value("id") OVER (PARTITION BY "locality_pid" ORDER BY "st_numgeometries" DESC) 
FROM public.cluster_all_addresses_qld
ORDER BY 1;

-- alter the cluster table by adding an indicator to flag the primary cluster from above

alter table public.cluster_all_addresses_qld
add column primary_cluster character (1);

update public.cluster_all_addresses_qld a
set primary_cluster = 'Y'
from public.cluster_largest_qld b
where a.id = b.first_value;

--*********************************************
-- Create clusters of address points in NSW/ACT
--*********************************************

drop table if exists public.cluster_all_addresses_nsw_act;
create table public.cluster_all_addresses_nsw_act as 
SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  f.locality_pid,
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / 2*(pi())) AS radius --creates pretty circles
FROM (
  SELECT locality_pid,unnest(ST_ClusterWithin(geom, 0.002)) gc -- tolerances have been tried at varying levels to obtain 'best fit' for entire state based datasets 
  FROM gnaf.addresses
  where locality_pid like 'NSW%' or locality_pid like 'ACT%'
  group by locality_pid
) f;

-- select the cluster created for each locality that has the most number of GNAF points in it, have found this approach better than using largest radius

drop table if exists public.cluster_largest_nsw_act;
create table public.cluster_largest_nsw_act as
SELECT DISTINCT
  first_value("id") OVER (PARTITION BY "locality_pid" ORDER BY "st_numgeometries" DESC) 
FROM public.cluster_all_addresses_nsw_act
ORDER BY 1;

-- alter the cluster table by adding an indicator to flag the primary cluster from above

alter table public.cluster_all_addresses_nsw_act
add column primary_cluster character (1);

update public.cluster_all_addresses_nsw_act a
set primary_cluster = 'Y'
from public.cluster_largest_nsw_act b
where a.id = b.first_value;

--*****************************************
-- Create clusters of address points in VIC
--*****************************************

drop table if exists public.cluster_all_addresses_vic;
create table public.cluster_all_addresses_vic as 
SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  f.locality_pid,
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / 2*(pi())) AS radius --creates pretty circles
FROM (
  SELECT locality_pid,unnest(ST_ClusterWithin(geom, 0.002)) gc -- tolerances have been tried at varying levels to obtain 'best fit' for entire state based datasets
  FROM gnaf.addresses
  where locality_pid like 'VIC%'
  group by locality_pid
) f;

-- select the cluster created for each locality that has the most number of GNAF points in it, have found this approach better than using largest radius


drop table if exists public.cluster_largest_vic;
create table public.cluster_largest_vic as
SELECT DISTINCT
  first_value("id") OVER (PARTITION BY "locality_pid" ORDER BY "st_numgeometries" DESC) 
FROM public.cluster_all_addresses_vic
ORDER BY 1;

-- alter the cluster table by adding an indicator to flag the primary cluster from above

alter table public.cluster_all_addresses_vic
add column primary_cluster character (1);

update public.cluster_all_addresses_vic a
set primary_cluster = 'Y'
from public.cluster_largest_vic b
where a.id = b.first_value;

--*****************************************
-- Create clusters of address points in WA
--*****************************************

drop table if exists public.cluster_all_addresses_wa;
create table public.cluster_all_addresses_wa as 
SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  f.locality_pid,
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / 2*(pi())) AS radius --creates pretty circles
FROM (
  SELECT locality_pid,unnest(ST_ClusterWithin(geom, 0.002)) gc -- tolerances have been tried at varying levels to obtain 'best fit' for entire state based datasets
  FROM gnaf.addresses
  where locality_pid like 'WA%'
  group by locality_pid
) f;

-- select the cluster created for each locality that has the most number of GNAF points in it, have found this approach better than using largest radius

drop table if exists public.cluster_largest_wa;
create table public.cluster_largest_wa as
SELECT DISTINCT
  first_value("id") OVER (PARTITION BY "locality_pid" ORDER BY "st_numgeometries" DESC) 
FROM public.cluster_all_addresses_wa
ORDER BY 1;

-- alter the cluster table by adding an indicator to flag the primary cluster from above

alter table public.cluster_all_addresses_wa
add column primary_cluster character (1);

update public.cluster_all_addresses_wa a
set primary_cluster = 'Y'
from public.cluster_largest_wa b
where a.id = b.first_value;

--*****************************************
-- Create clusters of address points in SA
--*****************************************

drop table if exists public.cluster_all_addresses_sa;
create table public.cluster_all_addresses_sa as 
SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  f.locality_pid,
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / 2*(pi())) AS radius --creates pretty circles
FROM (
  SELECT locality_pid,unnest(ST_ClusterWithin(geom, 0.002)) gc -- tolerances have been tried at varying levels to obtain 'best fit' for entire state based datasets
  FROM gnaf.addresses
  where locality_pid like 'SA%'
  group by locality_pid
) f;

-- select the cluster created for each locality that has the most number of GNAF points in it, have found this approach better than using largest radius

drop table if exists public.cluster_largest_sa;
create table public.cluster_largest_sa as
SELECT DISTINCT
  first_value("id") OVER (PARTITION BY "locality_pid" ORDER BY "st_numgeometries" DESC) 
FROM public.cluster_all_addresses_sa
ORDER BY 1;

-- alter the cluster table by adding an indicator to flag the primary cluster from above

alter table public.cluster_all_addresses_sa
add column primary_cluster character (1);

update public.cluster_all_addresses_sa a
set primary_cluster = 'Y'
from public.cluster_largest_sa b
where a.id = b.first_value;


--*****************************************
-- Create clusters of address points in TAS
--*****************************************

drop table if exists public.cluster_all_addresses_tas;
create table public.cluster_all_addresses_tas as 
SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  f.locality_pid,
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / 2*(pi())) AS radius --creates pretty circles
FROM (
  SELECT locality_pid,unnest(ST_ClusterWithin(geom, 0.002)) gc -- tolerances have been tried at varying levels to obtain 'best fit' for entire state based datasets
  FROM gnaf.addresses
  where locality_pid like 'TAS%'
  group by locality_pid
) f;

-- select the cluster created for each locality that has the most number of GNAF points in it, have found this approach better than using largest radius

drop table if exists public.cluster_largest_tas;
create table public.cluster_largest_tas as
SELECT DISTINCT
  first_value("id") OVER (PARTITION BY "locality_pid" ORDER BY "st_numgeometries" DESC) 
FROM public.cluster_all_addresses_tas
ORDER BY 1;

-- alter the cluster table by adding an indicator to flag the primary cluster from above

alter table public.cluster_all_addresses_tas
add column primary_cluster character (1);

update public.cluster_all_addresses_tas a
set primary_cluster = 'Y'
from public.cluster_largest_tas b
where a.id = b.first_value;


-- create a centroid table for the primary clusters

drop table if exists public.cluster_centroid;
create table public.cluster_centroid as 
select locality_pid,ST_PointOnSurface(circle) as the_geom
from public.cluster_all_addresses_tas
where primary_cluster = 'Y';

insert into public.cluster_centroid
select locality_pid,ST_PointOnSurface(circle) as the_geom
from public.cluster_all_addresses_nt
where primary_cluster = 'Y';

insert into public.cluster_centroid
select locality_pid,ST_PointOnSurface(circle) as the_geom
from public.cluster_all_addresses_vic
where primary_cluster = 'Y';

insert into public.cluster_centroid
select locality_pid,ST_PointOnSurface(circle) as the_geom
from public.cluster_all_addresses_wa
where primary_cluster = 'Y';

insert into public.cluster_centroid
select locality_pid,ST_PointOnSurface(circle) as the_geom
from public.cluster_all_addresses_sa
where primary_cluster = 'Y';

insert into public.cluster_centroid
select locality_pid,ST_PointOnSurface(circle) as the_geom
from public.cluster_all_addresses_nsw_act
where primary_cluster = 'Y';

insert into public.cluster_centroid
select locality_pid,ST_PointOnSurface(circle) as the_geom
from public.cluster_all_addresses_qld
where primary_cluster = 'Y';


-- add columns for X and Y

alter table public.cluster_centroid
add column OnSurface_X numeric (11,8), add column OnSurface_Y numeric (12,8);

update public.cluster_centroid
set OnSurface_X = ST_x(the_geom),OnSurface_Y = ST_y(the_geom);


-- add in a new column for the point in polygon(pip) locality pid to check the validity of the cluster centroid  - due to shape or concentration of address points
-- this can actually end up outside the polygon...NOT what we want.

alter table public.cluster_centroid
add column pip_locality_pid character varying(32);

UPDATE public.cluster_centroid AS pnts
  SET pip_locality_pid = bdy.loc_pid
  FROM public.locality_bdys_display AS bdy
  WHERE ST_Intersects(bdy.geom, pnts.the_geom);

-- add column to indicate centroid type
alter table public.cluster_centroid
add column centroid_type character varying(100);

-- set cluster centroids
update public.cluster_centroid
set centroid_type = 'CLUSTER_CENTROID'
where locality_pid = pip_locality_pid;

-- set the geometric centroids for those where the cluster centroids fall outside the polygon due to odd shapes, or incorrect cluster size for the locality
update public.cluster_centroid
set centroid_type = 'GEOMETRIC_CENTROID - CLUSTER CENTROID NOT PIP'
where locality_pid <> pip_locality_pid;

update public.cluster_centroid a
set OnSurface_X  = b.OnSurface_X, OnSurface_Y = b.OnSurface_Y
from public.locality_centroid b
where a.locality_pid = b.loc_pid and a.centroid_type = 'GEOMETRIC_CENTROID - CLUSTER CENTROID NOT PIP';

-- update the main table with the new coordinates where they have been able to be assigned....
-- for those that can't, retain their original on surface XY and update the centroid type to be geometric_centroid

alter table public.locality_centroid
add column new_x numeric (11,8),add column new_y numeric (11,8),add column centroid_type character varying(100);

update public.locality_centroid a
set new_x  = b.OnSurface_X, new_y = b.OnSurface_Y, centroid_type = b.centroid_type
from public.cluster_centroid b
where a.loc_pid = b.locality_pid;

update public.locality_centroid
set new_x = onsurface_x, new_y = onsurface_y, centroid_type = 'GEOMETRIC_CENTROID - NO CLUSTER CREATED FOR TOLERANCE'
where new_x is null and new_y is null and centroid_type is null;

-- add new geometry column GDA94 and index

ALTER TABLE public.locality_centroid ADD COLUMN geom geometry(POINT,4283);
UPDATE public.locality_centroid SET geom = ST_SetSRID(ST_MakePoint(new_x,new_y),4283);
CREATE INDEX idx_locality_centroid_geom ON public.locality_centroid USING GIST(geom);

drop table if exists public.locality_centroid_new;
create table public.locality_centroid_new as
select loc_pid,new_x,new_y,centroid_type,geom from public.locality_centroid;



-- output as json file

COPY (select array_to_json(array_agg(row_to_json(t)))
from (
   select locality_name,postcode,state,longitude::numeric(7,4),latitude::numeric(8,4) from public.iag_locality_postcodes_centroids where longitude is not NULL and latitude is not NULL
   ) t) TO 'c:\temp\localities.json';


-- to do, drop uncessary tables
-- vacuum/analyze
-- shapefile output
-- test code runs end to end with no stops


