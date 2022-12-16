import psql
import timeit
import fs_spatialtools
from shapely.wkt import loads
import geopandas

psql.init('nc_durham_2018')
#WHERE ST_Distance_Sphere(the_geom, ST_MakePoint(your_lon,your_lat)) <= radius_mi * 5280
radius = 1
lon = -78.902525
lat = 35.997910
#crs = 4326
crs = '2264'

#psql.query("SELECT pin_num FROM parcels_all WHERE ST_Distance_Sphere(ST_Centroid(geometry),ST_MakePoint(-78.709389, 35.771914)) <= 15 * 5280;")
start = timeit.default_timer()
#psql.query("SELECT site_address,pin_num FROM parcels_all WHERE round(CAST(ST_Distance_Sphere(ST_Centroid(geometry), ST_GeomFromText('POINT(%s %s)',%s)) As numeric),2) <= %i*5280" % (str(lon),str(lat),'4326',radius),counts_only=1)
psql.query("SELECT site_address,pin_num FROM parcels_all WHERE round(CAST(ST_Distance_Sphere(ST_Centroid(geometry), ST_GeomFromText('POINT(%s %s)',%s)) As numeric),2) <= %i*5280" % (str(lon),str(lat),'4326',radius),counts_only=1)
end = timeit.default_timer() 
print(end-start,"psql 3d")
#psql.query("SELECT pin_num FROM parcels_all WHERE ST_Distance(geometry,ST_MakePoint(-78.709389, 35.771914)) <= 1 * 5280;")

start = timeit.default_timer()
q = "SELECT pin_num FROM parcels_all WHERE ST_Distance(ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s),%s),%s),ST_Transform(ST_SetSRID(geometry,%s),%s)) <= %i * 5280;" % (str(lon),str(lat),'4326',crs,'4326',crs,radius)
print(q)
psql.query(q,counts_only=1)

print(end-start,"psql 2d")

parcels = fs_spatialtools.load_file("../NC_Durham/parcels.csv")
start = timeit.default_timer()
pt = [loads("POINT (%s %s)" % (str(lon),str(lat)))]
gdf = geopandas.GeoDataFrame({'geometry' : pt} ,crs={'init' : 'epsg:4326' },geometry=pt)
gdf = gdf.to_crs({'init': 'epsg:2264'})
q = (parcels[parcels.distance(gdf.iloc[0]['geometry']) < radius * 5280]['pin_num'])
print(len(q))
end = timeit.default_timer()
print(end-start,"python")
