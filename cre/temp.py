import psql

select_vars = "pin_num,site_address"
table = "parcels_joined"
radius = 0.3

psql.init("nc_triangle")
lat,lon = 35.772053, -78.709913
#psql.query("SELECT %s FROM %s WHERE ST_Distance(ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s),4326),2264),ST_Transform(ST_SetSRID(geometry,4326),2264)) <= %.4f * 5280;" % (selected_variables,table,str(lon),str(lat),radius))
print(psql.select_radius(lat,lon,radius,select_vars,table))
