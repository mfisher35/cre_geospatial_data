#*************************************************************************
#* 
#* copyright (c) Foresite.ai
#* __________________
#* 
#*  [2019] Foresite.ai 
#*  All Rights Reserved.
#* 
#* NOTICE:  All information contained herein is, and remains
#* the property of Foresite.ai and its suppliers,
#* if any.  The intellectual and technical concepts contained
#* herein are proprietary to Foresite.ai
#* and its suppliers and may be covered by U.S. and Foreign Patents,
#* patents in process, and are protected by trade secret or copyright law.
#* Dissemination of this information or reproduction of this material
#* is strictly forbidden unless prior written permission is obtained
#* from Foresite.ai
#*

from multiprocessing import Pool, cpu_count
import shapely
import sys
import geopandas as gpd
from geopy.geocoders import Nominatim
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point
from shapely.wkt import loads
import numpy as np
import re
import copy
from urllib import request,parse
import json
import csv
import timeit,time
import pickle
import fs_datatools as dt
import fs_nlp
import random
import imageio
from matplotlib import pyplot as plt
import os 
from shapely.geometry import *
import postal_abr

local_crs = {'nc_triangle' : '2264', 'ny_newyork' : '2263'}
gcounts = 0
geopt_names = ["geo point","geo_point_2d"]

#given a list, remove null values
def remove_nulls(clist):
    return [item for item in clist if not dt.is_null(item)]

#given a geometry get its type (example POINT, LINE, POLYGON, etc)
def geom_type(geom):
    geom_type = re.findall(r'[A-Z\s]+?\(', str(geom).upper())
    if len(geom_type) > 0:
        return geom_type[0].replace(" ","").replace("(","")    

#convert the given geometry to the type given
#def convert_geom(geom,to_type,delta=1e-8):
#    if geom_type(geom) == "LINESTRING" and "POLYGON" in to_type.upper():
#        points = [remove_nulls(item.split(" ")) for item in str(geom).replace("LINESTRING","").replace("( ","").replace("(","").replace(")","").split(", ")]
#        rpoints = copy.deepcopy(points)
#        for point in points:
#            rpoints.append([point[0],str(float(point[1])+delta)])
#        rpoints.append(rpoints[0])
#        polygon = "POLYGON (("
#        for points in rpoints:
#            polygon = polygon + " ".join(points) + ", "
#        polygon = polygon[0:len(polygon)-2]+"))"
#        return loads(polygon) 
#    if geom_type(geom) == "POINT" and if "POLYGON" in to_type.upper():
#        result.replace("POINT","POLYGON (")
#        numbers = re.findall(r'[0-9\.\s]+',result)
#        if len(numbers) > 0:
#            new_numbers = numbers[0].split[0]
#                  
#    if geom_type(geom) == "LINESTRING"

#unify all geometries to highest dimension.  For example if a gdf contains lines, points, and polygons, make everything polygons
def unify_geometries(gdf,geom_col='geometry'):
    types = set(gdf[geom_col].apply(lambda x: geom_type(str(x))))
     
#add an item to result dictionary of lists
def add_item(item,cat,rdict):
    if cat in rdict:
       rdict[cat].append(item)
    else:
       rdict[cat] = [item] 
    return rdict

#determine how many lines are in a file
def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def reset_counts():
    global gcounts
    gcounts = 0

#show how much time is left when currently processing row c of n total rows
def print_status(c,n):
    try:
        if c == 0 or c == 1:
            timefile = open("starttime.pkl",'wb')
            start_timer = timeit.default_timer()
            pickle.dump(start_timer,timefile)
            timefile.close()
            i = 1
        else:
           i = c
           start_timer = pickle.load(open("starttime.pkl","rb"))
           
        frac = float(i)/float(n)
        elapsed = timeit.default_timer() - start_timer
        left = elapsed/frac - elapsed
        if c > 1 and left > 0:
           hours = int(left/3600)
           minutes = int((left % 3600)/60)
   
           if hours > 0 or minutes > 0:
               sys.stdout.write("\r%sh:%sm left" % (str(hours).zfill(3),str(minutes).zfill(2)))
               sys.stdout.flush()
           else:
               sys.stdout.write("\r000m:%s left" % (str(left).zfill(2)))
               sys.stdout.flush()
    except Exception as e:
        print(e)
        print("Warning! Progress File Not Readable")

#add a new column to a given csv file
def add_col_csv(csv_filename,new_col_name,new_col,outfile=""):
    if outfile == "":
        outfile = csv_filename.replace(".","-%s." % new_col_name)
    csvinput = open(csv_filename,'r') 
    csvoutput = open(outfile, 'w')
    writer = csv.writer(csvoutput, lineterminator='\n')
    reader = csv.reader(csvinput)
    row_num = 0
    column = [new_col_name]  
    column.extend(new_col)
    for row in reader:
        row.append(column[row_num])
        row_num = row_num + 1
        writer.writerow(row)
    print ("%s written." % outfile)

#given a geodataframe, determine if geom column consists of shapely objects
def is_shapely(df,geom_col="geometry"):
    if len(df) > 0:
        return 'shapely' in str(type(df.iloc[0][geom_col]))
    else: 
        print("ERROR:  DF Length is 0!")  

#convert a regular geometry to multi geometry if possible.  For example: LineString --> MultiLineString
def convert_multi(df,geom_col='geometry'):
    contains_shapes = is_shapely(df)
    geoms = [str(item) for item in df[geom_col] if "MULTI" in str(item).upper()]
    if len(geoms) > 0 and len(geoms) < len(df):
        print('converting geometries to multi-geometries')
        df[geom_col] = df[geom_col].apply(lambda x: make_geom_multi(str(x)))
        if contains_shapes:
            df['geometry'] = [loads(shape) for shape in df[geom_col]]
    return df


def find_latlon_col(df,convert_geopt=1):   
    columns = list(df.columns)
    lat_col = -1
    lon_col = -1
    geo_col = -1
    global geopt_names

    for i in range (0,len(df.columns)):
        col_orig = list(df.columns)[i]
        col_low = dt.clean_specials(list(df.columns)[i]).lower()
        for i in range (0,len(df.columns)):
            if "latitude" in col_low or col_low == "lat" or "_lat" in col_low:
                lat_col = col_orig
            if "longitude" in col_low or col_low == "lon" or col_low == "long" or "_lon" in col_low:
                lon_col = col_orig
        if col_low in geopt_names:
            geo_col = col_orig
    if not lat_col == -1 and not lon_col == -1:
        print("Lat/Lon Columns Detected")
    if not geo_col == -1:
        print("Geo Point Column Detected")
    return lat_col,lon_col,geo_col

#return the column which contains the geometry (usually given by WKT) in given df
def find_geom_col(df):   
    columns = list(df.columns)
    geom_col = -1
    check_rows = 10
    curr_row = 0
    while geom_col == -1 and curr_row < check_rows and curr_row < len(df):
        for i in range (0,len(df.columns)):
            col_orig = list(df.columns)[i]
            val = str(df[col_orig].tolist()[curr_row]).upper()
            if ("POINT" in str(val) or "POLYGON" in str(val) or "LINE" in str(val)) and (("(" in str(val) and ")" in str(val)) or ("EMPTY" in str(val))):
                geom_col = col_orig
                print(geom_col)
        curr_row = curr_row + 1
    if geom_col != -1:
        print("Geometry Column Detected: %s" % geom_col)
    else:
        print("Geometry Column Not Detected")
    return geom_col 
   
def get_geom_type(df):
    geom_col = find_geom_col(df)
    if geom_col != -1:
        geoms = ["MULTIPOLYGON","MULTIPOINTZ","MULTIPOINTM","MULTIPATCH","POLYLINEZ","POLYLINEM","POLYLINE","POLYGONZ","POLYGONM","POLYGON","POINTZ","POINTM","POINT"]
        val = str(df.ix[0][geom_col]).upper()
        for geom in geoms:
            if geom in val:
                return geom
    print("No Geometry Column Found!")
    return "NULL"
     
#add a latitude and longitude column to an existing wkt type csv, input is a filename or geopandas dataframe
def add_latlon(wkt_file,lat_col="latitude",lon_col="longitude",write=1):
   if type(wkt_file) == str:
       gdf = load_file(wkt_file,to_crs="")
   elif type(wkt_file) == gpd.geodataframe.GeoDataFrame or type(wkt_file) == pd.DataFrame:
       gdf = wkt_file
   if lat_col in list(gdf.columns) or lon_col in list(gdf.columns):
      print("%s or %s already exists in file: %s" % (lat_col,lon_col,wkt_file))
   else:
      geom_col = find_geom_col(gdf)
      latlon = convert_wkt_latlon(gdf[geom_col])
      gdf[lat_col] = latlon[0]
      gdf[lon_col] = latlon[1]
      if write and type(wkt_file) == str:
          write_csv(gdf,wkt_file)
   return gdf

#find first latitude and longitude pairs inside a column of polygons/shapes and return them
def convert_wkt_latlon(wkt_col):
    print("Converting Column to lat/lon...")
    lat = []
    lon = []
    row_num = 1
    for wkt in wkt_col:
       matches = re.findall(r'\((.+?)\)',str(wkt)) 
       try:
           lat.append(float(matches[0].replace("(","").replace(",","").split(" ")[1]))
           lon.append(float(matches[0].replace("(","").replace(",","").split(" ")[0]))
       except:
           print ("error: no lat/lon found in WKT for row %i" % row_num)
       row_num = row_num + 1
    return lat,lon

#determine the type of geometry that the given df has an load and return geopandas df appropriately
def load_df_geom(df,sep="",crs={'init': 'epsg:4326'},to_crs=""):
    geom_col = find_geom_col(df)
    lat_col,lon_col,geopt_col = find_latlon_col(df)
    if geom_col != -1:
        df = load_wkt(df,geom_col=geom_col,sep=sep,crs=crs,to_crs=to_crs)
        return df
    elif lat_col != -1 and lon_col != -1:
        df = load_latlon(df,lon_col=lon_col,lat_col=lat_col,sep=sep,crs=crs,to_crs=to_crs)
        return df
    elif geopt_col != -1:
        convert_geopt_latlon(df,sep=";")
        lat_col,lon_col,geopt_col = find_latlon_col(df)
        if lat_col != -1 and lon_col != -1:
            df = load_latlon(df,lon_col=lon_col,lat_col=lat_col,crs=crs,to_crs=to_crs)
            return df
        else:
            print("Warning! No Geometry Found, Loading Plain Pandas DF...") 
            return df
    else:
        print("Warning! No Geometry Found, Loading Plain Pandas DF...") 
        return df


#figure out file type and geometry and open it up with correct geopandas load function, CRS are converted to 2264 in order to make calculations much easier/faster (dealing with (x,y) plane distances rather than spherical geodesics etc.
def load_file(in_filename,sep="",crs={'init': 'epsg:4326'},to_crs="",header_row=-1):
    if ".shp" in in_filename:
        print("loading shapefile: %s" % in_filename)
        df = load_shapefile(in_filename,to_crs=to_crs)
        return df
    elif ".geojson" in in_filename or ".json" in in_filename:
        print("loading geojson: %s" % in_filename)
        df = gpd.read_file(in_filename)
        return df
    elif ".xls" in in_filename:
        df = pd.read_excel(in_filename)
        if header_row > -0.5:
            df.columns = [dt.clean_specials(item,add_keeps=[" ","_","-",":"]) for item in list(df.iloc[header_row])]
            df = df.iloc[header_row+1:len(df)]
        return load_df_geom(df,sep=sep,crs=crs,to_crs=to_crs)
    if dt.are_any_in(in_filename,['.csv','.tsv','.txt']):
        print("loading csv: %s" % in_filename)
        if sep == "":
            sep = dt.get_sep(in_filename)
        if sep == "geojson":
            temp_filename = in_filename.replace(".csv","-orig.geojson")
            os.system("cp %s %s" % (in_filename,temp_filename))
            df = gpd.read_file(temp_filename)
            return df
        df = load_df_geom(pd.read_csv(in_filename,sep=sep),sep=sep,crs=crs,to_crs=to_crs)
        return df 
    else:
        print("Unknown Format! File Must Be .shp or .csv")
            
#convert geopoint column to latitude/longitude
def convert_geopt_latlon(in_obj,sep=","):
    global geopt_names
    if type(in_obj) == str:
        df = pd.read_csv(in_filename,sep=sep)
    else:
        df = in_obj

    for col in list(df.columns):
        if col.lower() in geopt_names:
            df['latitude'] = df[col].apply(lambda x: x.split(",")[0].strip())
            df['longitude'] = df[col].apply(lambda x: x.split(",")[1].strip())
    df.to_csv(in_filename,index=False,sep=",") 
    print("%s updated!" % in_filename)


#load shapes while converting null strings to Point (0 0)
def load_shape(s):
    if str(s) == "nan" or str(s) == "None":
        s = "Point (0 0)"
    return loads(s)
 
#load a WKT CSV file into a geopandas dataframe
def load_wkt(in_obj,geom_col="geom",sep=',',crs = {'init': 'epsg:4326'},to_crs=""):
    if type(in_obj) == str:
        df = pd.read_csv(in_obj,sep=sep)
    else:
        df = in_obj
    geometry = [load_shape(shape) for shape in df[geom_col]]
    gdf = GeoDataFrame(df, crs=crs, geometry=geometry)
    if to_crs != "":
        gdf = gdf.to_crs(to_crs)
    return gdf


#if df doesn't have geometry column assisnged, assign it using this function
def set_geometry(df,crs={'init': 'epsg:4326'},geom_col=""):
    if geom_col == "":
        geom_col = find_geom_col(df)
        lat_col,lon_col,geopt_col = find_latlon_col(df)
        if geom_col != -1:
            geometry = [loads(shape) for shape in df[geom_col]]
            gdf = GeoDataFrame(df, crs=crs, geometry=geometry)
            return gdf
        elif lat_col != -1 and lon_col != -1:
            geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
            gdf = GeoDataFrame(df, crs=crs, geometry=geometry)
            return gdf
        else:
            return df
    else:
        geometry = [loads(shape) for shape in df[geom_col]]
        gdf = GeoDataFrame(df,crs=crs,geometry=geometry)
        return gdf 

#load a CSV file that has a latitude and longitude column into a geopandas dataframe
def load_latlon(in_obj,lon_col="longitude",lat_col="latitude",sep=',',crs = {'init': 'epsg:4326'},to_crs=""):
    if type(in_obj) == str:
        df = pd.read_csv(in_obj,sep=sep)
    else:
        df = in_obj
    geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
#    df = df.drop([lon_col, lat_col], axis=1)
    gdf = GeoDataFrame(df, crs=crs, geometry=geometry)
    if not to_crs == "":
        gdf = gdf.to_crs(to_crs)
    return gdf
 
#load a shapefile into a pandas dataframe
def load_shapefile(in_filename,to_crs=""):
    shape = gpd.GeoDataFrame.from_file(in_filename)
    shape = shape[shape.geometry.notnull()]
    if not to_crs == "":
        shape = shape.to_crs(to_crs)
    return shape

#count the number of points (given in a geopandas series or df called 'count_pts') around a radius (in miles) from each point given in source_points (a geopandas series/df as well).  If instead of counting the points, you wish to aggregate a numerical column in the count_points df, assign the desired column name to count_agg_col
def count_radius(source_points, count_points,radius=1,source_geom_col="geometry", count_geom_col="geometry",count_agg_col=-1,join_col_name=-1):
     results = []
     i = 0
     for i in range(0,len(source_points)):
         pt = source_points.iloc[i]
         distances = count_points[count_geom_col].distance(pt['geometry'])/5280
         if count_agg_col == -1:
             results.append((distances < radius).sum())
         else:
             indices = (distances < radius)
             results.append(count_points.iloc[list(indices)][count_agg_col].sum())
     if join_col_name != - 1:
         results = {join_col_name : results}
         return source_points.assign(**results)
     else:
         return results

#find all geometries in agg_df that are within the given radius from given origin point.  Sum the given columns of the rows corresponding to those geometries
def agg_radius(origin_pt,agg_df,radius,agg_cols,total=0):
    if total != 0:
        global gcounts
        gcounts += 1
        if gcounts % 20 == 0:
            print_status(gcounts,total)
    results = {col +"_agg" : 0 for col in agg_cols}
    distances = agg_df['geometry'].distance(origin_pt)/5280 
    indices = (distances < radius)
    for col in agg_cols:
        results[col + "_agg"] = agg_df.iloc[list(indices)][col].sum()
    results['count_agg'] = sum(indices)
    return results

#write the given geopandas dataframe to a WKT csv file in given CRS if spatial convert is enabled            
def write_csv(gdf,filename, to_crs = {'init':'epsg:4326'}):
    try:
        if not to_crs == "":
           gdf = gdf.to_crs(to_crs)
    except Exception as e:
        print("Warning! CRS could not be converted to 4326 (ignore this warning if dataframe is not spatial)")
        print("Exception",e)
    gdf.to_csv(filename,index=False)
    print ("%s written." % filename)

#write the given geopandas dataframe to a Shapefile in given CRS if spatial convert is enabled            
def write_shp(gdf,filename, to_crs = ""):
    try:
        if not to_crs == "":
           gdf = gdf.to_crs(to_crs)
    except Exception as e:
        print("Warning! CRS could not be converted to 4326 (ignore this warning if dataframe is not spatial)")
        print("Exception",e)
    gdf.to_file(filename)
    print ("%s written." % filename)


#count the number of points (given by geopandas dataframe points), inside each polygon given in geopandas dataframe polys.  Result is a list of the same length as the number of polygons
def count_points_in_poly(points,polys):
   pts = points.copy() 
   pts_in_polys = []
   
   for i, poly in polys.iterrows():
       pts_in_this_poly = []
   
       for j, pt in points.iterrows():
           if poly.geometry.contains(pt.geometry):
               pts_in_this_poly.append(pt.geometry)
               pts = pts.drop([j])
       # We could do all sorts, like grab a property of the
       # points, but let's just append the number of them.
       pts_in_polys.append(len(pts_in_this_poly))
   
   polys['number of points'] = gpd.GeoSeries(pts_in_polys)
   return polys 



#given 2 columns from geopandas dataframes check to see if each point in the first given column interects with any of the geometries from the secondm a geopandas dataframe, return 1 if any of the shapes from the list intersect with shape1 
def do_any_intersect(gdf1,gdf2,geom_col1="geometry",geom_col2="geometry"):
    is_within = [sum(gdf2[geom_col2].intersects(gdf1.iloc[i][geom_col2])) > 0 for i in range(0,len(gdf1))]
    return pd.DataFrame({'intersects' : is_within})


#find the first polygon (geopandas df) that contains or intersects with a given point (single row from a geopandas df) and return the attribute of that polygon given by output_attr.  Optional: the percentage of overlap given from 0-1
def find_first_intersecting_poly(gdf1_row,gdf2,output_attrs,gdf1_row_geom_col="geometry",gdf2_geom_col="geometry",threshold=-1):
    if not type(output_attrs) == list:
        output_attrs = [output_attrs]
    intersects =  gdf2[gdf2_geom_col].intersects(gdf1_row[gdf1_row_geom_col])
    temp = gdf2.iloc[intersects.tolist()]
    if len(temp) > 0:
        for i in range(0,len(temp)):
            if threshold > 0:
                if type(gdf1_row[gdf1_row_geom_col]) == Point or type(temp.iloc[i][gdf2_geom_col]) == Point:
                    overlap = 1
                else:
                    overlap = temp.iloc[i][gdf2_geom_col].intersection(gdf1_row[gdf1_row_geom_col]).area/gdf1_row[gdf1_row_geom_col].area
                if overlap > threshold:
                    return dict(gdf2.iloc[i][output_attrs])
            else:
                return dict(gdf2.iloc[i][output_attrs])
    result = {}
    for key in output_attrs:
        result[key] = ""
    return result

#concat a list of filenames of csv files, optionally keep only the cols in the keep_cols list
def concat_files(file_list,out_filename,keep_cols=[],drop_dups=0):
   first = 1
   for cfile in file_list:
       print("Loading %s..." % cfile)
       if len(keep_cols) > 0:
           if first:
               first = 0
               result_df = load_file(cfile,to_crs="")
               ccols = list(set(keep_cols).intersection(set(result_df.columns)))
               result_df = result_df[ccols]
           else:
               temp_df = load_file(cfile,to_crs="")
               ccols = list(set(keep_cols).intersection(set(result_df.columns)))
               temp_df = temp_df[ccols]
               result_df = pd.concat([result_df,temp_df],sort=True)
       else:
           if first:
               first = 0
               result_df = load_file(cfile,to_crs="")
           else:
               temp_df = load_file(cfile,to_crs="")
               result_df = pd.concat([result_df,temp_df],sort=True)
   if drop_dups:
       result_df = result_df.drop_duplicates()
   result_df.to_csv(out_filename,index=False)
   print("%s written." % out_filename)
   return result_df

#find the first polygon that contains or intersects each point in the gdf1 geoseries (geopandas dataframe) and each of those polygon's attributes given by output_attrs
def find_intersecting_polys(gdf1,gdf2,output_attrs,gdf1_geom_col="geometry",gdf2_geom_col="geometry",join=1,threshold=-1):
    results = []
    for i in range(0,len(gdf1)):
       results.append(find_first_intersecting_poly(gdf1.iloc[i],gdf2,output_attrs,gdf1_geom_col,gdf2_geom_col,threshold=threshold))
    results = {k: [dic[k] for dic in results] for k in results[0]}
    if join:
       return gdf1.assign(**results)
    else:
       return results


#remove any geometries in a given gdf that are beyond a given radius (in miles) from a given point
def trim_shapes(point,gdf,radius=30):
    rad = radius * 5280
    circle_buffer = point.buffer(rad) 
    drops = []
    for i in range(0,len(gdf)):
        poly = gdf.iloc[i]
        if not poly.geometry.within(circle_buffer):
            drops.append(i)
    return gdf.drop(drops)

def report_progress_pll(total_chnk,pfile="progress.tmp",report_every=10):
    try:
        progress = open(pfile,"r")
        finished = int(progress.read()) + 1
        progress.close()
        if finished % report_every  == 0:
            print_status(finished,total_chnk)
        progress = open(pfile,"w")
        progress.write("%i" % finished)
        progress.close()
    except:
        aqer = 1

#Apply a function separately to a chunk of rows row in a dataframe, in parallel.
def process_chunk(args):
    function, chunk, params, total_chnk = args
    results = function(**params)       
    report_progress_pll(total_chnk) 
    return results

#run a given function (which must return a dataframe) in parallel, using the multiprocessing package with the given parameters
def run_parallel(function,  source_df, source_df_param_name , params, chunk_size=600, num_processes=None):
    progress = open("progress.tmp","w")
    progress.write("0")
    progress.close()

    function_list = []
    chunk_list = []
    params_list = []
    total_list = []

    print("Generating Parameters...") 
    print("Source DF N =",len(source_df))
    num = 0
    chnk_num = 0
    n = len(source_df)
    total_chnk = int(float(n)/float(chunk_size))
    print_status(1,total_chnk)

    # Generate chunks
    while num  < n:
        chnk_num = chnk_num + 1
        if num + chunk_size <= n:
            function_list.append(function)
            chunk_list.append(chnk_num)
            total_list.append(total_chnk)
            params[source_df_param_name] = source_df.iloc[num:num+chunk_size]
            params_list.append(copy.copy(params))
            #print("Generating Chunk",chnk_num,"Rows",num,"to",num+chunk_size)
        else:
            if not num > n:
                function_list.append(function)
                chunk_list.append(chnk_num)
                total_list.append(total_chnk)
                params[source_df_param_name] = source_df.iloc[num:n]
                #print("Generating Chunk",chnk_num,"Rows",num,"to",n)
                params_list.append(copy.copy(params))
        num = num + chunk_size

    args = zip(function_list,chunk_list,params_list,total_list)
    # If num_processes is not specified, default to minimum(#columns, #machine-cores)
    if num_processes==None:
        num_processes = min(len(source_df), cpu_count())
    
    print("Starting %s Processes (%i chunks of %i)..." % (num_processes,len(function_list),chunk_size))
    # 'with' context manager takes care of pool.close() and pool.join() for us
    with Pool(num_processes) as pool:
        # pool.map returns results as a list
        results_list = pool.map(process_chunk, args)
        # return list of processed columns, concatenated together as a new dataframe
        return pd.concat(results_list,axis=0)

def convert_geojson_str(geo_str):
    result = ""
    geo_json =  ast.literal_eval(geo_str)
    if geo_json['type'].lower() == "point":
       result = geo_json['type'].upper() + "(" + str(geo_json['coordinates'][0]) + " " + str(geo_json['coordinates'][1]) + ")"
       return result
    else:
       print("Error, Shape Type: %s is Unrecognized" % geo_json['type'])


def convert_geojson(in_filename,list_key="features"):
    results = {}
    f = open(in_filename)
    s = f.read()
    jsn = json.loads(s)
    if list_key != "":
        jsn = jsn[list_key]
    for item in jsn:
        props = item['properties'] 
        for key in list(props.keys()):
            update_dict(results,key,props[key])
        geom  = str(item['geometry'])
        update_dict(results,'geometry',convert_geojson_str(geom))
    df = pd.DataFrame(results)
    out_filename = in_filename.replace(".geojson",".csv")
    out_filename = out_filename.replace(".json",".csv")
    df.to_csv(out_filename,index=False)
    print(out_filename,"Written!")
    return out_filename

#drop any row in the DF that is less than threshold amount (default 0.002 miles) away from a previous value, only first item is kept
def drop_near_places(gdf,thresh=0.002,geom_col="geometry"):
    if thresh == 0:
        cols = list(gdf.columns)
        temp = gdf[geom_col].astype(str).tolist()
        gdf = gdf.assign(**{"__geo_str__": temp})
        return gdf.drop_duplicates(["__geo_str__"], keep='first')[cols] 
    radius = 5280*thresh
    M = calc_distance_matrix(gdf,geom_col=geom_col)
    n = len(gdf)
    bools = np.array([True]*n)
    for i in range(0,len(gdf)):
        bools = bools & (np.append(np.array([float("Inf")]*(i+1)),np.array(M[i][i+1:n])) > radius)
    return gdf[bools]
 
def calc_distance_matrix(df,geom_col="geometry"):
    print("Calculating Distance Matrix...")
    n = len(df)
    M = np.zeros(shape=(n,n)) 
    for i in range(n):
        if i % 300  == 0:
            print_status(i,n)
        temp_geom = df.iloc[i:n][geom_col]
        distances = temp_geom.distance(df.iloc[i][geom_col]).tolist()
        temp_row = np.append(np.zeros(i),distances)
        M[i] = temp_row
    return M

 
#clean up addresses for geocoder
def clean_address(addr):
    addr = addr.lower()
    repl_dict = {"us hwy ":"us-","hwy 54":"nc-54"," s ": " south ", " w " : " west ", " n " : " north ", " nw " : " northwest ", " ne " : " northeast ", " se " : " southeast ", " sw " : " southwest ", " mlk " : " martin luther king "}
    for key in list(repl_dict.keys()):
        addr = addr.replace(key,repl_dict[key])
    return addr

#abbreviate address in standardized fashion
def standardize_address(addr):
    if dt.is_null(addr):
        return addr
    addr_split = addr.upper().split(" ")
    for abbr_list in postal_abr.word_abbreviations: 
        to_abbr = abbr_list[len(abbr_list)-1]
        for i in range(0,len(abbr_list)-1):
            from_abbr = abbr_list[i]
            try:
                index = addr_split.index(from_abbr)
                addr_split[index] = to_abbr
            except:
                err = 1
    result = " ".join(addr_split)        
    for abbr_list in postal_abr.part_abbreviations: 
        target = abbr_list[len(abbr_list)-1]
        for i in range(0,len(abbr_list)-1):
            result = result.replace(abbr_list[i],target)
    return result


#instead of using census geocoder API, match the given full address to the closest full address in the given geopandas df and return it's centroid.  This function assumes the gdf given has columns 'geometry' and 'site_address'.  
def geocode_local(num,street,compl_geom_df,split_geom_df,input_split_val,geom_split_col,verbose=1,total=0):
    geom_df = compl_geom_df
    global gcounts
    if total > 0:
        gcounts += 1
        if gcounts % 200 == 0:
            print_status(gcounts,total)
    if dt.is_null(street):
        return "POINT (0 0)"
    if dt.is_null(num):
        num = 0
    if geom_split_col != "" and input_split_val != "" and input_split_val in split_geom_df:
        geom_df = split_geom_df[input_split_val]
    street = standardize_address(dt.clean_specials(street,add_keeps=[" "]).upper())
    geom_df_st = geom_df[geom_df['site_street'] == street]
    geom_df_st_num = geom_df_st[geom_df['site_num'] == num]
    if len(geom_df_st_num) > 0:
        if not dt.is_null(geom_df_st_num.iloc[0]['geometry']):
            if verbose:
                print(num,street,'matched with',geom_df_st_num.iloc[0]['site_num'],geom_df_st_num.iloc[0]['site_street'])
            return geom_df_st_num.iloc[0]['geometry'].centroid
    elif len(geom_df_st) > 0:
        result = geom_df_st['site_num'].apply(lambda x: abs(dt.force_numeric(x) - dt.force_numeric(num)) if not dt.is_null(x) else dt.force_numeric(num))
        row =  geom_df_st[result == result.min()]
        if len(row) > 0:
            row = row.iloc[0]
            if not dt.is_null(row['geometry']):
                if verbose:
                    print(num,street,'matched with',row['site_num'],row['site_street'])
                return row['geometry'].centroid
    else:
        if verbose:
            print('no match for',num,street)

##instead of using census geocoder API, match the given full address to the closest full address in the given geopandas df and return it's centroid.  This function assumes the gdf given has columns 'geometry' and 'site_address'.  If cache_val and cache_col a zipcode match will be done first
#def geocode_local(address,gdf,verbose=1,cache_val=0,cache_col="",cache={},total=0,grab_other_cols=[]):
#    global gcount
#    if total > 0:
#       gcounts += 1
#       if gcounts % 10 == 0:
#           print_status(gcount,total)
#    try:
#        c_address = standardize_address(dt.clean_specials(address,add_keeps=[" "]).upper())
#        if cache_val != 0 and cache_col != "":
#            if not cache_val in list(cache.keys()):
#                gdf2 = gdf[gdf[cache_col] == cache_val].copy()
#                cache[cache_val] = gdf2
#            else: 
#                gdf2 = cache[cache_val] 
#        else:
#            gdf2 = gdf
#        result = gdf2['site_address'].apply(lambda x: standardize_address(dt.clean_specials(x,add_keeps=" "))) 
#        result = result.apply(lambda x: 99999 if len(set(c_address.split(" ")).intersection(set(str(x.upper()).split(" ")))) < 2 else dt.edit_distance(x,address))
#        row =  gdf2[result == result.min()].iloc[0]
#        if verbose:
#            print(c_address,'matched with',row['site_address'])
#        if len(grab_other_cols) == 0:
#            return row['geometry'].centroid
#        else:
#            final_result = [row['geometry'].centroid]
#            for col in grab_other_cols:
#                final_result.append(row[col])
#            return final_result
#    except Exception as e:
#        print('failed to match %s' % address)
#        return ""


#try joining two dfs first on split num/street address field, if that doesn't work resort to geocode_local
def fast_geocode_local(input_gdf,geom_gdf,input_street_col="",input_num_col="",geom_street_col="",geom_num_col="",verbose=1,total=0,grab_other_cols=[],input_split_col="",geom_split_col=""):
    reset_counts()
    if input_num_col == "":
        input_gdf['site_num'] = input_gdf[input_street_col].apply(lambda x: fs_nlp.get_street_info(str(x),num=1))
        input_gdf['site_street'] = input_gdf[input_street_col].apply(lambda x: standardize_address(fs_nlp.get_street_info(str(x),num=0)))
        input_street_col = "site_street"
        input_num_col = 'site_num'
    if geom_num_col == "":
        geom_gdf['site_num'] = geom_gdf[geom_street_col].apply(lambda x: fs_nlp.get_street_info(str(x),num=1))
        geom_gdf['site_street'] = geom_gdf[geom_street_col].apply(lambda x: standardize_address(fs_nlp.get_street_info(str(x),num=0)))
        geom_street_col = "site_street"
        geom_num_col = 'site_num'
    join_cols = ['geometry']
    join_cols.extend(grab_other_cols) 
    print('generating standard address columns...')
    input_gdf['temp_st'] =  input_gdf[input_street_col].apply(lambda x: standardize_address(dt.clean_specials(x,add_keeps=[" "]).upper()))
    geom_gdf['temp_st'] =  geom_gdf[geom_street_col].apply(lambda x: standardize_address(dt.clean_specials(x,add_keeps=[" "]).upper()))
    input_gdf['temp_addr'] = input_gdf[input_num_col] + " " + input_gdf['temp_st']
    geom_gdf['temp_addr'] = geom_gdf[geom_num_col] + " " + geom_gdf['temp_st']
    print('done.')
    if input_split_col != "":
        print('splits...')
        input_gdf[input_split_col] = input_gdf[input_split_col].apply(lambda x: str(x).replace(".0",""))
        input_gdf['temp_addr'] = input_gdf['temp_addr'] + " " + input_gdf[input_split_col]
    if geom_split_col != "":
        geom_gdf[geom_split_col] = geom_gdf[geom_split_col].apply(lambda x: str(x).replace(".0",""))
        geom_gdf['temp_addr'] = geom_gdf['temp_addr'] + " " + geom_gdf[geom_split_col]
        print('done adding split col...')
    
    result = dt.join(input_gdf,geom_gdf,"temp_addr","temp_addr",cols=join_cols,drop_nulls_join=1)
    n = sum(dt.get_non_nulls(result,'geometry')) 

    split_df = ""
    if input_split_col != "":
        split_df = dt.split_df(geom_gdf,geom_split_col)
        print(result.columns)
        
    result['geometry'] = result.apply(lambda row: geocode_local(row[input_num_col],row[input_street_col],geom_gdf,split_df,dt.value_or_default(row,input_split_col),geom_split_col,total=n,verbose=verbose) if dt.is_null(row['geometry']) else row['geometry'],axis=1)

    keep_cols = [col for col in result.columns if not "temp_" in col]
    result = result[keep_cols]
    return result

#given a street, city, state, and zipcode return point shape object with proper lat/long
def geocode_full_addr(full_address,return_shape=1):
    print("geolocating:",full_address)
    try:
        url = ("https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress?address=%s&benchmark=4&vintage=4&format=json" % (parse.quote(full_address)))
        req = request.Request(
        url, 
        data=None, 
        headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }
        )
        response = request.urlopen(req)
        data = json.loads(response.read())
        result = [data['result']['addressMatches'][0]['coordinates']['x'], data['result']['addressMatches'][0]['coordinates']['y']] 
        print("Success: %s" % str(result))
        if return_shape:
            return loads("POINT(%f %f)" % (result[0],result[1]))
        else:
            return result
        
    except:
        try:
            geolocator = Nominatim(user_agent="my-app")
            location = geolocator.geocode(full_address)
            if location != None:
                print("Success: %s" % str(location))
                if return_shape:
                    return loads("POINT(%f %f)" % (location.longitude,location.latitude))
                else:
                    return [location.longitude, location.latitude]
            return location
        except:
            print("Failed")
            return None
 
#given a street, city, state, and zipcode return point shape object with proper lat/long
def geocode_addr(street,city,state,zipcode,return_shape=1):
    print("geolocating:",street,city,state,zipcode)
    try:
        url = ("https://geocoding.geo.census.gov/geocoder/locations/address?street=%s&city=%s&state=%s&zip=%s&benchmark=4&format=json" % (parse.quote(street),parse.quote(city),parse.quote(state),str(zipcode)))
        req = request.Request(
        url, 
        data=None, 
        headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }
        )
        response = request.urlopen(req)
        data = json.loads(response.read())
        result = [data['result']['addressMatches'][0]['coordinates']['x'], data['result']['addressMatches'][0]['coordinates']['y']] 
        print("Success: %s" % str(result))
        if return_shape:
            return loads("POINT(%f %f)" % (result[0],result[1]))
        else:
            return result
        
    except:
        try:
            geolocator = Nominatim(user_agent="my-app")
            addr = (street.strip() +" "+ city.strip() +" "+ state.strip() +" " + zipcode.strip())
            addr = addr.replace("  "," ").replace("  "," ")
            location = geolocator.geocode(addr)
            if location != None:
                print("Success: %s" % str(location))
                if return_shape:
                    return loads("POINT(%f %f)" % (location.longitude,location.latitude))
                else:
                    return [location.longitude, location.latitude]
            return location
        except:
            print("Failed")
            return None
            
#return lat/longs of given a list of addresses, a delay in seconds is given to avoid spamming openstreet maps
def geocode_addrs_lat_long(streets,cities,states,zips,delay=2):
    lats = []
    longs = []
    for i in range(len(streets)):
        street = clean_address(streets[i])
        location = geocode_addr(street,cities[i],states[i],zips[i])
        if location == None:
            addr = (street.strip() +" "+ cities[i].strip() +" "+ states[i].strip() +" " + zips[i].strip())
            addr = addr.replace("  "," ").replace("  "," ")
            print("Warning! %s Not Found." % addr)
            lats.append(None)
            longs.append(None)
        else:
            longs.append(location[0])
            lats.append(location[1])
        time.sleep(delay)
    return({'latitude' : lats, 'longitude' : longs}) 

#take a sample of points from the given dataframe
def sample_df(df,sample_size):
    n = len(df)
    rset = [False]*n
    while sum(rset) < min(sample_size,n):
       rset[random.randint(0, n-1)] = True
    return df[rset]

#make a plot of the geometries in the given df and plot a map of the actual location as a background
def plot_map(gdf,geom_col="geometry",preview=3000):
    non_null = np.invert(gdf[geom_col].isna().values)
    gdf2 = gdf[non_null]
    if preview>0:
        n = min(preview,len(gdf2))
        gdf2 = sample_df(gdf2,n)
    gdf2 = gdf2.to_crs({'init':"epsg:4326"})
    tb = gdf2.total_bounds
    gdf2 = gdf2.to_crs({'init':"epsg:2264"})
    tb2 = gdf2.total_bounds
    xyratio = float(tb2[2]-tb2[0]) / float(tb2[3]-tb2[1])
    ysize = 1000
    xsize = ysize*xyratio
   
    while xsize > 1920 or ysize > 1920:
        xsize = int(xsize*0.8)
        ysize = int(xsize*0.8)

    mq_key = open("../General_Tools/mqk.txt",'r').read().replace("\n","")
    url = "https://www.mapquestapi.com/staticmap/v5/map?key=%s&boundingBox=%s,%s,%s,%s&size=%i,%i" % (mq_key,tb[1],tb[0],tb[3],tb[2],xsize,ysize)
    #print(url)
    request.urlretrieve(url,"temp.jpg")
    img = imageio.imread("temp.jpg")
    gdf2.plot(zorder=1,alpha=0.6)
    tb = gdf2.total_bounds
    plt.imshow(img,extent=[tb2[0],tb2[2],tb2[1],tb2[3]],zorder=0)
    plt.show()

#drop any row from the given df that does not have a geometry associated with it
def drop_null_geoms(df,geom_col='geometry',drop_invalid=1):
    keeps = np.invert(df[geom_col].isna())
    print("Dropping",len(df)-sum(keeps),'NaN')
    df = df[keeps]
    origin = loads("POINT(0 0)")
    keeps = np.invert(df[geom_col].distance(origin) < 0.0005)
    print("Dropping",len(df)-sum(keeps),'POINT(0 0)')
    df = df[keeps]
    if drop_invalid:
        print("Dropping",len(df)-sum(df['geometry'].is_valid),'Invalid Geoms')
        df = df[df['geometry'].is_valid]
    return df

#given a csv with a geometry column, convert the given in_crs to out_crs
def convert_crs(csv_filename,in_crs,out_crs,out_filename=""):
    if out_filename == "":
        out_filename = csv_filename 
    gdf = load_file(csv_filename,crs=in_crs,to_crs="")
    write_csv(gdf,out_filename,to_crs=out_crs)

#write dataframe to geojson file
def write_geojson(gdf,filename,to_crs = "",drop_null_geom=0):
    if drop_null_geom:
        print("Dropping Null Geometries...")
        gdf = drop_null_geoms(gdf)
    try:
        if not to_crs == "":
           gdf = gdf.to_crs(to_crs)
    except Exception as e:
        print("Warning! CRS could not be converted to 4326 (ignore this warning if dataframe is not spatial)")
        print("Exception",e)
    gdf.to_file(filename, driver="GeoJSON")
    print ("%s written." % filename)

#given a pair of x,y coordinates (can be longitude and latitude) convert them from given crs to the given to_crs, either as a shapely point() or x,y pair
def convert_pt(x,y,from_crs={'init': 'epsg:4326'},to_crs={'init': 'epsg:2264'},return_geom=1):
    temp_gdf = gpd.GeoDataFrame(crs=from_crs, geometry=[loads('POINT(%s %s)' % (str(x),str(y)))])
    temp_gdf = temp_gdf.to_crs(to_crs)
    if return_geom:
        return temp_gdf.iloc[0]['geometry']
    else:
        temp = str(temp_gdf.iloc[0]['geometry']).replace("POINT (","").replace("POINT","").replace("(","").replace(")","").split(" ")
        return temp[0],temp[1]


#given a WKT String return the centroid point
def get_centroid(wkt_str):
    if wkt_str == None or str(wkt_str) == "nan":
        return None
    if 'shapely' in str(type(wkt_str)):
        shape = wkt_str
        return shape.centroid
    if type(wkt_str) == str:
        try:
            shape = loads(wkt_str)
            return shape.centroid
        except:
            print("Warning! %s not valid geom" % wkt_str)
    return None

#geocode unknown or null geometries, given a row of data containing attributes: geometry,address,city,state,zip 
def geocode_split_addr(row):
    if not 'geometry' in row:
        row['geometry'] = None
    if str(row['geometry'])  == "nan" or row['geometry'] =="" or row['geometry'] == None or str(row['geometry']) == "None":
        time.sleep(3)
        result = str(geocode_addr(row['address'],row['city'],row['state'],row['zip']))
        if not result == "None" and not result == "nan":
            row['geometry'] = result
            return row
    return row

#get the smallet polygon that encompasses all the centroids of the geometries in a given geopandas dataframe
def get_bounding_polygon(gdf,geom_col='geometry'):
    centroids = gdf['geometry'].centroid
    multipoint = shapely.geometry.MultiPoint(centroids)
    bounding_poly = multipoint.convex_hull
    return bounding_poly

#given a row of data which contains columns:  address, city, state, zip and geometry fill in any null geometries with a geocoded point, meant to be used as an apply function
def fill_geom(row):
    if (str(row['geometry'])  == "nan" or row['geometry'] =="" or row['geometry'] == None or str(row['geometry']) == "None") and (not str(row['address']).lower() == 'nan'):
        time.sleep(3)
        result = str(geocode_addr(row['address'],row['city'],row['state'],row['zip']))
        if not result == "None" and not result == "nan":
            row['geometry'] = result
            return row
    return row
 
#given a row of data which contains a full address column and geometry column fill in any null geometries with a geocoded point, meant to be used as an apply function
def fill_geom_full_addr(row,full_addr_col,total=0):

    if total != 0:
        global gcounts
        gcounts += 1
        if gcounts % 10 == 0:
            print_status(gcounts,total)

    if not "geometry" in row:
        row['geometry'] = 'nan'

    if (str(row['geometry'])  == "nan" or row['geometry'] =="" or row['geometry'] == None or str(row['geometry']) == "None") and (not str(row[full_addr_col]).lower() == 'nan'):
        time.sleep(3)  
        result = str(geocode_full_addr(row[full_addr_col]))
        if not result == "None" and not result == "nan":
            row['geometry'] = result
            return row
    return row

#get the elevation (in feet) of a given longitude,latitude
def get_elevation(lon,lat):
    time.sleep(2)
    try:
        url = "https://nationalmap.gov/epqs/pqs.php?x=%s&y=%s&units=Feet&output=json" % (str(lon),str(lat))
        response = request.urlopen(url)
        data = response.read().decode('utf-8').replace("\\r\\n"," ").replace("\\n"," ").replace("\\r"," ").replace("\xef\xbb\xbf","")
        response = json.loads(data)
        return(response['USGS_Elevation_Point_Query_Service']['Elevation_Query']['Elevation'])
    except Exception as e:
        print(e)
        return "" 


#convert a linestring into a multilinestring or convert a point into a multipoint
def make_geom_multi(geom):
    result = geom.upper()
    if not "MULTI" in result:
        lstrings = re.findall(r'LINESTRING\s*\(',result) 
        if len(lstrings) > 0:
            result = result.replace(lstrings[0],"MULTILINESTRING ((") + ")"
        polys = re.findall(r'POLYGON\s*\(',result) 
        if len(polys):
            result = result.replace(polys[0],"MULTIPOLYGON ((") + ")"
    return result


#remove Z coords from geoms
def remove_z(geom):
    try:
        if geom.is_empty:
            return geom
    
        if isinstance(geom, Polygon):
            exterior = geom.exterior
            new_exterior = remove_z(exterior)
    
            interiors = geom.interiors
            new_interiors = []
            for int in interiors:
                new_interiors.append(remove_z(int))
    
            return Polygon(new_exterior, new_interiors)
    
        elif isinstance(geom, LinearRing):
            return LinearRing([xy[0:2] for xy in list(geom.coords)])
    
        elif isinstance(geom, LineString):
            return LineString([xy[0:2] for xy in list(geom.coords)])
    
        elif isinstance(geom, Point):
            return Point([xy[0:2] for xy in list(geom.coords)])
    
        elif isinstance(geom, MultiPoint):
            points = list(geom.geoms)
            new_points = []
            for point in points:
                new_points.append(remove_z(point))
    
            return MultiPoint(new_points)
    
        elif isinstance(geom, MultiLineString):
            lines = list(geom.geoms)
            new_lines = []
            for line in lines:
                new_lines.append(remove_z(line))
    
            return MultiLineString(new_lines)
    
        elif isinstance(geom, MultiPolygon):
            pols = list(geom.geoms)
    
            new_pols = []
            for pol in pols:
                new_pols.append(remove_z(pol))
    
            return MultiPolygon(new_pols)
    
        elif isinstance(geom, GeometryCollection):
            geoms = list(geom.geoms)
    
            new_geoms = []
            for geom in geoms:
                new_geoms.append(remove_z(geom))
    
            return GeometryCollection(new_geoms)
    
        else:
            raise RuntimeError("Currently this type of geometry is not supported: {}".format(type(geom)))
    except:
        print("Warning! Geometry %s cannot be processed" % str(geom))
        return geom
