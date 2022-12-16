#!/usr/bin/python

#TO DO: make this a class

#instructions
#sudo apt-get install libpq-dev (centos postgresql-libs and postgresql-devel) then pip install pygresql
#apt-get install 

#use create_db function to make your databse, or manually use the following postgres commands:
#CREATE EXTENSION postgis;
#CREATE EXTENSION postgis_topology;
#CREATE EXTENSION postgis_sfcgal;
#CREATE EXTENSION fuzzystrmatch
#CREATE EXTENSION postgis_tiger_geocoder;
#CREATE EXTENSION address_standardizer;

import pandas
import pg
import csv 
import os 
import re
import dbconf
import fs_spatialtools as su
import time
import math

db = ""

def create_db(database):
    db2 = pg.DB(host=dbconf.hostname, port=dbconf.port, user=dbconf.username, passwd=dbconf.password)
    db2.query("CREATE database %s;" % database)
    db2 = pg.DB(host=dbconf.hostname, dbname = database,port=dbconf.port, user=dbconf.username, passwd=dbconf.password)
    db2.query("CREATE EXTENSION postgis")
    db2.query("CREATE EXTENSION postgis_topology")
    db2.query("CREATE EXTENSION postgis_sfcgal")
    db2.query("CREATE EXTENSION fuzzystrmatch")
    db2.query("CREATE EXTENSION postgis_tiger_geocoder")
    db2.query("CREATE EXTENSION address_standardizer")
    print("Database %s Created With PostGIS Enabled" % database)

#initialize module to connect the db object to the given database
def init(database):
    global db
    print("connecting to db: %s -> %s" % (dbconf.hostname,database))
    dbconf.database = database
    if not dbconf.hostname == "":
        db = pg.DB(dbname=dbconf.database, host=dbconf.hostname, port=dbconf.port, user=dbconf.username, passwd=dbconf.password)
    else:
        db = pg.DB(dbname=dbconf.database, port=dbconf.port, user=dbconf.username, passwd=dbconf.password)
    return db


#if value is null return default
def is_null(val,default):
    if str(val).lower() == "nan" or str(val).lower() == "None" or ((type(val) == float or type(val) == int) and math.isnan(val)) or val == "" or val == None:
       return default
    return val


def clean_value(value):
    result = value.replace("'","")
    if len(result) < 1:
        result = "NULL"

    return result


def dump_csv(table):
    global db
    writer = csv.writer(open(table+".csv",'w'))
    cur = db.query( "select column_name from information_schema.columns where table_name='%s';" % table)
    header = [item[0] for item in cur.getresult()]
    writer.writerow(header)
    for i in range(0,len(header)):
        if 'geom' in header[i]:
            header[i] = "ST_AsText(%s)" % header[i]
    cur = db.query( "SELECT %s FROM %s;" % (",".join(header),table))
    for item in cur.getresult():
        writer.writerow(item)

    print(table+".csv", "written")

    # from os - seems to ignore geometry: os.system("psql -h %s -U %s -W -d %s -c \"\copy %s TO '%s/%s' DELIMITER ',' CSV HEADER;\"" % (hostname,username,database,table,os.getcwd(),table+'.csv'))



def create_insert(table,row,types):
    result = "INSERT INTO %s VALUES (" % table
    for i in range(0,len(row)):
        if "varchar" in types[i]:
           result = result + "'" + clean_value(row[i]) + "', "
        else:
           result = result + "%s" % clean_value(row[i]) + ", "
    result = result[0:len(result)-2] + ");"
    return result
    

def import_csv(csv_file,target_table,target_db,chunk_size = 100,bulk_ingest = 1,int_as_string=0,crs="",to_crs=""):
    global db
    temp_filename1 = csv_file
    if (not crs == to_crs) and not to_crs == "":
        temp_filename1 = csv_file.replace(".","-import.")
        su.convert_crs(csv_file,in_crs=crs,out_crs=to_crs,out_filename=temp_filename1)
    db = init(target_db)
    db.query("DROP TABLE IF EXISTS %s;" % target_table)
    print ("creating table...")
    tbl_obj = create_table_schema(temp_filename1,target_table,int_as_string)
    db.query(tbl_obj["statement"])
    types = tbl_obj["types"]
    row_num = 0
    print ("inserting data...")
    if bulk_ingest:
        os.system("sed '1d' %s > temp.csv2" % temp_filename1)
        print("psql -h %s -p %s -U %s -W -d %s -c \"\copy %s FROM '%s/%s' WITH (FORMAT csv);\"" % (dbconf.hostname,dbconf.port,dbconf.username,dbconf.database,target_table,os.getcwd(),"temp.csv2"))
        os.system("psql -h %s -p %s -U %s -W -d %s -c \"\copy %s FROM '%s/%s' WITH (FORMAT csv);\"" % (dbconf.hostname,dbconf.port,dbconf.username,dbconf.database,target_table,os.getcwd(),"temp.csv2"))
        os.system("rm temp.csv2 %s" % temp_filename1)    

def get_types(csv_filename,int_as_string): 
    results = []
    df = pandas.read_csv(csv_filename)
    first_row = list(df.iloc[1])
    types = df.dtypes
    for i in range(0,len(types)):
        item = str(types[i])
        ctype = ""
        if "float" in "%s" % item:
           ctype = "float"
        elif "int" in "%s" % item and not int_as_string:
           ctype = "int"
           if(sum(df[df.columns[i]] > 214748364) > 0):
               ctype = 'bigint'
        elif ("POINT" in str(first_row[i]).upper() or "POLYGON" in str(first_row[i]).upper() or "POINT" in str(first_row[i]).upper() or "POLYGON" in str(first_row[i]).upper()) and "(" in str(first_row[i]) and ")" in str(first_row[i]):
           ctype = "geometry"
        else:
           max_length = int(sorted(df[list(df.columns)[i]].apply(lambda x: len(str(x))).tolist(),reverse=True)[0]*1.5)
           ctype = "varchar(%i)" % max_length
        results.append(ctype)
    return results

def create_table_schema(csv_filename, table_name, int_as_string=0):
    reader = csv.reader(open(csv_filename,'r'))
    header = next(reader)
    header[0] = header[0].replace("\xef\xbb\xbf","")
    data = next(reader)
    types = get_types(csv_filename,int_as_string)
    statement = "CREATE TABLE %s (" % table_name 
    for i in range(0,len(data)):
        statement = statement + header[i] + " " + types[i] + ","
    statement = statement[0:len(statement)-1] + ");"
    return {"types" : types, "statement" : statement}

def print_select(query,counts_only=0):
    cur = db.query(query) 
    if not counts_only:
        for item in cur.getresult():
            print(item)
    print(len(cur.getresult()))

#select a list of columns corresponding to the given index_cat:index_value in the given table in the given database.  Optionally provide a results dictionary to be updated with the results
def select_index(columns,index_cat,index_val,table,result={}):
    global db
    select_str = "SELECT %s from %s"  % (",".join(columns),table)
    if index_val != 'ALL':
        select_str = select_str + (" WHERE %s = '%s'" % (index_cat,index_val))

    qres  = db.query(select_str).getresult()
    for rtuple in qres:
        for i in range(0,len(rtuple)):
            result[columns[i]] = is_null(rtuple[i],"")
    return result

 
def add_geom(table,lon_col="",lat_col="",geom_col = "geometry",crs=4326):
    print ("Adding %s and spatial indices..." % geom_col)
    db.query("ALTER TABLE %s ADD COLUMN gid serial PRIMARY KEY;" % table)

    if lat_col != "":
       db.query("DELETE FROM %s WHERE %s is NULL" % (table,lat_col))
    if lon_col != "":
       db.query("DELETE FROM %s WHERE %s is NULL" % (table,lon_col))
    
    db.query("ALTER TABLE %s ADD COLUMN %s geometry(POINT,%s);" % (table,geom_col,crs))
    db.query("UPDATE %s SET %s = ST_SetSRID(ST_MakePoint(%s,%s),%s);" % (table,geom_col,lon_col,lat_col,crs))
    db.query("ALTER TABLE %s ADD COLUMN %s geography;" % (table,geom_col))
    db.query("UPDATE %s SET %s = ST_POINT(%s,%s);" % (table,geom_col,lon_col,lat_col))
#    db.query("CREATE INDEX %s ON %s USING GIST(%s);" % (idx_name,table,geom_col))

#given a list of tuples and a list of column names, return the corresponding dict
def tuples_to_dict(tuple_list,colname_list):
    results = {item : [] for item in colname_list}
    for r in tuple_list:
       for i in range(0,len(r)):
           results[colname_list[i]].append(r[i])
    return results


#get comma separated variables given in select_vars string from given table in a given radius (miles) around a given lat/lon
def select_radius(lat,lon,radius,select_vars,table,where_clause="",query_crs="4326",geom_crs="4326",to_crs="",result={}):
    global db
    #mile_mult = 1609.34
    if geom_crs == "2264" or to_crs == "2264":
       mile_mult = 5280

    if type(select_vars) == str:
        select_vars = select_vars.split(",")
    if not to_crs == "": 
        sql_cmd = "SELECT %s FROM %s WHERE ST_Distance(ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s),%s),%s),ST_Transform(ST_SetSRID(geometry,%s),%s)) <= %.4f * %i" % (",".join(select_vars),table,str(lon),str(lat),query_crs,to_crs,geom_crs,to_crs,radius,mile_mult)
    else:
        sql_cmd = "SELECT %s FROM %s WHERE ST_Distance(ST_SetSRID(ST_MakePoint(%s, %s),%s),ST_SetSRID(geometry,%s)) <= %.4f * %.2f" % (",".join(select_vars),table,str(lon),str(lat),query_crs,geom_crs,radius,mile_mult)
    if not where_clause == "":
        sql_cmd = sql_cmd + " AND " + where_clause
    sql_cmd = sql_cmd + ";"

    results = db.query(sql_cmd).getresult()
    temp_result = tuples_to_dict(results,clean_functions_from_strings(select_vars))
    for col in temp_result:
        result[col] = temp_result[col]
    return result

#perform psql query and return results as a pandas dataframe
def query(sql_cmd):
    query = db.query(sql_cmd)
    fields = query.listfields()
    result = {item : [] for item in fields}
    for row in query.dictresult():
       for cat in fields:
           result[cat].append(row[cat])
    return pandas.DataFrame(result)
 
#given a table name, retrieve all the column names, optionally converting the geometry col with ST_AsText(geometry)
def get_column_names(table,conv_geom=0,crs="2264",to_crs="4326"):
    global db
    results = []
    col_sel = "SELECT column_name FROM information_schema.columns WHERE table_name = '%s';" % table
    qres  = db.query(col_sel).getresult()
    for rtuple in qres:
        for i in range(0,len(rtuple)):
            tres = rtuple[0]
            if conv_geom and tres.lower() == "geometry":
                tres = "ST_AsText(ST_Transform(ST_SetSRID(geometry,%s),%s))" % (crs,to_crs)
            results.append(tres) 
    return results

#clean transformation functions from list of strings
def clean_functions_from_strings(list_of_strings):
    results = []
    functions_etc = ["ST_AsText","ST_Transform","ST_SetSRID","(",")","2264","4326",","]
    for i in range(0,len(list_of_strings)):
        item = list_of_strings[i]
        for clean_item in functions_etc:
            item = item.replace(clean_item,"")
        results.append(item)
    return results
