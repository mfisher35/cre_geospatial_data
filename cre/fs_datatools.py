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

import geopandas as gpd
import pandas as pd
import fs_spatialtools as su
import census
import json
import sys
import ast
import csv
import os
from dateutil import parser
import datetime
import time
import numpy as np
import re
import urllib
import random
from multiprocessing import Pool, cpu_count

#Apply a function to a chunk of rows in a dataframe, for use in conjunction with parallel_apply
def process_papply_chunk(args):
    df_vec, function, chunk, params, total_chnk = args
    if type(df_vec) == pd.DataFrame:
        df_vec = df_vec.apply(lambda row: function(row,**params),axis=1)    
    elif type(df_vec) == pd.Series:
        df_vec = df_vec.apply(lambda x: function(x,**params))    
    su.report_progress_pll(total_chnk) 
    return df_vec

#do pd apply using multiprocessing module in parallel
def parallel_apply(df_vec,apply_func,params={},chunk_size=1000,num_processes=None):
    progress = open("progress.tmp","w")
    progress.write("0")
    progress.close()

    function_list = []
    df_vec_list = []
    chunk_list = []
    params_list = []
    total_list = []
    params_list = []

    print("Generating Parameters...") 
    print("Source DF N =",len(df_vec))
    num = 0
    chnk_num = 0
    n = len(df_vec)
    total_chnk = int(float(n)/float(chunk_size))
    su.print_status(1,total_chnk)

    # Generate chunks
    while num  < n:
        chnk_num = chnk_num + 1
        if num + chunk_size <= n:
            function_list.append(apply_func)
            df_vec_list.append(df_vec.iloc[num:num+chunk_size])
            chunk_list.append(chnk_num)
            total_list.append(total_chnk)
            params_list.append(params)
        else:
            if not num > n:
                function_list.append(apply_func)
                chunk_list.append(chnk_num)
                total_list.append(total_chnk)
                df_vec_list.append(df_vec.iloc[num:n])
                params_list.append(params)
        num = num + chunk_size


    args = zip(df_vec_list,function_list,chunk_list,params_list,total_list)
    # If num_processes is not specified, default to minimum(#columns, #machine-cores)
    if num_processes==None:
        num_processes = min(len(df_vec), cpu_count())
    
    print("Starting %s Processes (%i chunks of %i)..." % (num_processes,len(function_list),chunk_size))
    # 'with' context manager takes care of pool.close() and pool.join() for us
    with Pool(num_processes) as pool:
        # pool.map returns results as a list
        results_list = pool.map(process_papply_chunk, args)
        # return list of processed columns, concatenated together as a new dataframe
        return pd.concat(results_list,axis=0)



#return param list as booleans
def get_params(plist,params={}):
    for param in plist:
        params[param] = are_any_equal(param,sys.argv)
    return params

#clean html symbols from text
def clean_html(raw_html):
  cleanr = re.compile('<.*?>')
  clean_text = re.sub(cleanr, ' ', raw_html)
  clean_text = clean_text.replace("&nbsp;"," ")
  clean_text = re.sub(r"\s\s+", " ", clean_text)
  return clean_text

#read first n lines of given file
def head(filename,n=10):
    try:
        f = open(filename,'r')
        head = [next(f) for x in range(n)]
        return "".join(head)
    except:
        print("Error!, are you sure %s exists and is over %i lines?" % (filename,n))

#get unix time for given number of past days from today or other given date (given as a tuple/list start=[year,month,day]
def get_past_uxtime(start="today",days_before=365):
    if start == "today": 
        return int(time.mktime((datetime.datetime.now() - datetime.timedelta(days=days_before)).timetuple()))
    else:
        return int(time.mktime((datetime.datetime(start[0],start[1],start[2]) - datetime.timedelta(days=days_before)).timetuple()))

#given a csv file, find the delimiting character from the list of possible separators
def get_sep(in_filename):
    possible_seps = [",",";","\t"]
    with open(in_filename) as f:
        f.seek(0)
        header = (f.read(1000)).split('\n')[0]
    if "{" in header and 'geometry' in header and 'type' in header and 'coordinates' in header and ":" in header:
       return "geojson"
    for sep in possible_seps:
       if sep in header:
          return sep

#given a df and a known column name, locate the correct row number which contains that name
def find_header(df,known_col_name):
    for i in range(0,len(df)):
        if are_any_equal(known_col_name,df.iloc[i],clean=1):
            return i
    print(known_col_name,'Not Found!')


#given a string, trim it to the given number of maximum characters, if necessary    
def trim_string(in_str,max_chars):
    test = str(in_str)
    if len(test) > max_chars:
       return test[0:max_chars-1]
    else:
       return in_str

#find column name that contains the given substringn
def find_col(substr,df,case_sensitive=0):
    if not case_sensitive:
        for col in df.columns:
            if substr.upper() in col.upper():
                return col
    else:
        for col in df.columns:
            if substr in col:
                return col
    print("column name containing %s not found!" % substr)

#given a dataframe, trim all the strings to a maximum given size
def trim_all_strings(df,max_chars):
    for col in get_string_columns(df):
        df[col] = df[col].apply(trim_string,args=(max_chars,))
    return df

#given a list and index number, if the index is contained in the list, return the value, else return given default value
def ix_value_or_default(tlist,ix,default=""):
    if len(tlist) > ix:
        return(tlist[ix])
    else:
        return default


#given a dictionary and key, if the key is contained in the dictionary, return the value, else return given default value
def value_or_default(tdict,key,default=""):
    if key in tdict:
        return(tdict[key])
    else:
        return default

#given a dictionary and key, if any key in the dictionary is a substring of the given key, return the corresponding value, else return given default value
def value_or_default_partialkey(tdict,in_key,default=""):
    for dict_key in tdict:
        if str(dict_key).lower() in str(in_key).lower():
            return(tdict[dict_key])
    return default

#given two numbers divide the first (numerator) by the second (denominator) safely without getting infinities if possible, return given default value if division fails
def divide_safely(numer,denom,default=""):
    try:
        if float(numer) == 0:
            return 0
        elif float(denom) == 0:
            return default
        else:
            return float(numer)/float(denom)
    except:
        return default

#concat values if they are non-null with the given separator
def concat_non_nulls(value_list,sep=" "):
    res_list = [item for item in value_list if not is_null(item)]
    return sep.join(res_list)


#concat two strings if neither is nan, otherwise return whichever is not null if possible
def cat_strings(str1,str2,symbol=" "):
   if not is_null(str1) and not is_null(str2):
       return str(str1) + symbol + str(str2)
   elif is_null(str2) and not is_null(str1):
       return str(str1)
   elif is_null(str1) and not is_null(str2):
       return str(str2)
   else:
       return str1
   

#concat multiple string cols of a df into a single columns with optional symbol and optionally dropping the orginal columns 
# example (col list: ["first_owner","second_owner"] into new_col_name "owners"  with symbol "&" and dropping original columns:
#
#
#  first_owner,last_owner
#  matt,terry
#
# becomes:
#
# owners:
# matt & terry
# 
# (where columns first_owner and last_owner have been dropped
def cat_cols(df,col_list,new_col_name,symbol=" ",drop_orig=0):
    cat_cols_f = [find_matching_col(df,item) for item in col_list]
    remove_cols = []
    df[new_col_name] = [np.nan]*len(df)
    for i in range(0,len(cat_cols_f)):
        col = cat_cols_f[i]
        if col != -1:
            if drop_orig:
                remove_cols.append(col)
            df[new_col_name] = df.apply(lambda x: cat_strings(x[new_col_name],x[col],symbol),axis=1)
        else:
            print ("Warning!, Column %s Not Found!" % col_list[i])

    keeps = [col for col in list(df.columns) if not col in remove_cols]
    return df[keeps]

 
#replace the columns names of a given data file with those provided in the norm_dict dictionary. Special directives in the converted column names can be given in the norm_dict. For example "Z":"A__SPACE__B" results in a new column "Z" that contains the value of column A then a space then the value of column B (A and B are then dropped if drop_orig=1)
def normalize_categories(input_data,norm_dict,sep=",",drop_orig=0):
    print("Normalizing Categories...")
    gdf,dtype = load_df_or_csv(input_data)
    rename_dict = {}
    print("Done.")
    remove_cols = []
    for key in list(norm_dict.keys()):
        print("Renaming %s to %s" % (norm_dict[key].replace("__PLUS__","").replace("__SPACE__"," ").replace("__DATE1__"," ").replace("__AND__"," & ").replace("__DATE2__","").replace("__DIV__",""),key))
        val = norm_dict[key]
        failed = 0
        if "__PLUS__" in val:
            vals = val.split("__PLUS__")
            for i in range(0,len(vals)):
               dfcol = find_matching_col(gdf,vals[i])
               if dfcol != -1:
                   if drop_orig:
                      remove_cols.append(dfcol)
                   if i == 0:
                      temp_col = gdf[dfcol] 
                   else:
                      temp_col = temp_col + gdf[dfcol] 
               else:
                   print ("Warning!, Column %s Not Found!" % vals[i])
                   failed = 1

            if failed == 0:
                gdf[key] = temp_col         
        if "__MINUS__" in val:
            vals = val.split("__MINUS__")
            for i in range(0,len(vals)):
               dfcol = find_matching_col(gdf,vals[i])
               if dfcol != -1:
                   if drop_orig:
                      remove_cols.append(dfcol)
                   if i == 0:
                      temp_col = gdf[dfcol] 
                   else:
                      temp_col = temp_col.apply(lambda x: force_numeric(x)) - gdf.apply[dfcol].apply(lambda x: force_numeric(x)) 
               else:
                   print ("Warning!, Column %s Not Found!" % vals[i])
                   failed = 1

            if failed == 0:
                gdf[key] = temp_col         

        elif "__DIV__" in val:
            vals = val.split("__DIV__")
            gdf[key] = divide_safely(gdf[vals[0]],gdf[vals[1]])
        elif "__SPACE__" in val:
            gdf = cat_cols(gdf,val.split("__SPACE__"),key,symbol=" ",drop_orig=drop_orig)
        elif "__AND__" in val:
            gdf = cat_cols(gdf,val.split("__AND__"),key,symbol=" & ",drop_orig=drop_orig)
        elif "__DATE1__" in val:
            val = val.replace("__DATE1__","")
            if val in list(gdf.columns):
                gdf[val] = gdf[val].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d') if len(x) > 6 else '')
                rename_dict[val] = key
            else:
                print ("Warning!, Column %s Not Found!" % val)
        elif "__DATE2__" in val:
            val = val.replace("__DATE2__","")
            if val in list(gdf.columns):
                gdf[val] = gdf[val].apply(lambda x: pd.to_datetime(str(x)) if len(x) > 6 else '')
                rename_dict[val] = key
            else:
                print ("Warning!, Column %s Not Found!" % val)
                  
        else:
            dfcol = find_matching_col(gdf,val)
            if dfcol != -1:
                rename_dict[dfcol] = key 
    keeps = []
    gdf = gdf.rename(columns=rename_dict)
    for col in list(gdf.columns):
       if not col in remove_cols:
           keeps.append(col)
    
    print("Finished Normalizing")
    return gdf[keeps]

#converts a string in camelCase to snake_case
def convert_camel_case(in_str):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', in_str)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

# return the edit distance between two strings (number of single edits to make string1 into string2
def edit_distance(string1,string2):
   m = len(string1)
   n = len(string2)
   d = []
   tempd = []

   for i in range(0,n+1):
     tempd.append(0)
   for i in range(0,m+1):
     d.append(tempd[:])

   for i in range(0,m+1):
     d[i][0] = i
     # the distance of any first string to an empty second string
     # (transforming the string of the first i characters of s into
     # the empty string requires i deletions)
   for j in range(0,n+1):
     d[0][j] = j
     # the distance of any second string to an empty first string
   for j in range(1,n+1):
     for i in range(1,m+1):
       if string1[i-1] == string2[j-1]:  
         d[i][j] = d[i-1][j-1]      
       else:
         d[i][j] = min(d[i-1][j] + 1, d[i][j-1] + 1, d[i-1][j-1] + 1 )

   return d[m][n]


#given a list of strings, return the value of minimum edit distance and the string corresponding to it 
def get_minimum_edit_distance(ref_string,list_strings):
    cmin = float("Inf")
    min_string = None

    for item in list_strings:
       cdist = edit_distance(ref_string, item)
       if cdist < cmin:
          min_string = item
          cmin = cdist
    
    return cmin,min_string

#split a dataframe into a dictionary of multiple dataframes via the values of a given column 
def split_df(df,col):
    result = {}
    for val in set(df[col].values):
        result[val] = df[df[col] == val]
    return result

#find and return the true name of a column when the case of the characters is not known
def find_matching_col(df,col):
    for icol in list(df.columns):
        if col.lower() == icol.lower():
            return icol
    print ("Warning!, Column %s Not Found!" % col)
    return -1

#clean values of data using foresite standard format
def std_clean(value,include_comma=1): 
    keeps = ["&",":",")","(","-","'","/","\\",".","_","."," ","-","/"]
    if include_comma:
        keeps.append(",")
    result = clean_specials(value,add_keeps=keeps,replace_dict={"\n" : " ", "\r" : " "},transform="u",strip=1,fix_multispace=1)
    return result
    
#determine if a given value type is numeric (if input is given as a string, e.g. value="123" this will return 0)
def is_numeric_type(value):
    types = [np.intc,np.intp,np.int8,np.int16,np.int32,np.int64,np.uint8,np.uint16,np.uint32,np.uint64,np.float_,np.float16,np.float32,np.float64,int,float]  
    types_str = ["int64","int","int_","int8","int16","int32","int64","uint8","uint16","uint32","uint64","float64","float","float_","float16","float32","float64","np.intc","np.intp","np.int8","np.int16","np.int32","np.int64","np.uint8","np.uint16","np.uint32","np.uint64","np.float_","np.float16","np.float32","np.float64"]
    if type(value) in types or value in types_str or value in types: 
        return 1
    return 0


#check if a given value could potentially become numeric, for example "3" would return true, while "shoe" would return false
def could_become_numeric(value,nan_false=1):
    if is_null(value) and nan_false:
        return 0
    try:
        a = float(value)
        return 1
    except:
        return 0
    return 0


#try to convert a value to numeric if possible
def convert_numeric(value,verbose=0):
    result = value
    try:
        if "." in str(result):
            result = float(result)
        else:
            result = int(result)
    except:
        if verbose:
            print("Warning!",value,"could not be converted!")
        return result
    return result

#clean all special characters from the values of a given string
def clean_specials(in_str,add_keeps=[],replace_dict={},lowercase_only=-1,transform="",strip=0,fix_multispace=1):
    if in_str == None or str(in_str) == "nan":
        return ''
    if is_numeric_type(in_str):
        return in_str
    if type(in_str) != str:
        in_str = str(in_str)

    for key in list(replace_dict.keys()):
        in_str = in_str.replace(key,replace_dict[key])
    result = ""
    keeps  = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","0","1","2","3","4","5","6","7","8","9","_"]
    keeps.extend(add_keeps)
    if lowercase_only < 0:
        keeps.extend([i.upper() for i in keeps[0:26]])
    for char in in_str:
       if char in keeps:
           result = result + char
    if "u" in transform.lower():
        result = result.upper()
    if "l" in transform.lower():
        result = result.lower()
    if strip:
        result = result.strip()
    if fix_multispace:
        result = re.sub(r"\s\s+", " ", result)
    return result

#given a string keep only the given characters. 
def keep_chars(in_str,keeps=[],case_sensitive=0):
    result = ""
    if not case_sensitive:
        keeps = [item.lower() for item in keeps]
    for i in range(0,len(in_str)):
        if in_str[i] in keeps or (in_str[i].lower() in keeps and not case_sensitive):
            result = result + in_str[i]
    return result 


#given a df, get the columns which are string or object types, excluding columns containing strings given in exclude list
def get_string_columns(df, excludes=['geometry','geom']):
    string_cols = [df.columns[i] for i in range(0,len(df.columns)) if not df.columns[i] in excludes and (df.dtypes[i] == object or df.dtypes[i] == str)]
    return string_cols

#clean all special characters from the values of a given dataframe or given csv filename using standard foresite format
def clean_values(input_data,exclude_cols=[]):
    df,dtype = load_df_or_csv(input_data)
    print("Cleaning Values...")
    for col in get_string_columns(df):
        print("    %s" % col)
        df[col] = df[col].apply(lambda x: std_clean(x))
    return df
 
#clean all special characters from the values of a given dataframe or given csv filename 
def clean_values_custom(input_data,add_keeps=[],replace_dict={},transform="u"):
    df,dtype = load_df_or_csv(input_data)
    print("Cleaning Values...")
    for col in get_string_columns(df):
        df[col] = df[col].apply(lambda x: clean_specials(x,add_keeps=add_keeps,replace_dict=replace_dict,transform=transform))
    return df
        
#remove special characters from the header of a given input (csv filename or dataframe)
def clean_header(input_obj,fix_lat_lon=1):
    if type(input_obj) == str:
        print("Cleaning Header of: %s" % filename)
        cfile = open(filename,'r')
        data = cfile.readlines()
        cfile.close()
        header = clean_specials(data[0].lower(),add_keeps=[",",";","\t"])
        if (not "latitude" in header or not "longitude" in header) and fix_lat_lon:
            header = header.replace(",x,",",longitude,")
            header = header.replace(",y,",",latitude,")
            if header[0:2] == "x,":
                header = "longitude" + header[1:len(header)] 
            if header[0:2] == "y,":
                header = "latitude" + header[1:len(header)] 
            if header[len(header)-2:len(header)] == ",x":
                header = header[0:len(header)-1] + "longitude"
            if header[len(header)-2:len(header)] == ",y":
                header = header[0:len(header)-1] + "latitude"
        data[0] = header + "\n"
        cfile = open(filename,'w')
        cfile.write("".join(data))
        cfile.close()
        print(filename,"Header Cleaned.")
    elif type(input_obj) == pd.DataFrame or type(input_obj) == gpd.geodataframe.GeoDataFrame:
        input_obj = input_obj.rename(str.lower, axis='columns')
        input_obj = input_obj.rename(clean_specials,axis='columns')
        return input_obj
    print("Done")

#given a list, a name for the list and a filename, write out the contents of the list in a human readable pythonic way
def write_list(in_list,list_name,filename):
    f = open(filename,'w')
    f.write('%s = ["%s",\n'  % (list_name,in_list[0]))
    for i in range(0,len(in_list)):
        if i % 1000 == 0:
            f.flush()
        f.write('"%s",\n' % in_list[i])
    f.write("]")
    f.close()

#given a dataframe prefix each column name with a given string
def prefix_header(df,prefix,skip=[],clean=1):
    header = list(df.columns)
    for i in range(0,len(header)):
        if not header[i] in skip:
            header[i] = prefix+header[i]
    if clean:
        census.clean_meta(header)
    df.columns = header

#return all elements in a that are not in b (elements that are in b are allowed if they are the "include" list)
def a_not_in_b(a,b,include=[]):
    return [ item for item in a if (not item in b) or item in include]

#give a list of strings, if it has multiple occurences of the same string, suffix the each with the occurence number.  For example: ["A","A","A","B","B","C"] would become ["A","A2","A3","B","B2","C"]. 
def suffix_strings_count(str_list):
    values = list(set(str_list))
    counts = {item : 0 for item in values}
    results = []
    for string in str_list:
       counts[string] += 1
       if counts[string] == 1:
           tresult = str(string)
       else:
           tresult = str(string) + str(counts[string])
       results.append(tresult)
    return results


#determine if a value is null or nan
def is_null(value):
    if len(str(value)) < 1 or str(value).upper() == "NAN" or value == None or value == np.nan or str(value).upper() == "POINT (0 0)":
       return True
    return False

#replace the (col,val) tuple pairs in the repl_list ((col,val),(col2,val2),....]) from the results of the given query
def query_repl_value(df,query,repl_list):
   try:
       res_df = df.query(query)
       if len(res_df) > 0:
           for i in range(0,len(res_df)):
               name = res_df.iloc[i].name
               for col,val in repl_list:
                    df.at[name,col] = val
           print("replaced %i values" % len(res_df))
       else:
           print("query: %s returned no results!" % query)
   except Exception as e:
       print(e)

#determine if input is a path to a csv file or a dataframe. In either case return a dataframe and corresponding type
def load_df_or_csv(obj,to_crs=""):
    if type(obj) == str:
       ctype = "str"
       df = su.load_file(obj,to_crs=to_crs)
       return df,ctype
    elif type(obj) == pd.DataFrame or type(obj) == gpd.geodataframe.GeoDataFrame or type(obj) == pd.DataFrame.Series:
       df = obj
       ctype = "df"
       return df,ctype
    else:
       print("Error Loading Object:",obj)


#if the given dtype is a string, write the given dataframe, otherwise simply return df
def return_or_write(df,dtype,orig):
    if dtype=="str":
        df.to_csv(orig,index=False)
    return df

#given two bool lists take the AND, e.g. [True, False, False, True]  AND [False,True,True, True] would give [False, False, False, True]
def and_lists(list1,list2):
    result = [a and b for a,b in zip(list1,list2)] 
    return result

#given a df and column, get the inidices of non-null values
def get_non_nulls(df,col):
    temp = df[col].apply(lambda x: not is_null(x))
    return temp

#remove all rows that contain a null value in the given column
def drop_nulls(df,col):
    idx = get_non_nulls(df,col)
    return df[idx]

#given a df and list of columns, get the max number of non-null values
def get_max_non_nulls(df,cols):
    cmax = 0
    for col in cols:
        non_nulls = get_non_nulls(df,col)
        if sum(non_nulls) > cmax:
            cmax = sum(non_nulls)
    return cmax

#convert list of dictionaries to dictionary of lists, example [{'a':1},{'a':2}] --> {'a' : [1,2] }
def lod_to_dol(lod):
    result = {}
    all_keys = set([])
    for cdict in lod:
        for key in list(cdict.keys()):
           all_keys.add(key)
    all_keys = list(all_keys)
    for cdict in lod:
        for key in all_keys:
           value = ""
           if key in cdict:
               value = cdict[key]
           if key in result:
               result[key].append(value)
           else:
               result[key] = [value]
    return result

#given two csv filenames or dataframes and the names of their common join columns output the fully joined csv, or specify which of the join file's columns to keep using cols=[..]
def join(in_obj,join_obj,ix_col_in,ix_col_join,cols=[],join_prefix="",out_filename="",overwrite=0,clean_ix_cols=0,drop_dups_in=0,drop_dups_join=0,drop_nulls_join=0,verbose=1):

    df_in,dtype1 =  load_df_or_csv(in_obj)
    df_join,dtype2 =  load_df_or_csv(join_obj)
    orig_n = len(df_in)
    orig_m = len(df_in.columns)

    if len(cols) == 0:
        cols = list(df_join.columns)

    if clean_ix_cols:    
        df_in = clean_index_col(df_in,ix_col_in)
        df_join = clean_index_col(df_join,ix_col_join)
           
    if drop_nulls_join:
        df_join = drop_nulls(df_join,ix_col_join)

    in_indices = df_in[ix_col_in].tolist()
    join_indices = df_join[ix_col_join].tolist()
    in_indices = set(in_indices)
    overlap = in_indices.intersection(set(join_indices))	
    diff_in = list(set(in_indices).difference(set(join_indices)))
    diff_join = list(set(join_indices).difference(set(in_indices)))

    if verbose:
       print("Num Input DF Indices: ", len(in_indices),"Num Shared Indices in Join DF:", len(overlap)," - %.2f%%" % (100.0*float(len(overlap)) / float(len(in_indices))),"Overlap")
       if len(diff_in) > 0:
           print("In input df, not in join df: %s" % (diff_in[random.randint(0,len(diff_in)-1)])) 
       if len(diff_join) > 0:
           print("In join df, not in input df: %s" % (diff_join[random.randint(0,len(diff_join)-1)])) 
    if not overwrite:
        cols = [col for col in cols if not col in list(df_in.columns)]
    else:
        intersect = intersection(cols,list(df_in.columns))
        keep_in = [col for col in list(df_in.columns) if not col in intersect or col == ix_col_in]
        df_in = df_in[keep_in]

    if drop_dups_in:
        df_in = df_in.drop_duplicates([ix_col_in], keep='last')  
    if drop_dups_join:
        df_join = df_join.drop_duplicates([ix_col_join], keep='last')  

    cols.append(ix_col_join)
    df_join = df_join[list(set(cols))]

    if join_prefix != "":
        prefix_header(df_join,join_prefix+"_",skip=[ix_col_join])
    
    df_join = df_join.rename(columns={ix_col_join : ix_col_in})
    df_in = df_in.join(df_join.set_index(ix_col_in), on=ix_col_in)
    cols.remove(ix_col_join)
    num_matches =  get_max_non_nulls(df_in,cols)
    if verbose:
       print("Non-Null Rows Matched on Join Using %s: %i" % (ix_col_in,num_matches))
       print("Orig N: %i, New N: %i" % (orig_n,len(df_in)))
       print("Orig m: %i, New m: %i" % (orig_m,len(df_in.columns)))
    if len(cols) < 1:
       print("Warning! No Non-Overlapping valid categories detected, nothing has been joined!\nIf you wish to overwrite the existing columns in the input DF with columns from the join DF, set overwrite to 1")

    if out_filename == "" and dtype1 == "str":
        out_filename = in_obj
    if not out_filename == "":
        df_in.to_csv(out_filename,index=False)
        print("%s written" % out_filename)
    return df_in


#determine the number of lines in a file
def num_rows(input_obj):
    if type(input_obj) == str:
        with open(input_obj) as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    elif type(input_obj) == pd.DataFrame or type(input_obj) == gpd.geodataframe.GeoDataFrame:
        return len(input_obj)
    else:
        print("Cannot determine the number of rows of %s" % type(input_obj))

#update a given dictionary key with the given value. If the key doesn't exist a new list is declared
def update_dict(rdict,key,value):
    if key in list(rdict.keys()):
        rdict[key].append(value)
    else:
        rdict[key] = [value]
    return rdict

#read a csv file and return a dictionary.  Alternative to dict(pd.read_csv(''))
def read_csv_dict(in_filename):
    result = {}
    reader = csv.DictReader(open(in_filename))
    for row in reader:
        for col in row:
            update_dict(result,col,row[col]) 
    return result

#convert geojson geom json to WKT
def convert_geojson_str(geo_str):
    result = ""
    geo_json =  ast.literal_eval(geo_str)
    if geo_json['type'].lower() == "point":
       result = geo_json['type'].upper() + "(" + str(geo_json['coordinates'][0]) + " " + str(geo_json['coordinates'][1]) + ")"
       return result
    else:
       print("Error, Shape Type: %s is Unrecognized" % geo_json['type'])

#convert geojson to csv file. 
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
    

#keep only designated characters to remove nonstandard ones
#example add keeps = [".","-","\n"," ","&","'",'"',","_","]","[",")","(","=","+","/","\\"]
def clean_file(in_filename,add_keeps=[],replace_dict={}):
    text = clean_specials(open(in_filename,"r").read(),add_keeps,replace_dict)
    outfile = open(in_filename,'w')
    outfile.write(text)
    outfile.close()
    
#given a file and street, city, state and zip column names go through and geocode each row, output is a dataframe
def geocode_lat_lon(in_filename,street_col,city_col,state_col,zip_col):
    df = su.load_file(in_filename)
    latlongs = su.geocode_addrs_lat_long(df[street_col].tolist(), df[city_col].tolist(), df[state_col].tolist(), df[zip_col].astype(str).tolist())
    return df.assign(**latlongs) 


#given a set of directories and target data filenames, determine what column names they have in common  
def find_all_overlaps(dirs,data_filenames,case_sensitive=0,verbose=1):
    results = {}
    for cdir in dirs: 
        files = os.listdir(cdir)
        for cfile in files:
            if cfile in data_filenames:
                path = cdir+cfile 
                df = pd.read_csv(path,nrows=5)
                if case_sensitive:
                    cols = set([x.lower() for x in list(df.columns)])
                else:
                    cols = set(list(df.columns))
                if verbose:
                    print("\n"+ path)
                    print(cols)
                    print("\n")
                if cfile in list(results.keys()):
                   results[cfile] = results[cfile].intersection(cols)
                else:
                   results[cfile] = cols
    return results

#given a list of folders and a common data filename in those folders, find which files do not contain the target column name
def find_missing_col(dirs,data_filename,target_col):
   for cdir in dirs:
       files = os.listdir(cdir)
       for cfile in files:
           if data_filename in cfile:
               path = cdir+cfile
               df = pd.read_csv(path,nrows=5)
               if not target_col in list(df.columns):
                   print(target_col,"missing from",path)

#return list of intersecting elements of list a and b 
def intersection(a,b):
    return list(set(a).intersection(set(b)))


#function which returns true if the addresses are similar
def are_addresses_similar(addr1,addr2):
    wordmatch_thresh = 3
    numbers1 = re.findall(r'[0-9]+',addr1)
    numbers2 = re.findall(r'[0-9]+',addr2)
    for num1 in numbers1:
        if not num1 in numbers2:
            return False 
    set1 = set([item for item in addr1.split(" ") if len(item) > 3])
    set2 = set([item for item in addr2.split(" ") if len(item) > 3])
    if len(set1.intersection(set2)) <= wordmatch_thresh:
        return False
    #print(numbers1,numbers2)
    return True
    
#drop near dupes via matching addresses - drop any row where a previous row in the df has a similar address for the given column name.  For example drop_dups on streetx below, the last row will be removed:
# Street                       Name           
# 1324 Swan St                 A shop       
# 1408 North Hills St          Another shop       
# 1408 North Hills Street         Yet Another Name       
def drop_addr_dupes(df,addr_column,verbose=1):
    keep_ix = []
    for i in range(1,len(df)):
        cur_ix = 0
        addr1 = df.iloc[i][addr_column]
        temp = df[addr_column].iloc[0:i-1]
        matches = temp.apply(lambda addr2: are_addresses_similar(addr1,addr2))
        if sum(matches) == 0:
            cur_ix = 1
        try:
             if not cur_ix and verbose:
                 print(temp[matches.tolist().index(1)] + " | " +df[addr_column].iloc[i])
        except Exception as e:
            a=1
        keep_ix.append(cur_ix)
    return df.iloc[keep_ix]

#concat a list of filenames of csv files, optionally keep only the cols in the keep_cols list
def concat_csvs(file_list,out_filename,keep_cols=[],drop_dups=0):
    dfs = []
    for cfile in file_list:
        print("Loading %s..." % cfile)
        if len(keep_cols) > 0:
            cdf = pd.read_csv(cfile)
            ccols = list(set(keep_cols).intersection(set(cdf.columns)))
            dfs.append(cdf[ccols])
        else:
            dfs.append(pd.read_csv(cfile))
    df_final = pd.concat(dfs,ignore_index=True)
    if drop_dups:
        df_final = df_final.drop_duplicates()
    df_final.to_csv(out_filename,index=False)
    return df_final

#concat a list of dataframes, optionally keep only the cols in the keep_cols list
def concat_dfs(dfs,out_filename="",keep_cols=[],force_unique_indices=1):
   if len(keep_cols) > 0:
       for i in range(0,len(dfs)):
           dfs[i] = dfs[i][list(set(keep_cols).intersection(set(dfs[i].columns)))]
   if force_unique_indices:
       df_final = pd.concat(dfs,ignore_index=1)
   else:
       df_final = pd.concat(dfs)
   if not out_filename == "":
       df_final.to_csv(out_filename,index=False)
   return df_final

#add an object id to an existing dataframe
def add_obj_id(gdf,source_name):
    ids = [ source_name+"_"+ str(i) for i in range(1,len(gdf)+1)]
    gdf['object_id'] = ids
    return gdf
 
#given the path to a csv file, convert it to a geojson
def csv_to_geojson(in_filename,drop_null_geoms=0):
    df = su.load_file(in_filename,to_crs="")
    out_filename = in_filename.replace(".csv",".geojson")
    su.write_geojson(df,out_filename,drop_null_geom=drop_null_geoms)
    return out_filename

#given a file path get the corresponding county 
#returns standard YYYY-MM-DD string, month, year or unix time automatically from most datestring types, with options "S","M","Y", and "U", respectively
def convert_date(x,option="M"):
    try:
        dt = parser.parse(str(x))
        if not str(x) == "nan" and option=="S":
            return str(dt.date())
        elif not str(x) == "nan" and option=="M":
            return dt.month
        elif not str(x) == "nan" and option=="Y":
            return dt.year
        elif not str(x) == "nan" and option=="U":
            return int((dt - datetime.datetime(1970,1,1,tzinfo=dt.tzinfo)).total_seconds())
        return ""
    except:
        return ""

#give a dataframe and a desired date string column, extract the month, year and unix time and add to the df
def add_date_info(df,date_col,prefix="",dtype="u",drop_invalid=1):
    if drop_invalid:
        df[date_col] = df[date_col].apply(remove_invalid_dates)

    if prefix == "":
        prefix = date_col.replace("_date","")
    if dtype.lower() == "m" or dtype.lower() == "a":
        df['%s_mo' % prefix] = df[date_col].apply(lambda x: convert_date(x,"M")) 
    if dtype.lower() == "y" or dtype.lower() == "a":
        df['%s_yr' % prefix] = df[date_col].apply(lambda x: convert_date(x,"Y")) 
    if dtype.lower() == "u" or dtype.lower() == "a":
        df['%s_uxtime' % prefix] = df[date_col].apply(lambda x: convert_date(x,"U")) 
    return df
 
#given a column return that colum removing all dashes and unnecessary decimal points
def clean_index_col(df,col_name):
    df[col_name] = df[col_name].apply(lambda x: str(x).replace("-","").replace(".0","").strip().lstrip("0").strip())
    return df

#given a pd/gpd df find the column with the least number of null values, optionally where the column name contains a keyword.  
def find_best_col(df,keyword=''):
    if keyword == '':
        cols = list(df.columns)
    else:
        cols = [item for item in list(df.columns) if keyword in item]
    best = ""
    best_min = len(df)*2
    for col in cols:
        nulls = sum(df[col].isna())
        if nulls < best_min:
            best = col
            best_min = nulls
    return best 


#if value for current row in df for col1 is empty/null, fill it with the value from col2, meant to be used as an apply function
# example: df = df.apply(fs_datatools.fill_column_na,args=('column1','column2'),axis=1)
def fill_column_na(row,col1,col2):
    if str(row[col1]).lower() == 'nan' or row[col1] == None:
        row[col1] = row[col2]
    return row


#return val2 if val1 is na
def val_if_na(val1,val2):
    if is_null(val1):
        return val2

#if columns overlap between the two given dataframes, fill n/a values of df1 with values from df2 for rows with the same ix_col value. If no cols list is provided, all df2 columns found in df1 will be considered.  To-Do: optimize this function.  
def fill_df_na(df1,df2,ix_col,cols="",verbose=0,drop_ix_nulls=1):
    if drop_ix_nulls:
        df1 = drop_nulls(df1,ix_col)
        df2 = drop_nulls(df2,ix_col)
    if cols == "":
        cols = [item for item in list(df2.columns) if not item == ix_col and item in list(df1.columns)]
    if type(cols) == str:
        cols = [cols]
    if sum(df1.duplicated(ix_col))>0:
        print("Warning! Index Column of DF1 Contains Duplicate ids")
    if sum(df2.duplicated(ix_col))>0:
        print("Warning! Index Column of DF2 Contains Duplicate ids")
    for col in cols:
        for i in range(0,len(df1)):
            if is_null(df1.iloc[i][col]):
                name1 = df1.iloc[i].name
                ix = df1.iloc[i][ix_col]
                ix2 = df2[df2[ix_col] == ix]
                if(len(ix2) > 0):
                    name2 = ix2.iloc[0].name
                    df1.at[name1,col] = df2.at[name2,col]
        if verbose:
            print(col,"finished.")

# replace the text matched by given regex expression with replacement text, meant to be used as an apply function
def replace_regex(value,match_regex,replacement_text):
    return re.sub(match_regex,str(value),replacement_text) 

#sort strings containing alphabetic and numeric items the way a human would expect e.g. ["aa120","aa9","aa90"] --> ["aa9","aa90","aa120"]
def natural_sort(clist): 
    convert = lambda text: int(text) if text.isdigit() else text.lower() 
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(clist, key = alphanum_key)


# replace the text given with replacement text, meant to be used as an apply function
def replace_text(value,match_text,replacement_text):
   return str(value).replace(match_text,replacement_text)


#return true if any of the items in the given list are found as a substring in the given value (optional minimum treshold of matched substrings)
def are_any_in(value,poss_list,min_num_matches=1):
    matches = 0
    val_upper = str(value).upper()
    for poss in poss_list:
       if poss.upper() in val_upper:
          matches = matches + 1
    if matches >= min_num_matches:
        return 1
    else:
        return 0

#return true if any of the items in the given list are equal to the given value
def are_any_equal(value,poss_list,case_sensitive=0,clean=0):
    for poss in poss_list:
       if not case_sensitive:
          poss =  str(poss).upper()
          value =  str(value).upper()
       if clean:
          poss = std_clean(poss)
          value = std_clean(value)
       if poss == value:
          return True
    return False


#returns true if substring is contained within the given df
def substr_in_df(df,val):
    for col in get_string_columns(df):
        if sum(df[col].str.upper().str.contains(val.upper()))>1:
             return 1
    return 0
 
#find the index of the nth occurence of an item in the given list that contains the substring. 
def find_index(str_list,substring,occurrence=1,verbose=0):
    try:
        count_occur = 0
        for i in range(0,len(str_list)):
            if substring in str_list[i]:
                count_occur = count_occur + 1
                if count_occur == occurrence:
                  return i
    except Exception as e:
        if verbose:
            print(e)
    if verbose:
        print("Error: %s not found!" % substring)
    return None

#given a df with 2 columns (numerator and denominator), return a an output column with the ratio (if neither is null/zero). Can be an apply function.
def apply_ratio(row,numer_col,denom_col,output_col):
    numer = convert_numeric(row[numer_col])
    denom = convert_numeric(row[denom_col])
    result = np.nan
    if not str(row[denom_col]).upper() == "NAN" and not str(row[numer_col]).upper() == "NAN" and not denom == 0 and is_numeric_type(numer) and is_numeric_type(denom):
       result = numer/denom
    row[output_col] = result
    return row

#strip any non-numeric characters except for decimal point, return numeric object. Can be used as an apply function
def force_numeric(value):
    keeps = ["0","1","2","3","4","5","6","7","8","9","."]
    temp = str(value)
    if is_null(value):
       return np.nan
    result = ""
    for i in range(0,len(temp)):
        if temp[i] in keeps:
            result = result + temp[i]
    result = convert_numeric(result,verbose=1)
    if is_numeric_type(result):
        return result
    else:
        return np.nan

#get all dates that are either invalid, before the before year or after the after year.  Can be used as an apply function
def is_invalid_date(val,before=1960,after=datetime.datetime.now().year + 5):
    try:
        dt = parser.parse(str(val))
        if dt.year < before or dt.year > after:
           return True
        else:
           return False
    except:
        return True

#remove all dates that are either invalid, before the before year or after the after year.  Can be used as an apply function
def remove_invalid_dates(val,before=1960,after=datetime.datetime.now().year):
    try:
        dt = parser.parse(str(val))
        if dt.year < before or dt.year > after:
           return np.nan
        else:
           return val
    except:
        return np.nan

#returns true if substring is contained within the given df
def substr_in_df(df,val):
    for col in get_string_columns(df):
        if sum(df[col].str.upper().str.contains(val.upper()))>1:
             return 1
    return 0

def bin_time_std(df,time_var):
    cmin = df[time_var].min()
    cmax = df[time_var].max()
    crange =  cmax - cmin
    df[time_var] = pd.to_datetime(df[time_var], unit='s')
    time_unit = 'YS'

    if crange < 8640000:
        time_unit = 'D'
    elif crange < 62899200:
        time_unit = 'W'
    elif crange < 125798400:
        time_unit = 'SMS'
    elif crange < 251596800:
        time_unit = 'MS'
    elif crange < 503193600:
        time_unit = 'QS'

    df = df.reset_index().set_index(time_var)
    df = df.resample(time_unit).sum()
    df[time_var] = [convert_date(item,"U") for item in df.index.values]
    return df 

#increment dictionary value every time value occurs, for example if value = "A" when rdict = {"A":4} then {"A":5} woll be returned
def increment_obj(rdict,value):
    if value in rdict:
        rdict[value] = rdict[value] + 1
    else:
        rdict[value] = 1
