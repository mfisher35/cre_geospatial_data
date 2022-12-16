import sys
import os
cwd = os.getcwd().split(os.sep)
cwd = "/".join(cwd[0:len(cwd)-1]) + os.sep + "General_Tools"
sys.path.append(cwd)
import txt_to_csv as txtparser
import download 
import datetime
import copy
import fs_spatialtools as su
import nc_parcel_shapes as nc_shp
import pandas
import fs_datatools 
import zipfile
import gzip 
import urllib
import requests
import time
import getpass
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from xvfbwrapper import Xvfb

#figure out what your OS's download directory is an return it
def get_download_dir():
    try:
        if os.path.exists("/home/"):
            tpath = "/home/" + getpass.getuser() + "/Downloads"
            if os.path.exists(tpath):
                return tpath
        if os.path.exists("/Users/"):
            tpath = "/Users/" + getpass.getuser() + "/Downloads"
            if os.path.exists(tpath):
                return tpath
    except:
        a=1
    print("Download Folder Not Found!")

#check if a file download has finished
def check_download_finished(filename,download_folder=""):
    old_size = -1 
    new_size = -2
    if download_folder == "":
        download_folder = get_download_dir()
    time.sleep(40)
    path = download_folder+os.sep+filename
    tpath = path + ".crdownload"
    count = 0
    if not os.path.exists(tpath):
        print("Warning! %s.crdownload temp file does not exist.  Please verify that this file is small (under 1 GB) and properly downloaded, otherwise there may have been a problem." % filename)
    count = 0
    while os.path.exists(tpath):
        count += 1
        time.sleep(3)
        try:
            size = os.path.getsize(tpath) 
            if count % 4 == 0:
                print("Downloading file: %s   %.2f MB transferred" % (filename,size/1e6))
        except:
            a = 1
    return path

def listdir_fullpath(c_dir,files=[]):
    for f in os.listdir(c_dir):
        cpath = os.path.join(c_dir, f)
        if os.path.isfile(cpath):
           files.append(cpath)
        elif os.path.isdir(cpath):
           listdir_fullpath(cpath,files)
    return files 
#    return [ for f in os.listdir(c_dir)]


#clean and normalize a given file with the given configuration
def clean_normalize(config,out_filename,target_file="",sep=",",exclude_cleans=["geometry"],return_gdf=0,temp_filename="",spatial=1):
    try:
        if temp_filename == "":
            folder = extract(out_filename) 
        else:
            folder = extract(temp_filename) 

        if folder != "":
            print('finding target:',target_file)
            target = find_target(folder,target_file=target_file)
            print('found',target)
            if 'norm_fields' in config.data_sources[out_filename]:
                gdf = fs_datatools.normalize_categories(target,config.data_sources[out_filename]['norm_fields'],sep=sep)
            else:
                gdf = su.load_file(target,to_crs="") 
        else:
            if 'norm_fields' in config.data_sources[out_filename]:
                if temp_filename == "":
                    gdf = fs_datatools.normalize_categories(out_filename,config.data_sources[out_filename]['norm_fields'],sep=sep)
                else:
                    gdf = fs_datatools.normalize_categories(temp_filename,config.data_sources[out_filename]['norm_fields'],sep=sep)
            else:
                gdf = su.load_file(out_filename,to_crs="") 


        out_filename = out_filename.replace(".shp",".csv")
        out_filename = out_filename.replace(".zip",".csv")
        out_filename = out_filename.replace(".xlsx",".csv")
        out_filename = out_filename.replace(".geojson",".csv")
        gdf = gdf.rename(str.lower, axis='columns')
        print("Cleaning Header")
        gdf = fs_datatools.clean_header(gdf)
        gdf = fs_datatools.clean_values(gdf,exclude_cols=exclude_cleans)
        n = fs_datatools.num_rows(gdf)
        if spatial:
            su.write_csv(gdf,out_filename,to_crs={'init': 'epsg:4326'}) 
        else:
            gdf.to_csv(out_filename,index=False)
        print("%s contains %i rows" % (out_filename,n-1))
    except Exception as e:
        print("Error!",e)
    if return_gdf:
        return gdf


def mkdir(folder,verbose=0):
    try:
        os.mkdir(folder)
    except Exception as e:
        if verbose:
            print(e)

#determines if a file is a zipfile. If it is, then the file is extracted  
def extract(in_filename,out_filename=""):
    if zipfile.is_zipfile(in_filename) and not ".xlsx" in in_filename:
        print("zip file detected")
        folder = "." + os.sep + in_filename.replace(".","")
        mkdir(folder,verbose=0)
        zfile = zipfile.ZipFile(in_filename)
        zfile.extractall(folder)
        return folder
    try:
        with gzip.open(in_filename, 'rb') as f:
            file_content = f.read()
            out_file = open(out_filename,'wb')
            out_file.write(file_content)
            print("gzip file detected")
            return out_filename
    except Exception as e:
        a = 1
    return ""

#any shapefile/csv file found in the given folder is returned.  If there are multiple shapefiles/csv files, the user will have to specify which file is desired from the archive as the "target_file'
def find_target(folder,target_file="",ext=[".shp",".csv",".txt",".tsv",".xls"]):
    target = ""
    files = listdir_fullpath(folder)
    target = ""
    for cfile in files:
       is_target=0
       for cex in ext:
           if cex in cfile[cfile.rfind(".")-1:len(cfile)] and (target_file == "" or target_file in cfile):
              is_target = 1 
       if is_target:
           target  = cfile
    return target

def get_parcel(config,county,key,state="NC"):
    if state == "NC":
        shapefile = nc_shp.get_parcels(county)
        gdf = fs_datatools.normalize_categories(shapefile,config.data_sources[key]['norm_fields'])
        gdf = gdf.rename(str.lower, axis='columns')
        su.write_csv(gdf,key)
    

def bulk_download(url_list,config,out_filename,col_example="",target_file="",exclude_cleans=['geometry'],sep=",",spatial=1):
    needs_clean = 0
    dfs = []
    folder = out_filename.replace(".","")
    try:
        os.mkdir(folder)
    except:
        a=1
    
    for url in url_list:
        cpath = folder + os.sep + url.split("/")[len(url.split("/"))-1]
        download.download_binary(url,cpath)
        if not col_example == "":
            temp_df = su.load_file(cpath)
            header_row = fs_datatools.find_header(temp_df,col_example)
            df = su.load_file(cpath,header_row=fs_datatools.find_header(su.load_file(cpath),col_example))
            needs_clean = 1
        else:
            df = clean_normalize(config,out_filename,target_file=target_file,sep=sep,exclude_cleans=exclude_cleans,return_gdf=1,temp_filename=cpath,spatial=spatial)
        dfs.append(df.copy())
    final_df = fs_datatools.concat_dfs(dfs,out_filename=out_filename)
    if needs_clean:
        final_df = clean_normalize(config,out_filename,target_file=target_file,sep=sep,exclude_cleans=exclude_cleans,return_gdf=1)
    return final_df


def get(config,out_filename,update=0,sep="",target_file="",exclude_cleans=["geometry"],col_example="",spatial=1):
    try:
        if 'link' in config.data_sources[out_filename].keys():
            download.download_link(config.data_sources[out_filename]['link']['index_url'],config.data_sources[out_filename]['link']['link_pattern'],out_filename)
        else:
            address = config.data_sources[out_filename]['url']
            if type(address) == list:
                gdf = bulk_download(address,config,out_filename,col_example=col_example,sep=sep,target_file=target_file,spatial=spatial) 
            if "nc_parcels:" in address:
                out_filename = get_parcel(config,address.replace("nc_parcels:",""),out_filename)
                clean_normalize(config,out_filename,target_file=target_file,sep=sep,exclude_cleans=exclude_cleans)
            elif not os.path.isfile(out_filename) or update:
                download.download_binary(address,out_filename)
                clean_normalize(config,out_filename,target_file=target_file,sep=sep,exclude_cleans=exclude_cleans)
    except Exception as e:
        print("Error!",e)    
    

 
def get_all(config):
     urls = config.get_urls()
     for key in urls:
        get(key)


#get all ids from a public arcgis database
def get_ids_arcgis(url,wc_query):
    q = urllib.parse.quote(wc_query)
    url = url + '/query?where=%s&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=true&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=pjson' % q
    result = requests.get(url).json()
    return result['objectIds']

#given ids for a given arcgis database, download all the rest of the corresponding data
def get_data_arcgis(url,ids,out_filename,wait_secs=5):
    sids = [str(nid) for nid in ids]
    url = url + ('/query?where=&text=&objectIds=%s&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=geojson' % urllib.parse.quote(",".join(sids)))
    print(url)
    result = requests.get(url).text
    outfile = open(out_filename,'w')
    outfile.write(result)
    outfile.close()
    print('%s written.' % out_filename)
    time.sleep(wait_secs)

#get the file at the url using an actual firefox instance
def get_browser(config,out_filename,download_filename,headless=1,download_folder="",update=0,sep="",target_file="",exclude_cleans=["geometry"],col_example=""):
    url = config.data_sources[out_filename]['url']
    print("Downloading %s" % url)
    if headless:
        try:
            vdisplay = Xvfb()
            vdisplay.start()
        except:
            err = 1
    cpath = os.getcwd().split(os.sep)
    chromedriver = os.sep.join(cpath[0:len(cpath)-1]) + "/General_Tools/chromedriver"
    os.environ["webdriver.chrome.driver"] = chromedriver
    chrome_options = Options()

    # this is the preference we're passing
    prefs = {'profile.default_content_setting_values.automatic_downloads': 1}
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(chromedriver,chrome_options=chrome_options)
    driver.get(url)
 
    download_path = check_download_finished(download_filename,download_folder)
    final_path = os.getcwd() + os.sep + out_filename
    os.system("mv %s %s" % (download_path,final_path))     

    if headless:
       try:
           vdisplay.stop
       except:
           err = 1
    clean_normalize(config,out_filename,target_file=target_file,sep=sep,exclude_cleans=exclude_cleans)
 

#given a large set of ids, individuall query the server in batches of n = batch_size, return the folder where they are stored
def get_data_arcgis_chunks(url,ids,batch_size,outfile):
    count = 0
    i = 0
    outfolder = outfile.replace(".","_")
    mkdir(outfolder)
    while i + batch_size < len(ids):
        outpath = outfolder + os.sep + outfile.replace(".","%i." % count)
        get_data_arcgis(url,ids[i:i+batch_size],outpath)
        i = i + batch_size
        count += 1
    
    if i < len(ids):
        outpath = outfolder + os.sep + outfile.replace(".","%i." % count)
        get_data_arcgis(url,ids[i:len(ids)],outpath)
    return outfolder

#step1 get all ids for a given arcgis database then step2 download all the rest of the corresponding data
def get_all_data_arcgis(url,batch_size,wc_query,outfile):
    ids = get_ids_arcgis(url,wc_query)
    outfolder = get_data_arcgis_chunks(url,ids,batch_size,outfile)
    files = [outfolder + os.sep + item for item in os.listdir(outfolder)]
    su.concat_files(files,outfile)   

#given a file that countains data from an arcgis server w/ column for objectid, query the main db again to see if there are any new/missing ids, if so download them and add them to the input file
def update_data_arcgis(input_filename,url,wc_query,id_col,batch_size=20):
    temp_filename = "temp.csv"
    df = su.load_file(input_filename,to_crs="")
    all_ids = get_ids_arcgis(url,wc_query)
    existing_ids = df[id_col].tolist()
    new_ids = [cid for cid in all_ids if not cid in existing_ids] 
    if len(new_ids) > 0:
        print('found new ids:', str(new_ids))

    outfolder = get_data_arcgis_chunks(url,new_ids,batch_size,"temp.geojson")
    files = [outfolder + os.sep + item for item in os.listdir(outfolder)]
    files.append(input_filename)
    su.concat_files(files,input_filename)   
 
