import download
import os
import re

def get_shapefile(path,keywords=['poly']):
    result = ""
    for cfile in os.listdir(path):
        all_kw = True
        for kw in keywords:
           if not kw in cfile:
               all_kw = False
        if ".shp" in cfile and all_kw:
            return path+os.sep+cfile
    print("Path Containing Keywords: %s Not Found!" % ", ".join(keywords))


def extract_zip(zfile,folder):
    os.system("unzip %s -d %s" % (zfile,folder))


def get_parcels(county):
     root = "http://data.nconemap.gov"
     index_file = "nc_parcel_index.html" 
     os.system("rm -f %s" % index_file)
     if not os.path.isfile(index_file):
         url = root + "/downloads/vector/parcels/"
         download.download_binary(url,index_file)
     if os.path.isfile(index_file):
         html_file = open(index_file)
         html = html_file.read().split("\n")
         county_link = next((s for s in html if county.lower() in s.lower()), None)
         pattern = r'"([\w\/\.]+)"'
         m = re.search(pattern, county_link)
         url = root + (m.group().replace('"',''))
         path = os.getcwd() + os.sep + county
         filename = path + os.sep + url[url.rfind('/')+1:len(url)]
         if not os.path.exists(county):
             os.mkdir(county)
         html_file.close()
         if not os.path.isfile(filename):
             download.download_binary(url,filename)
             extract_zip(filename,path)
             return(get_shapefile(path))
         else:
             print("%s already exists!" % (county + os.sep + url[url.rfind('/')+1:len(url)])) 
             if (get_shapefile(path) != ""):
                 return (get_shapefile(path))
             else:
                 extract_zip(filename,path)
                 return(get_shape(path))

