import os
from bs4 import BeautifulSoup
import ssl
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
from requests.packages.urllib3.util.ssl_ import create_urllib3_context
import urllib
import time
import download
import get_data
from html.parser import HTMLParser
import pandas as pd 
import re
import pickle 
import fs_datatools as  dt
from fs_spatialtools import file_len,print_status
import pprint
from selenium import webdriver
from xvfbwrapper import Xvfb
from selenium.webdriver.firefox.options import Options


CIPHERS = (
    'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+HIGH:'
    'DH+HIGH:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+HIGH:RSA+3DES:!aNULL:'
    '!eNULL:!MD5'
)

class DESAdapter(HTTPAdapter):
    """
    A TransportAdapter that re-enables 3DES support in Requests.
    """
    def create_ssl_context(self):
        #ctx = create_urllib3_context(ciphers=FORCED_CIPHERS)
        ctx = ssl.create_default_context()
        # allow TLS 1.0 and TLS 1.2 and later (disable SSLv3 and SSLv2)
        #ctx.options |= ssl.OP_NO_SSLv2
        #ctx.options |= ssl.OP_NO_SSLv3 
        #ctx.options |= ssl.OP_NO_TLSv1
        ctx.options |= ssl.OP_NO_TLSv1_2
        ctx.options |= ssl.OP_NO_TLSv1_1
        #ctx.options |= ssl.OP_NO_TLSv1_3
        ctx.set_ciphers( CIPHERS )
        #ctx.set_alpn_protocols(['http/1.1', 'spdy/2'])
        return ctx

    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context(ciphers=CIPHERS)
        kwargs['ssl_context'] = self.create_ssl_context()
        return super(DESAdapter, self).init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        context = create_urllib3_context(ciphers=CIPHERS)
        kwargs['ssl_context'] = self.create_ssl_context()
        return super(DESAdapter, self).proxy_manager_for(*args, **kwargs)



class DataHTMLParser(HTMLParser):

    def __init__(self):
        self.all_data = []
        HTMLParser.__init__(self)
    
    #add data item to all_data list
    def handle_data(self, data):
        self.all_data.append(data.strip())

    #return an item based on the target text and number of items to skip
    def return_schema_item(self,data_cat_text,data_val_skips,occur_num,all_categories,break_char=""):
       try:
           if data_val_skips == "*":
               idx = dt.find_index(self.all_data,data_cat_text,occurrence=occur_num)+1
               result = ""
               while not self.all_data[idx] in all_categories and idx < len(self.all_data)-1:
                   result = result + " " + self.all_data[idx]
                   idx = idx + 1
               return result
           else:
               idx = dt.find_index(self.all_data,data_cat_text,occurrence=occur_num)
               if idx == None:
                   return ""
               result = self.all_data[idx+int(data_val_skips)]
               if int(data_val_skips) == 0:
                   result = result.replace(data_cat_text,"")
               if not result in all_categories:
                   return result
               return ""
       except Exception as  e:
           print(e)
           print("Invalid Schema: Item %s" % data_cat_text)
           return ""
       return ""

    #preview all_data list of parsed data values
    def preview(self):
        print("\n".join(self.all_data))
        
#start a des session using the DES Adapter Class
def start_des_session(url):
    proxies={}
    session = requests.session()
    session.mount(url, DESAdapter())
    return session


#extract a table from the given html text with the given table id into a pandas dataframe.  If the column names are arranged vertically, use column_axis= 'y' or horizontally use 'x'.
def extract_table(html,target="",columns_axis="x",prefix="",col_loc=0):
    if len(prefix) > 0:
        prefix = prefix + "_"

    if target != "":
        tables = re.findall(r'<TABLE ID="%s".+?</TABLE>' % target,html.upper(),re.DOTALL)
    else:
        tables = re.findall(r'<TABLE.+?</TABLE>',html.upper(),re.DOTALL)

    if len(tables) > 0:
        dfs = pd.read_html(tables[0])
        if len(dfs) > 0:
            df = dfs[0]
            if columns_axis.lower() == 'x':
                df.columns = df.iloc[col_loc] 
                df = df.iloc[col_loc+1:len(df)]
                good_cols = [col for col in df.columns if str(col).lower() != 'nan']
                return df[good_cols].rename(columns={col : prefix+dt.clean_specials(col,replace_dict={' ':'_','#':'num'}) for col in good_cols}).dropna(axis=0, how='all')
            elif columns_axis.lower() == 'y':
                df = df.T
                df.columns = df.iloc[0] 
                df = df.iloc[1:len(df)]
                good_cols = [col for col in df.columns if str(col).lower() != 'nan']
                return df[good_cols].rename(columns={col : prefix+dt.clean_specials(col,replace_dict={' ':'_','#':'num'}) for col in good_cols}).dropna(axis=0, how='all')
        else:
            return pd.DataFrame({}) 


#get all fields of the form "|hiddenField|__VIEWSTATE|/wEPDwUKLTUzNjYxMTI2OA....."  from the given html text
def get_all_hidden_fields(html):
    obj = re.findall('\|hiddenField|\|(.*?)\|\d+\|', html)
    final = {k[0]:k[1] for k in [k.split('|') for k in obj if k != '']}
    return final
 
#retrieve the values for #__VIEWSTATE and #__VIEWSTATEGENERATOR for aspx pages
def get_viewstates(session,url,headers,timeout=30,proxies={}):
    f = session.post(url, timeout=timeout, headers = headers, proxies=proxies).text
    soup_temp = BeautifulSoup(f,"html5lib")
    viewstate = soup_temp.select("#__VIEWSTATE")[0]['value']
    viewstategen = soup_temp.select("#__VIEWSTATEGENERATOR")[0]['value']
    return viewstate,viewstategen

#using the provided requests session, post to a url with optional given headers and payload data tuples 
def session_post(session,url,headers,data,timeout=30,proxies={}):
    encoded_data = bytes(urllib.parse.urlencode(data).encode())
    return session.post(url, timeout=timeout, data=encoded_data, headers = headers, proxies=proxies).text
    

#count the number of null values in a given dictionary
def count_nulls(in_dict):
    count = 0
    for key in in_dict:
       if  in_dict[key][0]  == None or str(in_dict[key][0]) == "nan" or in_dict[key][0] == '':
           count = count + 1
    return count

#given an html string and list of name, begin, end delimeters, extract the corresponding values
def parse_html_regex_match(html,delims):
    results = {}
    for i in range(0,len(delims)):    
        value = match(delims[i]['begin'],delims[i]['end'],html,greedy=0)
        results[delims[i]['name']] = dt.std_clean(value)
    return results

#parse html using python module, return data values based on given pd data frame schema which contains desired text fields and number of html data items to skip
def parse_html(html,schema_df,method="skipschema"):
    parser = DataHTMLParser()
    parser.feed(html)
    results = {}
    for i in range(0,len(schema_df)):
        if method == "skipschema":
            results[schema_df.iloc[i]['cat_name']] = dt.std_clean(parser.return_schema_item(schema_df.iloc[i]['data_cat_text'],schema_df.iloc[i]['data_val_skips'],schema_df.iloc[i]['occur_num'],schema_df['data_cat_text'].tolist()),include_comma=0)
    return results

def extract_pdf(fname):
    result = ""
    pdfFileObj = pdfReader = PyPDF2.PdfFileReader(open(fname,'rb'))
    for i in range(0,pdfReader.numPages):
        result = result + pdfReader.getPage(i).extractText() 
    return result

#try to extract the file if possible and return a new name, otherwise just return input_filename
def get_extracted_name(in_filename,out_filename):
    out_file_extr = get_data.extract(in_filename,out_filename)
    if not out_file_extr == "":
        return out_file_extr
    else:
        return in_filename

#given a string, extract tables from it
def extract_tables(text):
    line_num = 0
    last_num = 0
    lines = text.split("\n")
    num_cols = float("inf")
    for line_num in range(0,len(lines)):
        if len(re.findall(r'\s\s\s\s+',lines[line_num])) >2:
            if last_num == 0 or line_num-last_num < 7:
                print(lines[line_num])
                last_num = line_num
            else:
                print('\n\n\n\n'+lines[line_num])
                last_num = line_num
                num_cols = len(re.findall(r'\w+',lines[line_num]))
 
#given a url and a parsing schema given as a data frame, return the corresponding values 
def get_values_html(session,url,cid,schema="",method="skipschema",headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'},sleeptime=3,year="",init_url="",html_folder="./html",init_page_val={},update=0):
    if not os.path.exists(html_folder):
        os.system("mkdir -p %s" % html_folder)
    out_filename = "%s/%s%s.html" % (html_folder,str(year),cid)
    init_url = init_url.replace("[id]",cid).replace("[Y]",str(year))
    if not os.path.isfile(out_filename) or update:
        if sleeptime > 0:
            time.sleep(sleeptime)
        download.download_url_from_session(session,url.replace("[id]",cid).replace("[Y]",str(year)),out_filename,headers=headers,init_url=init_url,init_page_val=init_page_val)
    if os.path.isfile(out_filename):
        out_filename = get_extracted_name(out_filename,out_filename.replace(".html","-extr.html"))
        html_file = open(out_filename)
        symbols= ['']
        html = html_file.read()
        result = (parse_html(html,schema,method=method))
        return result

#update (add) given values from a results dictionary with new values given in a current results dictionary
def update_dict(results,cresults):
    ckeys = set(cresults.keys())
    keys = set(results.keys())
    
    for ckey in ckeys:
        if ckey in keys:
            results[ckey].append(cresults[ckey])
        else:
            results[ckey] = [cresults[ckey]]
    return results

def parse_url(url,schema_filename="",headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'},init_url="",des=0,preview=1):
    out_filename = "temp.html"  
    if des:
        session = start_des_session(url)
        time.sleep(1)
    else:
        session = requests.session()
    
    download.download_url_from_session(session,url,out_filename,init_url=init_url,headers=headers)
    temp_file = get_extracted_name("temp.html","temp2.html")
    html = open(temp_file).read()
    parser = DataHTMLParser()
    parser.feed(html)
    if preview:
       parser.preview()
    if schema_filename != "":
        schema_df = pd.read_csv(schema_filename)
        print("parse result: ",parse_html(html,schema_df))
    return parser.all_data
       
#check if the current results are valid (i.e. above a certain thresh of nulls).  If so, add them to the master results dictionary and update the corresponding file and return 1, else just return 0
def check_update_results(cresult,result,output_filename,thresh=0.7,id_name="",cid=""):
    if count_nulls(cresult.to_dict(orient='list')) < len(cresult.columns)*thresh:
        if not id_name in list(cresult.columns) and not (id_name == "" or cid == ""):
            cresult[id_name] = cid
        if len(result) > 0: 
            result = pd.concat([result,cresult],ignore_index=True,sort=False)
        else:
            result = cresult
        result.to_csv(output_filename,index=False)
        return 1,result
    else:   
        return 0,result   

#download html and parse values for given html parsing schema file, ids can be provided as a range or provided in a list
#if using selenium, actions are 'click','get','post'
def run_all(url,output_filename,method="skipschema",schema_filename="",id_name="permit_num",id_min=0,id_max=3000,sleeptime=3,headers={},year="",init_url="",zfill_id=0,des=0,html_folder="./html",init_page_val={},id_list=[],update=0,use_selenium=0,selenium_init="",selenium_run="",headless=1,num_bad_thresh=1000):
    if des and not use_selenium:
       session = start_des_session(url)       
    elif use_selenium:
       #options = Options()
       if headless:
           try:
               vdisplay = Xvfb()
               vdisplay.start()
           except:
               print("headless mode failed.  (Normal if using macOS)")
               err = 1
       session = webdriver.Firefox(executable_path = '../General_Tools/geckodriver')
       #session = webdriver.Firefox(executable_path = '../General_Tools/geckodriver',firefox_options=options)
       if selenium_init != "":
           selenium_init(session)
    else:
       session = requests.session()

    if method == "skipschema":
        schema_df = pd.read_csv(schema_filename)
    else:
        schema_df = ""
    if os.path.exists(output_filename):
        try:
            result = pd.read_csv(output_filename)
        except:
            result = []
    else:
        result = []
    tot = 0
    goods = 0
    bads = 0 
    consecutive_bads = 0
    fails = 0
    if update:
        if len(result) > 0:
            id_min = dt.natural_sort(result['id'].apply(lambda x: str(x).replace("nan","")).tolist())
            id_min = id_min[len(id_min)-1] 
            if "-" in str(id_min):
                split = id_min.split("-")
                if year == split[0]:
                   id_min = int(split[1])+1
                else:
                   id_min = 1
            else:
                id_min = int(id_min)+1
            id_max = int(id_min)+99999
    if len(id_list) > 0:
       id_min = 0
       id_max = len(id_list)
    print('min,max',id_min,id_max)
    for i in range(id_min,id_max):
        if tot > -1:
            if len(id_list) > 0:
                cid = id_list[i]
            else:
                cid = str(i) 
                if zfill_id > 0:
                    cid = cid.zfill(zfill_id)
            try:
                if not use_selenium: 
                    cresult = get_values_html(session,url,cid,schema_df,method=method,headers=headers,sleeptime=sleeptime,year=year,init_url=init_url,html_folder=html_folder,init_page_val=init_page_val,update=update)
                else:
                               
                    html = selenium_run(session,url.replace("[id]",cid).replace("[Y]",str(year)))
                    cresult = parse_html(html,schema_df,method=method)

                cresult['id'] = i
                if year != "":
                    cresult['id'] = str(year) + "-" + str(i) 
                good,result = check_update_results(pd.DataFrame({cat : [cresult[cat]] for cat in cresult}),result,output_filename,id_name=id_name,cid=cid) 
                if good:
                    goods = goods + 1
                    consecutive_bads = 0
                    print("ID: %i - Good %s!  Progress: %i / %i (Good: %i, Bad:%i, Failed: %i)" % (i,id_name,tot+1,id_max-id_min,goods,bads,fails))
                else:
                    consecutive_bads += 1
                    if update and consecutive_bads > num_bad_thresh:
                        print("null permit max reached!")
                        break
                    bads = bads + 1
                    print("ID: %i - Bad %s!   Progress: %i / %i (Good: %i, Bad:%i, Failed: %i)" % (i,id_name,tot+1,id_max-id_min,goods,bads,fails))
            except Exception as e:
                print(e)
                fails = fails + 1
                consecutive_bads += 1
                if update and consecutive_bads > num_bad_thresh:
                    print("null permit max reached!")
                    break
                print("Failed %s!   Progress: %i / %i (Good: %i, Bad:%i, Failed: %i)" % (id_name,tot+1,id_max-id_min,goods,bads,fails))

        tot = tot + 1
    result.to_csv(output_filename,index=False)
    if headless and not err:
        try:
            vdisplay.stop()
        except:
            err = 1
    
#escape all regex special characters with \ character
def esc_regex(in_str,escape_chars=['"',".","*","[","]","+","?","{","}","|","(",")"]):
    for escape_char in escape_chars:
        in_str = in_str.replace(escape_char,"\\%s" % escape_char)
    return in_str    
    
#return text that is sandwiched between begin text and end text
def match(begin, end, string, greedy=0):
    result =  None
    if greedy:
        pattern = re.compile(r'(?<=%s).+(%s)' % (esc_regex(begin),esc_regex(end)),re.DOTALL)
    else:
        pattern = re.compile(r'(?<=%s).+?(%s)' % (esc_regex(begin),esc_regex(end)),re.DOTALL)

    matched = re.search(pattern,string)
    if not matched == None:
        match = str(matched[0])
        result = match[0:len(match)-len(end)]
    return result

#remove all html tags from given string
def clean_html(html):
  pattern = re.compile('<.*?>')
  result = re.sub(pattern, '', html)
  return result

#write single value of csv
def write_val(outfile,value,j,n,delim=",",transform="u",strip=1):
   if j < n-1:
      outfile.write('"%s"%s' % (dt.std_clean(value),delim))
   else:
      outfile.write('"%s"\n' % dt.std_clean(value))

#parse a text file with delims of the form: [[1,35,"owner1"],....]
def parse_text(in_filename,out_filename,delims,delim_format,keeps = [],max_rows = float("Inf"),use_length = 0):
    print("parsing text file...")
    infile = open(in_filename)
    outfile =  open(out_filename,"w")
    
    i = 0

    if len(keeps)>0:
        header = keeps[:]
    else:    
        header = []
        for param_set in delims:
               header.append(param_set[delim_format['Name']])
    n = len(header)

    for j in range(0,n):
        write_val(outfile,header[j],j,n,transform='l')

    num_rows = file_len(in_filename)

    for row in infile.readlines():
        if i < max_rows:
           if i % 10000 == 0:
               print_status (i,num_rows)
           j = 0
           for param_set in delims:
               if param_set[delim_format['Name']] in header:
                  if not use_length:
                      write_val(outfile,dt.std_clean(row[param_set[delim_format['Begin']]-1:param_set[delim_format['End']]]),j,n)
                  else:
                      write_val(outfile,dt.std_clean(row[param_set[delim_format['Begin']]-1:param_set[delim_format['Begin']]+param_set[delim_format['Length']]-1]),j,n)
                  j = j + 1
        i = i + 1
    outfile.close()
    print("%s written." % out_filename)
