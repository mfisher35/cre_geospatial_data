#*************************************************************************
#* 
#* Foresite.ai
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

import pandas
import sys
import re
import time
from bs4 import BeautifulSoup
import os
cwd = os.getcwd().split(os.sep)
cwd = "/".join(cwd[0:len(cwd)-1]) + os.sep + "General_Tools"
sys.path.append(cwd)
import fs_parsetools
import fs_datatools
import fs_spatialtools
import grocery_names as grocery
import pickle

grocery_names = grocery.get_grocery_names()

def download_cdpehs_restaurant_table(year,county,state="NC"):
    county_code = get_cdpehs_county_code(county,state)
    state_code = get_cdpehs_state_code(state)


    out_filename = "restaurants%i.csv" % year
    if not os.path.exists(out_filename):
        url = 'https://public.cdpehs.com/%s/ESTABLISHMENT/ShowESTABLISHMENTTablePage.aspx?ESTTST_CTY=%s' % (state_code,county_code)
    
        headers = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'en-US,en;q=0.9',
            'Cache-Control':'max-age=0',
            'Connection':'keep-alive',
        #    'Cookie':'ASP.NET_SessionId=nkzcyjwqfnui5csgaaeuzh4f; _ga=GA1.2.742427841.1539361236; _gid=GA1.2.253624565.1539361236',
            'DNT':'1',
            'Host':'public.cdpehs.com',
            'Upgrade-Insecure-Requests':'1',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
        }
        
        session = fs_parsetools.start_des_session(url)
        viewstate,viewstategen = fs_parsetools.get_viewstates(session,url,headers)
        
        data = (
            ('__VIEWSTATE', viewstate),
            ('__VIEWSTATEGENERATOR', viewstategen),
            ("ctl00$scriptManager1","ctl00$PageContent$UpdatePanel1|ctl00$PageContent$INSPECTIONFilterButton$_Button"),
            ("ctl00$pageLeftCoordinate",""),
            ("ctl00$pageTopCoordinate",""),
            ("ctl00$PageContent$_clientSideIsPostBack","Y"),
            ("ctl00$PageContent$PREMISE_NAMEFilter1",""),
            ("ctl00$PageContent$PREMISE_CITYFilter1",""),
            ("ctl00$PageContent$PREMISE_NAMEFilter","--ANY--"),
            ("ctl00$PageContent$PREMISE_CITYFilter","--ANY--"),
            ("ctl00$PageContent$PREMISE_ZIPFilter","--ANY--"),
            ("ctl00$PageContent$EST_TYPE_IDFilter","1"),
            ("ctl00$PageContent$INSPECTION_DATEFromFilter","1/1/%i" % (year)),
            ("ctl00$PageContent$INSPECTION_DATEToFilter","1/1/%i" % (year+1)),
            ("ctl00$PageContent$FINAL_SCOREFromFilter","--ANY--"),
            ("ctl00$PageContent$COUNTY_IDFilter", county_code),
            ("ctl00$PageContent$ESTABLISHMENTPagination$_CurrentPage","1"),
            ("ctl00$PageContent$ESTABLISHMENTPagination$_PageSize","9999"),
            ('__EVENTTARGET','ctl00$PageContent$ESTABLISHMENTPagination$_PageSizeButton'),
            ('__EVENTARGUMENT',''),
            ('__LASTFOCUS','ctl00_PageContent_ESTABLISHMENTPagination__PageSize'),
            ("__ASYNCPOST","true")
        )
        
        # second HTTP request with form data
        headers['Content-Type'] = "application/x-www-form-urlencoded; charset=UTF-8"
        headers['Origin'] = "https://public.cdpehs.com"
        headers['Referer'] = "https://public.cdpehs.com/NCENVPBL/ESTABLISHMENT/ShowESTABLISHMENTTablePage.aspx?ESTTST_CTY=%s" % county_code
        headers['User-Agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"
        headers['X-MicrosoftAjax'] = "Delta=true"
        headers['X-Requested-With'] = "XMLHttpRequest"
        
        soup = BeautifulSoup(fs_parsetools.session_post(session,url,headers,data),"html5lib")
        
        keys = ['name','address','city','state','zip','type','year']
        results = {key : [] for key in keys}
        cur_col = 0
        
        for name in soup.find_all("td", class_="ttc"):
            results[keys[cur_col]].append(name.get_text().strip())
            cur_col = cur_col + 1
            if cur_col > 5:
                results[keys[cur_col]].append(year) 
                cur_col = 0
        
        df = pandas.DataFrame(results)
        df['name'] = df['name'].apply(clean_name)
        df = df.apply(classify_amenities,axis=1)
        df = df.apply(fs_spatialtools.geocode_split_addr,axis=1)
        df['tempid'] = df['name'] + df['address']
        details = get_cdpehs_details(year,county,state,current_year=2018)
        details['tempid'] = details['name'] + details['address']
        df = fs_datatools.join(df,details,'tempid','tempid',cols=['close_date', 'inspect_score', 'grade', 'open_date','estab_id'])
        df = df[[item for item in df.columns if not "temp" in item]]
        df.to_csv(out_filename,index=False)
        print(out_filename,'written')
    else:
        print(out_filename,"already exists.  Please remove it if you wish to re-download data")
#function that checks if all words in a given input string are all contained in some string in list of strings
def match_all_words(input_str,list_of_strings):
    input_set = set(input_str.split(" "))
    for string in list_of_strings:
       match_vector = set(string.split(" "))
       if match_vector.issubset(input_set):
           return True
    return False

def classify_amenities(row,default="RESTAURANT/CAFE"):
    name = str(row['name']).upper()
    global grocery_names
    if fs_datatools.are_any_in(name,["A&W","A W","ARBY","BISCUITVILLE","BOSTON MARKET","CARLS JR","CARL'S JR","CARLSJR","CINNABON","HARDEE","SONIC","MIAMI SUB","CHURCH'S CHICKEN","JACK IN THE BOX","JACK-IN","DAIRY QUEEN","BOJANGLES","BURGER KING","CAPTAIN D S","CAPTAIN D'S","CAPTAIN DS","CHICKFILA","CHICK FIL A","CHICK-FIL-A","CHIPOTLE","KFC","MC DONALD","MCDONALDS","MCDONALD'S","MCDONALD S","POPEYE","PIZZA HUT","SHEETZ","5 GUYS","FIVE GUYS","STARBUCK","SUBWAY","TACO BELL","TACOBELL","WENDY S","WENDY'S","WENDYS","DEL TACO","ZAXBY",'QDOBA','WHATABURGER','KRISPY KREME','PANDA EXPRESS']):
        row['type'] = "FAST FOOD"
        row['upscale'] = 0
        return row
    if match_all_words(name,grocery_names):
        if fs_datatools.are_any_in(name,["WHOLE FOODS","TRADER JOE"]):
            row['type'] = "GROCERY"
            row['upscale'] = 1 
            return row
        else:
            row['type'] = "GROCERY"
            row['upscale'] =  0
            return row
    elif fs_datatools.are_any_in(name,['CIRCLE K','BP','VALERO','CORNER STORE','711','SEVEN ELEVEN','CONVENIENCE','GAS','QUICK STOP','7 ELEVEN','VICTORY MART','C-STORE','C STORE',]):
        row['type'] = 'QUICKMART/GAS'
        row['upscale'] = 0
        return row
    elif fs_datatools.are_any_in(name,['CAFE','STARBUCK','BAKERY','BAGEL']):
        row['type'] = 'RESTAURANT/CAFE'
        row['upscale'] = ''
    elif "WALMART" in name or "TARGET" in name:
        row["type"] = "SUPERCENTER" 
        row["upscale"] = 0
    else:
        row["type"] = default
        row["upscale"] = ""
    return row

#make the first letter of every word in a string such that the first letter of every word is capitalized, example: 'to kill a mockingbird' --> 'To Kill a Mockingbird'
def make_title_case(string):
    string = string.lower().split(" ")
    result = ""
    for item in string:
        if len(item)>1:
            result = result + item[0].upper() + item[1:len(item)] + " "
        else:
            result = result + item + " "
    result = result[0:len(result)-1] 
    return result

#Make a single amenity name looks nice
def clean_name(string):
    string = fs_datatools.clean_specials(string,add_keeps=[" ","&","'"],replace_dict={'-':' ',',':' ',"\n" : " ", "\r" : " "},transform='u')
    string = string.rstrip('1234567890 #')
    string = string.strip()
    string = re.sub(r"\s\s+", " ", string)
    string = string.replace(" S ","'S")
    if string[len(string)-2:len(string)]  == " S":
        string = string[0:len(string)-2] + "'S"
    return string

#sort strings containing alphabetic and numeric items the way a human would expect e.g. ["aa120","aa9","aa90"] --> ["aa9","aa90","aa120"]
def natural_sort(clist): 
    convert = lambda text: int(text) if text.isdigit() else text.lower() 
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(clist, key = alphanum_key)

def get_cdpehs_county_code(county,state="NC"):
    if state.upper() == "NC":
        county_code_dict = {'DURHAM' : "32","ORANGE":"68"}
    return county_code_dict[county.upper()]

def get_cdpehs_state_code(state):
    state_code_dict = {'NC' : 'NCENVPBL'}
    return state_code_dict[state.upper()]

def init_cdpehs_details(year,county,state="NC",current_year=2018):
    if county.upper() == "DURHAM":
        min_year = 2008
    state_code = get_cdpehs_state_code(state)
    county_code = get_cdpehs_county_code(county,state)


    url = 'https://public.cdpehs.com/%s/ESTABLISHMENT/ShowESTABLISHMENTTablePage.aspx?ESTTST_CTY=%s' % (state_code,county_code)

    headers = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'en-US,en;q=0.9',
        'Cache-Control':'max-age=0',
        'Connection':'keep-alive',
    #    'Cookie':'ASP.NET_SessionId=nkzcyjwqfnui5csgaaeuzh4f; _ga=GA1.2.742427841.1539361236; _gid=GA1.2.253624565.1539361236',
        'DNT':'1',
        'Host':'public.cdpehs.com',
        'Upgrade-Insecure-Requests':'1',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
    }
    
    session = fs_parsetools.start_des_session(url)
    viewstate,viewstategen = fs_parsetools.get_viewstates(session,url,headers)
    data1 = (
        ('__VIEWSTATE', viewstate),
        ('__VIEWSTATEGENERATOR', viewstategen),
        ("ctl00$scriptManager1","ctl00$PageContent$UpdatePanel1|ctl00$PageContent$INSPECTIONFilterButton$_Button"),
        ("ctl00$pageLeftCoordinate",""),
        ("ctl00$pageTopCoordinate",""),
        ("ctl00$PageContent$_clientSideIsPostBack","Y"),
        ("ctl00$PageContent$PREMISE_NAMEFilter1",""),
        ("ctl00$PageContent$PREMISE_CITYFilter1",""),
        ("ctl00$PageContent$PREMISE_NAMEFilter","--ANY--"),
        ("ctl00$PageContent$PREMISE_CITYFilter","--ANY--"),
        ("ctl00$PageContent$PREMISE_ZIPFilter","--ANY--"),
        ("ctl00$PageContent$EST_TYPE_IDFilter","1"),
        ("ctl00$PageContent$INSPECTION_DATEFromFilter","1/1/%i" % (year)),
        ("ctl00$PageContent$INSPECTION_DATEToFilter","1/1/%i" % (year+1)),
        ("ctl00$PageContent$FINAL_SCOREFromFilter","--ANY--"),
        ("ctl00$PageContent$COUNTY_IDFilter",county_code),
        ("ctl00$PageContent$ESTABLISHMENTPagination$_CurrentPage","1"),
        ("ctl00$PageContent$ESTABLISHMENTPagination$_PageSize","9999"),
        ('__EVENTTARGET','ctl00$PageContent$ESTABLISHMENTPagination$_PageSizeButton'),
        ('__EVENTARGUMENT',''),
        ('__LASTFOCUS','ctl00_PageContent_ESTABLISHMENTPagination__PageSize'),
        ("__ASYNCPOST","true")
    )

    # second HTTP request with form data
    headers['Content-Type'] = "application/x-www-form-urlencoded; charset=UTF-8"
    headers['Origin'] = "https://public.cdpehs.com"
    headers['Referer'] = "https://public.cdpehs.com/NCENVPBL/ESTABLISHMENT/ShowESTABLISHMENTTablePage.aspx?ESTTST_CTY=%s" % county_code
    headers['User-Agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"
    headers['X-MicrosoftAjax'] = "Delta=true"
    headers['X-Requested-With'] = "XMLHttpRequest"

    text = fs_parsetools.session_post(session,url,headers,data1)
    obj = re.findall('\|hiddenField|\|(.*?)\|\d+\|', text)

    final = {k[0]:k[1] for k in [k.split('|') for k in obj if k != '']}
    return session,final["__VIEWSTATE"],final["__VIEWSTATEGENERATOR"],url,headers,text


#get the opening date, closing date latest score and establishment id from the cdpehs inspections website
def get_cdpehs_details(year,county,state="NC",current_year=2018):
    min_year = 2008
    out_filename = "%s-%s-%s-restraunt_details.csv" % (state,county,str(year))
    county_code = get_cdpehs_county_code(county,state)

    if not os.path.exists(out_filename):
        session,viewstate,viewstategen,url,headers,text = init_cdpehs_details(year,county,state="NC")
        ctl_nums =  natural_sort(list(set(re.findall(r'ctl[0-9]+',text))))
    
        results = {'name': [], 'address': [], 'close_date': [], 'inspect_score': [], 'grade': [], 'open_date': [], 'estab_id':[]}
        count = 0
        for ctl_num in ctl_nums:
            time.sleep(1)
            result_temp = {'name': '', 'address': '', 'close_date': '', 'inspect_score': '', 'grade': '', 'open_date': '', 'estab_id': ''}
            count = count + 1
            if(count % 20) == 0:
               print("retreiving dates and scores for place %i/%i" % (count,len(ctl_nums)))
            session,viewstate,viewstategen,url,headers,text = init_cdpehs_details(year,county,state="NC")
        
        
            data2 = (
                ('__VIEWSTATE', viewstate),
                ('__VIEWSTATEGENERATOR', viewstategen),
                ('ScriptPath', '/NCEnvPbl/ScriptResource.axd?d=uTb7zHAO7r35kxrZA4Fqb48NP2hld3f2-Pwad3sftW6iC8j_z6El9VkbDznO97WZioyf5nBpOXFGpt59XmUSXcliKHMdyKAvNdoTnoQROpx8G31EVmpXcKIooKBdBdKu2ezi38EMGWtjXzbIDQozug2&t=ffffffffbd2983fc'),
                ("ctl00$scriptManager1","ctl00$PageContent$UpdatePanel1|ctl00$PageContent$ESTABLISHMENTPagination$_PageSizeButton"),
                ('ScriptContentWithTags', '{"text":"var ctrl = \\"\\"; var ctrlPB = \\"ctl00_PageContent_ESTABLISHMENTPagination__PageSize\\"; function pageLoadedHandler1(sender, args) { if(!isPostBack()) {setTimeout(\\"setTimeoutFocus()\\", 1000);} else {setTimeout(\\"setPostBackFocus()\\", 100);}}function setTimeoutFocus() { setFocus(ctrl); }function setPostBackFocus() { doFocus(ctrlPB);}function doFocus(ctrlID) { if((ctrlID == null) '),
                ("ctl00$pageLeftCoordinate",""),
                ("ctl00$pageTopCoordinate",""),
                ("ctl00$PageContent$_clientSideIsPostBack","Y"),
                ("ctl00$PageContent$PREMISE_NAMEFilter1",""),
                ("ctl00$PageContent$PREMISE_CITYFilter1",""),
                ("ctl00$PageContent$PREMISE_NAMEFilter","--ANY--"),
                ("ctl00$PageContent$PREMISE_CITYFilter","--ANY--"),
                ("ctl00$PageContent$PREMISE_ZIPFilter","--ANY--"),
                ("ctl00$PageContent$EST_TYPE_IDFilter","1"),
                ("ctl00$PageContent$INSPECTION_DATEFromFilter","1/1/%i" % (year)),
                ("ctl00$PageContent$INSPECTION_DATEToFilter","1/1/%i" % (year+1)),
                ("ctl00$PageContent$FINAL_SCOREFromFilter","--ANY--"),
                ("ctl00$PageContent$COUNTY_IDFilter",county_code),
                ("ctl00$PageContent$ESTABLISHMENTPagination$_CurrentPage","1"),
                ("ctl00$PageContent$ESTABLISHMENTPagination$_PageSize","9999"),
                ('__EVENTTARGET','ctl00$PageContent$ESTABLISHMENTTableControlRepeater$%s$Button$_Button' % ctl_num),
                ('__EVENTARGUMENT',''),
                ('__LASTFOCUS','ctl00_PageContent_ESTABLISHMENTPagination__PageSizeButton'),
                ('__LASTFOCUS','ctl00_PageContent_ESTABLISHMENTPagination__PageSize'),
                ("__ASYNCPOST","true")
            )
        
            text = fs_parsetools.session_post(session,url,headers,data2)
            estab_code =  re.findall(r'%3d.+%26',text)[0].replace("%3d","").replace("%26","")
            result_temp['estab_id'] = estab_code
            new_url = "https://public.cdpehs.com/NCENVPBL/INSPECTION/ShowESTABLISHMENTPage.aspx?ESTABLISHMENT=%s&esttst_cty=%s" % (estab_code,county_code)
            session = fs_parsetools.start_des_session(new_url)
            viewstate,viewstategen = fs_parsetools.get_viewstates(session,new_url,headers)
        
            data = (
               ('__VIEWSTATE', viewstate),
               ('__VIEWSTATEGENERATOR', viewstategen),
               ("ctl00$scriptManager1","ctl00$PageContent$UpdatePanel1|ctl00$PageContent$INSPECTIONPagination$_PageSizeButton"),
               ("__EVENTTARGET","ctl00$PageContent$INSPECTIONPagination$_PageSizeButton"),
               ("__EVENTARGUMENT",""),
               ("ctl00$pageLeftCoordinate",""),
               ("ctl00$pageTopCoordinate",""),
               ("ctl00$PageContent$_clientSideIsPostBack","N"),
               ("ctl00$PageContent$INSPECTIONPagination$_CurrentPage","1"),
               ("ctl00$PageContent$INSPECTIONPagination$_PageSize","9999"),
               ("__ASYNCPOST","true")
            )
            text = fs_parsetools.session_post(session,new_url,headers=headers,data=data)
            soup = BeautifulSoup(text,"html5lib")
        
            i = 0
            for name in soup.find_all("td", class_="dfv"):
                if i == 0:
                    result_temp['name'] = (name.get_text().strip())
                if i == 2:
                    result_temp['address'] = (name.get_text().strip())
                i = i + 1
        
            i = 0
            parse_list = soup.find_all("td", class_="ttc")
            for item in parse_list:
                if i == 0:
                    if not str(current_year) in item.get_text() and not "%i" % current_year-1 in item.get_text().strip():
                        result_temp['close_date'] = item.get_text().strip()
                if i == 1:
                    result_temp['inspect_score'] = item.get_text().strip()
                if i == 3:
                    result_temp['grade'] = (item.get_text().strip())
                if i == len(parse_list)-5:
                    if not str(min_year) in item.get_text().strip():
                        result_temp['open_date'] = item.get_text().strip()
                i = i + 1
            for key in list(result_temp.keys()):
                results[key].append(result_temp[key])
        temp_result = pickle.dump(results,open("temp.pkl",'wb'))
        final_result = pandas.DataFrame(results) 
        final_result['name'] = final_result['name'].apply(clean_name)
        final_result.to_csv(out_filename,index=False)
        print(out_filename,"written.")
        return final_result
    else:
        return pandas.read_csv(out_filename)

