import re
from urllib import request
import requests
import ftplib

#download files on the given ftp server (with the given credentials) that match any of the text given in "filename_match_text" 
def ftp(hostname,user,passwd,remote_path,filename_match_text,local_folder):
    try:
        os.mkdir(local_folder)
    except:
        a=1

    session= ftplib.FTP(host=hostname, user=user, passwd=passwd)
    file_list = session.nlst(remote_path)
    rfiles = [item for item in file_list if filename_match_text.lower() in item.lower()]
    for rfile in rfiles:
        print("Downloading %s..." % rfile),
        local_filename = local_folder + os.sep + rfile.split("/")[len(rfile.split("/"))-1]
        local_file = open(local_filename,'wb')
        session.retrbinary("RETR " + rfile, local_file.write, 8*1024)
        print("Done")


#given a requests session, download a given url
def download_url_from_session(session,url,out_filename,headers={},proxies={},timeout=20,init_url="",init_page_val={}):
    try:
        if not init_url == "":
            init_text = session.get(init_url).text
            if len(init_page_val) > 0:
                for key in (init_page_val.keys()):
                    matches = re.findall(init_page_val[key],init_text)
                    if len(matches) > 0:
                        url = url + "%s=%s" % (key,matches[0])
                    else:
                        print("Warning! Not match for regex expression: %s" % init_page_val[key])
            print ("Downloading: %s" % url)
            f = session.get(url, timeout=timeout, proxies=proxies).text
           
        else:
            print ("Downloading: %s" % url)
            f = session.get(url, timeout=timeout, headers = headers, proxies=proxies).text
        outfile = open(out_filename,'w')
        outfile.write(f)
        outfile.close()
        print("Finished: %s saved." % out_filename)
    except Exception as e:
        print ("Error Downloading %s:" % url)
        print (e)


#Download binary file
def download_binary(url,out_filename,headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'},verbose=1):
    try: 
        if verbose:
            print ("Downloading: %s" % url)
        req = request.Request(
            url, 
            data=None, 
            headers = headers 
        )
        response = request.urlopen(req)
        data = (response.read())
        outfile = open(out_filename,'wb')
        outfile.write(data)
        if verbose:
            print("Finished: %s saved." % out_filename)
    except Exception as e:
        print ("Error Downloading %s:" % url)
        print (e)

#Download link contained in an index page (target link contains target_text and matches provided regex pattern)
def download_link(index_url,link_pattern,outfile,prefix_link="",verbose=1):
    download_binary(index_url,"index.tmp",verbose=verbose)
    html_file = open("index.tmp")
    html = html_file.read()
    target_link = re.findall(link_pattern,html,re.DOTALL)
    if len(target_link) > 0:
        url = prefix_link + target_link[0]
        if verbose:
            print(url)
        download_binary(url,outfile,verbose=verbose)
    else:
        print(link_pattern,'Not Found!')


#Download text file, make all lowercase if lower=1
def download_text(url,out_filename,lower= 1,verbose=1):
    try:
        if verbose:
            print ("Downloading: %s" % url)
        response = request.urlopen(url)
        #clean windows characters, make everything lower case
        data = response.read().decode('utf-8').replace("\\r\\n","\n").replace("\\n","\n").replace("\\r","\n")
        data = data.replace("\xef\xbb\xbf","")
        if lower:
            data = data.lower()
        outfile = open(out_filename,'w')
        outfile.write(data)
        if verbose:
            print("Finished: %s saved." % out_filename)
    except Exception as e:
        print ("Error Downloading %s:" % url)
        print (e)


#return just the response header for a given url using the given request header
def get_response_header(url,req_headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}):
    req = request.Request(url, data=None, headers = req_headers)
    response = request.urlopen(req)
    return response.info()

 
 
