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

import re
import os
from dateutil import parser
import fs_spatialtools as su
import fs_datatools as dt
import pickle
import streets
import math

st_suffix = ['ST','STREET','BLVD','BOULEVARD','RD','ROAD','WAY','DR','DRIVE','AVE','AVENUE','CT','COURT','PL','PLACE','TR','TRAIL','LANE','LN','PARKWAY','PKWAY']
states = ["AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","PR","RI","SC","SD","TN","TX","UT","VT","VA","VI","WA","WV","WI","WY"]
directions = ["S","N","W","E","SE","SW","NE","NW"]


stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

#given some text, extract dates from it.
def get_dates(text,drop_invalid=0):
    results = []
    final_indices = []

    bad = 0
    
    months = ["january","february","march","april","may","june","july","august","september","october","november","december","jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec"]
    candidates = re.findall(r'[0-9]?[0-9][//-][0-9]?[0-9][//-][0-9]?[0-9]?[0-9][0-9]',text)
    candidates.extend(re.findall(r'[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]',text))
    for month in months: 
        candidates.extend(re.findall(r'%s[ ,0-9]*[0-9]' % month,text.lower()))
    indices = [text.index(item) for item in candidates]

    for i in range(0,len(candidates)):
        candidate = candidates[i]
        try:
            dt = parser.parse(candidate)
            results.append(str(dt))
            final_indices.append(indices[i])
        except:
            bad = bad + 1
    if drop_invalid:
        results_ix = [not dt.is_invalid_date(result) for result in results]
        tres = []
        tidx = []
        for i in range(0,len(results)):
            if results_ix[i]:
                tres.append(results[i])
                tidx.append(final_indices[i])
        final_indices = tidx
        results = tres

    if len(final_indices) > 0 and len(results) > 0:
        final_indices, results = (list(x) for x in zip(*sorted(zip(final_indices, results)))) 
    return results

#given a list of strings, check if they are valid addresses and return only the valid ones
def check_addresses(addr_list):
    global states
    global st_suffix 
    global directions 

    results = []
    for item in addr_list:
       spl = item.split(" ")
       n = len(spl)
       good = 0
       for item2 in spl:
          if dt.could_become_numeric(item2) or  item2.upper() in states or item2.upper() in st_suffix or item2.upper() in directions or item2.upper() in streets.streets:
             good = good + 1
          if good == n:
              results.append(item)
    return results

#given an address add it to known addresses
def learn_address(address):
    new_streets = set(streets.streets)
    addr_cleaned = dt.clean_specials(address.upper(),add_keeps=[" ",":","<",">","*","=","+","}","[","(",")","!","%","$","^","@"],replace_dict={"\n":' ',"\t" : ' '},fix_multispace=1)
    addr_words = addr_cleaned.split(" ")
    for word in addr_words:
       if not dt.could_become_numeric(word):
           new_streets.add(word)
    if len(new_streets) > len(set(streets.streets)):
        dt.write_list(list(new_streets),'streets','streets.py')

#given some text, extract addresses from it. If prioritize_full = 1, the function will return the full_address list if it's length>0 and ignore the partials, otherwise return partials
def get_addresses(text,prioritize_full=0):
    global states
    global st_suffix 
 
    results = {'full_addresses' : [], 'partial_addresses' : []}
    bad = 0
    text_cleaned = dt.clean_specials(text.upper(),add_keeps=[" ",":","<",">","*","=","+","}","[","(",")","!","%","$","^","@"],replace_dict={"\n":' ',"\t" : ' ',"-":' '},fix_multispace=1)
  
    for suffix in st_suffix:
        results['partial_addresses'].extend(check_addresses(re.findall(r'[0-9]+\s[0-9A-Za-z,]+\s[A-Za-z,]*\s?[A-Za-z,]*\s?[A-Za-z,]*\s?[A-Za-z,]*\s?%s' % suffix,text_cleaned)))
    tsuffix = [" "+item+" " for item in st_suffix]
    tstates = [" "+item for item in states]
 
   
    full_candidates = []
    for pc in results['partial_addresses']:
        full_candidates.extend(re.findall(r'%s[\s\n][A-Za-z]+\s[A-Za-z][A-Za-z]\s?[0-9\-]*' % pc,text_cleaned))

    for fc in full_candidates:
        if dt.are_any_in(fc.upper(),tstates) and dt.are_any_in(fc.upper(),tsuffix):
            results['full_addresses'].append(fc)     

    if prioritize_full and len(results['full_addresses']) > 0:
        results = results['full_addresses']
    else:
        results = results['partial_addresses']

    return results

def is_full_addr(string):
    return 1

def get_street_info(addr,num=0):
    ands = re.findall(r'[0-9\s]+\&[\s0-9]+\s',addr)
    if len(ands) > 0:
        addr = addr.replace(ands[0],ands[0].split(" ")[0]+" ") 
    result = re.findall(r'(\d+).*?\s+(.+)',addr)
    if len(result) > 0 and num:
        return result[0][0]
    elif len(result) > 0 and not num:
        return result[0][1]
    elif not num and str(addr).upper() != "NAN":
        return addr
 


#load a module given by string path
def load_module(module_name):
    importlib = __import__('importlib')
    mod = importlib.import_module(module_name)
    return mod

#load a given namelist
def load_namelist(namelist):
    nl_path = "namelists."+ namelist.lower()
    return load_module(nl_path)

def save_namelist(namelist_data,namelist_name):
    nl_path = "namelists"+os.sep+namelist_name.lower()+".py"
    outfile = open(nl_path,'w')
    outfile.write("namelist = ")
    pprint.pprint(namelist_data,outfile)
    outfile.close()

#print list of words which are substrings of existing items in namelist
def print_matches(value,namelist):
    matched = 0
    for row in range(0,len(namelist)):
        if dt.are_any_in(value,namelist[row]):
            print('input word:',value,'namelist row:',row,"-",namelist[row]) 
            matched = 1
    if matched == 0:
       print("No mataches for %s!" % value)

def entropy(count,N):
    if count <= 0 or N <= 0:
        return 0
    else:
        p = count/N
        return abs(math.log(p)*p)

def make_ngrams(in_string,ngram_size,stoplist=[]):
    results = []
    split = [item for item in in_string.split(" ") if not dt.are_any_equal(item,stoplist)]
    i = 0
    while i+ngram_size < len(split)+1:
        results.append(" ".join(split[i:i+ngram_size]))
        i += 1
    return results
        

#given a list of input names/phrases try to add the most common ones to the given namelist if necessary 
def add_to_namelist(in_list,namelist_name,ngram_size=1,technique="ngram"):
    global stopwords
    nl_path = "../General_Tools/namelists/" + namelist_name + ".py"


    if technique == "exact_dups":
        final_candidates = [(count, elem) for count,elem in sorted(((in_list.count(item), item) for item in set(in_list)), reverse=True)]
        for cand in final_candidates:
            print_matches(cand[1],namelist)
            user_input = input("Enter Row Number Of Match, C to Create a new row, else S for skip, Q to save and quit")
            if dt.could_become_numeric(user_input):
                if int(user_input) < len(namelist):
                    namelist[int(user_input)].append(cand[1]) 
            elif user_input.upper() == "Q":
                save_namelist(namelist,namelist_name)         
                break
            elif user_input.upper() == "C":
                namelist.append([cand[1]])
        
    if technique == "ngram":
        ngrams = []
        print("splitting candidate ngrams into ngram list...")
        for item in in_list:
           ngrams.extend(make_ngrams(item,ngram_size,stoplist=stopwords))
    
        N = len(ngrams)
        print("making counts list...")
        final_candidates = [(score, elem) for score,elem in sorted(((entropy(ngrams.count(ngram),N), ngram) for ngram in set(ngrams)), reverse=True)]
        if os.path.exists(nl_path):
            namelist = load_namelist(namelist_name)
        else:
            namelist = []
    
        for cand in final_candidates:
            print_matches(cand[1],namelist)
            user_input = input("Enter Row Number Of Match, C to Create a new row, else S for skip, Q to save and quit")
            if dt.could_become_numeric(user_input):
                if int(user_input) < len(namelist):
                    namelist[int(user_input)].append(cand[1]) 
            elif user_input.upper() == "Q":
                save_namelist(namelist,namelist_name)         
                break
            elif user_input.upper() == "C":
                namelist.append([cand[1]])

curr_namelist = ""

def apply_namelist(value,namelist_name):
    global curr_namelist
    if curr_namelist == "":
        curr_namelist = load_namelist(namelist_name)
    for vlist in curr_namelist.namelist_partial:
        if dt.are_any_in(value,vlist): 
            value = vlist[0]
    for vlist in curr_namelist.namelist_whole:
        if len(set(vlist).intersection(set(str(value).split(" ")))) > 0:
            value = vlist[0]
    return value 
