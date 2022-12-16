#!/usr/bin/env python
#
#  http://sourceforge.net/projects/mdbtools/
#
#  sudo apt-get install python-pyodbc
#  sudo apt-get install mdbtools libmdbodbc1

import sys, subprocess,csv,timeit # the subprocess module is new in python v 2.4


def dump_tables(dbfile,filename):
    start = timeit.default_timer()
    table_names = subprocess.Popen(["mdb-tables", "-1", dbfile], 
                               stdout=subprocess.PIPE).communicate()[0]
    tables = table_names.split('\n')
    i = 1
    for table in tables:
        if table != '':
            file = open(filename.replace(".","%i." % i), 'w')
            print("Dumping " + table)
            contents = subprocess.Popen(["mdb-export", dbfile, table],
                                    stdout=subprocess.PIPE).communicate()[0]
            file.write(contents)
            file.close()



def format(item,i,n,delim):
    item2 = item.replace('"','')
    if i < n -1:
       return "%s%s" % (item2.replace(delim," ").replace("  "," "),delim)
    else:
       return "%s\n" % (item2.replace(delim," ").replace("  "," "))


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def print_status(start,c,n):
    if c == 0:
       i = 1
    else:
       i = c

    frac = float(i)/float(n)
    elapsed = timeit.default_timer() - start
    left = elapsed/frac - elapsed
    if left > 60:
        print ("%ih:%im left" % ((left/3600),(left % 3600)/60))
    else:
        print ("%is left" % left)


def make_row(items,sep=","):
    result = ""
    for i in range(0,len(items)):
       if i < len(items)-1:
           result = result + "%s" % items[i] + sep
       else:
           result = result + "%s" % items[i] + "\n"
    return result


def clean_seps(filename):
    start = timeit.default_timer()
    print "cleaning..." 
    outfile = open(filename.replace(".","_cleaned."),'w')
    n = file_len(filename)
    row_num = 1
    delim = ","
    reader = csv.reader(open(filename))
    for row in reader:
        if row_num % 80000 == 0:
            print_status(start,row_num,n)
                        
        for i in range(0,len(row)):
            outfile.write(format(row[i],i,len(row),delim))
        
        row_num = row_num + 1
    
dump_tables(sys.argv[1],sys.argv[2])
clean_seps(sys.argv[2].replace(".","1."))
