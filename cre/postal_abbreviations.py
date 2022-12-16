import pandas
import fs_datatools as dt


output_file = open('postal_abr.py','w')
output_file.write("abbreviations = [\n")
a = pandas.read_csv("~/postal_abbr.tsv",sep="\t")
is_new_col = 0
abbrvs = []
official_code = ""
for i in range(0,len(a)):
    row = a.iloc[i]
#    print(official_code)
#    if "LN" in official_code:
#        print(row['c0'],row['c1'])
    if not dt.is_null(row['c2']):
        if len(abbrvs) > 0:
            final_list = list(set(abbrvs))
            try:
                final_list.remove(official_code)
            except:
                fail = 1
            if len(final_list) > 0:
                final_list.append(official_code)
                output_file.write("    " + str(final_list)+",\n")
        official_code = " " + row['c2']
        abbrvs = [" " + row['c0']]
        if not dt.is_null(row['c1']):
            abbrvs.append(" " + row['c1']) 
    else:
        abbrvs.append(" " + row['c0'])
        if not dt.is_null(row['c1']):
            abbrvs.append(" " + row['c1'])


output_file.write("]")
