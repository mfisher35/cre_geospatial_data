from fs_spatialtools import file_len,print_status


def clean_val(item,delim=","):
   clean = ['"',"*","'",",","\t","%","!","#","$","^","\n",delim]
   result = item[:]
   for char in clean:
      result = result.replace(char,"")
   for i in range(0,7):
       result = result.replace("  "," ")
   return result.strip()


def write_val(outfile,value,j,n,delim=","):
   if j < n-1:
      outfile.write("%s%s" % (clean_val(value),delim))
   else:
      outfile.write("%s\n" % value)
 

#delims of the form: [[1,35,"owner1"],....]
def parse_text(infilename,outfilename,delims,delim_format,keeps = [],max_rows = float("Inf"),use_length = 0):
    print("parsing text file...")
    infile = open(infilename)
    outfile =  open(outfilename,"w")
    
    i = 0

    if len(keeps)>0:
        header = keeps[:]
    else:    
        header = []
        for param_set in delims:
               header.append(param_set[delim_format['Name']])
    n = len(header)

    for j in range(0,n):
        write_val(outfile,header[j],j,n)

    num_rows = file_len(infilename)

    for row in infile.readlines():
        if i < max_rows:
           if i % 10000 == 0:
               print_status (i,num_rows)
           j = 0
           for param_set in delims:
               if param_set[delim_format['Name']] in header:
                  if not use_length:
                      write_val(outfile,clean_val(row[param_set[delim_format['Begin']]-1:param_set[delim_format['End']]]),j,n)
                  else:
                      write_val(outfile,clean_val(row[param_set[delim_format['Begin']]-1:param_set[delim_format['Begin']]+param_set[delim_format['Length']]]),j,n)
                  j = j + 1
        i = i + 1
    outfile.close()
    print("%s written." % outfilename)
