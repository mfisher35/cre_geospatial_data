import postal_abr

def reshuffle(row):
    result = row[0:len(row)-1]
    result.sort(key=len, reverse=True)
    result.append(row[len(row)-1])
    return result

ofile = open('postal_abr2.py','w')
ofile.write('abbreviations = [\n')
for row in postal_abr.abbreviations:
   ofile.write("    " +str(reshuffle(row)) + ",\n")
ofile.write("]\n")

