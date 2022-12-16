import fs_datatools
import os 
import permit_types

#given a column of wk_class items return their general description.  Meant to be used as an apply function (df['wk_class'].apply(get_permit_wkclass))
pdict = permit_types.permit_types

def get_permit_wkclass(value):
   global pdict
   tval = str(value).strip().upper()
   for key in pdict:
      if tval in pdict[key]:
         return key
   return "UNKNOWN"
