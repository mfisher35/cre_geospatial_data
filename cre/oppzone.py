#sudo pip3 install xlrd
import os
import sys
import numpy as np
import pandas
import fs_spatialtools as su
import fs_datatools
import download
import re
import census

#get latest excel spreadsheet from https://www.cdfifund.gov/Pages/Opportunity-Zones.aspx
def get_oppzones_us(path="../US"):
    download.download_link("https://www.cdfifund.gov/Pages/Opportunity-Zones.aspx",r'Designated.+?xlsx',path+os.sep+'oppzones.xls',prefix_link="https://www.cdfifund.gov/Documents/",verbose=1)


def add_oppzones(state):
    fips = census.get_fips(state)
    state_census_bound_file = "../%s/census/shapes/cb_2016_%s_bg_500k.shp" % (state,fips)
    if not os.path.exists(state_census_bound_file):
        census.get_census_shapes("2016",state)        
    out_filename = "../%s/oppzone_shapes_%s.csv" % (state,state)
    census_shapes = su.load_file(state_census_bound_file,to_crs="")
    opp_df = pandas.read_excel("../US/oppzones.xls")
    opp_df = opp_df.reset_index()
    opp_df.columns = ['state','county','geoid2','oppzone_type','oppzone_date']
    opp_df['is_oppzone'] = pandas.Series(np.ones(len(opp_df))).astype(int)
    census_shapes['geoid2'] = census_shapes['AFFGEOID'].apply(lambda x: x[9:20])
    opp_df = fs_datatools.join(opp_df,census_shapes,'geoid2','geoid2',cols=['geometry'])
    opp_df = opp_df[np.invert(opp_df['geometry'].isna())]
    opp_df.to_csv(out_filename,index=False)

#get_oppzones_us()
#add_oppzones("NC","../NC/census/shapes/cb_2016_37_bg_500k.shp")
