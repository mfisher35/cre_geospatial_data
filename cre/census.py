import os
import re
import pandas
import urllib.request as urllib2
import fs_spatialtools as su
import fs_datatools
import pandas as pd

#list of acs data tables and corresponding codes
acs_tables = {
"age":"B01001",
"med_age":"B01002",
"tot_pop":"B01003",
"race":"B02001",
"geomob":"B07201",
"work_comm_type":"B08136",
"transport":"B08301",
"work_commute":"B08303",
"hhtype":"B11016",
"education":"B15003",
"yrmed_hshld_inc":"B19013",
"percap_inc":"B19301",
"employ":"B23025",
"vacancy":"B25004",
"rent":"B25061",
"price":"B25085",
}

#LODES Job data codes
lodes_codes = {
"c000":"total_num_jobs",  "ca01":"jobs_29_younger",  "ca02":"jobs_30_54",  "ca03":"jobs_55_older",  "ce01":"jobs_lt1250",  "ce02":"jobs_1251_3333",  "ce03":"jobs_3333_gt",  "cns01":"jobs_ag_fish",  "cns02":"jobs_mining_gas",  "cns03":"jobs_utilities",  "cns04":"jobs_constr",  "cns05":"jobs_manufacture",   "cns06":"jobs_wholesale",  "cns07":"jobs_retail",  "cns08":"jobs_transp_warehouse",   "cns09":"jobs_information",  "cns10":"jobs_fin_insure",  "cns11":"jobs_real_estate",  "cns12":"jobs_tech_scientific",  "cns13":"jobs_management",  "cns14":"jobs_admin_supp_wm",  "cns15":"jobs_education",   "cns16":"jobs_health_social",  "cns17":"jobs_art_entertainment",  "cns18":"jobs_accomodation_food",  "cns19":"jobs_other_service",  "cns20":"jobs_public_admin",   "cr01":"jobs_white",  "cr02":"jobs_aa",  "cr03":"jobs_am_indian",   "cr04":"jobs_asian",  "cr05":"jobs_pacisland",  "cr07":"jobs_mixed",  "ct01":"jobs_not_hispanic",  "ct02":"jobs_hispanic",  "cd01":"jobs_lths",   "cd02":"jobs_hs_nclg",  "cd03":"jobs_some_clg",   "cd04":"jobs_bachelors",  "cs01":"jobs_male",   "cs02":"jobs_female", "cfa01":"jobs_firm_age_lt1", "cfa02":"jobs_firm_age_2_3", "cfa03":"jobs_firm_age_4_5", "cfa04":"jobs_firm_age_6_10", "cfa05":"jobs_firm_age_11p", "cfs01":"jobs_firm_size_0_19", "cfs02":"jobs_firm_size_20-49", "cfs03":"jobs_firm_size_50_249", "cfs04":"jobs_firm_size_250_499","cfs05":"jobs_firm_size_500p" }

def join_all_geos(state="NC",year="2016",census_folder="census"):
   global acs_table
   for table in list(acs_tables.keys()):
       join_geo(table,year=year,census=census, state=state,boundaries="bg",update=1)


def mkdir(path):
    if not os.path.isdir(path):
        os.mkdir(path)

#prefix every colmn name with input prefix string
def prefix_header(df,prefix,skip=[],clean=1):
    header = list(df.columns)
    for i in range(0,len(header)):
        if not header[i] in skip:
            header[i] = prefix+header[i]
    if clean:
        clean_meta(header) 
    df.columns = header

#return list of all columns except those given in the remove_cols list
def get_keep_cols(df,remove_cols):
    columns = list(df.columns) 
    keep_cols = []

    for col in columns:
       if not col in remove_cols:
          keep_cols.append(col)
    return keep_cols

#Get the ACS filename for a given year and table
def get_acs_filenames(year,table):
     table_code = acs_tables[table]
     year = str(year)
     year = year[2:4]
     result = {}
     result["data"] = "ACS_%s_5YR_%s_with_ann.csv" % (year,table_code)
     result['meta'] = "ACS_%s_5YR_%s_metadata.csv" % (year,table_code)
     return result

#get the FIPS code for a given state
def get_fips(state):
    state = state.upper()
    fips={"AL":"01","AK":"02","AS":"60","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10","DC":"11","FL":"12","FM":"64","GA":"13","GU":"66","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MH":"68","MD":"24","MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38","MP":"69","OH":"39","OK":"40","OR":"41","PW":"70","PA":"42","PR":"72","RI":"44","SC":"45","SD":"46","TN":"47","TX":"48","UM":"74","UT":"49","VT":"50","VA":"51","VI":"78","WA":"53","WV":"54","WI":"55","WY":"56"}
    if state in fips:
       return fips[state]
    else:
       print ("ERROR: State %s not in fips list!" % state)


def get_table_type(code):
    types = []
    for key in acs_tables.keys():
       if code.lower() in acs_tables[key].lower():
          return str(key)


#download US census boundary shapefiles from the TIGER system
def get_census_shapes(year,state,boundaries="bg",overwrite=0,folder=""):
    if folder == "":
        folder = "../%s" % state
        mkdir(folder)
        folder = "../%s/census" % state
        mkdir(folder)
    state_code = get_fips(state)
    if boundaries == "bg" or boundaries == "tract":
        address = "https://www2.census.gov/geo/tiger/GENZ%s/shp/" % (year)
        filename = "cb_%s_%s_%s_500k.zip" % (year,state_code,boundaries)
    elif boundaries == "b":
        address = "https://www2.census.gov/geo/tiger/TIGER%s/TABBLOCK/" % (year)
        filename = "tl_%s_%s_tabblock10.zip" % (year,state_code)

    final_outfolder = folder + os.sep + "shapes" 
    mkdir(final_outfolder)
    final_output_path = final_outfolder + os.sep + filename
    if overwrite or not os.path.exists(final_output_path):
        address = address + filename
        print("downloading %s..." % address)
        response = urllib2.urlopen(address)
        f = open(final_output_path,'wb')
        data = response.read()
        f.write(data)
        f.close() 
        os.system("unzip -d %s %s" % (final_outfolder,final_output_path))
    return final_output_path.replace(".zip",".shp") 

#keep only alphanumeric characters
def clean_specials(in_str,add_keeps=[]):
    in_str = in_str.replace(" ","_").lower()
    result = ""
    keeps  = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","0","1","2","3","4","5","6","7","8","9","10","-","_"]
    keeps.extend(add_keeps)
    for char in in_str:
       if char in keeps:
           result = result + char
    return result

#abbreviate certain words and clean up special characters from input text
def clean_meta(metadata):
    replace = {
    "_-_":"-",
    "_-_":"-",
    "margin":"",
    " for ":"",
    " use ":"",
    "_of":"",
    "display":"disp",
    "geography":"geo",
    "recreational":"",
    "occasional":"",
    "estimated":"",
    "total":"",
    "occupied":'occup',
    "idid":'id',
    "id2id2":'id2',
    "united_states":"us",
    "different":"diff",
    "estimate":"",
    "including":"incl",
    "excluding":"excl",
    "include":"incl",
    "exclude":"excl",
    "remainder":"rmndr",
    "median":"med",
    "african_american":"afr_amer",
    "ownership":"ownshp",
    "metropolitan":"metrp",
    "micropolitan":"microp",
    "statistical": "stat",
    "employment":"employ",
    "principal":"princ",
    "error":"err",
    "microp_stat_area" : "microp", 
    "metrop_stat_area" : "metrop", 
    "city":"cty",
    "1_year_ago":"1yr",
    "same_metrp":"sm_metrp",
    "stat_area":"",
    "est_tot":"",
    "not_in_a":"non",
    "__":"_",
    
    }

    while '' in metadata:
       metadata.remove('')
    for i in range(0,len(metadata)):
       metadata[i] = metadata[i].replace(",Estimate; Total","Num")
       tmeta = clean_specials(metadata[i])
       tmeta = re.sub(r'hd[0-9][0-9]_vd[0-9][0-9]','',tmeta)
       for item in replace.keys():
          tmeta = tmeta.lower().replace(item,replace[item])
       tmeta = tmeta.replace("-","_")
       tmeta = tmeta.replace("__","_")
       tmeta = tmeta.replace("__","_")
       if tmeta[0] == "_":
           tmeta = tmeta[1:len(tmeta)]
       if tmeta[len(tmeta)-1] == "_":
           tmeta = tmeta[0:len(tmeta)-1]
       metadata[i] = tmeta
       for item in replace.keys():
          tmeta = tmeta.lower().replace(item,replace[item])
    return metadata

def fix_census_header(year,table,census,overwrite=1):
    filenames =  get_acs_filenames(year,table)
    infilename_data = census + os.sep + filenames['data']
    infilename_meta = census + os.sep + filenames['meta']
    if os.path.exists(infilename_meta) and os.path.exists(infilename_data):
        out_folder = census + os.sep + "processed"
        mkdir(out_folder)
        outpath = out_folder + os.sep + filenames['data']
        if overwrite or not os.path.exists(outpath):
            print("fixing census header for file %s" % filenames['data'])
            infile_data = open(infilename_data,'r')
            data = infile_data.read().split("\n")
            infile_meta = open(infilename_meta,'r')
            new_header = clean_meta(infile_meta.read().split("\n"))
            old_header = data[0].split(",")
            outfile = open(outpath,'w')
            data[0] = ",".join(new_header)
            outfile.write("\n".join(data))
            outfile.close()
            print("%s written" % (out_folder + os.sep + filenames['data']))
        return outpath
    else:
       print("Error! %s and/or %s not found, please make sure to extract your aff_download.zip to that path!" % (infilename_data,infilename_meta))

#given a parcel filename, join all census attributes to it
def get_census(input_parcelfile,state="NC",year=16,update_census=0,join_census=0):
    update_census_geoid = 0
    if update_census:
       update_census_geoid = 1

    base_remove_cols = ["statefp","countyfp","tractce","blkgrpce","geoid2_census","name","lsad","aland","awater","geometry","geoid2_tab","geodisp_labelgeo"]
    remove_cols = []
    for prefix in list(acs_tables.keys()):
        for base_item in base_remove_cols:
            remove_cols.append(prefix + "_" + base_item)
    out_filename = input_parcelfile.replace(".csv","_census.csv")
    if update_census or not os.path.exists(out_filename) or 1:
        census_files =[
        "../%s/census/processed/ACS_%s_5YR_B01001_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B01002_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B01003_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B02001_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B07201_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B08303_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B15003_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B19013_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B23025_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B25004_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B25061_with_ann-geo.csv" % (state,str(year)),
        "../%s/census/processed/ACS_%s_5YR_B25085_with_ann-geo.csv" % (state,str(year))
        ]
        print("loading source parcels...")
        
        input_parcels = su.load_file(input_parcelfile)[["geometry","pin_num"]]
        census_tracts = su.load_file(census_files[0])
        census_tracts = su.trim_shapes(input_parcels.ix[0].geometry,census_tracts,radius=50)
        
        print("Done. locating census geoids for each parcel...")

        input_parcels = su.run_parallel(su.find_intersecting_polys, input_parcels, 'points',{"polys" : census_tracts, "output_attrs" : ["geoid"]},num_processes=3)
        print("Done.  Joining census files")
        if not join_census:
            input_parcels = input_parcels[['geoid','pin_num']]
        for census_file in census_files:
            code = re.search(r'_B[0-9]+_', census_file ).group(0).replace("_","")
            table = get_table_type(code)            
            print("joining %s" % census_file)
            gdf = su.load_file(census_file)
            prefix_header(gdf,table+"_",skip=['geoid'])
            input_parcels = input_parcels.join(gdf.set_index('geoid'), on='geoid', lsuffix='_source', rsuffix='_dest')
            keep_cols = get_keep_cols(input_parcels,remove_cols) 
            input_parcels = input_parcels[keep_cols]
            if not join_census:
                keep_cols = get_keep_cols(input_parcels,remove_cols) 
                input_parcels = input_parcels[keep_cols]
                su.write_csv(input_parcels,"%s.csv" % table,to_crs="")
                input_parcels = input_parcels[['geoid','pin_num']]
        if join_census:
            keep_cols = get_keep_cols(input_parcels,remove_cols) 
            input_parcels = input_parcels[keep_cols]
            su.write_csv(input_parcels,out_filename)

#build custom variables from acs census csv columns, things like percentages and weighted averages
def build_composite_attrs(table,census="/media/sf_shared/workspace/cre/NC/census/",year="2016"):
    print('processing table: %s' % table)
    filenames =  get_acs_filenames(year,table)
    in_path = (census + os.sep +  "processed" + os.sep + filenames['data']).replace(".csv","-geo.csv")
    df = su.load_file(in_path)
    if table == "age":
        df['perc_u18'] = (df['male_under_5_years']+df['male_5_to_9_years']+df['male_10_to_14_years']+df['male_15_to_17_years']+df['female_under_5_years']+df['female_5_to_9_years']+df['female_10_to_14_years']+df['female_15_to_17_years']) / df['num']
        df['perc_2034'] = (df['female_18_and_19_years']+df['male_18_and_19_years']+df['male_20_years']+df['male_21_years']+df['male_22_to_24_years']+df['male_25_to_29_years']+df['male_30_to_34_years']+df['female_20_years']+df['female_21_years']+df['female_22_to_24_years']+df['female_25_to_29_years']+df['female_30_to_34_years']) / df['num']
        df['perc_3564'] = (df['male_35_to_39_years']+df['male_40_to_44_years']+df['male_45_to_49_years']+df['male_50_to_54_years']+df['male_55_to_59_years']+df['male_60_and_61_years']+df['male_62_to_64_years']+df['female_35_to_39_years']+df['female_40_to_44_years']+df['female_45_to_49_years']+df['female_50_to_54_years']+df['female_55_to_59_years']+df['female_60_and_61_years']+df['female_62_to_64_years']) / df['num']
        df['perc_65'] = (df['male_65_and_66_years']+df['male_67_to_69_years']+df['male_70_to_74_years']+df['male_75_to_79_years']+df['male_80_to_84_years']+df['male_85_years_and_over']+df['female_65_and_66_years']+df['female_67_to_69_years']+df['female_70_to_74_years']+df['female_75_to_79_years']+df['female_80_to_84_years']+df['female_85_years_and_over']) / df['num']
    if table == "work_commute":
        df['avg_min_comm'] = (df['num_5_to_9_minutes']*5+df['num_10_to_14_minutes']*10+df['num_15_to_19_minutes']*15+df['num_20_to_24_minutes']*20+df['num_25_to_29_minutes']*25+df['num_30_to_34_minutes']*30+df['num_35_to_39_minutes']*35+df['num_40_to_44_minutes']*40+df['num_45_to_59_minutes']*45+df['num_60_to_89_minutes']*60+df['num_90_or_more_minutes']*90) / df['num']
    if table == "transport":
        df['perc_bike'] = df['bicycle']/df['num']
        df['perc_walk'] = df['walked']/df['num']
        df['perc_wfh'] = df['worked_at_home']/df['num']
        df['perc_pt'] = df['public_transportation_excl_taxicab']/df['num']
    if table == "education":
        df['perc_coll'] = (df['num_bachelors_degree']+df['num_masters_degree']+df['num_professional_school_degree']+df['num_doctorate_degree'])/df['num']
    if table == "employ":
        df['perc_empl'] = (df['in_labor_force_civilian_labor_force_employed'] + df['in_labor_force_armed_forces']) / df['num']
    if table == "hhtype":
        df['perc_alon'] = df['nonfamily_households_1_person_household']/df['num']
    if table == "yrmed_hshld_inc":
        df = df.rename(columns={"med_household_income_in_the_past_12_months_in_%s_inflation_adjusted_dollars"  % year: "med_hh_inc"})
    if table == "percap_inc":
        df = df.rename(columns={"per_capita_income_in_the_past_12_months_in_%s_inflation_adjusted_dollars" % year: "pc_inc"})
    if table == "geomob":
        df['perc_mov'] = df['num_diff_house_in_us_1yr_diff_metrp'] / df['num']
    if table == "tot_pop":
        df = su.load_file(in_path)
        df = df.rename(columns={'num':'pop'})
    su.write_csv(df,in_path.replace("geo.csv","geo-comp.csv"))

#joins a single census acs csv file to it corresponding boundary shapefile geometries, table may either be a string specifying an acs table name (ie "med_age") or a data frame with a geoid column
def join_geo(table="",year="2016",census = "/media/sf_shared/workspace/cre/NC/census/", out_filename = "",state="NC",boundaries="bg",update=0):

    mkdir(census)

    if type(table) == str:
        in_filename = fix_census_header(year,table,census)
        if out_filename == "":
            out_filename = in_filename.replace(".csv","-geo.csv")

    if update or not os.path.exists(out_filename):

        if type(table) == pandas.DataFrame:
            census_in = table
        elif type(table) == str:
            census_in = pandas.read_csv(in_filename)
            spatial_filename = get_census_shapes(year,state,folder=census,boundaries=boundaries,overwrite=0) #census + "/shapes/cb_%s_%s_%s_500k.shp" % (year,get_fips(state),boundaries) 
        if boundaries == "bg":
            census_shapes = su.load_file(spatial_filename,to_crs="")
            join_field = "geoid"
            census_shapes = census_shapes.rename(columns={"GEOID" : "geoid2"})
            census_shapes = census_shapes.rename(columns = {"AFFGEOID":"geoid"})
        elif boundaries  == "b" and int(year) > 2010:
            spatial_filename = census + "/shapes/tl_%s_%s_tabblock10.shp" % (year,get_fips(state)) 
            spatial_filename = get_census_shapes(year,state,folder=census,boundaries=boundaries,overwrite=0) #census + "/shapes/cb_%s_%s_%s_500k.shp" % (year,get_fips(state),boundaries) 
            census_shapes = su.load_file(spatial_filename,to_crs="")
            join_field = "GEOID10"
        if not os.path.exists(spatial_filename):
            get_census_shapes(year,state,folder=census,boundaries="bg")

        census_in = fs_datatools.clean_index_col(census_in,join_field)
        census_shapes = fs_datatools.clean_index_col(census_shapes,join_field)

        print("Joining Geoids...")
        census_in = census_in.join(census_shapes.set_index(join_field), on=join_field, lsuffix='_tab', rsuffix='_census')
        census_in = census_in.rename(str.lower, axis='columns')
        if out_filename == "":
           return census_in
        else:
            census_in.to_csv(out_filename,index=False)
        return out_filename


def intersect(a,b):
   return list(set(a).intersection(set(b)))

#join all acs csv files together, optionally only keep given rows (geometry and geoid always kept)
def final_join_all(keeps=['med_age','perc_u18','perc_2034','perc_3564','perc_65','avg_min_comm','perc_bike','perc_walk','perc_wfh','perc_pt','perc_coll','perc_empl','perc_alon','med_hh_inc','pc_inc','perc_mov','pop'], year="2016",census = "/media/sf_shared/workspace/cre/NC/census/"):
   keeps.append("geoid")
   global acs_tables
   filenames = [ census+ os.sep + "processed" + os.sep + get_acs_filenames(year,table)['data'].replace(".csv","-geo-comp.csv") for table in list(acs_tables.keys())] 
   df = su.load_file(filenames[0])
   if len(keeps) > 1:
       keeps.append("geometry")
       df = df[(intersect(list(df.columns),keeps))]
       keeps.remove("geometry")
       for i in range(1,len(filenames)):
           df2 = su.load_file(filenames[i])
           intr_cols = intersect(list(df2.columns),keeps)
           if len(intr_cols) > 1:
              print("processing %s. Adding cols: %s" % (filenames[i],", ".join(intr_cols)))
              df = df.join(df2[intr_cols].set_index('geoid'), on='geoid', lsuffix='_source', rsuffix='_dest')
   else:
       for i in range(1,len(filenames)):
           df = df.join(su.load_file(filenames[i]).set_index('geoid'), on='geoid', lsuffix='_source', rsuffix='_dest')
   su.write_csv(df,census + "/demographics_final_%s.csv" % year)

#given a year and state run all the necessary joins and fixes.  Assumes american fact files are in "./census" and TIGER shapes are in ./census/shapes"   
def run(year,state,census="./census"):
    for table in acs_tables:
        fix_census_header(year,table,census)
    join_all_geos(state=state,year=str(year),census=census)
    for table in acs_tables:
       build_composite_attrs(table,census=census,year=str(year))
    demo_path = census + os.sep + "demographics_final_%s.csv" % str(year)
    final_join_all(keeps=['med_age','perc_u18','perc_2034','perc_3564','perc_65','avg_min_comm','perc_bike','perc_walk','perc_wfh','perc_pt','perc_coll','perc_empl','perc_alon','med_hh_inc','pc_inc','perc_mov','pop'], year=str(year),census = census)
    demog = su.load_file(demo_path,to_crs="")
    demog['med_hh_inc'] = demog['med_hh_inc'].apply(lambda x: fs_datatools.force_numeric(x))
    demog['pc_inc'] = demog['pc_inc'].apply(lambda x: fs_datatools.force_numeric(x))
    demog['med_age'] = demog['med_age'].apply(lambda x: fs_datatools.force_numeric(x))
    demog['pop_dens'] = demog['pop']/(demog['geometry'].area*3.86102e-7)
    demog.to_csv(demo_path,index=False)
    print(demo_path,'written')  
