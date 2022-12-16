import zoning_nc
import fs_datatools

zoning_dict = zoning_nc.zoning
distinfo_layout = zoning_nc.distinfo_layout


def update_result(result,key,value):
    if (result[key] == "" or result[key] == None) and not (value == "" or value == None):
       result[key] = fs_datatools.convert_numeric(value)
       return 1
    else:
       return 0

#given a zoning code column and a given path (.e.g. "NC_Apex/zoning.csv") return the proper zoning data.  This function is meant to be used as an apply function, Example: df.apply(apply_zoning_details, axis=1, args=(path,)) 
def apply_zoning_details(row,path,zoning_col="zoning"):
    global zoning_dict
    global distinfo_layout
    result = { 'zoning_distr_descr' : "", 'zoning_units_per_acre' : "", 'zoning_height_ft' : "", 'zoning_height_stories' : "", 'zoning_conditional' : "", 'zoning_planned' : ""}
    for city in zoning_dict:
        if city in path:
            for rule_name in zoning_dict[city]['rules']:
                rules = zoning_dict[city]['rules'][rule_name]
                if type(rules) != list:
                    rules = [rules]
                found,i = 0,0
                while found == 0 and i < len(rules):
                    found = update_result(result,rule_name,zoning_dict[city]['rules'][rule_name][i](row[zoning_col]))
                    i = i + 1
            for district_name in zoning_dict[city]['district_info']:
                if str(district_name) in str(row[zoning_col]):
                    for i in range(0,len(distinfo_layout)):
                        update_result(result,distinfo_layout[i],zoning_dict[city]['district_info'][district_name][i])
    for key in result:
        row[key] = result[key]
    return row
