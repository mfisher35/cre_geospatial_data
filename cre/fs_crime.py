import fs_datatools

def classify_crime(crime_descr):
    if fs_datatools.are_any_in(crime_descr,["THEFT","LARCENY","STOLEN","STOLE","STEAL","BURGLARY","ROBBERY","POCKET-PICKING","SHOPLIFTING","POCKETPICKING","PICK POCKET","PICK-POCKET"]):
        return "THEFT"
    elif fs_datatools.are_any_in(crime_descr,["ASSAULT","ATTACK"]):
        return "ASSAULT"
    elif fs_datatools.are_any_in(crime_descr,["DRUG","MARIJUANA","COCAINE","PARAPHERNALIA","NARCOTIC","LSD","OVERDOSE"]):
        return "DRUGS"
    elif fs_datatools.are_any_in(crime_descr,["MURDER","HOMICIDE",'MANSLAUGHTER']):
        return "MURDER"
    elif fs_datatools.are_any_in(crime_descr,["ASSAULT WITH A DEADLY WEAPON","SHOOTING"]):
        return "ASSAULT WITH DEADLY WEAPON"
    elif fs_datatools.are_any_in(crime_descr,['GUNSHOTS','GUN SHOT']):
        return "GUNSHOTS"
    elif fs_datatools.are_any_in(crime_descr,["DAMAGE TO PROPERTY","PROPERTY DAMAGE","VANDALISM","ARSON","DAMAGE TO PERSONAL PROPERTY","INJURY TO REAL PROPERTY","BURNING PERSONNEL PROPERTY"]):
        return "PROPERTY DAMAGE"
    elif fs_datatools.are_any_in(crime_descr,["SEX","PROSTITUTE","PROSTITUTION","INDECENT EXPOSURE","RAPE","MOLESTATION","PORNOGRAPHY","PEEPING TOM","HUMAN TRAFFICKING"]):
        return "SEX CRIME"
    elif "KIDNAP" in crime_descr:
        return "KIDNAPPING"
    elif fs_datatools.are_any_in(crime_descr,["JUVENILE","UNDERAGE"]):
        return "JUVENILE"
    else:
        return "OTHER"
