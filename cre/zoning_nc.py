import re
import fs_datatools

nc_raleigh_ft = {"3":50,"4":62,"5":75,"7":90,"12":150,"20":250,"40":500}

distinfo_layout = ["zoning_distr_descr", "zoning_units_per_acre", "zoning_height_ft", "zoning_height_stories"]

zoning = {
    "NC_Apex" : {
        "url" : "https://www.apexnc.org/DocumentCenter/View/24/Unified-Development-UDO-PDF?bidId=",
        "rules" : {
            'zoning_conditional' : [
                lambda x : "Y" if "CU" in str(x) or "CZ" in str(x) else "N", 
            ],
            'zoning_planned' : [
                lambda x : "Y" if "PC" in str(x) else "N",
            ]
        },
        "district_info" : { ### [zoning_distr_descr, zoning_units_per_acre, zoning_height_ft, zoning_height_stories] 
            "RA":["Residential Agricultural","0.2","50",""],
            "RR":["Rural Residential","1","50",""],
            "LD":["Low Density Residential","3","60",""],
            "MD":["Medium Density Residential","6","65",""],
            "HDSF":["High Density Single Family Residential","8","65",""],
            "HDMF":["High Density Multifamily Residential","14","65",""],
            "MH":["Manufactured Housing","6","65",""],
            "MHP":["Mobile Home Park","8","65",""],
            "MORR":["Mixed Use","12","70",""],
            "O&I":["Office and Institutional","","48",""],
            "B1":["Neighborhood Business","","48",""],
            "B2":["Downtown Business","","48",""],
            "PC":["Planned Commercial","","75",""],
            "TF":["Tech/Flex","","48",""],
            "LI":["Light Industrial","","48",""],
            "CB":["Conservation Buffer","","36",""]
        }
    },
    "NC_Carrboro" : { 
        "url" : "http://gis.ci.carrboro.nc.us/ZoningQuery/pickzone.asp",
        "rules" : {
            'zoning_conditional' : [
                lambda x : "Y" if "CU" in str(x) or "CZ" in str(x) else "N", 
            ],
            'zoning_planned' : [
                lambda x : "Y" if fs_datatools.are_any_in(str(x),["PDD","PUD"]) else "N",
            ]
        },
        "district_info" : {### [zoning_distr_descr, zoning_units_per_acre, zoning_height_ft, zoning_height_stories] 
            "B1C":["Town Center Business","","50",""],
            "B1G":["General Business","14.5","50",""],
            "B2":["Fringe Commercial","5.8","28","2"],
            "B3":["Neighborhood Business","5.8","28",""],
            "B3T":["Transition Area Business","5.8","28",""],
            "B4":["Outlying Concentrated Business","14.5","50",""],
            "B5":["Watershed Commercial","","35",""],
            "C":["Conservation","","35",""],
            "CT":["Corporate Town","5.8","45","3"],
            "M1":["Light Manufacturing","","45","3"],
            "M3":["Special Manufacturing-Conditional Use","","45","3"],
            "O":["Office","5.8","35",""],
            "OA":["Office/Assembly","5.8","35",""],
            "ORMU":["Office-Residential Mixed Use","14.5","60","4"],
            "RR":["Rural Residential","1.0","35",""],
            "RSIR":["Residential/Intensive Residential","4.4","100",""],
            "VMU":["Village Mixed Use","","",""],
            "WM3":["Watershed Light Industrial","","35",""],
            "WR":["Watershed Residential","","40",""],
            "R20":["Residential","2.2","35",""],
            "R15":["Residential","2.9","35",""],
            "R10":["Residential","4.4","35",""],
            "R75":["Residential","5.8","35",""],
            "R3":["Residential","14.5","35",""],
            "R2":["Residential","29.1","50",""],
         }
    },
    "NC_ChapelHill":{ 
        "url" : "https://www.townofchapelhill.org/town-hall/government/code-of-ordinances",
        "rules" : {
            'zoning_conditional' : [
                lambda x : "Y" if "-C" in str(x) or "CZD" in str(x) else "N"
            ],
            'zoning_planned' : [
                lambda x : "N",
            ]

        },
        "district_info" : { ### [zoning_distr_descr, zoning_units_per_acre, zoning_height_ft, zoning_height_stories] 
            "TC-1":["Town Center","","60",""],
            "TC-2":["Town Center","","90",""],
            "TC-3":["Town Center","","120",""],
            "CC":["Community Commercial","15","60",""],
            "NC":["Neighborhood Commercial","10","60",""],
            "OI-1":["Office/Institutional","10","60",""],
            "OI-2":["Office/Institutional","15","60",""],
            "OI-3":["Office/Institutional","","",""],
            "OI-4":["Office/Institutional","","",""],
            "LI":["Light Industrial","0","90",""],
            "I":["Industrial","","50",""],
            "RT":["Rural Transition","0.4","35",""],
            "MH":["Material Handling","","35",""],
            "MU-OI-1":["Mixed Use","","90",""],
            "MU-R-1":["Mixed Use","","90",""],
            "MU-V Art":["Mixed Use","20","114",""],
            "MU-V Col":["Mixed Use","15","90",""],
            "MU-V Local":["Mixed Use","15","40",""],
            "MU-V":["Mixed Use","","",""],
            "R-LD5":["Residential","0.2","35",""],
            "R-LD1":["Residential","1","35",""],
            "R-1A":["Residential","2","38",""],
            "R-1":["Residential","3","40",""],
            "R-2A":["Residential","3.5","50",""],
            "R-2":["Residential","4","50",""],
            "R-3":["Residential","7","60",""],
            "R-4":["Residential","10","60",""],
            "R-5":["Residential","15","60",""],
            "R-6":["Residential","15","60",""],
            "R-SS":["Residential","","60",""],
        }
    },
    "NC_Cary": {
        "url":"http://library.amlegal.com/nxt/gateway.dll/North%20Carolina/cary_nc/appendixalanddevelopmentordinance*?f=templates$fn=default.htm$3.0$vid=amlegal:cary_nc$anc=JD_LandDevelopmentOrdinance",
        "rules" : {	
            'zoning_conditional' : [
                lambda x : "Y" if fs_datatools.are_any_in(str(x),["CU","-C"]) else "N",
            ],
            'zoning_planned' : [
                lambda x : "Y" if fs_datatools.are_any_in(str(x),["PDD","PUD"]) else "N",
            ]

        },
        "district_info" : { ### [zoning_distr_descr, zoning_units_per_acre, zoning_height_ft, zoning_height_stories] 
            "R80":["Residential","0.5","35",""],
            "R40":["Residential","1.1","35",""],
            "R20":["Residential","2.2","35",""],
            "R12":["Residential","3.6","35",""],
            "R8":["Residential","5.4","35",""],
            "TR":["Transitional Residential","6","35",""],
            "RMF":["Residential Multifamily","12","35",""],
            "R/R":["Resource/Recreation","","",""],
            "OI":["Office/Industrial","","50",""],
            "GC":["General Commercial","","50",""],
            "ORD":["Office/R&D","","50",""],
            "I":["Industrial","","50",""],
            "MXD":["Mixed Use District","25","45",""],
            "PDD":["Planned Devel","","",""],
            "TC":["Town Center","","",""],
            "CT":["Walnut St Cor Transitional Dist","1","35",""]
        }
    },
    "NC_FuquayVarina" : {
        "url":"http://www.fuquay-varina.org/DocumentCenter/View/1679/Land-Development-Ordinance-LDO-PDF?bidId=",
        "rules" : {	
            'zoning_conditional' : [
                lambda x : "Y" if "CZD" in str(x) else "N", 
            ],



        },
        "district_info" : { ### [zoning_distr_descr, zoning_units_per_acre, zoning_height_ft, zoning_height_stories] 			
            "RC":["Resource Conservation","1","40",""],
            "RA":["Residential Agricultural","2","40",""],
            "RLD":["Residential Low Density","3","40",""],
            "RMD":["Residential Medium Density","6","50",""],
            "RHD":["Residential High Density","16","70",""],
            "O&I":["Office & Institutional","","65",""],
            "NC":["Neighborhood Commercial","","65",""],
            "CC":["Corridor Commercial","16","65",""],
            "GC":["General Commercial","16","65",""],
            "RLI":["Research Light Industrial","18","65",""],
            "HI":["Heavy Industrial","","65",""],
            "DC-1":["Downtown Center 1","","70","5"],
            "DC-2":["Downtown Center 2","22","70","5"],
            "TCR":["Town Center Residential","10","50",""],
            "RMU":["Residential Mixed Use","18","40","2"],
            "NMU":["Neighborhood Mixed Use","22","50","3"],
            "UMU":["Urban Mixed Use","","70","5"],
        }
    },
    "NC_Garner": {
        "url":"https://www.garnernc.gov/home/showdocument?id=6079",
        "rules" : {
            'zoning_conditional' : [
                lambda x : "N",
            ],
            'zoning_planned' : [
                lambda x : "N",
            ]
        },
        "district_info" : { ### [zoning_distr_descr, zoning_units_per_acre, zoning_height_ft, zoning_height_stories] 
            "R-40":["Residential","1.1","35",""],
            "R-20":["Residential","2.2","35",""],
            "R-15":["Residential","2.9","35",""],
            "R-12":["Residential","3.6","35",""],
            "R-9":["Residential","4.8","35",""],
            "RMH":["Manufactured Homes","8.7","35",""],
            "MF-1":["Multifamily, Mid-density","9.35","35",""],
            "MF-2":["Multifamily, High-density","13.8","35",""],
            "NO":["Neighborhood Office","7","35",""],
            "O&I":["Office and Industrial","7","",""],
            "NC":["Neighborhood Commercial","7","35",""],
            "CBD":["Central Business District","7","",""],
            "CR":["Community Retail","7","",""],
            "SB":["Service Business","","",""],
            "I-1":["Light Industrial","","",""],
            "I-2":["Heavy Industrial","","",""]
        },
    },
    "NC_Raleigh" : {
        "url":"https://www.raleighnc.gov/content/extra/Books/PlanDev/UnifiedDevelopmentOrdinance/32/#zoom=z",
        "rules" : {
            'zoning_conditional' : [
                lambda x : "Y" if "CU" in str(x) else "N"
            ],
            'zoning_height_stories' : [
                lambda x : re.findall(r'[0-9]+',x)[0] if fs_datatools.are_any_in(str(x),["RX","OP","OX","NX","CX","DX","IX"]) else "",
                lambda x : 3 if fs_datatools.are_any_in(str(x),["R-","CM","AP","IH","CMP"]) else ""
            ],
            'zoning_height_ft' : [
                lambda x : nc_raleigh_ft[re.findall(r'[0-9]+',x)[0]] if fs_datatools.are_any_in(str(x),["RX","OP","OX","NX","CX","DX","IX"]) else "",
            ],
            'zoning_planned' : [
                lambda x : "Y" if "PD" in str(x) else "N",
            ]
        },
        "district_info" : { ### [zoning_distr_descr, zoning_units_per_acre, zoning_height_ft, zoning_height_stories] 
            "R-10":["Residential","10","45","3"],
            "R-6":["Residential","6","40","3"],
            "R-4":["Residential","4","40","3"],
            "R-2":["Residential","2","40","3"],
            "R-1":["Residential","1","40","3"],
            "RX":["Residential Mixed Use","","",""],
            "OP":["Office Park","","",""],
            "OX":["Office Mixed Use","","",""],
            "NX":["Neighborhood Mixed Use","","",""],
            "CX":["Commercial Mixed Use","","",""],
            "DX":["Downtown Mixed Use","","",""],
            "IX":["Industrial Mixed Use","","",""],
            "CM":["Conservation Management","","40","3"],
            "AP":["Agriculture Productive","1","40","3"],
            "IH":["Heavy Industrial","","50","3"],
            "MH":["Manufactured Housing","6","40",""],
            "CMP":["Campus","","50","3"],
            "PD":["Planned Devel","","",""],
        }
    },
    "NC_WakeForest":{
        "url":"https://www.wakeforestnc.gov/Data/Sites/1/media/Residents/Planning/development%20services/currentudo.pdf",
        "rules" :{
            'zoning_conditional' : [
                lambda x : "N",
            ],
            'zoning_planned' : [
                lambda x : "N",
            ]
        },
        "district_info" : { ### [zoning_distr_descr, zoning_units_per_acre, zoning_height_ft, zoning_height_stories] 
            "GR3":["General Residential","3","40","3"],
            "GR5":["General Residential","5","40","3"],
            "GR10":["General Residential","10","40","3"],
            "HB":["Highway Business","","40","3"],
            "HI":["Heavy Industrial","","35",""],
            "ICD":["Institutional Campus","","35",""],
            "LI":["Light Industrial","","35",""],
            "NB":["Neighborhood Business","","40","3"],
            "NMX":["Neighborhood Mixed Use","","80","6"],
            "OS":["Open Space","","",""],
            "RA HC":["Renaissance Urban Core","","80","6"],
            "RD":["Rural Holding","1","35",""],
            "RMX":["Mixed Use Residential","24","80","6"],
            "UMX":["Urban Mixed Use","","80","6"],
            "UR":["Urban Residential","10","40","3"],
        }
    }
}
