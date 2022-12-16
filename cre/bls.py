import urllib.request as urllib2
import requests
import json
import datetime
import county
import pprint

def lookup(params):

   gis_dictionary = {

                      


                      "ny" : {
                                     'non-farm' : 'SMS36000000000000001',
                                     'wages' : {
                                          "Albany":"ENU3600110010",
                                          "Allegany":"ENU3600310010",
                                          "Bronx":"ENU3600510010",
                                          "Broome":"ENU3600710010",
                                          "Cattaraugus":"ENU3600910010",
                                          "Cayuga":"ENU3601110010",
                                          "Chautauqua":"ENU3601310010",
                                          "Chemung":"ENU3601510010",
                                          "Chenango":"ENU3601710010",
                                          "Clinton":"ENU3601910010",
                                          "Columbia":"ENU3602110010",
                                          "Cortland":"ENU3602310010",
                                          "Delaware":"ENU3602510010",
                                          "Dutchess":"ENU3602710010",
                                          "Erie":"ENU3602910010",
                                          "Essex":"ENU3603110010",
                                          "Franklin":"ENU3603310010",
                                          "Fulton":"ENU3603510010",
                                          "Genesee":"ENU3603710010",
                                          "Greene":"ENU3603910010",
                                          "Hamilton":"ENU3604110010",
                                          "Herkimer":"ENU3604310010",
                                          "Jefferson":"ENU3604510010",
                                          "Kings":"ENU3604710010",
                                          "Lewis":"ENU3604910010",
                                          "Livingston":"ENU3605110010",
                                          "Madison":"ENU3605310010",
                                          "Monroe":"ENU3605510010",
                                          "Montgomery":"ENU3605710010",
                                          "Nassau":"ENU3605910010",
                                          "New York":"ENU3606110010",
                                          "Niagara":"ENU3606310010",
                                          "Oneida":"ENU3606510010",
                                          "Onondaga":"ENU3606710010",
                                          "Ontario":"ENU3606910010",
                                          "Orange":"ENU3607110010",
                                          "Orleans":"ENU3607310010",
                                          "Oswego":"ENU3607510010",
                                          "Otsego":"ENU3607710010",
                                          "Putnam":"ENU3607910010",
                                          "Queens":"ENU3608110010",
                                          "Rensselaer":"ENU3608310010",
                                          "Richmond":"ENU3608510010",
                                          "Rockland":"ENU3608710010",
                                          "St. Lawrence":"ENU3608910010",
                                          "Saratoga":"ENU3609110010",
                                          "Schenectady":"ENU3609310010",
                                          "Schoharie":"ENU3609510010",
                                          "Schuyler":"ENU3609710010",
                                          "Seneca":"ENU3609910010",
                                          "Steuben":"ENU3610110010",
                                          "Suffolk":"ENU3610310010",
                                          "Sullivan":"ENU3610510010",
                                          "Tioga":"ENU3610710010",
                                          "Tompkins":"ENU3610910010",
                                          "Ulster":"ENU3611110010",
                                          "Warren":"ENU3611310010",
                                          "Washington":"ENU3611510010",
                                          "Wayne":"ENU3611710010",
                                          "Westchester":"ENU3611910010",
                                          "Wyoming":"ENU3612110010",
                                          "Yates":"ENU3612310010",
                                   }
                   
                     }
            }
   key = "9811b90b770c490b938a739857863315"
   method = "POST"
   params['county'] = county.get_county(params['zip'])
   year = datetime.datetime.today().year
   handler = urllib2.HTTPHandler()
   opener = urllib2.build_opener(handler)
   data = json.dumps({"seriesid": [gis_dictionary[params['state']][params['dataField']][params['county']]],"startyear":"%i" % (year-1), "endyear":"%i" % year, "registrationkey" : key})

   url = "https://api.bls.gov/publicAPI/v2/timeseries/data/?registrationkey=000f4e000f204473bb565256e8eee73e&catalog=true&startyear=2010&endyear=2014&calculations=true &annualaverage=true"
   request = urllib2.Request(url, data=data)
   request.add_header("Content-Type",'application/json')
   request.get_method = lambda: method
   try:
      connection = opener.open(request)
   except Exception as e:
      connection = e
 
   # check. Substitute with appropriate HTTP code.
   if connection.code == 200:
       data = connection.read()
       json_data = json.loads(data)
       for series in json_data['Results']['series']:
          for i in range(0,min(len(series['data']),12)):
             print (i,series['data'][i]['value'],series['data'][i]['periodName'],series['data'][i]['year'])




