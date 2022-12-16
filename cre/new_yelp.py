from urllib import request
import json
import requests
import yelpkey


def get_yelp(lat,lon,radius=805,categories=['restaurants','parks','cafes','bars','landmarks','shopping']):
    headers = {'Authorization': 'Bearer %s' % yelpkey.key}
    results = {}
    results['coords'] = {}
    
    for cat in categories:
        results[cat] = []
        results['coords'][cat] = {}
    
        i = 0 
        url = 'https://api.yelp.com/v3/businesses/search?latitude=%f&longitude=%f&categories=%s&radius=%i' % (lat,lon,cat,radius)
    
        for offset in range(0, 1000, 50):
            params = {
                'limit': 50, 
                'offset': offset
            }
    
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            if response.status_code == 200:
                for item in data['businesses']:
                    i = i + 1
                    temp_result = [item['name'],"{0:.1f} mile".format(float(item['distance'])/5280),float(item['rating']),item["review_count"]]
                    results['coords'][cat][item['name']] = item['coordinates']
                    results[cat].append(temp_result)
            elif response.status_code == 400:
                print('400 Bad Request')
                break
        results[cat] = sorted(results[cat], key=lambda x:(-x[2],x[1]))
        for i in range(0,len(results[cat])):
            results[cat][i] = [i+1] + results[cat][i]
            
    return results

