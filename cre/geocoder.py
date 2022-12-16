
addr = "13729  US 183 hwy austin tx 78729"
#addr = "1408 swallow drive, raleigh nc 27606"
#addr = "14016 fm 620 austin tx 78717"
addr = "123 st djs3 342"
from geopy.geocoders import Nominatim
geolocator = Nominatim()
location = geolocator.geocode(addr)
#print(location.address)
print((location.latitude, location.longitude))
#print(location.raw)
