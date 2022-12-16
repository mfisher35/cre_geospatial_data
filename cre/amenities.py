import fs_spatialtools as su
import numpy as np
import pandas as pd
import amenity_names

#function that checks if all words in a given input string are all contained in some string in list of strings
def match_all_words(input_str,list_of_strings):
    input_set = set(input_str.split(" "))
    for string in list_of_strings:
       match_vector = set(string.split(" "))
       if match_vector.issubset(input_set):
           return True
    return False
      

#given a list of places, return a list which has a 1 if that place is a grocery store or 0 if not
def is_grocery(places_list):
    results = []
    return [ match_all_words(place.lower(),amenity_names.grocery_names) for place in places_list ]
 
#given a file with place names, filter out grocery stores from the list and save them in a separate file
def separate_grocery_stores(in_filename="restaurants_all.csv",name_col="name",filtered_name="restaurants.csv"):
    restaurants = su.load_file(in_filename)
    is_groc = is_grocery(list(restaurants[name_col]))
    groceries = restaurants[is_groc]
    groceries = su.drop_near_places(groceries,thresh=0)
    su.write_csv(groceries,"grocery.csv")
    restaurants = restaurants[np.invert(is_groc)]
    su.write_csv(restaurants,filtered_name)

#Make a single amenity name looks nice
def clean_name(string):
    string = string.rstrip('1234567890 #')
    string = string.lower().split(" ")
    result = ""
    for item in string:
        if len(item)>1:
            result = result + item[0].upper() + item[1:len(item)] + " "
        else:
            result = result + item + " "
    result = result[0:len(result)-1] 
    return result

#clean all amenity name strings:
def clean_amenity_names(in_filename):
    df = su.load_file(in_filename)
    df['name'] = df['name'].apply(lambda x: clean_name(str(x)))
    su.write_csv(df,in_filename)
