#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
"""
This code was adapted from the lesson exercises for the Wrangle OpenStreetMap Data Project
Before you process the data and add it into your database, you should check the
"k" value for each "<tag>" and see if there are any potential problems.

Using the 3 regular expressions provided in the lesson to check for certain patterns
in the tags. As we saw in the quiz earlier, we would like to change the data
model and expand the "addr:street" type of keys to a dictionary like this:
{"address": {"street": "Some value"}}
So, we have to see if we have such tags, and if we have any tags with
problematic characters.

The function 'key_type' ensures that we have a count of each of
four tag categories in a dictionary:
  "lower", for tags that contain only lowercase letters and are valid,
  "lower_colon", for otherwise valid tags with a colon in their names,
  "problemchars", for tags with problematic characters, and
  "other", for other tags that do not fall into the other three categories.
"""

#regex to determine if all elements are lowercase
lower = re.compile(r'^([a-z]|_)*$')
#regex to dtermine if all elements are lowercase and have a colon
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
#regex to identify any problematic characters
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

#this function uses iterparse to fetch all of the elements in the dataset
#Uses code that ensures the elements are not stored in memory
#Reference: https://discussions.udacity.com/t/lingering-questions-as-i-head-into-sql-portion-of-p3/237251/27
def get_element(osm_file, tags=('node', 'way')):
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end':
            yield elem
            root.clear()

#Using regex keys, check each tag for the categories listed in the header description
#If the regex keys find a match, increment the counter for that category in a Python Dict
def key_type(element, keys):
    if element.tag == "tag":
        k = element.get("k")
        if re.search(lower,k):
            keys['lower'] +=1
        elif re.search(lower_colon,k):
            keys['lower_colon'] +=1
        elif re.search(problemchars,k):
            keys['problemchars'] +=1
        else: 
            keys['other'] +=1
        
    return keys

#Use this function to define the keys dict, and iterate over all elements in the dataset
#should return the final tally of keys categories as dict keys when completed
def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    context = ET.iterparse(filename , events=('start', 'end'))
    _, root = next(context)
    for element in get_element(filename):
        keys = key_type(element, keys)

    return keys

#Main function
if __name__ == "__main__":
    keys = process_map('C:\\Users\\Andy\\Documents\\OpenStreetMap\\new-orleans_louisiana_sample.osm')
    pprint.pprint(keys)
