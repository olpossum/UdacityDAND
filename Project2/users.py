#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
"""
This code was adapted from the lesson exercises for the Wrangle OpenStreetMap Data Project
Your task is to explore the data a bit more.
Adapted this code from the lesson exercises to find out how many unique users
have contributed to the map in this particular area!

The function process_map should return a set of unique user IDs ("uid")
"""
#this function uses iterparse to fetch all of the elements in the dataset
#Uses code that ensures the elements are not stored in memory
#Reference: https://discussions.udacity.com/t/lingering-questions-as-i-head-into-sql-portion-of-p3/237251/27
def get_element(osm_file):
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end':
            yield elem
            root.clear()

#This function will do the heavy lifting for determining the unique users in the dataset
def process_map(filename):
    #create a set() to store the unique users in
    users = set()
    #Loop over all elements in the input osm file
    #Check the element.tag type and fill the set() with unigue users if type== "node", "way" or "relation"
    for element in get_element(filename):
        if element.tag == "node" or element.tag == "way" or element.tag == "relation":
            users.add(element.get('user'))

    return users

if __name__ == "__main__":
    #call the process map function to determine unique users and store it in the users variable
    users = process_map('new-orleans_louisiana_sample.osm')
    #print the list of unique users to the command line
    pprint.pprint(users)
    #print the number of unique users to the command line
    print("Total of " + str(len(users)) + " unique users!")

