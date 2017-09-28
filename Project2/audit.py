"""
Your task in this exercise has two steps:

- audit the OSMFILE and change the variable 'mapping' to reflect the changes needed to fix 
    the unexpected street types to the appropriate ones in the expected list.
    You have to add mappings only for the actual problems you find in this OSMFILE,
    not a generalized solution, since that may and will depend on the particular area you are auditing.
- write the update_name function, to actually fix the street name.
    The function takes a string with street name as an argument and should return the fixed name
    We have provided a simple test so that you see what exactly is expected
"""
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

#input file here
OSMFILE = "new-orleans_louisiana.osm"

#regex key definitions
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
ave_re = re.compile(r'\bAv', re.IGNORECASE)
rd_re = re.compile(r'\bRd', re.IGNORECASE)

#Lisy of expected street names in the dataset
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Bayou", "Circle", "Bend", "Highway", "Point", "Way", 
            "Hollow", "Trace", "Run", "Loop", "Knee", "Ridge", "Park", "Hill", "Cove", "Alley"]

#Mapping dict to keep track of abbreiations that can be replaced
mapping = { "St": "Street",
            "St.": "Street"
            }

#this function uses iterparse to fetch all of the elements in the dataset
#Uses code that ensures the elements are not stored in memory
#Reference: https://discussions.udacity.com/t/lingering-questions-as-i-head-into-sql-portion-of-p3/237251/27
def get_element(osm_file, tags=('node', 'way')):
    """Yield element if it is the right type of tag"""
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

#this function checks to see if the street type is in the expected set()
#In not, and the street types matches one of our regex keys, we add it to the mapping dict for cleaning later
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)
            if ave_re.search(street_type):
                mapping[street_type] = 'Avenue'
            elif rd_re.search(street_type):
                mapping[street_type] = 'Road'


#this function will return True if the element passed to it is a street
def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

#main function used to control the auditing process
def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    #iterate through the elements and check to see if they are street names
    for elem in get_element(osm_file):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                #If the tag is a street name pass it to our auditing function
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types

#function used to update abbreviations 
def update_name(name, mapping):
    # YOUR CODE HERE
    split = name.split(" ")
    for item in split:
        if mapping.get(item):
            name = name.replace(item,mapping[item])
    return name

if __name__ == '__main__':
    st_types = audit(OSMFILE)
    pprint.pprint(dict(st_types))
    pprint.pprint(dict(mapping))
    

