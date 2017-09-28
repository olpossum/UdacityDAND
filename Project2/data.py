#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the 
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET
from collections import defaultdict
import cerberus
import schema

#Input File Here
OSM_PATH = "D:\\UdacityDAND\\Project2\\MapsDatabase\\new-orleans_louisiana_sample.osm"

#Output file definitions here
NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

#regex keys definitions here
LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

#import schema definitions from provided file schema.py
SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

#define regex keys for different street type abbreviations here
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
ave_re = re.compile(r'\bAv', re.IGNORECASE)
rd_re = re.compile(r'\bRd', re.IGNORECASE)
st_re = re.compile(r'\bSt', re.IGNORECASE)
lp_re = re.compile(r'\bLp', re.IGNORECASE)
blvd_re = re.compile(r'\bBlvd', re.IGNORECASE)
hwy_re = re.compile(r'\bHwy', re.IGNORECASE)

#define regex key for numeric character cheks
non_decimal = re.compile(r'[^\d]+')

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Bayou", "Circle", "Bend", "Highway", "Point", "Way", 
            "Hollow", "Trace", "Run", "Loop", "Knee", "Ridge", "Park", "Hill", "Cove", "Alley"]

# Mapping dict to keep track of abbreviations to clean
mapping = { "St": "Street",
            "St.": "Street" }

#keeps track of postal codes that we change
postcode_changes = {}
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
            elif st_re.search(street_type):
                mapping[street_type] = 'Street'
            elif lp_re.search(street_type):
                mapping[street_type] = 'Loop'
            elif blvd_re.search(street_type):
                mapping[street_type] = 'Boulevard'
            elif hwy_re.search(street_type):
                mapping[street_type] = 'Highway'

"""this function checks to see if the postal code is all numeric digits and > len=5
if not, it will remove the non numeric characters"""
def audit_post_code(postal_code,postcode_changes):
    print postal_code
    if not postal_code.isdigit() and len(postal_code) > 5:
        new_postal = non_decimal.sub('',postal_code)
        postcode_changes[postal_code] = new_postal
        print postcode_changes
        return new_postal
    else:
        return postal_code

#this function will return True if the element passed to it is a street
def is_street_name(elem):
    isstreet = (elem.attrib['k'] == "addr:street")
    return isstreet

#this function will return True if the element passed to it is a postal code
def is_postal_code(elem):
    isstreet = (elem.attrib['k'] == "addr:postcode")
    return isstreet

#takes in an abbreviation, looks it up in the mapping dict, and replaces it
def update_name(name, mapping):
    # YOUR CODE HERE
    split = name.split(" ")
    for item in split:
        if mapping.get(item):
            name = name.replace(item,mapping[item])
    return name

#cleans up the data and shapes it into a SQL friendly schema
def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    street_types = defaultdict(set)
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    #If tag type is node, shape the data according to the specifications in the header of this file
    if element.tag == 'node':
        for field in NODE_FIELDS:
            node_attribs[field] = element.attrib[field]
        for elem in element:
            for tag in elem.iter("tag"):
                #if the tag is a street type, pass it to the audit function to check the name for consistency
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
                #here is where the actual shaping is done
                if elem.tag:
                    tag_dict = {}
                    tag_dict['id'] = element.attrib['id']
                    #split tag "k" attribute on colon
                    split_tag = elem.attrib['k'].split(':')
                    # if the tag "k" value did not have a colon, we can directly work with K ad v
                    #here we can assume the type is 'regular'
                    #also checking if the type is street
                    if len(split_tag) == 1 and is_street_name(tag):
                        tag_dict['key'] = elem.attrib['k']
                        #we know the type is street_name so we should check to see if any abbreviations can be updated
                        tag_dict['value'] = update_name(elem.attrib['v'], mapping)
                        tag_dict['type'] = 'regular'
                    # if the tag has a colon, handle the multiple components to accurately represent key and type
                    elif len(split_tag) > 1 and is_street_name(tag):
                        keyiter = iter(split_tag)
                        keyiter.next()
                        key = ''
                        for item in keyiter:
                            key += item + ":"
                        tag_dict['key'] = key[:-1]
                        #we know the type is street_name so we should check to see if any abbreviations can be updated
                        tag_dict['value'] = update_name(elem.attrib['v'], mapping)
                        tag_dict['type'] = split_tag[0]
                    #here we check if the type is postal_code and audit accordingly
                    if len(split_tag) == 1 and is_postal_code(tag):
                        tag_dict['key'] = elem.attrib['k']
                        #we know the type is street_name so we should check to see if any abbreviations can be updated
                        tag_dict['value'] = audit_post_code(elem.attrib['v'], postcode_changes)
                        tag_dict['type'] = 'regular'
                    # if the tag has a colon, handle the multiple components to accurately represent key and type
                    #here we check if the type is postal_code and audit accordingly
                    elif len(split_tag) > 1 and is_postal_code(tag):
                        keyiter = iter(split_tag)
                        keyiter.next()
                        key = ''
                        for item in keyiter:
                            key += item + ":"
                        tag_dict['key'] = key[:-1]
                        #we know the type is street_name so we should check to see if any abbreviations can be updated
                        tag_dict['value'] = audit_post_code(elem.attrib['v'], postcode_changes)
                        tag_dict['type'] = split_tag[0]
                    # if the tag "k" value did not have a colon, we can directly work with K ad v
                    #here we can assume the type is 'regular'' and not street
                    elif len(split_tag) == 1: 
                        tag_dict['key'] = elem.attrib['k']
                        tag_dict['value'] = elem.attrib['v']
                        tag_dict['type'] = 'regular'
                    # if the tag has a colon, handle the multiple components to accurately represent key and type
                    #here we know type is not street
                    elif len(split_tag) > 1: 
                        keyiter = iter(split_tag)
                        keyiter.next()
                        key = ''
                        for item in keyiter:
                            key += item + ":"
                        tag_dict['key'] = key[:-1]
                        tag_dict['value'] = elem.attrib['v']
                        tag_dict['type'] = split_tag[0]
                    #append our results from the if/elifs above to the tags dict
                    tags.append(tag_dict)
        return {'node': node_attribs, 'node_tags': tags}
    #If tag type is way, shape the data according to the specifications in the header of this file
    elif element.tag == 'way':
        for field in WAY_FIELDS:
            way_attribs[field] = element.attrib[field]
        i = 0
        for elem in element:
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
                if elem.tag == 'tag':
                    tag_dict = {}
                    tag_dict['id'] = element.attrib['id']
                    split_tag = elem.attrib['k'].split(':')
                    if len(split_tag) == 1:
                        tag_dict['key'] = elem.attrib['k']
                        tag_dict['value'] = elem.attrib['v']
                        tag_dict['type'] = 'regular'
                    if len(split_tag) > 1:
                        keyiter = iter(split_tag)
                        keyiter.next()
                        key = ''
                        for item in keyiter:
                            key += item + ":"
                        tag_dict['key'] = key[:-1]
                        tag_dict['value'] = elem.attrib['v']
                        tag_dict['type'] = split_tag[0]
                    tags.append(tag_dict)
                #if tag is type 'nd' keep track of position and store in way_nodes dict
                elif elem.tag == 'nd':
                    nd_dict = {}
                    nd_dict['id'] = element.attrib['id']
                    nd_dict['node_id'] = elem.attrib['ref']
                    nd_dict['position'] = i
                    way_nodes.append(nd_dict)
                    i += 1
        #print {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        print element
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""
    validate_count = 0
    element_count = 1


    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    if validate_count == 100:
                        print "validating 100x" + str(element_count) + "th element"
                        validate_count = 0
                        element_count += 1
                    validate_element(el, validator)
                    validate_count += 1

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=True)
    pprint(postcode_changes)

