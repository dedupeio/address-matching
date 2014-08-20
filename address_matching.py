#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates how to use dedupe with to match messy records
against a deduplicated, canonical dataset. In this example, we'll be
matching messy address strings against a list of valid adddresses in
Chicago.
"""

import os
import csv
import re
import logging
import optparse
from numpy import nan

import dedupe
import streetaddress

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added for convenience.
# To enable verbose logging, run `python examples/csv_example/csv_example.py -v`

optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING 
if opts.verbose == 1:
    log_level = logging.INFO
elif opts.verbose >= 2:
    log_level = logging.DEBUG
logging.basicConfig(level=log_level)

def preProcess(column):
    """
    Do a little bit of data cleaning with the help of [AsciiDammit](https://github.com/tnajdek/ASCII--Dammit) 
    and Regex. Things like casing, extra spaces, quotes and new lines can be ignored.
    """

    column = dedupe.asciiDammit(column)
    column = re.sub('\n', ' ', column)
    column = re.sub('-', '', column)
    column = re.sub('/', ' ', column)
    column = re.sub("'", '', column)
    column = re.sub(",", '', column)
    column = re.sub(":", ' ', column)
    column = re.sub('  +', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def readData(input_file, prefix=None):
    """
    The data we'll be matching against are address strings. We'll
    use the python-streetaddress library to attempt to parse the 
    string into meaningful subcomponents.
    """

    data = {}
    with open(input_file) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = dict((k, preProcess(v)) for (k, v) in row.items())

            parsed_address = streetaddress.parse(clean_row['Address'] 
                                             + ', chicago, il')
            if parsed_address :
                clean_row['Address Parsed'] = 1
            else :
                clean_row['Address Parsed'] = 0
                parsed_address = {}

            key_components = {'number' : '', 'prefix' : '', 
                              'street' : '', 'type' : ''}

            key_components.update(parsed_address)
            
            clean_row.update(key_components)
            clean_row['Original Address'] = row['Address']

            data[input_file + str(i)] = clean_row

    return data


# These are custom comparison functions we'll use in our datamodel
def allParsed(field_1, field_2) :
    return field_1 * field_2


# ## Setup
output_file = 'address_matching_output.csv'
settings_file = 'address_matching_learned_settings'
training_file = 'address_matching_training.json'
canonical_file = 'data/building_footprints.csv'
messy_file = 'data/messy_addresses.csv'

    
print 'importing data ...'
canonical_addresses = readData(canonical_file)
messy_addresses = readData(messy_file)

# ## Training
if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file) as sf :
        linker = dedupe.StaticGazetteer(sf)

else:
    # Define the fields dedupe will pay attention to
    #
    # Notice how we are telling dedupe to use a custom field comparator
    # for the 'Zip' field. 
    fields = [ {'field' : 'Address', 'type': 'String',
                'variable name' : 'Address_String'}, 
               {'field' : 'Address', 'type': 'Text',
                'variable name' : 'Address_Text'}, 
               {'field' : 'number', 'type' : 'Exact'}, 
               {'field' : 'prefix', 'type' : 'ShortString',
                'has missing' : True},
               {'field' : 'street', 'type': 'ShortString'},
               {'field' : 'type', 'type' : 'Exact',
                'has missing' : True},
               {'field' : 'Address Parsed', 'type' : 'Custom', 
                'comparator' : allParsed},
               {'type' : 'Interaction',
                'interaction variables' : ['Address_String', 'Address Parsed']},
               {'type' : 'Interaction',
                'interaction variables' : ['Address_Text', 'Address Parsed']},
              ]

    # Create a new linker object and pass our data model to it.
    linker = dedupe.Gazetteer(fields)
    # To train dedupe, we feed it a random sample of records.
    linker.sample(messy_addresses, canonical_addresses, 3000000)

    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        with open(training_file) as tf :
            linker.readTraining(tf)


    dedupe.consoleLabel(linker)
    linker.train()

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf :
        linker.writeTraining(tf)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, 'w') as sf :
        linker.writeSettings(sf)

    linker.cleanupTraining()

#import pdb
#pdb.set_trace()
print 'indexing'
linker.index(canonical_addresses)

clustered_dupes = []

print 'clustering...'
for i, (k, v) in enumerate(messy_addresses.iteritems()) :
    print i
    results = linker.search({k : v})
    if results :
        clustered_dupes.append(results)

#clustered_dupes = linker.match(messy_addresses, canonical_addresses, 
#                               0.0)

print '# duplicate sets', len(clustered_dupes)

canonical_lookup = {}
for n_results in clustered_dupes :
    (source_id, target_id), score = n_results[0]
    canonical_lookup[source_id] = (target_id, score)

with open(output_file, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['Messy Address', 'Canonical Address', 
                     'Score', 'x_coord', 'y_coord'])

    for record_id, record in messy_addresses.items() :
        row = [record['Original Address'], '', '', '', '']
        if record_id in canonical_lookup :
            canonical_id, score = canonical_lookup[record_id]
            row[1] = canonical_addresses[canonical_id]['Original Address']
            row[2] = score
            row[3] = canonical_addresses[canonical_id]['x_coord']
            row[4] = canonical_addresses[canonical_id]['y_coord']
        writer.writerow(row)
