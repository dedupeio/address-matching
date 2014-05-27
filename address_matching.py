#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates how to use dedupe with a comma separated values
(CSV) file. All operations are performed in memory, so will run very
quickly on datasets up to ~10,000 rows.

We start with a CSV file containing our messy data. In this example,
it is listings of early childhood education centers in Chicago
compiled from several different sources.

The output will be a CSV with our clustered results.

For larger datasets, see our [mysql_example](http://open-city.github.com/dedupe/doc/mysql_example.html)
"""

import os
import csv
import re
import logging
import optparse
from cStringIO import StringIO
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


# ## Setup

# Switch to our working directory and set up our input and out put paths,
# as well as our settings and training file locations
output_file = 'address_matching_output.csv'
settings_file = 'address_matching_learned_settings'
training_file = 'address_matching_training.json'

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

def merge_address_fields(filename, merge_address_fields=[]):
    """
    Read in our data from a CSV file and create a dictionary of records, 
    where the key is a unique record ID.
    """

    data_d = []

    with open(filename) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = dict([(k, preProcess(v)) for (k, v) in row.items()])
            if len(merge_address_fields) > 1:
                full_address = ''
                for f in merge_address_fields:
                    full_address += row[f] + ' '
                clean_row['Address'] = full_address

            data_d.append(clean_row)

    with open('merge_address_fields.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=data_d[0].keys())
        writer.writeheader()
        writer.writerows(data_d)

def readData(input_file, prefix=None):
    """
    Read in our data from a CSV file and create a dictionary of records, 
    where the key is a unique record ID and each value is a 
    [frozendict](http://code.activestate.com/recipes/414283-frozen-dictionaries/) 
    (hashable dictionary) of the row fields.

    **Currently, dedupe depends upon records' unique ids being integers
    with no integers skipped. The smallest valued unique id must be 0 or
    1. Expect this requirement will likely be relaxed in the future.**
    """

    data = {}
    reader = csv.DictReader(StringIO(input_file))
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
        
        if prefix :
            row_id = (prefix, i)
        else :
            row_id = i
        data[row_id] = dedupe.core.frozendict(clean_row)

    return data


def writeLinkedResults(clustered_pairs, input_1, input_2, output_file, inner_join = True) :
    logging.info('saving unique results to: %s' % output_file)

    matched_records = []
    seen_1 = set()
    seen_2 = set()

    input_1 = [row for row in csv.reader(StringIO(input_1))]
    row_header = input_1.pop(0)
    length_1 = len(row_header)

    input_2 = [row for row in csv.reader(StringIO(input_2))]
    row_header_2 = input_2.pop(0)
    length_2 = len(row_header_2)
    row_header += row_header_2

    for pair in clustered_pairs :
        index_1 = pair[0][1]
        index_2 = pair[1][1]

        matched_records.append(input_1[index_1] + input_2[index_2])
        seen_1.add(index_1)
        seen_2.add(index_2)

    writer = csv.writer(output_file)
    writer.writerow(row_header)

    for matches in matched_records :
        writer.writerow(matches)
   
    if not inner_join :

        for i, row in enumerate(input_1) :
            if i not in seen_1 :
                writer.writerow(row + [None]*length_2)

        for i, row in enumerate(input_2) :
            if i not in seen_2 :
                writer.writerow([None]*length_1 + row)


def allParsed(field_1, field_2) :
    return field_1 * field_2

def sameOrNot(field_1, field_2) :
    if field_1 and field_2 :
        if field_1 == field_2 :
            return 1
        else:
            return 0
    else :
        return nan

    
print 'importing data ...'
canonical_file = open('data/building_footprints.csv', 'rU').read()
messy_file = open('data/messy_addresses.csv', 'rU').read()
canonical_addresses = readData(canonical_file, prefix='canonical')
messy_addresses = readData(messy_file, prefix='messy')

# ## Training

if os.path.exists(settings_file):
    print 'reading from', settings_file
    linker = dedupe.StaticGazetteer(settings_file)

else:
    # Define the fields dedupe will pay attention to
    #
    # Notice how we are telling dedupe to use a custom field comparator
    # for the 'Zip' field. 
    fields = { 'Address': {'type': 'String'},
               'number' : {'type' : 'Custom', 
                           'comparator' : sameOrNot},
               'prefix' : {'type' : 'String', 
                           'Has Missing' : True},
               'street' : {'type': 'String'},
               'type' : {'type' : 'Custom', 
                         'comparator' : sameOrNot,
                         'Has Missing' : True},

               'Address Parsed' : {'type' : 'Custom', 
                                   'comparator' : allParsed},
               'address-parse' : {'type' : 'Interaction',
                                  'Interaction Fields' : ['Address', 
                                                          'Address Parsed']},
               'number-parse' : {'type' : 'Interaction',
                                  'Interaction Fields' : ['number', 
                                                          'Address Parsed']},
               'prefix-parse' : {'type' : 'Interaction',
                                  'Interaction Fields' : ['prefix', 
                                                          'Address Parsed']},
               'street-parse' : {'type' : 'Interaction',
                                  'Interaction Fields' : ['street', 
                                                          'Address Parsed']},
               'type-parse' : {'type' : 'Interaction',
                                'Interaction Fields' : ['type', 
                                                        'Address Parsed']}
              }

    # Create a new linker object and pass our data model to it.
    linker = dedupe.Gazetteer(fields)
    # To train dedupe, we feed it a random sample of records.
    linker.sample(messy_addresses, canonical_addresses, 3000000)

    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        linker.readTraining(training_file)


    dedupe.consoleLabel(linker)
    linker.train()

    # When finished, save our training away to disk
    linker.writeTraining(training_file)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    linker.writeSettings(settings_file)


# ## Blocking

# ## Clustering

# Find the threshold that will maximize a weighted average of our precision and recall. 
# When we set the recall weight to 2, we are saying we care twice as much
# about recall as we do precision.
#
# If we had more data, we would not pass in all the blocked data into
# this function but a representative sample.

threshold = linker.threshold(messy_addresses, canonical_addresses, recall_weight=10)

# `duplicateClusters` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

print 'clustering...'
clustered_dupes = linker.match(messy_addresses, canonical_addresses, threshold)

print '# duplicate sets', len(clustered_dupes)
# write out our results
with open(output_file, 'w') as f:
    writeLinkedResults(clustered_dupes, 
                       messy_file,
                       canonical_file,
                       f,
                       inner_join=True)
