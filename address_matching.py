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
import unidecode
import usaddress

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
logging.getLogger().setLevel(log_level)


def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.DictReader(utf_8_encoder(unicode_csv_data),
                                dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield {k : unicode(v, 'utf-8') for k, v in row.items()}

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.decode('utf-8').encode('utf-8')

def readData(input_file):
    """
    The data we'll be matching against are address strings. We'll
    use the python-streetaddress library to attempt to parse the 
    string into meaningful subcomponents.
    """

    data = {}
    with open(input_file) as f:
        reader = unicode_csv_reader(f)
        for i, row in enumerate(reader):
            data[input_file + unicode(i)] = row

    return data


# ## Setup
output_file = 'address_matching_output.csv'
settings_file = 'address_matching_learned_settings'
training_file = 'address_matching_training.json'
canonical_file = 'data/chicago_addresses.csv'
messy_file = 'data/messy_addresses.csv'

    
print 'importing data ...'
messy_addresses = readData(messy_file)

canonical_addresses = readData(canonical_file)

# ## Training
if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file) as sf :
        linker = dedupe.StaticGazetteer(sf, num_cores=2)

else:
    # Define the fields dedupe will pay attention to
    fields = [{'field' : 'Address', 'type' : 'Address'}]

    # Create a new linker object and pass our data model to it.
    linker = dedupe.Gazetteer(fields, num_cores=2)
    # To train dedupe, we feed it a random sample of records.
    linker.sample(messy_addresses, canonical_addresses, 30000)

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

print 'indexing'
linker.index(canonical_addresses)

clustered_dupes = []

print 'clustering...'
clustered_dupes = linker.match(messy_addresses, 0.0)

print '# duplicate sets', len(clustered_dupes)
print 'out of', len(messy_addresses) 

canonical_lookup = {}
for n_results in clustered_dupes :
    (source_id, target_id), score = n_results[0]
    canonical_lookup[source_id] = (target_id, score)

with open(output_file, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['Messy Address', 'Canonical Address', 
                     'Score', 'x_coord', 'y_coord'])

    for record_id, record in messy_addresses.items() :
        row = [record['Address'], '', '', '', '']
        if record_id in canonical_lookup :
            canonical_id, score = canonical_lookup[record_id]
            row[1] = canonical_addresses[canonical_id]['Address']
            row[2] = score
            row[3] = canonical_addresses[canonical_id]['LONGITUDE']
            row[4] = canonical_addresses[canonical_id]['LATITUDE']
        writer.writerow(row)
