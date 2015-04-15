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
import itertools

import psycopg2
import psycopg2.extras

import dedupe


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

class Database(object) :
    def __init__(self, *args, **kwargs) :
        self.con = psycopg2.connect("dbname=address",
                                    cursor_factory=psycopg2.extras.RealDictCursor)

        super(Database, self).__init__(*args, **kwargs)
        


    def unindex(self, data) : # pragma : no cover
        pass

    def _blockData(self, messy_data) :
        c = self.con.cursor()

        block_groups = itertools.groupby(self.blocker(messy_data.viewitems()), 
                                         lambda x : x[1])

        for i, (record_id, block_keys) in enumerate(block_groups) :
            print i

            A = [(record_id, messy_data[record_id], set())]


            c.execute("SELECT DISTINCT record_id "
                      "FROM blocking_map WHERE "
                      "block_key IN %s", 
                      (tuple(block_key for block_key, _ in block_keys),))

            B = [(rec_id, self.indexed_data[rec_id], set())
                 for rec_id, in c]

            if B :
                yield (A, B)


class DatabaseGazetteer(Database, dedupe.Gazetteer) :
    def train(self, ppc=.1, uncovered_dupes=1, index_predicates=False) : 
        super(DatabaseGazetteer, self).train(ppc, 
                                             uncovered_dupes, 
                                             index_predicates)

    def index(self, data) : # pragma : no cover
        c = self.con.cursor()
        c.execute("DROP TABLE IF EXISTS blocking_map")
        c.execute("CREATE TABLE blocking_map "
                  "(block_key VARCHAR(200), record_id VARCHAR(200))")

        c.executemany("INSERT INTO blocking_map VALUES (%s, %s)",
                      self.blocker(data.viewitems()))

        self.con.commit()

        self.indexed_data = data



class StaticDatabaseGazetteer(Database, dedupe.StaticGazetteer) :

    def index(self, data) : # pragma : no cover
        self.indexed_data = data

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
messy_file = 'Annual_Taxpayer_Location_Address_List-Chicago-2014 (1).csv'

    
print 'importing data ...'
messy_addresses = readData(messy_file)

canonical_addresses = readData(canonical_file)

# ## Training
if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file) as sf :
        linker = StaticDatabaseGazetteer(sf, num_cores=2)

else:
    # Define the fields dedupe will pay attention to
    fields = [{'field' : 'Address', 'type' : 'Address'}]

    # Create a new linker object and pass our data model to it.
    linker = DatabaseGazetteer(fields, num_cores=2)
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

with open(output_file, 'wb') as f:
    writer = csv.writer(f)
    header = messy_addresses.values()[0].keys()
    writer.writerow(header + ['Canonical Address', 'Score', 
                              'Longitude', 'Latitude'])

    for record_id, record in messy_addresses.items() :
        row = record.values() 
        if record_id in canonical_lookup :
            canonical_id, score = canonical_lookup[record_id]
            row += [canonical_addresses[canonical_id].get('Address', None),
                    score, 
                    canonical_addresses[canonical_id].get('LONGITUDE', None),
                    canonical_addresses[canonical_id].get('LATITUDE', None)]
        writer.writerow(row)
