address-matching
================

Python script for matching a list of messy addresses against a gazetteer using dedupe. This also functions as a pseudo geocoder if your Gazetteer has lat/long information.

## Setup
Here's how to get this script working - without having dedupe already installed.
```bash
git clone git@github.com:datamade/address-matching.git
cd address-matching
pip install "numpy>=1.6"
pip install -r requirements.txt
```

## Gazetteer
You will need a Gazetteer of all unique addresses in a given area. For this example, we used the [Cook County Address Point shapefile](https://datacatalog.cookcountyil.gov/GIS-Maps/ccgisdata-Address-Point-Chicago/jev2-4wjs).


## List addresses you want to match
This program takes a list of addresses and matches them to individual records in the Gazetteer. For this example, we are using a messy list of early childhood education locations in Chicago. This file can have multiple entries referring to the same place. 

## Usage
Once you have a Gazetteer and a messy input file, run `address_matching.py`

```bash
python address_matching.py
```

You will be prompted to label some training pairs for dedupe to do its thing. [More on this here](https://github.com/datamade/dedupe/blob/master/README.md#training).

The output will be saved to `address_matching_output.csv`
