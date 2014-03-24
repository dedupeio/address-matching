address-matching
================

Python script for matching a list of messy addresses against a gazetteer using dedupe. This also functions as a pseudo geocoder if your Gazetteer has lat/long information.

## Setup

```bash
git clone git@github.com:datamade/address-matching.git
cd address-matching
pip install "numpy>=1.6"
pip install git+https://github.com/datamade/dedupe.git@gazetteer#egg=dedupe
```



## Gazetteer
You will need a Gazetteer of all unique addresses in a given area. For this example, we used the [Building Footprints shapefile](https://data.cityofchicago.org/Buildings/Building-Footprints/qv97-3bvb) and extracted the table attributes from the DBF file using csvkit. This file is in the `data` folder and should be unzipped if you want to use it.

```bash
cv data
unzip building_footprints.csv.zip
```

Then, set the path to your Gazetteer in `address_matching.py`

```python
gazetteer_file = open('data/building_footprints.csv', 'rU').read()
```

## List addresses you want to match
This program takes a list of addresses and matches them to individual records in the Gazetteer. For this example, we are using a messy list of early childhood education locations in Chicago. This file can have multiple entries referring to the same place. 

Then, set the path to your messy list of addresses in `address_matching.py`

```python
messy_file = open('data/csv_example_messy_input.csv', 'rU').read()
```

## Usage
Once you have a Gazetteer and a messy input file, run `address_matching.py`

```bash
python address_mathing.py
```

You will be prompted to label some training pairs for dedupe to do its thing. [More on this here](https://github.com/datamade/dedupe/blob/master/README.md#training).

The output will be saved to `address_matching_output.csv`
