address-matching
================

Python script for matching a list of messy addresses against a gazetteer using dedupe.

## Setup

```bash
git clone git@github.com:datamade/address-matching.git
cd address-matching
pip install "numpy>=1.6"
pip install git+https://github.com/datamade/dedupe.git@gazetteer#egg=DedupeGazetteer
```

## Usage

```bash
unzip data/building_footprints.csv.zip
python address_mathing.py
```