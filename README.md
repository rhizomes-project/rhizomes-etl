# Rhizomes ETL Scripts 

# Overview 

# How to Install 

- Install python3.9 (see https://www.python.org/downloads/)
- Install and activate pyvenv (optional - see https://docs.python.org/3/library/venv.html)
- Install pip (see https://pip.pypa.io/en/stable/installing/)
- Clone this repo and navigate to it in a terminal window (e.g., bash)
- Install required modules:

```
pip install -r requirements.txt
```

# How to Run the etl scripts

- From a terminal window (e.g., bash), run the script for the provider whose metadata you want, e.g., 

```
etl/etl_<data provider>.py
```

For instance, to get metadata for PTH, you can simply call the python script for that provider, and pipe the output to a csv file, e.g.,

```
etl/etl_pth.py > PTH.csv
```

The same holds true for Calisphere, DPLA and ICAA. (Note that the python script filenames are in all lower-case).


Note that DPLA tends to contain a lot of records that are originally from Calisphere, so in order to avoid duplicates, you should do the following (note
that future functionality should make this unnecessary - see "Features not yet supported", below):

- store Calisphere metadata first in a csv file
- now store DPLA metadata, and pass in the name of the Calisphere metadata file, so that the script knows how to de-dupe the DPLA metadata:

```
etl/etl_dpla.py --dupes_file=calisphere.csv > DPLA.csv
```

# Features not yet supported 

- In the near-future we will support only retrieving new records (i.e., records that are not-yet available in the website). This will
also make the need to de-dupe DPLA records that are also present in Calisphere unnecessary.

# Description of metadata providers 

## Calisphere 

## DPLA 

## ICAA 

As of this writing (early July, 2021), ICAA's documentation is quite new and sparse. However, their API and the metadata it serves
are quite clean, well thought-out, and easy to work with. Our team received some direct instruction from Bruno Favaretto, who developed
much of the ICAA API and was quite helpful in pointing us in the right direction in terms of how to use their API. Also, the ICAA team
has put extensive work into making their metadata exceedingly consistent.

## PTH 

PTH has excellent documentation for their OAIPMH API, and their support staff are generally quite helpful via email, but unfortunately
their API is not intended to support the sort of queries that our project ideally needs - it really is intended to copy the entire
database of metadata to the user. Because of this, our PTH ETL scripts first download the entire PTH universe of metadata, and then
filter the metadata locally to identify relavant records. Another aspect of PTH is the metadata is voluminous but not entirely consistent -
for instance, PTH uses many different formats and notations to indicate what date or date range a record is associated with.