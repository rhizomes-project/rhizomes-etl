# Rhizomes ETL Scripts 

# Overview 

# Description of metadata providers 

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


Note that DPLA tends to contain a lot of records that are originally from Calisphere, so in order to avoid duplicates, you should do the following:

- store Calisphere metadata first in a csv file
- now store DPLA metadata, and pass in the name of the Calisphere metadata file, so that the script knows how to de-dupe the DPLA metadata:

```
etl/etl_dpla.py --dupes_file=calisphere.csv > DPLA.csv
```