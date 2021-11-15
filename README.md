# Rhizomes ETL Scripts 

# Overview 

These Extract, Transform, Load (ETL) scripts pull metadata relevant to the Rhizomes project
from several institutions' APIs and ultimately output that metadata in csv format.
The steps that the scripts use to do this can be described in 3 phases: Extract, Transform and Load:

#### Extract
The scripts first download relevant records from an institution's API - how relevancy is
determined is accomplished in various ways, depending on the capabilities of each institution's API.

#### Transform
Next, the scripts perform transformations on the metadata to address any problems with the
data, including inconsistent date formats, missing values and duplicate records.

#### Load
This final step involves outputing the metadata in csv format to standard out so that it can be written to
a file.

#### Institutions Currently Supported:

- Calisphere
- Digital Public Library of America (DPLA)
- Portal to Texas History (PTH)
- International Center for the Arts of the Americas at the Museum of Fine Arts, Houston, Documents of Latin American and Latino Art (ICAA)


# Description of metadata providers 

#### Calisphere

Calisphere has a fairly well-documented, clean API - our ETL script queries it using a combination of collections and keywords to retrieve
relevant records.

#### DPLA

While DPLA's API is also fairly well-documented, their metadata arguably errs on the side of supplying too much possibly-unneeded information.
One issue with DPLA is they ingest metadata from other sources, including Calisphere, which has meant that the resulting duplicate records
has been an issue we have had to work around. In addition, DPLA includes a string (under the key "originalRecord") in their JSON which contains
the original metadata as it was received by DPLA from the original data source - parsing this string can be cumberson and error-prone, since the 
format of the original string varies considerably from one source to another.

#### ICAA

As of this writing (early July, 2021), ICAA's documentation is quite new and sparse. However, their API and the metadata it serves
are quite clean, well thought-out, and easy to work with. Our team received some direct instruction from Bruno Favaretto, who developed
much of the ICAA API and was quite helpful in pointing us in the right direction in terms of how to use their API. Also, the ICAA team
has put extensive work into making their metadata exceedingly consistent.

#### PTH

PTH has excellent documentation for their OAIPMH API, and their support staff are generally quite helpful via email, but unfortunately
their API is not intended to support the sort of queries that our project ideally needs - it really is intended to copy the entire
database of metadata to the user. Because of this, our PTH ETL script first downloads the entire PTH universe of metadata, and then
filters the metadata locally to identify relavant records. Another aspect of PTH is the metadata is voluminous but not terribly consistent -
for instance, PTH uses many different formats and notations to indicate what date or date range a record is associated with. Also, we
occasionally receive error 500 messages from the server for reasons unknown (and the ETL script has error-handling to attempt to deal with this issue).


# How to Install

- Install python3.9 (see https://www.python.org/downloads/)
- Install and activate pyvenv (optional - see https://docs.python.org/3/library/venv.html)
- Install pip (see https://pip.pypa.io/en/stable/installing/)
- Clone this repo and navigate to it in a terminal window (e.g., bash)
- Copy your secrets file to <ROOT>/etl/secrets.json, where <ROOT> is whereever you cloned the repo (see
Karl Kaebnick or Colin McFadden if you need a copy of this file - its contents need to remain secret and so
it is not in the repo)
- Install required modules:

```
pip install -r requirements.txt
```

# How to Run the ETL scripts

- From a terminal window (e.g., bash), run the script for the provider whose metadata you want, e.g., 

```
etl/etl_<data provider>.py
```

For instance, to get metadata for PTH, you can simply call the python script for that provider, and pipe the output to a csv file, e.g.,

```
etl/etl_pth.py > PTH.csv
```

The same holds true for Calisphere, DPLA and ICAA. (Note that the python script filenames are all lower-case).

Regarding the issue of avoiding loading the same record more than once, the ETL script uses the Omeka API to identify
which records are already on the website (currently at https://romogis.frankromo.com/rhizomes-dev/api/items) - any records
that are already loaded will not be put into the csv files.

Note that DPLA contains a lot of records that are originally from Calisphere, so in order to avoid duplicates, you should do one of the following:

- Load the Calisphere data into the Rhizomes database *before* running the ETL script for DPLA. The ETL transform
process checks to see if each record is already present in the Rhizomes website (via the Omeka API) -
any records already loaded will be ignored to avoid duplicates. Note: it may be helpful to make sure the Calisphere
records are available via the Omeka API (currently at https://romogis.frankromo.com/rhizomes-dev/api/items) before 
running the dpla ETL script, to ensure the previously-loaded items can be identied.

- Store the Calisphere metadata first in a csv file. Now run the DPLA ETL script, and pass in the name of the Calisphere metadata file, so that the script knows how to de-dupe the DPLA metadata. For instance:

```
etl/etl_dpla.py --dupes_file=calisphere.csv > DPLA.csv
```

Command line options for the ETL scripts:

`--dupes_file` - pass in the name of a csv file (e.g., calisphere.csv) that contains items that may be duplicated by the current institution for whom you are running the ETL script (for more details, see note, above, about DPLA containing items from Calisphere)

`--rebuild_previous_items` - pass in 'yes' or 'no', indicating whether the ETL script should output metadata for items that are already loaded in the website (default is 'no').

`--images_only` - pass in 'yes' or 'no', indicating whether records that have no image should be ignored (default is 'no').
