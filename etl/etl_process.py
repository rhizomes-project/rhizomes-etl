#!/usr/bin/env python

import abc
import re
import os
import sys

from etl.setup import ETLEnv
from etl.tools import MetadataWriter, RhizomeField


import pdb



# REVIEW: Add a step to ETL process to create 1 display date and 1 searchable date, which should be a year.
# REVIEW: Add a step to ETL process to change title values of "[Unknown]" to "Unknown Title" ?
# REVIEW: Add collection name as a field to all metadata.
# REVIEW: TODO pull list of unique keywords (column M) for each provider. keep phrases in tact, get a count of # of occurrences of each and make it case-insensitive.


def get_date_four_chars(date_val, offset=0):

    return date_val[offset : 4 + offset]

def get_date_first_four(date_val):

    return get_date_four_chars(date_val=date_val, offset=0)

def get_date_second_four(date_val):

    return get_date_four_chars(date_val=date_val, offset=1)

def get_date_third_four(date_val):

    return get_date_four_chars(date_val=date_val, offset=2)

def get_date_fourth_four(date_val):

    return get_date_four_chars(date_val=date_val, offset=3)

def get_pth_year_range(date_val, offset=1):

    yr1 = int(date_val[1 : 5])
    yr2 = int(date_val[5 + offset : 9 + offset])

    return int(((yr2 + yr1) / 2) + .5)

def get_pth_year_range_skip2(date_val):

    return get_pth_year_range(date_val=date_val, offset=2)

def get_pth_year_range_offset6(date_val):

    return get_pth_year_range(date_val=date_val, offset=6)

def get_pth_date_range(date_val, offset=1):

    yr1 = int(date_val[1 : 5])
    yr2 = int(date_val[11 + offset : 15 + offset])

    return int(((yr2 + yr1) / 2) + .5)

def get_pth_date_range_2del(date_val):

    return get_pth_date_range(date_val=date_val, offset=2)

def get_pth_decade(date_val):

    yr = int(date_val.replace('X', '0'))
    return yr + 5

def get_pth_decade_estimate(date_val):

    return date_val[ : 3] + '5'



DATE_PARSERS = {

    r'^\d{4}\-\d{2}\-\d{2}':                                get_date_first_four,        # 1992-02-03
    r'^\d{4}':                                              get_date_first_four,        # 1992
    r'^\[\d{4}\.\.\]':                                      get_date_second_four,       # [1920..]
    r'^\{\d{4}\-\d{2}\-\d{2}':                              get_date_second_four,       # '{1843-10-01,1843-10-20}']
    r'^\[\d{4}\,\d{4}\]':                                   get_date_second_four,       # '[1930,1932]'
    r'^\[\d{4}\.\.\d{4}\]$':                                get_pth_year_range_skip2,   # [1992..1998]
    r'^\[\d{4}\-\d{2}\-\d{2}\.\.\d{4}\-\d{2}\-\d{2}\]$':    get_pth_date_range_2del,    # [1900-01-22..1900-01-24]
    r'^\[\d{4}\-\d{2}\-\d{2}\,\d{4}\-\d{2}\-\d{2}\]$':      get_pth_date_range,         # [1900-01-22,1900-01-24]
    r'^\d{3}X':                                             get_pth_decade,             # 192X
    r'^\.\.\d{4}':                                          get_date_third_four,        # ..1840
    r'^\[\.\.\d{4}\]':                                      get_date_fourth_four,       # [..1840]
    r'^\d{4}\-\d{2}\-\d{2}\.\.':                            get_date_first_four,        # 1840-01-28..
    r'^\[\d{4}\-\d{2}\-\d{2}\.\.\]':                        get_date_second_four,       # [1840-01-28..]

    r'^\d{3}[u\?\~]':                                       get_pth_decade_estimate,    # '188u' , '192?' , '196~'
    # r'^\d{3}\?':                                            get_pth_decade_estimate,    # '192?'
    # '196~'


    r'^\{\d{4}\,\d{4}\}':                                   get_pth_year_range,         # {1930,1949}

    r'^\{\d{4}\,\d{4}\,\d{4}\}':                            get_pth_year_range_offset6, # {1947,1956,1966}

    # Final catch-all: try to extract 1st year.
    r'^[\{\[]\d{4}':                                            get_date_second_four,       # {1936,1958~,1961,1962}

    # Other PTH date vals we do not handle (yet?):
    #
    # 'unknown/1896'
    # '19uu'

}

def get_searchable_date(record):
    "Parse the date val and extract a year from it."

    date_vals = record.get(RhizomeField.DATE.value, [])
    if type(date_vals) is not list:

        date_vals = [ date_vals ]

    searchable_date = None

    for date_val in date_vals:

        for pattern, parser in DATE_PARSERS.items():

            if re.match(pattern, date_val):

                searchable_date = parser(date_val=date_val)
                if searchable_date:

                    searchable_date = int(searchable_date)
                    if searchable_date < 1000 or searchable_date > 3000:


                        pdb.set_trace()


                        raise Exception(f"Error: invalid searchable date found: {searchable_date}")

                    return searchable_date


    # REVIEW: remove this?
    if date_vals:


        pdb.set_trace()


        print(f"Ignoring date val '{date_vals[0]}' - could not parse searchable date", file=sys.stderr)


def clean_value(value):
    "Cleans value, including removing bad whitespace."

    if type(value) is set:

        value = list(value)

    if type(value) is str:

        return value.strip()

    elif type(value) is list:

        value.sort()
        for idx, tmp in enumerate(value):

            value[idx] = tmp.strip()

    return value


class BaseETLProcess(abc.ABC):

    def __init__(self, format):

        self.format = format

        self.etl_env = ETLEnv.instance()
        self.etl_env.start()

        if self.etl_env.are_tests_running():

            self.init_testing()

    @abc.abstractmethod
    def init_testing(self):    # pragma: no cover (should never get called)

        pass

    @abc.abstractmethod
    def get_field_map(self):    # pragma: no cover (should never get called)

        pass

    @abc.abstractmethod
    def extract(self):    # pragma: no cover (should never get called)

        pass

    def transform(self, data):
        "Transform the data."

        field_map = self.get_field_map()

        # Make sure that all transforms map from a field_map key to another field_map key, not
        # from a RhizomeField directly to another.
        if field_map.keys() & RhizomeField.values():

            raise Exception(f"Invalid field map keys found: {field_map.keys() & RhizomeField.values()}")

        # De-dupe the records (make sure no record appears more than once).
        record_ids = set()
        id_key = list(field_map.keys())[0]
        for record in data:

            id_val = record[id_key]

            if type(id_val) is list:

                id_val = id_val[0]

            if id_val in record_ids:

                record["ignore"] = True

            else:

                record_ids.add(id_val)

        # Now map all the other values in the raw metadata to the correct output rhizome fields.
        for record in data:

            # Has this record been flagged to be skipped?
            if record.get("ignore", False):

                continue

            for name, descriptions in field_map.items():

                if not descriptions:

                    continue

                if type(name) is RhizomeField:

                    name = name.value

                if type(descriptions) is not list:

                    descriptions = [ descriptions ]

                value = record.get(name)
                if value:

                    for description in descriptions:

                        description = description.value

                        if description == name:

                            continue

                        if record.get(description):

                            record[description] += clean_value(value=value)

                        else:

                            record[description] = clean_value(value=value)

                        del record[name]

        # Populate our Searchable Date.
        for record in data:

            searchable_date = get_searchable_date(record=record)
            record[RhizomeField.SEARCHABLE_DATE] = searchable_date


    def load(self, data):
        "Load the data (into csv, json, database, etc.)"

        writer = MetadataWriter(format=self.format)
        writer.start_collection()

        for record in data:

            if record.get("ignore", False):

                continue

            writer.start_record()

            for name in RhizomeField.values():

                value = record.get(name)
                if value and name:

                    writer.add_value(name=name, value=value)

            writer.end_record()

        writer.end_collection()
