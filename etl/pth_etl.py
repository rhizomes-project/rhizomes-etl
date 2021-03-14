#!/usr/bin/env python

import os
import re
import requests
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField, get_oaipmh_record

from bs4 import BeautifulSoup


import pdb


protocol = "https://"
domain = "texashistory.unt.edu"

list_sets_path = "/oai/?verb=ListSets"
list_sets_url = protocol + domain + list_sets_path

RECORD_LIMIT = None
record_count = 0


records_path =       "/oai/?verb=ListRecords"
start_records_path = records_path + "&metadataPrefix=oai_dc&set="
start_records_url = protocol + domain + start_records_path
resume_records_url = protocol + domain + records_path


field_map = {
    "identifier":                             RhizomeField.ID,
    "title":                                  RhizomeField.TITLE,
    "creator":                                RhizomeField.AUTHOR_ARTIST,
    "contributor":                            RhizomeField.AUTHOR_ARTIST,
    "description":                            RhizomeField.DESCRIPTION,
    "date":                                   RhizomeField.DATE,
    "type":                                   RhizomeField.RESOURCE_TYPE,
    "format":                                 RhizomeField.DIGITAL_FORMAT,
    "dimensions":                             RhizomeField.DIMENSIONS,
    "url":                                    RhizomeField.URL,
    "source":                                 RhizomeField.SOURCE,
    "language":                               RhizomeField.LANGUAGE,
    "subjects_hist":                          RhizomeField.SUBJECTS_HISTORICAL_ERA,
    "subject":                                RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "subjects_geo":                           RhizomeField.SUBJECTS_GEOGRAPHIC,
    "thumbnail":                              RhizomeField.IMAGES,
}

keyword_limiters = [
    "chicano", "chicana", "chicanx",
    "mexican-american", "mexican american",
]

# REVIEW: See https://docs.google.com/document/d/1cD559D8JANAGrs5pwGZqaxa7oHTwid0mxQG0PmAKhLQ/edit for how to pull data.

DATA_PULL_LOGIC = {

    # # Not sure how to handle system-wide searches like this.
    # "subject": {
    #     "code": "Arts & Crafts"
    # }

    "partner": {

        # Mexic-Arte Museum
        "MAMU": {
            "results": {
                "min": 290,
                "max": 310
            },
            "ignore": True
        },

        # UNT Libraries
        "UNT": {
            "filters": {
                "keywords": {
                    "type": "include",
                    "values": keyword_limiters
                }
            },
            "ignore": False
        },

        # # UNT Libraries Special Collections
        # "UNTA": keyword_limiters,

        # # UNT Libraries Government Documents Department
        # "UNTGD": keyword_limiters,

        # TCU Mary Couts Burnett Library
        "TCU": {
            "results": {
                "number": 528
            },
            "ignore": True
        },
    },

    "collection" : {

        # Art Lies
        "ARTL": {
            "results" : {
                "number": 64
            },
            "ignore": True
        },

        # # Texas Borderlands Newspaper Collection
        # "BORDE": [ "obra de arte", "artista", "arte" ]

    },
}

# REVIEW TODO Get canonical list of PTH formats.
KNOWN_FORMATS = ('image', 'text')


def has_number(value):

    return re.search(r'\d', value)

def does_record_match(record, filters):
    "Returns True if the record matches the filters."

    does_match = False

    for name, filter_ in filters.items():

        if name == "keywords":

            title = ''.join(record.get('title', [])).lower()
            description = ''.join(record.get('description', [])).lower()

            for keyword in filter_["values"]:

                if keyword in title or keyword in description:

                    does_match = True

        else:


            pdb.set_trace()


    return does_match

def extract_records(records, config={}):

    keywords = config.get("keywords")

    filters = config.get("filters")

    data = []
    for record in records:

        record_data = get_oaipmh_record(record=record)

        do_add = True

        if filters:

            do_add = does_record_match(record=record_data, filters=filters)

        if do_add:

            data.append(record_data)

    return data

def extract_data_set(key_name, key, config={}, resumption_token=None):
    """
    Extract all records for the given data set.

    Note: 'data set' can be a PTH partner, collection, subject, etc.
    """

    global record_count

    if not resumption_token:

        record_count = 0

        url = f"{start_records_url}{key_name}:{key}"

    else:

        if ETLEnv.instance().are_tests_running():

            return []

        url = f"{resume_records_url}&resumptionToken={resumption_token}"

    response = requests.get(url)
    if not response.ok:

        raise Exception(f"Error retrieving data from PTH for {key_name} {key}, keywords: {keywords}, status code: {response.status_code}, reason: {response.reason}")

    xml_data = BeautifulSoup(markup=response.content, features="lxml-xml", from_encoding="utf-8")

    # Extract records from this partner.
    records = extract_records(records=xml_data.find_all("record"), config=config)

    # Loop through next set of data (if any).
    resumption_tokens = xml_data.find_all("resumptionToken")
    if resumption_tokens:

        record_count += len(records)
        print(f"{record_count} records ...", file=sys.stderr)

        # Make recursive call to extract all records.
        if not RECORD_LIMIT or record_count < RECORD_LIMIT:

            next_records = extract_data_set(key_name=key_name, key=key, config=config, resumption_token=resumption_tokens[0].text)
            records += next_records

    return records

def check_results(results, key_name='', key='', config={}):
    "Output error info if results are not what was expected."

    num_results = len(results)
    results_info = config.get("results", {})
    msg = None

    number = results_info.get("number")
    if number and num_results != number:

        msg = f"ERROR: {number} results were expected from PTH {key_name} {key}, {num_results} extracted"

    min_ = results_info.get("min")
    if min_ and num_results < min_:

        msg = f"ERROR: at least {min_} results were expected from PTH {key_name} {key}, {num_results} extracted"

    max_ = results_info.get("max")
    if max_ and num_results > max_:

        msg = f"ERROR: no more than {max_} results were expected from PTH {key_name} {key}, {num_results} extracted"

    if msg:

        if ETLEnv.instance().are_tests_running():

            raise Exception(msg)

        print(msg, file=sys.stderr)


class PTHETLProcess(BaseETLProcess):

    def init_testing(self):

        # Disable all data pulls except the one being tested.
        test_info = ETLEnv.instance().get_test_info()
        pos = test_info.tag.find('_')
        test_key_name = test_info.tag[ : pos]
        test_key = test_info.tag[pos + 1 : ].upper()

        for key_name, keys in DATA_PULL_LOGIC.items():

            for key, config in keys.items():

                ignore = not( key_name == test_key_name and key == test_key)
                config["ignore"] = ignore

        # Set record very low for testing.
        RECORD_LIMIT = 1

    def get_field_map(self):

        return field_map

    def extract(self):

        data = []

        for key_name, keys in DATA_PULL_LOGIC.items():

            for key, config in keys.items():

                if config.get("ignore"):

                    continue

                print(f"\nExtracting PTH {key_name} {key}:", file=sys.stderr)

                curr_data = extract_data_set(key_name=key_name, key=key, config=config)

                check_results(results=curr_data, key_name=key_name, key=key, config=config)


                if ETLEnv.instance().are_tests_running():

                    curr_data = curr_data[ : 1 ]

                data += curr_data

        return data

    def transform(self, data):

        for record in data:

            # Split 'format' into digital format and dimensions.
            formats = record.get("format", [])
            if formats:

                new_formats = []
                new_dimensions = []

                for format in formats:

                    if format.lower() in KNOWN_FORMATS:

                        new_formats.append(format)

                    else:

                        new_dimensions.append(format)

                record["format"] = new_formats
                record["dimensions"] = new_dimensions

            # Add in a URL value.
            identifiers = record["identifier"]
            new_ids = []
            new_urls = []

            for identifier in identifiers:

                if identifier.startswith('http'):

                    new_urls.append(identifier)

                else:

                    new_ids.append(identifier)

            record["identifier"] = new_ids
            record["url"] = new_urls

            # Add in a link to the thumbnail image.
            if new_urls:

                url = new_urls[0]
                if not url.endswith('/'):
                    url += '/'

                record["thumbnail"] = url + "thumbnail"

            # Split 'coverage' into values dealing with geography and values dealing with history (dates).
            coverage_values = record.get("coverage", [])
            if coverage_values:

                hist_vals = []
                geo_vals = []

                for value in coverage_values:

                    if has_number(value=value):

                        hist_vals.append(value)

                    else:

                        geo_vals.append(value)

                record["subjects_hist"] = hist_vals
                record["subjects_geo"] = geo_vals

        # Let base class do rest of transform.
        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    etl_process = PTHETLProcess(format="csv")

    data = etl_process.extract()
    etl_process.transform(data=data)
    etl_process.load(data=data)
