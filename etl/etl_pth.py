#!/usr/bin/env python

import os
import re
import requests
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField, get_oaipmh_record

from bs4 import BeautifulSoup


protocol = "https://"
domain = "texashistory.unt.edu"

list_sets_path = "/oai/?verb=ListSets"
list_sets_url = protocol + domain + list_sets_path

RECORD_LIMIT = None
record_count = 0
num_calls = 0


records_path =       "/oai/?verb=ListRecords"
start_records_path = records_path + "&metadataPrefix=oai_dc"
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


# REVIEW: See https://docs.google.com/document/d/1cD559D8JANAGrs5pwGZqaxa7oHTwid0mxQG0PmAKhLQ/edit for how to pull data.

DATA_PULL_LOGIC = {

    # Note: getting no hits for this.
    None: {
        "dummy": {
            "filters": {
                "subject": {
                    "type": "include",
                    "values": [ "Arts and Crafts" ]
                },
                "keywords": {
                    "type": "include",
                    "values": [
                        "chicano", "chicana", "chicanx",
                        "mexican-american", "mexican american",
                        "hispanic", "arte",
                    ]
                }
            },
            "results" : {
                "min": 230,
                "max": 240
            },
            "ignore": False
        }
    },

    "partner": {

        #  Austin Presbyterian Theological Seminary
        "ATPS": {
            "filters": {
                "subject": {
                    "type": "include",
                    "values": [ "Arts and Crafts", "People - Ethnic Groups - Hispanics" ]
                }
            },
            "results" : {
                "min": 230,
                "max": 240
            },
            "ignore": True
        },

        #  Dallas Museum of Art
        "DMA": {
            "filters": {
                "keywords": {
                    "type": "include",
                    "case-sensitive": False,
                    "exact-match": False,
                    "values": [
                        "texas panorama",
                        "american printmaking, 1913-1947",
                        "catalog list: the aldrich collection",
                        "modern american color prints",
                        "six latin american painters",
                        "visions of the west: american art from dallas collections",
                        "handbook of american painting and sculpture in the collection of the dallas museum of fine arts",
                        "concentrations 11: luis jimenez opens at the dallas museum of art",
                        "1930's expositions",
                        "handbook of collections, exhibitions and activities",
                        "texas painting and sculpture in the collection of the dallas museum of fine arts",
                        "twelfth annual dallas allied arts exhibition",
                        "christmas at the museum",
                        "the hand and the spirit: religious art in america",
                        "robert graham exhibition",
                        "poets of the cities: new york and san francisco",
                        "25th annual dallas county painting, sculpture and drawing",
                        "dallas museum of fine arts bulletin",
                        "dallas museum of art installation: museum of the americas, 1993",
                        "fifteenth exhibition of southwestern prints and drawings, january 27–february 17, 1965",
                        "twenty fourth annual texas painting and sculpture exhibition, 1962-1963",
                        "first annual exhibition by texas sculptors group",
                        "eighth texas general exhibition",
                        "26th annual dallas county exhibition: painting, drawing, sculpture",
                        "john hernandez",
                    ]
                },
            },
            "results" : {
                "number": 3,
            },
            "ignore": True
        },

        #  Hispanic Heritage Center
        "HHCT": {
            "filters": {
                "type": {
                    "type": "include",
                    "values": [ "Photograph" ]
                }
            },
            "results" : {
                "number": 119
            },
            "ignore": True
        },

        # Mexic-Arte Museum
        "MAMU": {
            "results": {
                "min": 290,
                "max": 310
            },
            "ignore": True
        },

         # Museum of South Texas History
        "MSTH": {
            "results": {
                "number": 99,
            },
            "ignore": True
        },

        # Texas A&M University Kingsville
        "AMKV": {
            "filters": {
                "type": {
                    "type": "include",
                    "values": [ "Photograph" ]
                }
            },
            "results" : {
                "number": 112
            },
            "ignore": True
        },

        # Pharr Memorial Library
        # Note: We unfortunately are getting zero results for Pharr Memorial Library via the current data pull.
        "PHRML": {
            "filters": {
                "keywords": {
                    "type": "include",
                    # "values": [ "chicano art", "lowriders club", "xochil art center" ]
                    "values": [ "chicana", "chicano", "lowrider", "xochil" ]
                }
            },
            "results" : {
                "number": 112
            },
            "ignore": True
        },

        # UNT Libraries Government Documents Department
        "UNTGD": {
            "filters": {
                "keywords": {
                    "type": "include",
                    "values": [ "mexican american art", "chicano art" ]
                }
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

        # Texas Borderlands Newspaper Collection
        "BORDE": {
            "filters": {
                "keywords": {
                    "type": "include",
                    "values": [ "obra de arte", "artista", "arte" ]
                }
            },
            "ignore": True
        },

        # Civil Rights in Black and Brown (part of TCU Mary Couts Burnett Library)
        "CRBB": {
            "filters": {
                "type": {
                    "type": "include"
                }
            },
            "ignore": True
        },

        # Diversity in the Desert (part of Marfa Public Library)
        "MDID": {
            "results" : {
                "min": 1740,
                "max": 1760,
            },
            "ignore": True
        },

        # The Mexican American Family and Photo Collection (part of Houston Metropolitan Research Center at Houston Public Library)
        "MAFP": {
            "filters": {
                "type": {
                    "type": "include",
                    "values": [ "Photograph" ]
                }
            },
            "results" : {
                "number": 431
            },
            "ignore": True
        },

         # Texas Trends in Art Education (part of Texas Art Education Association)
        "TTAE" : {
            "results" : {
                "number": 56
            },
            "ignore": True
        },
    },
}

# REVIEW TODO Get canonical list of PTH formats.
KNOWN_FORMATS = ('image', 'text')


def has_number(value):

    return re.search(r'\d', value)

def add_filter_match(filter_, value):
    "Increment the hit count for the given filter."

    if not filter_.get("matches"):

        filter_["matches"] = {}

    filter_["matches"][value] = filter_.get(value, 0) + 1

def do_include_record(record, filters):
    "Returns True if the record should be added, based on the filters."

    for name, filter_ in filters.items():

        if name == "keywords":

            title = ''.join(record.get('title', [])).lower()
            description = ''.join(record.get('description', [])).lower()

            for keyword in filter_["values"]:

                if keyword in title or keyword in description:

                    add_filter_match(filter_=filter_, value=keyword)

                    return filter_["type"] == "include"

        else:

            case_sensitive = filter_.get("case-sensitive", False)
            exact_match = filter_.get("exact-match", False)
            desired_values = filter_["values"]

            values = record.get(name)
            if type(values) is not list:

                values = [values]

            if not case_sensitive:

                for idx, value in enumerate(values):

                    values[idx] = value.lower()

            for desired_value in desired_values:

                desired_value_copy = desired_value if case_sensitive else desired_value.lower()

                matched = False

                if exact_match:

                    matched = desired_value_copy in values

                else:

                    for value in values:

                        if desired_value_copy in value:

                            matched = True

                if matched:

                    add_filter_match(filter_=filter_, value=desired_value)

                    return filter_["type"] == "include"

    return False

def extract_records(records, config={}):

    keywords = config.get("keywords")

    filters = config.get("filters")

    data = []
    for record in records:

        record_data = get_oaipmh_record(record=record)

        do_add = True

        if filters:

            do_add = do_include_record(record=record_data, filters=filters)

        if do_add:

            data.append(record_data)

    return data

def extract_data_set(key_name, key, config={}, resumption_token=None):
    """
    Extract all records for the given data set.

    Note: 'data set' can be a PTH partner, collection, subject, etc.
    """


    global record_count
    global num_calls

    if not resumption_token:

        record_count = 0
        num_calls = 0

        url = start_records_url
        if key_name:

            url += "&set={key_name}:{key}"

    else:

        url = f"{resume_records_url}&resumptionToken={resumption_token}"

        if ETLEnv.instance().are_tests_running():

            return []

    response = requests.get(url)
    if not response.ok:    # pragma: no cover (should never be True during testing)

        raise Exception(f"Error retrieving data from PTH for {key_name} {key}, keywords: {keywords}, status code: {response.status_code}, reason: {response.reason}")

    xml_data = BeautifulSoup(markup=response.content, features="lxml-xml", from_encoding="utf-8")

    # Check for search errors.
    errors = xml_data.find_all("error")
    if errors:

        raise Exception(errors[0].text)

    # Extract records from this partner.
    records = extract_records(records=xml_data.find_all("record"), config=config)

    # Loop through next set of data (if any).
    resumption_tokens = xml_data.find_all("resumptionToken")
    if resumption_tokens:

        record_count += len(records)
        num_calls += 1
        print(f"{record_count} records extracted from {num_calls * 1000} records ...", file=sys.stderr)

        # Make recursive call to extract all records.
        if not RECORD_LIMIT or record_count < RECORD_LIMIT:

            next_records = extract_data_set(key_name=key_name, key=key, config=config, resumption_token=resumption_tokens[0].text)
            records += next_records

    return records

def check_results(results, key_name='', key='', config={}):
    "Output error info if results are not what was expected."

    # Did we get the number of results we expected?
    num_results = len(results)
    results_info = config.get("results", {})
    msg = None

    if not results:

        msg = f"ERROR: NO results were extracted from PTH {key_name} {key}"    # pragma: no cover (should not get here)

    number = results_info.get("number")
    if number and num_results != number:

        msg = f"ERROR: {number} results were expected from PTH {key_name} {key}, {num_results} extracted"    # pragma: no cover (should not get here)

    min_ = results_info.get("min")
    if min_ and num_results < min_:

        msg = f"ERROR: at least {min_} results were expected from PTH {key_name} {key}, {num_results} extracted"    # pragma: no cover (should not get here)

    max_ = results_info.get("max")
    if max_ and num_results > max_:

        msg = f"ERROR: no more than {max_} results were expected from PTH {key_name} {key}, {num_results} extracted"    # pragma: no cover (should not get here)

    if msg:    # pragma: no cover (should not get here)

        if ETLEnv.instance().are_tests_running():

            raise Exception(msg)

        print(msg, file=sys.stderr)

    msgs = []

    # Now check how well the filters performed.
    for name, filter_ in DATA_PULL_LOGIC[key_name][key]["filters"].items():

        matches = filter_.get("matches", {})
        if not matches:

            msgs.append(f"ERROR: the {name} filter got no matches.")

        for value in filter_.get("values"):

            if not matches.get(value, 0):

                msgs.append(f"ERROR: the {name} filter '{value}' got no matches.")

    for msg in msgs:    # pragma: no cover (should not get here)

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
