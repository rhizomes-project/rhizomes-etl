#!/usr/bin/env python

import os
import re
import requests
import shutil
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField, get_oaipmh_record
from etl.date_parsers import *

from bs4 import BeautifulSoup


protocol = "https://"
domain = "texashistory.unt.edu"

list_sets_path = "/oai/?verb=ListSets"
list_sets_url = protocol + domain + list_sets_path

RECORD_LIMIT = None
record_count = 0


records_path =       "/oai/?verb=ListRecords"
start_records_path = records_path + "&metadataPrefix=oai_dc"
start_records_url = protocol + domain + start_records_path
resume_records_url = protocol + domain + records_path


field_map = {
    "identifier":                             RhizomeField.ID,
    "title":                                  RhizomeField.TITLE,
    "alternate_titles":                       RhizomeField.ALTERNATE_TITLES,
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


# Note: data pull instructions are here: https://docs.google.com/document/d/1cD559D8JANAGrs5pwGZqaxa7oHTwid0mxQG0PmAKhLQ

DATA_PULL_LOGIC = {

    None: {
        "site_wide_subject": {
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
                        "hispanic",

                        # Note: arte is matching too many things, like 'quartet'
                        # "arte",
                    ]
                }
            },
            "results" : {
                "min": 230,
                "max": 240
            },
            "ignore": False
        },

        "site_wide_creator": {
            "filters": {
                "creator": {
                    "type": "include",
                    "values": [
                        "Medellin, Octavio",
                        "Salinas, Porfirio",
                        "Jimenez, Luis",
                        "Munoz, Celia Alvarez",
                        "Casas, Mel",
                    ]
                }
            },
            "results" : {
                "min": 550,
                "max": 575
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
            "ignore": False
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
                "expected_number": 3,
            },
            "ignore": False
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
                "expected_number": 119
            },
            "ignore": False
        },

        # Mexic-Arte Museum
        "MAMU": {
            "results": {
                "min": 290,
                "max": 310
            },
            "ignore": False
        },

         # Museum of South Texas History
        "MSTH": {
            "results": {
                "expected_number": 99,
            },
            "ignore": False
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
                "expected_number": 112
            },
            "ignore": False
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
                "expected_number": 112
            },
            "ignore": False
        },

        "UNT": {
            "filters": {
                "subject": {
                    "type": "include",
                    "values": [ "Arts and Crafts" ]
                },

                # REVIEW: I think mary thomas wants these 2 filters to be OR, not AND.
                # "keywords": {
                #     "type": "include",
                #     "values": [ "charreada" ]
                # },
            },
            "results" : {
                "expected_number": 112
            },
            "ignore": False
        },

        # UNT Libraries Government Documents Department
        "UNTGD": {
            "filters": {
                "keywords": {
                    "type": "include",
                    "values": [ "mexican american art", "chicano art" ]
                }
            },
            "ignore": False
        },
    },

    "collection" : {

        # Art Lies
        "ARTL": {
            "results" : {
                "expected_number": 64
            },
            "ignore": False
        },

        # Texas Borderlands Newspaper Collection
        "BORDE": {
            "filters": {
                "keywords": {
                    "type": "include",
                    "values": [ "obra de arte", "artista", "arte" ]
                }
            },
            "ignore": False
        },

        # Civil Rights in Black and Brown (part of TCU Mary Couts Burnett Library)
        "CRBB": {
            "filters": {
                "type": {
                    "type": "include"
                }
            },
            "ignore": False
        },

        # Diversity in the Desert (part of Marfa Public Library)
        "MDID": {
            "results" : {
                "min": 1740,
                "max": 1760,
            },
            "ignore": False
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
                "expected_number": 431
            },
            "ignore": False
        },

         # Texas Trends in Art Education (part of Texas Art Education Association)
        "TTAE" : {
            "results" : {
                "expected_number": 56
            },
            "ignore": False
        },
    },
}

# REVIEW TODO Get canonical list of PTH formats.
KNOWN_FORMATS = ('image', 'text')


def has_number(value):

    return re.search(r'\d', value)


def de_dupe_substrings_impl(values):

    for value in values:

        for other_value in values:

            if value is not other_value and value in other_value:

                values.remove(value)
                return 1

    return 0

def de_dupe_substrings(values):

    values = set(values)

    continue_ = True
    while continue_:

        if not de_dupe_substrings_impl(values=values):

            continue_ = False

    return list(values)


class ResumptionToken():

    # Singleton instance.
    __instance = None

    def __init__(self):
        # Make sure only 1 instance of ResumptionToken is created

        if ResumptionToken.__instance:    # pragma: no cover (should never get here)

            raise Exception("call ResumptionToken.instance() to get access to ResumptionToken")

        self.token = None

    def instance():
        """
        Should return one-and-only instance of this class.
        """

        if ResumptionToken.__instance is None:
            ResumptionToken.__instance = ResumptionToken()

        return ResumptionToken.__instance


def get_data_impl(num_calls=0, resume=False):
    """
    Download PTH's metadata.
    """

    resumption_token = ResumptionToken.instance()

    # Resume a previous download?
    if resume:

        if not num_calls:

            raise Exception("num_calls is required to resume get_data_impl()")

        data = read_file(file_num=num_calls)
        if not data:

            raise Exception(f"File not found for num_calls: {num_calls}")

        last_xml_data = BeautifulSoup(markup=data, features="lxml-xml", from_encoding="utf-8")

        resumption_tokens = last_xml_data.find_all("resumptionToken")

        if resumption_tokens:

            resumption_token.token = resumption_tokens[0].text

        else:

            return False

    if not resumption_token.token:

        # Rename current metadata directory first to indicate it is old.
        if os.path.exists("etl/data/pth"):

            if os.path.exists("etl/data/pth_old"):

                raise Exception("Error: it looks like etl/data/pth_old still exists - please back it up or remove it.")

            os.rename("etl/data/pth", "etl/data/pth_old")

        os.mkdir("etl/data/pth")

        num_calls = 0

        url = start_records_url

    else:

        url = f"{resume_records_url}&resumptionToken={resumption_token.token}"

        if ETLEnv.instance().are_tests_running():

            return

    response = requests.get(url, timeout=120)
    if not response.ok:    # pragma: no cover (should never be True during testing)

        raise Exception(f"Error retrieving data from PTH, status code: {response.status_code}, reason: {response.reason}")

    # Force the encoding to be utf-8 (apparently it looks like ISO-8859-1 to requests.get() ... )
    response.encoding = "utf-8"

    xml_data = BeautifulSoup(markup=response.content, features="lxml-xml", from_encoding="utf-8")

    # Oheck for search errors.
    errors = xml_data.find_all("error")
    if errors:

        raise Exception(errors[0].text)

    with open(f"etl/data/pth/pth_{num_calls}.xml", "w") as output:

        output.write(response.text)

    resumption_tokens = xml_data.find_all("resumptionToken")
    curr_record_count = (num_calls + 1) * 1000

    # Loop through next set of data (if any).
    if resumption_tokens:

        resumption_token.token = resumption_tokens[0].text

        print(f"{curr_record_count} PTH records retrieved ...", file=sys.stderr)

        return False if RECORD_LIMIT and curr_record_count >= RECORD_LIMIT else True

    else:

        print(f"Finished retrieving PTH records - {curr_record_count} records retrieved ...", file=sys.stderr)

        return False

def get_data(num_calls=0, resume=False):
    """
    Download PTH's metadata.
    """

    continue_ = True
    while continue_:

        if not get_data_impl(num_calls=num_calls, resume=resume):

            continue_ = False

        resume = False
        num_calls += 1


def add_filter_match(key_name, key, filter_name, filter_, match):
    "Increment the hit count for the given filter."

    global DATA_PULL_LOGIC

    if not DATA_PULL_LOGIC[key_name][key].get("results"):

        DATA_PULL_LOGIC[key_name][key]["results"] = {}

    DATA_PULL_LOGIC[key_name][key]["results"]["number"] = DATA_PULL_LOGIC[key_name][key]["results"].get("number", 0) + 1

    if match:

        if not filter_.get("matches"):

            filter_["matches"] = {}

        filter_["matches"][match] = filter_.get(match, 0) + 1

def find_keyword_match(record, filter_):

    title = ''.join(record.get('title', [])).lower()
    description = ''.join(record.get('description', [])).lower()

    for keyword in filter_.get("values", []):

        if keyword in title or keyword in description:

            return keyword

def find_filter_match(record, filter_name, filter_):

    case_sensitive = filter_.get("case-sensitive", False)
    exact_match = filter_.get("exact-match", False)
    desired_values = filter_.get("values", [])

    values = record.get(filter_name, [])
    if type(values) is not list:

        values = [ values ]

    for desired_value in desired_values:

        desired_value_copy = desired_value if case_sensitive else desired_value.lower()

        if exact_match:

            if desired_value_copy in values:

                return desired_value

        else:

            for value in values:

                if case_sensitive:

                    if desired_value_copy in value:

                        return desired_value

                else:

                    if desired_value_copy in value.lower():

                        return desired_value

def do_include_record(record):
    """
    Returns True if the record should be added, based on the filters.
    """

    class IncludeVote():

        def __init__(self, key_name, key, filter_name=None, filter_=None, match=None, filter_includes=True, include_vote_value=True):

            self.key_name = key_name
            self.key = key
            self.filter_name = filter_name
            self.filter_ = filter_
            self.match = match
            self.filter_includes = filter_includes
            self.include_vote_value = include_vote_value


    # Loops through all the filters and check each one.
    for key_name, keys in DATA_PULL_LOGIC.items():

        for key, config in keys.items():

            # Ignore this filter completely?
            if config.get("ignore") and False:

                continue

            # First verify record belongs to this category.
            if key_name and key_name + ":" + key not in record["setSpec"]:

                    continue

            include_votes = []

            # Now apply any filters.
            filters = config.get("filters", {})

            # If the category matches and has no filters, then that is a vote to include the record.
            if not filters:

                include_votes.append(IncludeVote(key_name=key_name, key=key, filter_includes=True, include_vote_value=True))

            else:

                for filter_name, filter_ in filters.items():

                    match = None

                    if filter_name == "keywords":

                        match = find_keyword_match(record=record, filter_=filter_)

                    else:

                        match = find_filter_match(record=record, filter_name=filter_name, filter_=filter_)

                    # - For a filter that tries to exclude records:
                    #     - if matched, then add a False include vote.
                    #     - if not matched, then add a True include vote.
                    #
                    # - For a filter that tries to include records:
                    #     - if matched, then add a True include vote.
                    #     - if not matched, then add a False include vote.

                    filter_includes = filter_["type"] == "include"
                    include_vote_value = filter_includes if match else not filter_includes

                    include_votes.append(IncludeVote(key_name=key_name, key=key, filter_name=filter_name, filter_=filter_, match=match, filter_includes=filter_includes, include_vote_value=include_vote_value))

            # If nothing in this category matched, or any of the include votes were False,
            # then this category does not indicate the record should be included.
            category_matches = len(include_votes) > 0
            for vote in include_votes:

                if not vote.include_vote_value:

                    category_matches = False

            # Now record which filters caused the record to be included or excluded.
            category_set = set()
            for vote in include_votes:

                if (category_matches and vote.filter_includes) or (not category_matches and not vote.filter_includes):

                    if vote.key_name not in category_set:

                        category_set.add(vote.key_name)

                        add_filter_match(key_name=vote.key_name, key=vote.key, filter_name=vote.filter_name, filter_=vote.filter_, match=vote.match)

            if category_matches:

                return True

    # None of the categories indicated the record should be included.
    return False

def check_filter_results(key_name, key, config):
    "Output error info if results for this filter are not what was expected."

    results_info = config.get("results", {})
    msg = None

    expected_number = results_info.get("expected_number")
    num_results = DATA_PULL_LOGIC[key_name][key].get("results", {}).get("number", 0)

    if expected_number and num_results != expected_number:

        msg = f"ERROR: {expected_number} results were expected from PTH {key_name} {key}, {num_results} extracted"    # pragma: no cover (should not get here)

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

    # REVIEW: finish this.

    # msgs = []

    # # Now check how well the filters performed.
    # for filter_name, filter_ in DATA_PULL_LOGIC[key_name][key]["filters"].items():

    #     matches = filter_.get("matches", {})
    #     if not matches:

    #         msgs.append(f"ERROR: the {filter_name} filter got no matches.")

    #     for value in filter_.get("values"):

    #         if not matches.get(value, 0):

    #             msgs.append(f"ERROR: the {filter_name} filter '{value}' got no matches.")

    # for msg in msgs:    # pragma: no cover (should not get here)

    #     if ETLEnv.instance().are_tests_running():

    #         raise Exception(msg)

    #     print(msg, file=sys.stderr)

def check_results():
    "Output error info if results for any filters are not what was expected."

    # For now, don't check results when running tests.
    # REVIEW: fix this.
    if ETLEnv.instance().are_tests_running():

        return

    for key_name, keys in DATA_PULL_LOGIC.items():

        for key, config in keys.items():

            if not config.get("ignore"):

                check_filter_results(key_name=key_name, key=key, config=config)

def read_file(file_num):
    "Returns the contents of the current file."

    file_path = f"etl/data/pth/pth_{file_num}.xml"

    if not os.path.exists(file_path):

        return None

    with open(file_path, "r") as input:

        data = input.read()

    # if testing, use test data?
    etl_env = ETLEnv.instance()
    if etl_env.are_tests_running():

        from etl.tests.test_tools import try_to_read_test_data

        data, format_ = try_to_read_test_data()

    return data

def extract_data_impl(file_num=0):
    """
    Extract all relevant PTH records.
    """

    etl_env = ETLEnv.instance()

    if file_num and etl_env.are_tests_running():

        return None

    # Read current file and parse the xml.
    data = read_file(file_num=file_num)
    if not data:

        return None

    xml_data = BeautifulSoup(markup=data, features="lxml-xml", from_encoding="utf-8")

    # Check for search errors.
    errors = xml_data.find_all("error")
    if errors:

        raise Exception(errors[0].text)

    records = []

    # Get relevant records.
    for record in xml_data.find_all("record"):

        record = get_oaipmh_record(record=record)

        if do_include_record(record=record):

            records.append(record)

            if etl_env.are_tests_running():

                break

    return records

    # # Keep going until we have gone through all of PTH's metadata.
    # return extract_data(records=records, file_num=file_num+1)

def extract_data():

    file_num = 0
    records = []
    tmp = 1

    while tmp is not None:

        tmp = extract_data_impl(file_num=file_num)
        if tmp:

            records += tmp

        print(f"Extracted data from file {file_num}, {len(records)} PTH records extracted ...", file=sys.stderr)

        file_num += 1

    print(f"Finished extracting data, {len(records)} PTH records extracted ...", file=sys.stderr)

    return records


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

    def get_collection_name(self):

        return "Portal to Texas History"

    def get_date_parsers(self):

        return {

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

            r'^\{\d{4}\,\d{4}\}':                                   get_pth_year_range,         # {1930,1949}

            r'^\{\d{4}\,\d{4}\,\d{4}\}':                            get_pth_year_range_offset6, # {1947,1956,1966}

            # Final catch-all: try to extract 1st year.
            r'^[\{\[]\d{4}':                                            get_date_second_four,       # {1936,1958~,1961,1962}

            # Other PTH date vals we do not handle (yet?):
            #
            # 'unknown/1896'
            # '19uu'

        }

    def extract(self):

        etl_env = ETLEnv.instance()

        # Rebuild metadata cached?
        if not etl_env.use_cache():

            offset = etl_env.get_call_offset()
            resume = True
            if not offset:

                offset = 0
                resume = False

            get_data(num_calls=offset, resume=resume)

        records = extract_data()

        check_results()

        return records

    def transform(self, data):

        for record in data:

            # Strip brackets from titles.
            titles = record.get("title")
            if type(titles) is not list:

                titles = [ titles ]

            new_titles = []
            for title in titles:

                if title.startswith("[") and title.endswith("]"):

                    title = title[ 1 : -1]

                if title == "Unknown":

                    title = "Unknown Title"

                new_titles.append(title)

            # If any of the titles are substrings of the other titles, remove the substring titles.
            new_titles = de_dupe_substrings(values=new_titles)

            record["title"] = new_titles[0]

            # Add in alternate titles?
            if len(new_titles) > 1:

                record["alternate_titles"] = new_titles[ 1 : ]

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

    from etl.run import run_cmd_line

    args_ = [ "pth" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
