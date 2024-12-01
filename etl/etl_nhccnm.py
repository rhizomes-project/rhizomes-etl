#!/usr/bin/env python

import requests
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField
from etl.date_parsers import get_date_first_four


# Start up secrets engine and get our api key.
etl_env = ETLEnv.instance()
etl_env.start()
api_key = etl_env.get_api_key(name="nhccnm")

# REVIEW: Is this the correct URL? (currently returns html)
url = "https://collections.nhccnm.org/objects/json"

headers = {
    "Authorization": "Basic " + api_key,
    "Content-Type": "application/json",
}

# REVIEW: is this right?
url += "?key=" + api_key



field_map = {

    # "ACCESSNO":                             RhizomeField.ID,
    # "TITLE":                                RhizomeField.TITLE,
    # "CREATOR":                              RhizomeField.AUTHOR_ARTIST,
    # "OBJECT URL":                           RhizomeField.URL,
    # "DESCRIP":                              RhizomeField.DESCRIPTION,
    # "STERMS":                               RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    # "DATE":                                 RhizomeField.DATE,
    # "COLLECTION":                           RhizomeField.SOURCE,
    # "OBJECT IMAGE":                         RhizomeField.IMAGES,

    # # "Place Holder" values used to temporarily to fill in "real" values:
    # "CREATOR2":                             RhizomeField.AUTHOR_ARTIST,
    # "MEDIUM":                               None,
    # "OBJNAME":                              None,
    # "IMAGEFILE":                            None,

}


field_map_keys = list(field_map.keys())


# Map column name to column index in the input
# data (in case the column order changes)
column_indices = {

    # # ACCESSNO
    # field_map_keys[0]: 0,

    # # TITLE
    # field_map_keys[1]: 16,

    # # CREATOR
    # field_map_keys[2]: 3,

    # # OBJECT URL
    # field_map_keys[3]: 22,

    # # DESCRIP
    # field_map_keys[4]: 6,

    # # STERMS
    # field_map_keys[5]: 14,

    # # DATE
    # field_map_keys[6]: 5,

    # # COLLECTION
    # field_map_keys[7]: 1,

    # # OBJECT IMAGE
    # field_map_keys[8]: 23,

    # # CREATOR2
    # field_map_keys[9]: 4,

    # # MEDIUM
    # field_map_keys[10]: 9,

    # # OBJNAME
    # field_map_keys[11]: 11,

    # # IMAGEFILE
    # field_map_keys[12]: 7,

}


required_values = [
    # "ACCESSNO",
    # "TITLE",
    # "DESCRIP",
    # "OBJECT IMAGE"
]


# def is_record_valid(record):

#     for required_value in required_values:

#         if not record.get(required_value):

#             message = f"Missing {required_value}"

#             return False, message

#     return True, None

# def clean_value(value):

#     # Is this value on a blank line?
#     if not value:

#         return None

#     # This function can only clean strings.
#     if type(value) is not str:

#         return value

#     for bad_string in bad_strings:

#         value = value.replace(bad_string, " ")

#     return value.strip()

# def parse_date(value):

#     if not value:

#         return None

#     elif type(value) is not str:

#         return value

#     value = value.lower()

#     for invalid_prefix in invalid_prefixes:

#         if value.startswith(invalid_prefix):

#             value = value[ len(invalid_prefix) : ]
#             break

#     return clean_value(value=value)


class NHCCNMETLProcess(BaseETLProcess):

    def init_testing(self):

        pass

    def get_field_map(self):

        return field_map

    def get_collection_name(self):

        return "National Hispanic Cultural Center (NHCCNM)"

    def get_access_rights_stmt(self):

        return """<Still need access rights statement>"""

    def get_date_parsers(self):

        return {
            r'^\d{4}':  get_date_first_four
        }

    def extract(self):

        # Make one call to the eMuseum API to get a list of all records.
        response = requests.get(url=url)

        data = []

        # REVIEW: TODO: Parse all records.


        return data


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "nhccnm" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
