#!/usr/bin/env python

import requests
import sys
import time

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField
from etl.date_parsers import get_date_first_four


# Start up secrets engine and get our api key.
etl_env = ETLEnv.instance()
etl_env.start()
api_key = etl_env.get_api_key(name="nhccnm")


native_field_map = {

    "sourceId":                             RhizomeField.ID,
    "title":                                RhizomeField.TITLE,
    "description":                          RhizomeField.DESCRIPTION,
    "medium":                               RhizomeField.RESOURCE_TYPE,
    "dimensions":                           RhizomeField.DIMENSIONS,
    "displayDate":                          RhizomeField.DATE,
    "creditline":                           RhizomeField.SOURCE,
    "primaryMedia":                         RhizomeField.IMAGES,
    "description":                          RhizomeField.DESCRIPTION,

}

derived_field_map = {

    "web_url":                              RhizomeField.URL,
    "creation_year":                        RhizomeField.DATE,

}

field_map = derived_field_map | native_field_map

field_map_keys = list(field_map.keys())


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


def extract_value(elt):

    return elt["value"]

def extract_values(object_):

    record = {}

    for field in native_field_map:

        elt = object_.get(field)

        if elt:

            record[field] = extract_value(elt=elt)


    # Fill in missing values.
    source_id = record["sourceId"]
    record["web_url"] = f"https://collections.nhccnm.org/objects/{source_id}"


    # Try to get image url working
    # REVIEW: Not sure if this is the correct image url - currently
    # this gives us the error "The requested feature is disabled."
    image_url = record["primaryMedia"]
    record["primaryMedia"] = image_url.replace("http://localhost/internal/", "https://collections.nhccnm.org/")


    # REVIEW: Add in accession no somewhere

    if record.get("displayDate"):

        date_val = int(record["displayDate"])

        record["displayDate"] = date_val
        record["creation_year"] = date_val

    return record


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

        page_num = 1
        records = []
        total_records = 1


        # REVIEW: remove this
        everything_buffer = ""


        while len(records) < total_records:

            url = f"https://collections.nhccnm.org/objects/json?key={api_key}&page={page_num}"

            response = requests.get(url=url, timeout=60)

            collection_json = response.json()
            total_records = collection_json["count"]

            # Parse all records.
            for object_ in collection_json["objects"]:

                source_id = object_["sourceId"]["value"]

                object_url = f"https://collections.nhccnm.org/objects/{source_id}/json?key={api_key}"
                response = requests.get(url=object_url, timeout=60)

                object_json = response.json()

                if len(object_json["object"]) > 1:

                    raise Exception("Found too many objects!")

                record = extract_values(object_=object_json["object"][0])
                records += [ record ]



                # REVIEW: remove this ...
                import json
                everything_buffer += json.dumps(object_json) + "\n"



            # Continue paging through records.
            page_num += 1



        # REVIEW: remove this ...
        print("\n\n\n" + everything_buffer)



        return records


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "nhccnm" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
