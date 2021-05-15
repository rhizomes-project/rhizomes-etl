#!/usr/bin/env python

import csv
import json
import os
import requests
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField
from etl.rhizomes_keystone_artist_list import RHIZONES_KEYSTONE_ARTIST_LIST


protocol = "https://"
domain = "api.si.edu"
etl_env = ETLEnv.instance()
etl_env.start()
api_key = etl_env.get_api_key(name="smithsonian")

query_path = "/openaccess/api/v1.0/search"
query_url = protocol + domain + query_path + "?api_key=" + api_key

keys_to_ignore = ("title_sort", "type", "label")
keys_to_not_label = ("content", )


# Note: data pull instructions are here: https://docs.google.com/document/d/1Pub60G6w9QxhWamNssoV6tY303oMvfxME97waaj77hs
# Important SI collections (Use https://api.si.edu/openaccess/api/v1.0/terms/unit_code?q=online_media_type:Images&api_key=api_key to get updated list.)


# REVIEW double-check that "mexican-american" and "mexican american" get the same results.
search_terms = [
    "chicana", "chicano", "chicanx",
    "mexican-american",
]

DATA_PULL_INSTRUCTIONS_VIA_KEYWORDS = {

    # archives of american art
    "AAA": {
        "filters": {
            "keywords": {
                "type": "include",
                "values": search_terms
                # keyword search combined with filters below yields 71 hits
            },
            "content/indexedStructured/name": {
                "type": "exclude",
                "values": [
                    "Dows, Olin",
                    "Phillips, Harlan B",
                    "Penney, James",
                    "Trovato, Joseph S",
                    "Biberman, Edward",
                    "McGlynn, Betty Hoag",
                    "Laigo, Val M",
                    "Nakane, Kazuko",
                    "Lau, Alan Chong",
                    "Lechay, James",
                    "Brown, Robert F",
                    "Berkowitz, Leon",
                    "Fox, Ida",
                    "Goodman, Wally",
                    "Picher, William Stanton",
                    "Spencer, Niles",
                    "True, Allen Tupper",
                    "Cook, Lia",
                    "Baizerman, Suzanne",
                    "Lamarque, Abril",
                    "Bartlett, John Russell",
                ]
            },
            "title": {
                "type": "exclude",
                "values": [
                    "Ankrum Gallery records, circa 1900-circa 1990s bulk 1960-1990",
                    "Carnegie Institute, Museum of Art records, 1883-1962, bulk 1885-1940",
                ]
            },
        },
        "ignore": False
    },

    # smithsonian american art museum
    "SAAM": {
        "filters": {
            "keywords": {
                "type": "include",
                "values": search_terms
                # keyword search combined with filters below yields 109 hits
            },
            "content/indexedStructured/name": {
                "type": "exclude",
                "values": [
                    "Walcott, Mary Vaux",
                    "Stanley, John Mix",
                    "Black Knife",
                    "Hartley, Marsden",
                    "Gaines, Edmund Pendleton",
                    "Powers, Hiram",
                    "Catlin, George",
                    "Hyde, Helen",
                    "Posada",
                    "Picart, Bernard",
                ]
            }
        },
        "ignore": False
    },

    # national museum of american history
    "NMAH": {
        "filters": {
            "keywords": {
                "type": "include",
                "values": search_terms
                # keyword search combined with filters below yields 2799 hits
            },
            "content/indexedStructured/name": {
                "type": "exclude",
                "values": [
                    "Whitney, Eli",
                    "Taylor, Zachary",
                    "Graham, Samuel",
                    "Wilson, M. W.",
                    "Grant, Ulysses S. (Ulysses Simpson)",
                    "Lee, Robert E.",
                    "Giles, John Lawrence",
                    "Robertson, William C.",
                    "J. Kelly and Sons",
                    "Inger, Christian",
                    "Grant, Ulysses S. (Ulysses Simpson)",
                    "Lee, Robert E.",
                    "Hohenstein, Anton",
                    "Spohni, G.",
                    "Kelly, Thomas",
                    "Kurz & Allison-Art Studio",
                    "McClellan, George B.",
                    "Strong, Thomas W.",
                    "Sarony & Major",
                    "Skinner, Charles",
                    "American Bank Note Company",
                    "Sartain, John",
                    "Taylor, Zachary",
                    "Wilson, Woodrow",
                    "Twiggs, Levi",
                ]
            },
            "content/indexedStructured/object_type": {
                "type": "exclude",
                "values": [
                    "Coins (Money)",
                    "Medals",
                    "Army",
                    "Navy",
                    "Military",
                    "Sound Recording",
                    "Sound Recordings",
                    "Note",
                    "Fire Hats",
                    "Record",
                    "Tokens",
                ]
            }
        },
        "ignore": False
    },

    # national portrait gallery
    "NPG": {
        "filters": {
            "keywords": {
                "type": "include",
                "values": search_terms + [ "Chavez", "Kahlo", "Huerta", "Ruben Salazar", "Selena", "Mendoza, Lydia", "Antonio Orendain" ]
                # keyword search combined with filters below yields 65 hits
            },
            "content/indexedStructured/name": {
                "type": "exclude",
                "values": [
                    "United States Army",
                ]
            }
        },
        "ignore": False
    },
}


DATA_PULL_INSTRUCTIONS_VIA_ARTIST_LIST = {

    # archives of american art
    "AAA": {
        "filters": {
            "keywords": {
                "type": "include",
                "values": search_terms
                # keyword search combined with filters below yields 71 hits
            },
            "content/indexedStructured/name": {
                "type": "exclude",
                "values": [
                    "Dows, Olin",
                    "Phillips, Harlan B",
                    "Penney, James",
                    "Trovato, Joseph S",
                    "Biberman, Edward",
                    "McGlynn, Betty Hoag",
                    "Laigo, Val M",
                    "Nakane, Kazuko",
                    "Lau, Alan Chong",
                    "Lechay, James",
                    "Brown, Robert F",
                    "Berkowitz, Leon",
                    "Fox, Ida",
                    "Goodman, Wally",
                    "Picher, William Stanton",
                    "Spencer, Niles",
                    "True, Allen Tupper",
                    "Cook, Lia",
                    "Baizerman, Suzanne",
                    "Lamarque, Abril",
                    "Bartlett, John Russell",
                ]
            },
            "title": {
                "type": "exclude",
                "values": [
                    "Ankrum Gallery records, circa 1900-circa 1990s bulk 1960-1990",
                    "Carnegie Institute, Museum of Art records, 1883-1962, bulk 1885-1940",
                ]
            },
        },
        "ignore": False
    },

    # smithsonian american art museum
    "SAAM": {
        "filters": {
            "content/indexedStructured/name": {
                "type": "include",
                "values": RHIZONES_KEYSTONE_ARTIST_LIST
            }
        },
        "ignore": False
    },

    # national museum of american history
    "NMAH": {
        "filters": {
            "keywords": {
                "type": "include",
                "values": search_terms
                # keyword search combined with filters below yields 2799 hits
            },
            "content/indexedStructured/name": {
                "type": "exclude",
                "values": [
                    "Whitney, Eli",
                    "Taylor, Zachary",
                    "Graham, Samuel",
                    "Wilson, M. W.",
                    "Grant, Ulysses S. (Ulysses Simpson)",
                    "Lee, Robert E.",
                    "Giles, John Lawrence",
                    "Robertson, William C.",
                    "J. Kelly and Sons",
                    "Inger, Christian",
                    "Grant, Ulysses S. (Ulysses Simpson)",
                    "Lee, Robert E.",
                    "Hohenstein, Anton",
                    "Spohni, G.",
                    "Kelly, Thomas",
                    "Kurz & Allison-Art Studio",
                    "McClellan, George B.",
                    "Strong, Thomas W.",
                    "Sarony & Major",
                    "Skinner, Charles",
                    "American Bank Note Company",
                    "Sartain, John",
                    "Taylor, Zachary",
                    "Wilson, Woodrow",
                    "Twiggs, Levi",
                ]
            },
            "content/indexedStructured/object_type": {
                "type": "exclude",
                "values": [
                    "Coins (Money)",
                    "Medals",
                    "Army",
                    "Navy",
                    "Military",
                    "Sound Recording",
                    "Sound Recordings",
                    "Note",
                    "Fire Hats",
                    "Record",
                    "Tokens",
                ]
            }
        },
        "ignore": False
    },

    # national portrait gallery
    "NPG": {
        "filters": {
            "content/indexedStructured/name": {
                "type": "include",
                "values": RHIZONES_KEYSTONE_ARTIST_LIST
            },
            "content/indexedStructured/name": {
                "type": "include",

                # REVIEW: These don't seem to be working.
                "values": search_terms + [ "Chavez", "Kahlo", "Huerta", "Ruben Salazar", "Selena", "Mendoza, Lydia", "Antonio Orendain" ]
            }
        },
        "ignore": False
    },
}


DATA_PULL_INSTRUCTIONS = DATA_PULL_INSTRUCTIONS_VIA_ARTIST_LIST


field_map = {
    "id":                                      RhizomeField.ID,
    "content/indexedStructured/name":          RhizomeField.AUTHOR_ARTIST,
    "title":                                   RhizomeField.TITLE,
    "content/indexedStructured/object_type":   RhizomeField.RESOURCE_TYPE,
    "content/descriptiveNonRepeating/guid":    RhizomeField.URL,
    "date":                                    RhizomeField.DATE,
    "content/indexedStructured/date":          RhizomeField.SUBJECTS_HISTORICAL_ERA,
    "content/indexedStructured/topic":         RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "content/indexedStructured/geoLocation":   RhizomeField.SUBJECTS_GEOGRAPHIC,
    "content/freetext/notes":                  RhizomeField.NOTES,
    "image_urls":                              RhizomeField.IMAGES,
}

# REVIEW: see why not everything has a URL, e.g., archival materials, "Oral history interview with Angel Rodriguez-Diaz, 2004 April 23-May 7"
# REVIEW: look into why certain records have no artst, e.g., edanmdm-saam_1971.439.3


# 
# Helper functions for extracting data.
# 


def get_value(record, keys):
    # Gets the value for the given set of keys.

    if type(keys) is str:

        keys = keys.split('/')

    value = record.get(keys[0])
    if not value:

        return None

    keys = keys[ 1 : ]
    if keys:

        return get_value(record=value, keys=keys)

    else:

        return value

def traverse(record, key=None, indents=0):
    "Retrieve desired data from each record."

    data = {}

    for key in field_map.keys():

        value = get_value(record=record, keys=key)
        if value:

            data[key] = value

    # Is there media available online for this record?
    online_media_type = get_value(record=record, keys="content/indexedStructured/online_media_type")
    if online_media_type:

        data["online_media_type"] = online_media_type

    return data

def coalesce(record):

    # Extract relevant author info.
    names = record.get("content/indexedStructured/name", [])
    if names:

        new_names = []
        for name in names:

            if type(name) is dict:

                if name["type"] == "personal_main":

                    new_names.append(name["content"])

            else:

                new_names.append(name)

        record["content/indexedStructured/name"] = new_names

    # Clean up notes.
    notes = record.get('content/freetext/notes')
    if notes:

        new_notes = []
        for note in notes:

            if note.get('content'):

                new_notes.append(f"- {note['label']}: {note['content']}")

        record['content/freetext/notes'] = new_notes

    # Clean up geolocations.
    locations = record.get('content/indexedStructured/geoLocation')
    if locations:

        new_locations = set()
        for location in locations:

            for value in location.values():

                if type(value) is dict:

                    if value.get('content'):

                        new_locations.add(value['content'])

                else:

                    new_locations.add(value)

        record['content/indexedStructured/geoLocation'] = new_locations

    return record

def get_image_urls(id_):
    "Returns urls to images, if any, for the given object."

    url = f"https://api.si.edu/openaccess/api/v1.0/content/{id_}?api_key={api_key}"

    response = requests.get(url=url, timeout=60)
    if not response.ok:

        return None

    data = response.json()
    urls = []

    media_objects = data["response"]["content"].get("descriptiveNonRepeating", {}).get("online_media", {}).get("media", [])
    for media_object in media_objects:

        for resource in media_object.get("resources", []):

            url = resource.get("url", None)
            if url and url.endswith(".jpg"):

                urls.append(url)

    return urls

def finish_record(record):
    "Get any remaining data."

    # Is there media available online for this record?
    if record.get("online_media_type"):

        # Retrieve urls to any images for the record.
        urls = get_image_urls(id_=record["id"])
        if urls:

            record["image_urls"] = urls[0]

def do_include_record(record, config):
    """
    Returns True if the record should be added, based on the filters.
    """

    votes = []
    filters = config.get("filters", {})

    if not filters:

        votes.append(True)

    for filter_name, filter_ in filters.items():

        if filter_name == "keywords":

            votes.append(True)

            continue

        desired_values = filter_.get("values", [])
        values = record.get(filter_name, [])
        if type(values) is not list:

            values = [values]

        matched = False

        # Count individual matches.
        for value in values:

            for desired_value in desired_values:

                if value.lower() in desired_value.lower():

                    matched = True
                    break

            if matched:

                break

        # Does this record match the filter?
        if matched:

            votes.append(filter_["type"] == "include")

    if not votes or False in votes:

        return False

    else:

        return True

def extract_response(rows, config):

    data = []

    print(f"Querying {len(rows)} records ...", file=sys.stderr)

    for row in rows:

        # Retrieve the raw json.
        record = traverse(record=row)

        # Clean it up so it's easier to work with.
        record = coalesce(record=record)

        if do_include_record(record=record, config=config):

            finish_record(record=record)

            data.append(record)

            if etl_env.are_tests_running():

                break

    return data

def extract_query(provider, config, keyword=None):

    data = []

    start = 0
    row_limit = 1000
    row_count = 1

    # Loop through all records for each provider via start / rows logic - see http://edan.si.edu/openaccess/apidocs/
    while start <= row_count:

        if keyword:

            url = query_url + f"&q={keyword}+AND+unit_code:{provider}&start={start}&rows={row_limit}"

        else:

            url = query_url + f"&q=unit_code:{provider}&start={start}&rows={row_limit}"

        response = requests.get(url=url, timeout=60)

        if not response.ok:

            raise Exception(f"Error retrieving data from SI: {response.reason} - status code: {response.status_code}")

        json_data = response.json()
        row_count = json_data["response"]["rowCount"]
        rows = json_data["response"]["rows"]

        if not rows:

            if keyword:

                print(f"ERROR: keyword '{keyword}' returned zero rows for provider {provider}", file=sys.stderr)

            return []

        data += extract_response(rows=rows, config=config)

        start += len(rows) if rows else row_limit
        print(f"Filtered {start} records from provider {provider}, of {row_count}", file=sys.stderr)

        if ETLEnv.instance().are_tests_running():

            break

    return data


class SIETLProcess(BaseETLProcess):

    def init_testing(self):

        global DATA_PULL_INSTRUCTIONS

        first_provider = list(DATA_PULL_INSTRUCTIONS.keys())[0]

        DATA_PULL_INSTRUCTIONS = { first_provider : DATA_PULL_INSTRUCTIONS[first_provider] }

        DATA_PULL_INSTRUCTIONS[first_provider]["filters"]["keywords"]["values"] = [ DATA_PULL_INSTRUCTIONS[first_provider]["filters"]["keywords"]["values"][0] ]

    def get_field_map(self):

        return field_map

    def extract(self):

        data = []

        category = ETLEnv.instance().get_category()

        # Constrain results by keyword and institution.
        for provider, config in DATA_PULL_INSTRUCTIONS.items():

            if config.get("ignore", False):

                continue

            if category and category != provider:

                continue

            print(f"Querying provider {provider} ...", file=sys.stderr)

            if "keywords" in config.get("filters", {}):

                for keyword in config["filters"]["keywords"]["values"]:

                    curr_data = extract_query(provider=provider, config=config, keyword=keyword)

                    data += curr_data

                    print(f"Extracted {len(curr_data)} records from provider {provider}", file=sys.stderr)

            else:

                curr_data = extract_query(provider=provider, config=config)

                data += curr_data

                print(f"Extracted {len(curr_data)} records from provider {provider}", file=sys.stderr)

        return data

    def transform(self, data):

        # REVIEW: What needs to happen here?

        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "si" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
