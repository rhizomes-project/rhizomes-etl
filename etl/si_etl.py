#!/usr/bin/env python


import requests
import csv
import json

from setup import ETLEnv


import pdb


protocol = "https://"
domain = "api.si.edu"
etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="smithsonian")

query_path = "/openaccess/api/v1.0/search"
query_url = protocol + domain + query_path + "?api_key=" + api_key

keys_to_ignore = ("title_sort", "type", "label")
keys_to_not_label = ("content", )

field_map = {
    "title":                                   "Title",
    "content/indexedStructured/object_type":   "Resource Type",
    "id":                                      "Resource Identifier",
    "content/indexedStructured/date":          "Subjects (Historic Era)",
    "content/indexedStructured/topic":         "Subjects (Topic/Keywords)",
    "content/indexedStructured/geoLocation":   "Subjects (Geographic)",
    "content/freetext/notes":                  "Notes",
}


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
    # Retrieves desired data from each record.

    data = {}

    for key in field_map.keys():

        value = get_value(record=record, keys=key)
        if value:

            data[key] = value

    return data


def extract():

    data = []

    search_terms = [ "chicano" ]
    for search_term in search_terms:

        response = requests.get(query_url + "&q=" + search_term)

        for row in response.json()["response"]["rows"]:

            record = traverse(record=row)
            data.append(record)

    return data


# REVIEW TODO come up with generic default versions of transform() and load()

def transform(data):

    for record in data:

        for name, description in field_map.items():

            if not description:

                continue

            value = record.get(name)
            if value:

                if record.get(description):

                    record[description] += value

                else:

                    record[description] = value

                del record[name]

def load(data):

    for record in data:

        print("")

        prev_values = set()

        for name in field_map.values():

            value = record.get(name)
            if value and name not in prev_values:

                print(f"{name}: {value}")

                prev_values.add(name)


if __name__ == "__main__":

    data = extract()
    transform(data=data)
    load(data=data)
