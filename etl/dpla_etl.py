#!/usr/bin/env python


import requests
import csv
import json

from setup import ETLEnv


protocol = "https://"
domain = "api.dp.la"
etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="dpla")

list_collections_path = "/v2/collections"
list_collections_url = protocol + domain + list_collections_path + "?api_key=" + api_key

list_items_path = "/v2/items"
list_items_url = protocol + domain + list_items_path + "?page_size=25&api_key=" + api_key


field_map = {
    "title":                "Title",
    "creator/Contributor":  "Author/Artist",
    "description":          "Description ",
    "date":                 "Date",
    "type":                 "Resource Type",
    "format ":              "Digital Format",
    # REVIEW: Deal with Dimensions 
    "contributor":          "Source",
    "language":             "Language",
    "subject":              "Subjects (Topic/Keywords)",
    "spatial":              "Subjects (geographic)",
    "coverage ":            None,
}


def extract():

    # dpla_terms = [ "id", "@context", "aggregatedCHO", "dataProvider", "ingestDate", "ingestType", "isShownAt", "object" ]
    dpla_terms = [ "id", "dataProvider", "isShownAt", "object" ]
    original_data_terms = [ "title", "description", "creator", "contributor", "date", "subject", "language", "reference_image_dimensions", "collection_name", "type", "format", "spatial", "coverage" ]

    data = []

    search_terms = [ "chicano" ]
    for search_term in search_terms:

        response = requests.get(list_items_url + "&q=" + search_term)

        record = {}

        for doc in response.json()["docs"]:

            for term in dpla_terms:

                record[term] = doc[term]

            originalRecordString = doc["originalRecord"]["stringValue"]
            if originalRecordString.startswith('<'):

                # REVIEW TODO add support for OAIPMH
                continue


            original_data = json.loads(originalRecordString)
            for term in original_data_terms:

                if term in original_data:

                    record[term] = original_data[term]

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
