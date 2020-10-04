#!/usr/bin/env python


import requests
import csv
import json

from setup import ETLEnv


protocol = "https://"
domain = "api.dp.la"
etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.dpla_api_key

list_collections_path = "/v2/collections"
list_collections_url = protocol + domain + list_collections_path + "?api_key=" + api_key

list_items_path = "/v2/items"
list_items_url = protocol + domain + list_items_path + "?page_size=25&api_key=" + api_key


if __name__ == "__main__":

    # dpla_terms = [ "id", "@context", "aggregatedCHO", "dataProvider", "ingestDate", "ingestType", "isShownAt", "object" ]
    dpla_terms = [ "id", "dataProvider", "isShownAt", "object" ]
    original_data_terms = [ "title", "description", "creator", "contributor", "date", "subject", "language", "reference_image_dimensions", "collection_name", "type", "format", "spatial", "coverage" ]

    search_terms = [ "chicano" ]
    for search_term in search_terms:

        response = requests.get(list_items_url + "&q=" + search_term)
        data = response.json()

        dpla_data = {}

        for doc in data["docs"]:

            for term in dpla_terms:

                dpla_data[term] = doc[term]

                print(f"{term}: {doc[term]}")


            originalRecordString = doc["originalRecord"]["stringValue"]
            if originalRecordString.startswith('<'):

                # REVIEW TODO add support for OAIPMH
                continue


            original_data = json.loads(originalRecordString)
            for term in original_data_terms:

                if term in original_data:

                    value = original_data[term]

                    if type(value) is list:

                        for tmp in value:

                            print(f"{term}: {tmp}")


                    else:

                        print(f"{term}: {value}")

            print("")
