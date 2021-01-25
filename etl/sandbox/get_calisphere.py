#!/usr/bin/env python


import requests
import json

import solr

from etl.setup import ETLEnv


etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="calisphere")



# "https://solr.calisphere.org/solr/query/?q=fred"


SOLR_KEYS = [
    "title",
    "subject",
    "creator",
    "id",
    "date",
    "url_item",
    "description",
    "type",
    "repository_url",
    "reference_image_dimensions",
    'collection_data',
    'rights_holder',
    'campus_name',
    'facet_decade'
    'campus_data',
    'collection_url',
    'repository_name',
    'repository_data',
    'rights'
    'collection_name',
    'campus_url',
    'sort_collection_data',
    'timestamp',
    'score'
]


if __name__ == "__main__":

    # Get a connection to Calisphere's solr server.
    ss = solr.SolrConnection("https://solr.calisphere.org/solr", post_headers={ "X-Authentication-Token": api_key } )

    # Do a search
    response = ss.query('title:chicano', rows=25)
    for hit in response.results:
        
        print("")

        for key in SOLR_KEYS:

            if hit.get(key):

                value = hit[key]

                if type(value) is list:

                    print(f"{key}:")
                    for val in value:

                        print(f"    {val}")

                else:

                    print(f"{key}: {value}")
