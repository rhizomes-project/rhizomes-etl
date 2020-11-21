#!/usr/bin/env python


import requests
import json

import solr

from etl_process import BaseETLProcess
from setup import ETLEnv
from tools import RhizomeField


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

field_map = {
    "title":                 RhizomeField.TITLE,
    "creator":               RhizomeField.AUTHOR_ARTIST,
    "url_item":              RhizomeField.URL,
    "description":           RhizomeField.DESCRIPTION,
    "date":                  RhizomeField.DATE,
    "type":                  RhizomeField.DIGITAL_FORMAT,
    "id":                    RhizomeField.ID,
    "sort_collection_data":  RhizomeField.SOURCE,
    "subject":               RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    # REVIEW Add type here
}


class CalisphereETLProcess(BaseETLProcess):

    def get_field_map(self):

        return field_map

    def extract(self):

        # Get a connection to Calisphere's solr server.
        ss = solr.SolrConnection("https://solr.calisphere.org/solr", post_headers={ "X-Authentication-Token": api_key } )

        data = []

        # Do a search
        response = ss.query('title:chicano', rows=50)
        for hit in response.results:
            
            record = {}
            for key in SOLR_KEYS:

                if hit.get(key):

                    record[key] = hit[key]

            data.append(record)

        return data


if __name__ == "__main__":

    etl_process = CalisphereETLProcess(format="csv")

    data = etl_process.extract()
    etl_process.transform(data=data)
    etl_process.load(data=data)
