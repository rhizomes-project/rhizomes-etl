#!/usr/bin/env python


import requests
import json

from etl_process import BaseETLProcess
from setup import ETLEnv
from tools import RhizomeField



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
# REVIEW: Deal with format Dimensions
    "id":                   RhizomeField.ID,
    "title":                RhizomeField.TITLE,
    "creator":              RhizomeField.AUTHOR_ARTIST,
    "contributor":          RhizomeField.AUTHOR_ARTIST,
    "description":          RhizomeField.DESCRIPTION,
    "date":                 RhizomeField.DATE,
    "type":                 RhizomeField.RESOURCE_TYPE,
    "format":               RhizomeField.DIGITAL_FORMAT,
    "isShownAt":            RhizomeField.URL,
    "collection_name":      RhizomeField.SOURCE,
    "language":             RhizomeField.LANGUAGE,
    "subject":              RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "spatial":              RhizomeField.SUBJECTS_GEOGRAPHIC,
    # "coverage ":            NONE,
    "dataProvider":         RhizomeField.COLLECTION_INFORMATION,
}


class DPLAETLProcess(BaseETLProcess):

    def get_field_map(self):

        return field_map

    def extract(self):

        # dpla_terms = [ "id", "@context", "aggregatedCHO", "dataProvider", "ingestDate", "ingestType", "isShownAt", "object" ]
        dpla_terms = [ "id", "dataProvider", "isShownAt", "object" ]
        original_data_terms = [ "title", "description", "creator", "contributor", "date", "subject", "language", "reference_image_dimensions", "collection_name", "type", "format", "spatial", "coverage" ]

        data = []

        search_terms = [ "chicano" ]
        for search_term in search_terms:

            response = requests.get(list_items_url + "&q=" + search_term)

            for doc in response.json()["docs"]:

                record = {}

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


if __name__ == "__main__":

    etl_process = DPLAETLProcess(format="csv")

    data = etl_process.extract()
    etl_process.transform(data=data)
    etl_process.load(data=data)
