#!/usr/bin/env python


import requests
import json
import os
import re
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField


protocol = "https://"
domain = "api.dp.la"
etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="dpla")

list_collections_path = "/v2/collections"
list_collections_url = protocol + domain + list_collections_path + "?api_key=" + api_key

list_items_path = "/v2/items"
list_items_url = protocol + domain + list_items_path + "?page=1&page_size=500&api_key=" + api_key

# dpla_terms = [ "id", "@context", "aggregatedCHO", "dataProvider", "ingestDate", "ingestType", "isShownAt", "object" ]
dpla_terms = [ "id", "dataProvider", "isShownAt", "object" ]
original_data_terms = [ "title", "description", "creator", "contributor", "date", "subject", "language", "reference_image_dimensions", "collection_name", "type", "format", "spatial", "coverage" ]

field_map = {
    "id":                   RhizomeField.ID,
    "title":                RhizomeField.TITLE,
    "creator":              RhizomeField.AUTHOR_ARTIST,
    "contributor":          RhizomeField.AUTHOR_ARTIST,
    "description":          RhizomeField.DESCRIPTION,
    "date":                 RhizomeField.DATE,
    "type":                 RhizomeField.RESOURCE_TYPE,
    "format":               RhizomeField.DIGITAL_FORMAT,
    "dimensions":           RhizomeField.DIMENSIONS,
    "isShownAt":            RhizomeField.URL,
    "collection_name":      RhizomeField.SOURCE,
    "language":             RhizomeField.LANGUAGE,
    "subject":              RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "spatial":              RhizomeField.SUBJECTS_GEOGRAPHIC,
    "dataProvider":         RhizomeField.COLLECTION_INFORMATION,
}


def split_dimension(value):
    "Splits value into a list if necessary."

    return re.split(r';|:', value)

def is_dimension(value):
    "Returns True if value appears to describe a dimension."

    return re.search(r'\d.*x|X.*\d', value)


# REVIEW look into cleaning up / getting better dates
# REVIEW look into search terms, re "mexican american"


class DPLAETLProcess(BaseETLProcess):

    def get_field_map(self):

        return field_map

    def extract(self):

        data = []

        search_terms = [ "chicano", "chicana", "mexican-american" ]
        page_max = 100

        # Running tests?
        if os.environ.get("RUNNING_UNITTESTS"):

            search_terms = [ "chicano" ]
            page_max = 1

        # Extract metadata for all our terms.
        for search_term in search_terms:

            # For details on pagination, see https://pro.dp.la/developers/requests#pagination
            count = 1
            start = 0
            page = 1

            while count > start and page <= page_max:

                response = requests.get(f"{list_items_url}&page={page}&q={search_term}")

                count = response.json()["count"]
                start = response.json()["start"]

                print(f"search term: {search_term}, page: {page}, total docs: {count}, start: {start}, curr docs: {len(data)}", file=sys.stderr)

                page += 1

                for doc in response.json()["docs"]:

                    record = {}

                    for term in dpla_terms:

                        if doc.get(term):

                            record[term] = doc[term]

                    # Some records that are part of contributor collections have most of their metadata embedded in the sourceResource string.
                    if not record.get("title"):

                        for val in [ "title", "description" ]:

                            record[val] = doc['sourceResource'].get(val)

                    originalRecordString = doc["originalRecord"]
                    if type(originalRecordString) is str:

                        # REVIEW TODO handle situation where metadata is in a URL somwewhere. altho sometimes that url 
                        # may be unavailable (due to DNS error?)

                        continue

                    else:

                        originalRecordString = originalRecordString.get("stringValue")

                    if not originalRecordString or originalRecordString.startswith('<'):

                        # REVIEW TODO add support for OAIPMH
                        continue


                    original_data = json.loads(originalRecordString)
                    for term in original_data_terms:

                        if term in original_data:

                            record[term] = original_data[term]

                    data.append(record)

        return data

    def transform(self, data):

        for record in data:

            # Split 'format' into digital format and dimensions.
            formats = record.get("format", [])
            if formats:

                # Do any of the individual format values need to be split again?
                new_formats = []
                for format in formats:

                    new_formats += split_dimension(value=format)

                formats = new_formats

                # Split formats into 'actual' format info and whatever appears to be dimension info.
                new_formats = []
                new_dimensions = []

                for format in formats:

                    if is_dimension(value=format):

                        new_dimensions.append(format)

                    else:

                        new_formats.append(format)

                # del record['format']
                record["format"] = new_formats
                record["dimensions"] = new_dimensions

        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    etl_process = DPLAETLProcess(format="csv")

    data = etl_process.extract()
    etl_process.transform(data=data)
    etl_process.load(data=data)
