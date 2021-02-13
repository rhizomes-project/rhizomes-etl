#!/usr/bin/env python


import requests
import json
import os
import re
import sys

from bs4 import BeautifulSoup

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField, get_oaipmh_record


import pdb


running_tests = os.environ.get("RUNNING_UNITTESTS")

protocol = "https://"
domain = "api.dp.la"
etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="dpla")

list_collections_path = "/v2/collections"
list_collections_url = protocol + domain + list_collections_path + "?api_key=" + api_key

list_items_path = "/v2/items"
list_items_url = protocol + domain + list_items_path + "?page=1&page_size=500&api_key=" + api_key

dpla_terms = [
    "id", "dataProvider", "isShownAt", "object",
    { "sourceResource": [ "title", "description", "creator", "contributor", "date", "type", { "subject" : [ "name" ] }, "dataProvider", { "date" : [ "displayDate" ] } ] },
    ]
json_original_data_terms = [ "title", "description", "creator", "contributor", "date", "subject", "language", "reference_image_dimensions", "collection_name", "type", "format", "coverage" ]


# REVIEW: finish this
# xml_original_data_terms = [ "titleInfo", "abstract", "", "", "", "", "", "", ]datestamp, note

xml_original_data_terms = []

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
    "object":               RhizomeField.IMAGES,
}

# REVIEW uncommnent search_terms & providers.

# Note: "mexican american", "mexican+american" and "mexican-american" all get the same results via API.
search_terms = [ "chicano", ]
# "chicana", "chicanx", "%22mexican+american%22", ]

providers = {
    "UC Santa Barbara, Library, Department of Special Research Collections": search_terms,
    # "UC San Diego, Library, Digital Library Development Program": search_terms,
    # "Center for the Study of Political Graphics": None
    # "UC San Diego, Library, Special Collections and Archives": search_terms,
    # "University of Southern California Digital Library": search_terms,
    # "Los Angeles Public Library": search_terms,
    # "California State University, Fullerton, University Archives and Special Collections": search_terms,
}

page_max = 1

# Running tests?
if running_tests:

    search_terms = [ "chicano" ]
    page_max = 1


def split_dimension(value):
    "Splits value into a list if necessary."

    return re.split(r';|:', value)

def is_dimension(value):
    "Returns True if value appears to describe a dimension."

    return re.search(r'\d.*x|X.*\d', value)


# REVIEW look into cleaning up / getting better dates



def parse_json_terms(tree, terms):

    data = {}

    for term in terms:

        if type(term) is dict:

            for parent, children in term.items():

                child_data = parse_json_terms(tree=tree.get(parent, {}), terms=children)
                data |= child_data

        else:

            if type(tree) is list:

                for obj in tree:

                    data |= parse_json_terms(tree=obj, terms=[term])

            else:

                if tree.get(term):

                    data[term] = tree[term]

    return data


def parse_oaipmh_record(record):

    xml_data = BeautifulSoup(markup=record, features="lxml-xml")

    for record in xml_data.find_all("record"):

        return get_oaipmh_record(record=record)


def parse_original_string(doc, record):

    originalRecordString = doc["originalRecord"]
    if type(originalRecordString) is str:

        # REVIEW TODO handle situation where metadata is in a URL somwewhere. altho sometimes that url
        # may be unavailable (due to DNS error?)

        return

    else:

        originalRecordString = originalRecordString.get("stringValue")

    if originalRecordString:

        if originalRecordString.startswith('<'):

            # Original record appears to be in OAIPMH...
            original_data = parse_oaipmh_record(record=originalRecordString)
            original_data_terms = xml_original_data_terms

        else:

            # Original record is in json ...
            original_data = json.loads(originalRecordString)
            original_data_terms = json_original_data_terms

        # Add the original terms that we pulled out into the record data.
        for term in original_data_terms:

            if term in original_data and not record.get(term):

                record[term] = original_data[term]


class DPLAETLProcess(BaseETLProcess):

    def get_field_map(self):

        return field_map

    def extract(self):


        # pdb.set_trace()


        data = []

        # Extract metadata for the providers we are interested in.
        for provider, search_terms in providers.items():

            for search_term in search_terms:

                # For details on pagination, see https://pro.dp.la/developers/requests#pagination
                count = 1
                start = 0
                page = 1

                provider_encoded = provider.replace(' ', '+')

                while count > start and (page_max is None or page <= page_max):

                    # response = requests.get(f"{list_items_url}&page={page}&q={search_term}")

                    # response = requests.get(f"{list_items_url}&page={page}&dataProvider={provider_encoded}")

                    response = requests.get(f"{list_items_url}&page={page}&dataProvider={provider_encoded}&q={search_term}")

                    count = response.json()["count"]
                    start = response.json()["start"]

                    # print(f"search term: {search_term}, page: {page}, total docs: {count}, start: {start}, curr docs: {len(data)}", file=sys.stderr)

                    # print(f"provider: {provider}, page: {page}, total docs: {count}, start: {start}, curr docs: {len(data)}", file=sys.stderr)

                    print(f"provider: {provider}, search term: {search_term}, page: {page}, total docs: {count}, start: {start}, curr docs: {len(data)}", file=sys.stderr)

                    page += 1

                    for doc in response.json()["docs"]:

                        # Get the terms available for all DPLA records.
                        record = parse_json_terms(tree=doc, terms=dpla_terms)

                        # Some records that are part of contributor collections have most of their metadata embedded in the sourceResource string.
                        if not record.get("title"):

                            for val in [ "title", "description" ]:

                                record[val] = doc['sourceResource'].get(val)

                        # Try to load metadata out of the original string as well.
                        parse_original_string(doc=doc, record=record)

                        data.append(record)

                        if running_tests:

                            break

        return data

    def transform(self, data):

        for record in data:

            if record.get("displayDate"):

                record["date"] = record["displayDate"]

            elif record.get("date") == [{}]:

                del record["date"]

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
