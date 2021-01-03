#!/usr/bin/env python


import requests
import csv
import json

from etl_process import BaseETLProcess
from setup import ETLEnv
from tools import RhizomeField


protocol = "https://"
domain = "api.si.edu"
etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="smithsonian")

query_path = "/openaccess/api/v1.0/search"
query_url = protocol + domain + query_path + "?api_key=" + api_key + "&rows=25"

keys_to_ignore = ("title_sort", "type", "label")
keys_to_not_label = ("content", )


# REVIEW: Why isn't date coming out correctly? answer: because smithsonian metadata does not really have a date field consistently
# REVIEW: map "name" to author? problem: 'name' is all over the place in smithsonian's metadata - not consistent


field_map = {
# REVIEW: Rework this with RHizomeFields
    "id":                                      RhizomeField.ID,
    "title":                                   RhizomeField.TITLE,
    "content/indexedStructured/object_type":   RhizomeField.RESOURCE_TYPE,
    "content/descriptiveNonRepeating/guid":    RhizomeField.URL,
    "date":                                    RhizomeField.DATE,
    "content/indexedStructured/date":          RhizomeField.SUBJECTS_HISTORICAL_ERA,
    "content/indexedStructured/topic":         RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "content/indexedStructured/geoLocation":   RhizomeField.SUBJECTS_GEOGRAPHIC,
    "content/freetext/notes":                  RhizomeField.NOTES,
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


class SIETLProcess(BaseETLProcess):

    def get_field_map(self):

        return field_map


    def extract(self):

        data = []

        search_terms = [ "chicano" ]
        for search_term in search_terms:

            response = requests.get(query_url + "&q=" + search_term)

            for row in response.json()["response"]["rows"]:

                record = traverse(record=row)
                data.append(record)

        return data

    def transform(self, data):

        for record in data:

            # Clean up notes.
            notes = record.get('content/freetext/notes')
            if notes:

                new_notes = []
                for note in notes:

                    new_notes.append(f"- {note['label']}: {note['content']}")

                record['content/freetext/notes'] = new_notes

            # Clean up geolocations.
            locations = record.get('content/indexedStructured/geoLocation')
            if locations:

                new_locations = set()
                for location in locations:

                    for val in location.values():

                        if not val.get('content'):

                            # print(f"ignoring {str(list(location.keys()))}")
                            pass

                        else:

                            new_locations |= { value['content'] for value in location.values() }

                record['content/indexedStructured/geoLocation'] = new_locations

        super().transform(data=data)


if __name__ == "__main__":

    etl_process = SIETLProcess(format="csv")

    data = etl_process.extract()
    etl_process.transform(data=data)
    etl_process.load(data=data)
