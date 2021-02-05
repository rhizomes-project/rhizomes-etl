#!/usr/bin/env python

import csv
import json
import os
import requests

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField


# REVIEW remove these
from etl import setup
import pdb


running_tests = os.environ.get("RUNNING_UNITTESTS")


protocol = "https://"
domain = "api.si.edu"
etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="smithsonian")

query_path = "/openaccess/api/v1.0/search"
query_url = protocol + domain + query_path + "?api_key=" + api_key + "&rows=1000"

keys_to_ignore = ("title_sort", "type", "label")
keys_to_not_label = ("content", )


# REVIEW: Why isn't date coming out correctly? answer: because smithsonian metadata does not really have a date field consistently
# REVIEW: map "name" to author? problem: 'name' is all over the place in smithsonian's metadata - not consistent

# REVIEW mary thomas's contact at SI
# REVIEW try using edan API http://edan.si.edu/openaccess/docs/


# important SI collections:
#
# archives of american art
# american art museum
# national museum of american history
# national portrait gallery


field_map = {
    "id":                                      RhizomeField.ID,
    "title":                                   RhizomeField.TITLE,
    "content/indexedStructured/object_type":   RhizomeField.RESOURCE_TYPE,
    "content/descriptiveNonRepeating/guid":    RhizomeField.URL,
    "date":                                    RhizomeField.DATE,
    "content/indexedStructured/date":          RhizomeField.SUBJECTS_HISTORICAL_ERA,
    "content/indexedStructured/topic":         RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "content/indexedStructured/geoLocation":   RhizomeField.SUBJECTS_GEOGRAPHIC,
    "content/freetext/notes":                  RhizomeField.NOTES,
    "image_urls":                              RhizomeField.IMAGE,
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


def get_image_urls(id_):
    "Returns urls to images, if any, for the given object."

    url = f"https://api.si.edu/openaccess/api/v1.0/content/{id_}?api_key={api_key}"


    response = requests.get(url=url)
    if not response.ok:

        return None

    data = response.json()
    urls = []

    media_objects = data["response"]["content"].get("descriptiveNonRepeating", {}).get("online_media", {}).get("media", [])
    for media_object in media_objects:


        pdb.set_trace()


        for resource in media_object.get("resources", []):

            url = resource.get("url", None)
            if url and url.endswith(".jpg"):

                urls.append(url)

    return urls


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

                if running_tests:

                    break

        return data

    def transform(self, data):

        for record in data:

            # # Retrieve urls to any images for the record.
            # urls = get_image_urls(id_=record["id"])
            # if urls:


            #     pdb.set_trace()


            #     record["image_urls"] = urls

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


                    if record['id'] == 'edanmdm-siris_sil_1106151':

                        pdb.set_trace()


                    for value in location.values():

                        if type(value) is dict:

                            if value.get('content'):

                                new_locations.add(value['content'])

                        else:

                            new_locations.add(value)

                record['content/indexedStructured/geoLocation'] = new_locations

        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    etl_env = setup.ETLEnv()
    etl_env.start()

    # urls = get_image_urls(id_="edanmdm-nmaahc_2012.36.4ab")

    urls = get_image_urls(id_="edanmdm-nmah_1051480")

    print(urls)


    # etl_process = SIETLProcess(format="csv")

    # data = etl_process.extract()
    # etl_process.transform(data=data)
    # etl_process.load(data=data)
