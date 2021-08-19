#!/usr/bin/env python

import csv
import json
import os
import requests
import sys

from bs4 import BeautifulSoup

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField
from etl.date_parsers import *


#
# Idea to use constituents list for data pull
#
# 1. 'join' between constituents list and artworks list, using artist's constituentId.
# 2. get the objectNumber from the artworks list.
# 3. get metadata for the artwork from edan:
#
#    https://api.si.edu/openaccess/api/v1.0/content/:id
#
#    See http://edan.si.edu/openaccess/apidocs/ to documentation on Smithsonian's API.
#


etl_env = ETLEnv.instance()
etl_env.start()
api_key = etl_env.get_api_key(name="smithsonian")


field_map = {
    "id":                                      RhizomeField.ID,
    "artist":                                  RhizomeField.AUTHOR_ARTIST,
    "title":                                   RhizomeField.TITLE,
    "type":                                    RhizomeField.RESOURCE_TYPE,
    "url":                                     RhizomeField.URL,
    "date":                                    RhizomeField.DATE,
    # "content/indexedStructured/date":          RhizomeField.SUBJECTS_HISTORICAL_ERA,
    # "subjects":                                RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    # "content/indexedStructured/geoLocation":   RhizomeField.SUBJECTS_GEOGRAPHIC,
    # "content/freetext/notes":                  RhizomeField.NOTES,
    "medium":                                  RhizomeField.DIGITAL_FORMAT,
    "image_urls":                              RhizomeField.IMAGES,
}


def get_record(artwork):

    record = {}

    id_ = artwork["objectNumber"]
    record["id"] = id_

    record["artist"] = artwork["artworkConstituentRelationships"][0]["displayName"]
    record["title"] = artwork["title"]
    record["type"] = artwork["classification"]
    record["url"] = artwork["guid"]
    record["date"] = artwork.get("dated")
    record["medium"] = artwork.get("medium")

    # Now try to get a link to an image for the item.

    # REVIEW: would like to use edan API to retrieve metadata about the record, but it does not seem to work very often.

    # url = f"https://api.si.edu/openaccess/api/v1.0/content/{id_}?api_key={api_key}"
    # response = requests.get(url, timeout=60)


    url = f"http://edan.si.edu/saam/id/object/{id_}"

    response = requests.get(url, timeout=60, headers={'Accept': 'application/json'})
    soup = BeautifulSoup(response.text, 'html.parser')

    image_url = None

    image = soup.find("img", {"class": "mainimg"})
    if image:

        image_url = image.attrs['src']

    record["image_urls"] = image_url

    return record


class SIETLProcess(BaseETLProcess):

    def init_testing(self):

        pass

    def get_field_map(self):

        return field_map

    def get_collection_name(self):

        return "Smithsonian"

    def get_date_parsers(self):

        return {
            r'\d{4}':                   get_date_first_avail_4_digit_year, # sometime around 1984 we think
        }

    def extract(self):

        # First read all our artist ids.
        artists = {}

        with open("etl/data/permanent/si/constituents.csv", "r") as input:

            reader = csv.DictReader(input)
            first_row = True
            for row in reader:

                if first_row:

                    first_row = False

                else:

                    artists[int(row["\ufeffconstituentId"])] = True

        data = []

        with open("etl/data/permanent/si/artworks.json", "r") as input:

            buffer = input.read()
            artworks = json.loads(buffer)

            for artwork in artworks:

                constituentId = artwork["artworkConstituentRelationships"][0]["constituentId"]

                if constituentId in artists:

                    record = get_record(artwork=artwork)
                    data.append(record)


                    if len(data) % 25 == 0:

                        print(f"Extracted {len(data)} records", file=sys.stderr)

        return data


    def transform(self, data):

        # REVIEW: What needs to happen here?

        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "si" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
