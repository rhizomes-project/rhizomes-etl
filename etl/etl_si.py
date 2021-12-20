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
# 3. get metadata for the artwork from the artworks list, and supplement it with an image url from the deliveryService API
#


etl_env = ETLEnv.instance()
etl_env.start()
api_key = etl_env.get_api_key(name="smithsonian")


field_map = {
    "id":                                      RhizomeField.ID,
    "artist":                                  RhizomeField.AUTHOR_ARTIST,
    "title":                                   RhizomeField.TITLE,
    "caption":                                 RhizomeField.DESCRIPTION,
    "type":                                    RhizomeField.RESOURCE_TYPE,
    "url":                                     RhizomeField.URL,
    "date":                                    RhizomeField.DATE,
    "subjects":                                RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "medium":                                  RhizomeField.DIGITAL_FORMAT,
    "image_urls":                              RhizomeField.IMAGES,
    "access_rights":                           RhizomeField.ACCESS_RIGHTS,
}


def get_record(artwork, artist_name):

    record = {}

    record["id"] = artwork["objectNumber"]
    record["artist"] = artist_name
    record["title"] = artwork["title"]
    record["type"] = artwork["classification"]
    record["url"] = artwork["guid"]
    record["date"] = artwork.get("dated")
    record["medium"] = artwork.get("medium")
    record["access_rights"] = artwork.get("siUsageStatement")

    # Use ontology to build a list of subjects.
    subjects = []
    for ontology in artwork.get("ontology", []):

        path_term = ontology.get("pathTerm", "")
        pos = path_term.rfind("\\")
        subjects.append(path_term[ pos + 1 : ])

    record["subjects"] = subjects

    # Retrieve an image link for the record (if any).
    images = artwork.get("images", [])
    if images:

        record["caption"] = images[0]["caption"]
        record["image_urls"] = "https://ids.si.edu/ids/deliveryService?id=" + images[0]["fileName"]

    return record


class SIETLProcess(BaseETLProcess):

    def init_testing(self):

        pass

    def get_field_map(self):

        return field_map

    def get_collection_name(self):

        return "Smithsonian American Art Museum (SAAM)"

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

                    last_name = row.get("lastName")
                    first_name = row.get("firstName")

                    artist_name = last_name
                    if first_name:

                        artist_name += ", " + first_name

                    artists[int(row["\ufeffconstituentId"])] = artist_name

        data = []

        with open("etl/data/permanent/si/artworks.json", "r") as input:

            buffer = input.read()
            artworks = json.loads(buffer)

            for artwork in artworks:

                constituentId = artwork["artworkConstituentRelationships"][0]["constituentId"]

                if constituentId in artists:

                    artist_name = artists[constituentId]

                    record = get_record(artwork=artwork, artist_name=artist_name)
                    data.append(record)


                    if len(data) % 25 == 0:

                        print(f"Extracted {len(data)} records", file=sys.stderr)

        return data

    def rectify(self, data, curr_metadata):

        print(f"Rectifying new smithsonian subject metadata for smithsonian with content admins' changes.", file=sys.stderr)

        for item in data:

            url = item[RhizomeField.URL.value]

            # Is the record new?
            if url not in curr_metadata:

                continue

            curr_subjects = curr_metadata[url].get("subjects", [])
            if curr_subjects:

                item[RhizomeField.SUBJECTS_TOPIC_KEYWORDS.value] = curr_subjects


    def transform(self, data):

        # REVIEW: What needs to happen here?

        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "si" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
