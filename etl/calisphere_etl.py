#!/usr/bin/env python

import json
import os
import requests

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField


etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="calisphere")

running_tests = os.environ.get("RUNNING_UNITTESTS")


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
    'reference_image_md5',
    'repository_data',
    'rights'
    'collection_name',
    'campus_url',
    'sort_collection_data',
    'timestamp',
    'score'
]

field_map = {
    "id":                    RhizomeField.ID,
    "title":                 RhizomeField.TITLE,
    "creator":               RhizomeField.AUTHOR_ARTIST,
    "url_item":              RhizomeField.URL,
    "description":           RhizomeField.DESCRIPTION,
    "date":                  RhizomeField.DATE,
    "type":                  RhizomeField.DIGITAL_FORMAT,
    "sort_collection_data":  RhizomeField.SOURCE,
    "subject":               RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "reference_image_md5":   RhizomeField.IMAGES,
    # REVIEW Add type here?
}


def get_overview(hit, keys):

    overview = ''

    for key in keys:

        tmp = hit.get(key, [])
        if type(tmp) is str:

            overview += ' ' + tmp

        elif type(tmp) is list:

            for val in tmp:

                overview += ' ' + val

    return overview.lower()


class CalisphereETLProcess(BaseETLProcess):

    def get_field_map(self):

        return field_map

    def extract(self):

        # Search for each collection
        collections = [

            #
            # CEMA  - California Ethnic and Multicultural Archive
            #

            # Self-Help Graphics and Art archives - https://calisphere.org/collections/19853/
            19853,
            # Galería De La Raza archives - https://calisphere.org/collections/9080/
            9080,
            # Royal Chicano Air Force archives - https://calisphere.org/collections/18781/
            18781,
            # Centro Cultural de la Raza archives - https://calisphere.org/collections/9076/
            9076,
            # Villa (Esteban) papers - https://calisphere.org/collections/23847/
            23847,
            # Montoya (José) papers - https://calisphere.org/collections/15021/
            15021,
            # Prigoff (James) slide collection - https://calisphere.org/collections/17669/
            17669,
            # Ochoa (Victor) papers - https://calisphere.org/collections/16082/
            16082,
            # Vallejo (Linda) papers - https://calisphere.org/collections/23662/
            23662,
            # Torres (Salvador Roberto) papers - https://calisphere.org/collections/26299/
            26299,
            # Lucero (Linda) collection on La Raza Silkscreen Center/La Raza Graphics - https://calisphere.org/collections/13523/
            13523,

            #
            # inSite Archive (Pull All Content) - https://calisphere.org/collections/10703/
            #

            10703,

            #
            # Cinewest Archive
            #
            27075,

            #
            # California Revealed from Center for the Study of Political Graphics (Pull All Content) - https://calisphere.org/collections/27440/
            #

            27440,

            #
            # Robin Dunitz Slides of Los Angeles Murals, 1925-2002 (Pull subset of content based on search terms) - https://calisphere.org/collections/27354/
            #

            {
                "id": 27354,
                "terms": [
                    "chicano", "chicana",
                    "latino", "latina",
                    "hispanic",
                    "boyle heights",
                    "immigrant",
                ]
            },

            #
            # California Historical Society Collection, 1860-1960 (Pull subset of content based on search terms) - https://calisphere.org/collections/27087/
            #

            {
                "id": 27087,
                "terms": [
                    "mexican american", "mexican-american",
                ]
            }
        ]

        data = []
        rows = 5000

        # Running tests?
        if running_tests:

            collections = [
                {
                    "id": 27087,
                    "terms": [ "art" ]
                }
            ]

            rows = 50


        for collection in collections:

            terms = []

            # Is this a collections with specific search terms?
            if type(collection) is dict:

                terms = collection["terms"]
                collection = collection["id"]

            url = f"https://solr.calisphere.org/solr/query/?q=collection_url:https://registry.cdlib.org/api/v1/collection/{collection}/&wt=json&indent=true&rows={rows}"

            headers = { "X-Authentication-Token": api_key }
            response = requests.get(url, headers=headers)

            for hit in response.json()['response']['docs']:

                # Filter colletion results by term?
                if terms:

                    ignore = True
                    overview = get_overview(hit=hit, keys=SOLR_KEYS)

                    for term in terms:

                        if term in overview:

                            ignore = False
                            break

                    if ignore:

                        continue

                # Extract our data from the hit.
                record = {}
                for key in SOLR_KEYS:

                    if hit.get(key):

                        record[key] = hit[key]

                data.append(record)

                # Are we just testing?
                if running_tests:

                    break

        return data

    def transform(self, data):

        for record in data:

            # Transform the image md5's into urls, e.g.,
            # https://calisphere.org/clip/500x500/4d2a48ba900fccef9c01cae0fd5cf3bc
            reference_image_md5 = record.get("reference_image_md5")
            if reference_image_md5:

                record["reference_image_md5"] = f"https://calisphere.org/clip/500x500/{reference_image_md5}"

        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    etl_process = CalisphereETLProcess(format="csv")

    data = etl_process.extract()
    etl_process.transform(data=data)
    etl_process.load(data=data)
