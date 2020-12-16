#!/usr/bin/env python


import requests
import json

from etl_process import BaseETLProcess
from setup import ETLEnv
from tools import RhizomeField


etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.get_api_key(name="calisphere")


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
            # California Revealed from Center for the Study of Political Graphics (Pull All Content) - https://calisphere.org/collections/27440/
            #

            27440,

            #
            # Robin Dunitz Slides of Los Angeles Murals, 1925-2002 (Pull subset of content based on search terms) - https://calisphere.org/collections/27354/
            #

            {
                "id": 27354,
                "terms": [
                    [ "Chicano", "Chicana", ],
                    [ "Latino", "Latina", ],
                    "Hispanic",
                    "Boyle Heights",
                    "Immigrant",
                ]
            },

            #
            # California Historical Society Collection, 1860-1960 (Pull subset of content based on search terms) - https://calisphere.org/collections/27087/
            #

            {
                "id": 27087,
                "terms": [
                    ["mexican american", "mexican-american"],
                ]
            }
        ]

        data = []

        for collection in collections:

            # REVIEW: TODO handle collections with specific search terms
            if type(collection) is dict:

                continue

            url = f"https://solr.calisphere.org/solr/query/?q=collection_url:https://registry.cdlib.org/api/v1/collection/{collection}/&wt=json&indent=true&rows=5000"

            headers = { "X-Authentication-Token": api_key }
            response = requests.get(url, headers=headers)

            for hit in response.json()['response']['docs']:

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
