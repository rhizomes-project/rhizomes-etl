#!/usr/bin/env python

import json
import re
import requests
import sys
import time

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField
from etl.date_parsers import get_date_first_four


field_map = {
    "o:id":                             RhizomeField.ID,
    "o:title":                          RhizomeField.TITLE,
    "dcterms:creator/o:label":          RhizomeField.AUTHOR_ARTIST,
    # "@id":                          RhizomeField.URL, # Note: this is the url to the item's metadata in the API
    # https://icaa.mfah.org/s/en/item/1468561
    "id":                               RhizomeField.URL, # Note; this is "https://icaa.mfah.org/s/en/item/{id}"
    "dcterms:language/o:label":         RhizomeField.LANGUAGE,
    "dcterms:description/@value":       RhizomeField.DESCRIPTION,
    "o:created/@value":                 RhizomeField.DATE,
    # "type":                         RhizomeField.DIGITAL_FORMAT,
    "dcterms:type/o:label":             RhizomeField.RESOURCE_TYPE,
    # "sort_collection_data":         RhizomeField.SOURCE,
    "icaa:topicDescriptor/o:label":     RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "bibo:annotates/@value":            RhizomeField.NOTES,
    "o:media":                          RhizomeField.IMAGES,
}


def extract_image_url(record):

    media = record.get("o:media")
    if media:

        media_url = media[0]["@id"]

        response = requests.get(media_url, timeout=60)
        if not response.ok:

            raise Exception(f"ICAA API returned error {response.status_code} trying to retrieve image url")

        image_json = response.json()
        thumbnail_urls = image_json.get("o:thumbnail_urls")
        if thumbnail_urls:

            image_url = thumbnail_urls.get("large")
            if image_url:

                return "https://icaa.mfah.org" + image_url

    return None

def extract_field(record, field):
    "Extract metadata for the given field for the record."

    if "/" in field:

        pos = field.find("/")
        sub_field = field[ : pos]
        sub_data = record.get(sub_field, {})
        sub_field = field[pos + 1 :]

        if type(sub_data) is list:

            list_vals = []
            for sub_data_val in sub_data:

                tmp = extract_field(record=sub_data_val, field=sub_field)
                if tmp:

                    list_vals.append(tmp)

            return list_vals

        else:

            return extract_field(record=sub_data, field=sub_field)

    elif type(record) is list:

        return record

    else:

        return record.get(field)

def extract_record(record):
    "Extracts metadata for the given record."

    record_data = {}

    for field in field_map:

        if field == "id":

            value = f"https://icaa.mfah.org/s/en/item/{record_data.get('o:id')}"

        elif field == "o:media":

            value = extract_image_url(record=record)

        else:

            value = extract_field(record=record, field=field)

        if field == "dcterms:description/@value" and value:

            value = value[0][ : 256]

        record_data[field] = value

    return record_data


class ICAAETLProcess(BaseETLProcess):

    def __init__(self, format):

        self.keywords = [
            "chicano", "chicana", "chicanx",
            "border",
            "%22mexican-american%22",

            # Note: "%22mexican american%22" and "%22mexican-american%22" return the same results.
        ]

        super().__init__(format=format)

    def init_testing(self):

        self.keywords = [ "chicano" ]

    def get_field_map(self):

        return field_map

    def get_collection_name(self):

        return "ICAA"

    def get_date_parsers(self):

        # "@value": "2020-02-14T02:06:44+00:00"

        return {
            r'^\d{4}\-\d{2}\-\d{2}':  get_date_first_four
        }

    def extract(self):

        data = []

        for keyword in self.keywords:

            url = f"https://icaa.mfah.org/api/items?per_page=1000&fulltext_search={keyword}"

            response = requests.get(url, timeout=60)

            if not response.ok:    # pragma: no cover (should never be True during testing)

                raise Exception(f"Error retrieving data from ICAA, status code: {response.status_code}, reason: {response.reason}")

            json_data = response.json()

            print(f"\nProcessing {len(json_data)} ICAA records for keyword {keyword}...\n", file=sys.stderr)
            num_retrieved = 0

            for record in json_data:

                record = extract_record(record=record)
                data.append(record)

                if ETLEnv.instance().are_tests_running():

                    break

                num_retrieved += 1
                if num_retrieved % 25 == 0:

                    print(f"{num_retrieved} ICAA records retrieved ...", file=sys.stderr)

                    # Sleep a bit to try to keep from overwhelming the server.
                    time.sleep(5)

        return data

    def transform(self, data):

        for record in data:

            artists = record.get("dcterms:creator/o:label")
            if artists:

                if type(artists) is not list:

                    artists = [ artists ]

                for idx, artist in enumerate(artists):


                    # Try to remove trailing years from artist name. e.g., "artist name, 1932-" and "artist name, 1932-1934".
                    patterns = [
                        r'\,\ \d{4}',
                        r'\,\ \d{4}\-\d{4}'
                    ]

                    for pattern in patterns:

                        match = re.search(pattern, artist)
                        if match:

                            artists[idx] = artist[ : match.start()]
                            break

                record["dcterms:creator/o:label"] = artists

        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "icaa" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
