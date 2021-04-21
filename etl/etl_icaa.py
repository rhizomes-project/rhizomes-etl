#!/usr/bin/env python

import json
import requests
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField


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
    # "reference_image_md5":          RhizomeField.IMAGES,
}


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

    def extract(self):

        data = []

        for keyword in self.keywords:

            url = f"https://icaa.mfah.org/api/items?per_page=1000&fulltext_search={keyword}"

            response = requests.get(url, timeout=60)

            if not response.ok:    # pragma: no cover (should never be True during testing)

                raise Exception(f"Error retrieving data from ICAA, status code: {response.status_code}, reason: {response.reason}")

            json_data = response.json()

            for record in json_data:

                record = extract_record(record=record)
                data.append(record)

                if ETLEnv.instance().are_tests_running():

                    break

        return data

    def transform(self, data):

        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "icaa" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
