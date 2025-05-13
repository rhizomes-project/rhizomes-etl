#!/usr/bin/env python

import sys

from run import INST_ETL_MAP
from tools import get_current_metadata, RhizomeField


METADATA_MAP = {

    RhizomeField.TITLE: { "key": "dcterms:title", "selector": "@value" },
    RhizomeField.ALTERNATE_TITLES: { "key": "dcterms:alternative", "selector": "@value" },
    RhizomeField.AUTHOR_ARTIST: { "key": "dcterms:creator", "selector": "@value" },
    RhizomeField.IMAGES: { "key": "foaf:img", "selector": "@id" },
    RhizomeField.URL: { "key": "foaf:weblog", "selector": "@id" },
    RhizomeField.DESCRIPTION: { "key": "dcterms:description", "selector": "@value" },
    RhizomeField.SUBJECTS_TOPIC_KEYWORDS: { "key": "dcterms:subject", "selector": "@value" },
    RhizomeField.SEARCHABLE_DATE: { "key": "dcterms:date", "selector": "@value" },
    RhizomeField.RESOURCE_TYPE: { "key": "dcterms:type", "selector": "@value" },
    RhizomeField.DIGITAL_FORMAT: { "key": "dcterms:format", "selector": "@value" },
    RhizomeField.SOURCE: { "key": "dcterms:source", "selector": "@value" },
    RhizomeField.LANGUAGE: { "key": "dcterms:language", "selector": "@value" },
    RhizomeField.COLLECTION_NAME: { "key": "dcterms:contributor", "selector": "@value" },
    RhizomeField.ANNOTATES: { "key": "dcterms:contributor", "selector": "@value" }, # REVIEW: what is the key for this?
    RhizomeField.ACCESS_RIGHTS: { "key": "dcterms:accessRights", "selector": "@value" },

}


def get_key_values(obj, selector):

    values = [ sub_obj.get(selector) for sub_obj in obj ]
    return "|".join(values)


def do_backup(institution=None):

    metadata = get_current_metadata()

    for record in metadata:

        for rhizome_field, config in METADATA_MAP.items():

            key = config["key"]
            selector = config["selector"]

            obj = record.get(key)
            if obj:

                values = get_key_values(obj=obj, selector=selector)


    # REVIEW: todo add ability to filter by institution?

    # REVIEW: todo output to csv here using MetadataWriter.

    return 0


def do_usage():

    print("Usage: backup.py institution", file=sys.stderr)

    raise Exception("Invalid usage")


if __name__ == "__main__":    # pragma: no cover

    institution = sys.argv[ 1: ][0] if len(sys.argv) == 2 else None

    if institution and institution not in INST_ETL_MAP:

        do_usage()

    else:

        do_backup(institution=institution)
