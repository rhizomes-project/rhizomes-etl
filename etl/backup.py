#!/usr/bin/env python

import sys

from run import INST_ETL_MAP
from tools import get_current_metadata


METADATA_MAP = {

    RhizomeField.ID: "", # REVIEW: I don't see this in the csv files or the metadata???
    RhizomeField.TITLE: "dcterms:title",
    RhizomeField.ALTERNATE_TITLES: "", # REVIEW: what is this?
    RhizomeField.AUTHOR_ARTIST: "dcterms:creator",
    RhizomeField.IMAGES: "foaf:img",
    RhizomeField.URL: "foaf:weblog",
    RhizomeField.DESCRIPTION: "dcterms:description",
    RhizomeField.SUBJECTS_TOPIC_KEYWORDS: "dcterms:subject",
    RhizomeField.SEARCHABLE_DATE: "dcterms:date",
    RhizomeField.RESOURCE_TYPE: "dcterms:type",
    RhizomeField.DIGITAL_FORMAT: "dcterms:format",
    RhizomeField.SOURCE: "dcterms:source",
    RhizomeField.LANGUAGE: "dcterms:language",
    RhizomeField.COLLECTION_NAME: "dcterms:contributor",
    RhizomeField.ANNOTATES: "dcterms:contributor",
    RhizomeField.ACCESS_RIGHTS: "", # REVIEW: what is this?

}

# REVIEW: finish the metadata map ^^^


def do_backup(institution=None):

    metadata = get_current_metadata()

    # REVIEW: todo add ability to filter by institution?

    # REVIEW: todo output to csv here.

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
