#!/usr/bin/env python

import sys

from run import INST_ETL_MAP
from tools import get_current_metadata, MetadataWriter, RhizomeField


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
    RhizomeField.ANNOTATES: { "key": "bibo:annotates", "selector": "@value" },
    RhizomeField.ACCESS_RIGHTS: { "key": "dcterms:accessRights", "selector": "@value" },

}


def get_key_values(obj, selector):

    if not obj:

        return None

    values = [ sub_obj[selector] for sub_obj in obj if sub_obj.get(selector) ]
    if not values:

      return None

    return "|".join(values)


def do_backup(institution=None):

    # Filter by institution? If so, get institution name.
    if institution:

        process = INST_ETL_MAP[institution]
        institution = process(format="csv").get_collection_name()

    # Get all current metadata.
    metadata = get_current_metadata()

    # Output to csv here using MetadataWriter.
    #
    # Note:  Some records in the database seem to be corrupt (i.e., mostly or
    # completely empty - perhaps added by mistake?) Use data validation to
    # filter those out.
    #
    writer = MetadataWriter(format="csv", do_validate=True)

    writer.start_collection()
    record_count = 0

    # Parse each record and write it out.
    for record in metadata:

        writer.start_record()

        for rhizome_field, config in METADATA_MAP.items():

            key = config["key"]
            selector = config["selector"]

            obj = record.get(key)

            value = get_key_values(obj=obj, selector=selector)
            if value:

                writer.add_value(name=rhizome_field.value, value=value)

        # Filter by instition?
        #
        # Note: we *should* be able to search the API by institution name,
        # but I have not been able to get it to work.
        if institution and not writer.does_record_match(name=RhizomeField.COLLECTION_NAME.value, value=institution):

            pass

        else:

            writer.end_record()

            record_count += 1
            if record_count % 250 == 0:

                print(f"{record_count} written ...", file=sys.stderr)

    writer.end_collection()

    return 0


def do_usage():

    inst_list = list(INST_ETL_MAP.keys())
    inst_list = "|".join(inst_list)

    print("Usage: backup.py", file=sys.stderr)
    print(f"Usage: backup.py institution=[{inst_list}]", file=sys.stderr)


if __name__ == "__main__":    # pragma: no cover

    institution = sys.argv[ 1: ][0] if len(sys.argv) == 2 else None

    if institution and institution not in INST_ETL_MAP:

        do_usage()
        sys.exit(1)

    else:

        do_backup(institution=institution)
        sys.exit(0)
