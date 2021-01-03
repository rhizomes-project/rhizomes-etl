#!/usr/bin/env python

import os

from etl import setup
from etl.tools import MetadataWriter, RhizomeField


def clean_value(value):
    "Cleans value, including removing bad whitespace."

    if type(value) is str:

        return value.strip()

    else:

        for idx, tmp in enumerate(value):

            value[idx] = tmp.strip()

        return value


class BaseETLProcess():

    # REVIEW TODO make this an ABC

    def __init__(self, format):

        self.format = format

        self.etl_env = setup.ETLEnv()
        self.etl_env.start()

    def get_field_map(self):

        raise Exception("Base class get_field_map() not defined.")

    def extract(self):

        raise Exception("Base class extract() not defined.")

    def transform(self, data):

        # REVIEW TODO: make sure that all transforms map from a field_map key to another field_map key, not
        # to a RhizomeField directly.

        field_map = self.get_field_map()

        # De-dupe the records (make sure no record appears more than once).
        record_ids = set()
        id_key = list(field_map.keys())[0]
        for record in data:

            id_val = record[id_key]
            if id_val in record_ids:

                record["ignore"] = True

            else:

                record_ids.add(id_val)

        # Now map all the other values in the raw metadata to the correct output rhizome fields.
        for record in data:

            # Has this record been flagged to be skipped?
            if record.get("ignore", False):

                continue

            for name, descriptions in field_map.items():

                if not descriptions:

                    continue

                if type(name) is RhizomeField:

                    name = name.value

                if type(descriptions) is not list:

                    descriptions = [ descriptions ]

                value = record.get(name)
                if value:

                    for description in descriptions:

                        description = description.value

                        if description == name:

                            continue

                        if record.get(description):

                            record[description] += clean_value(value=value)

                        else:

                            record[description] = clean_value(value=value)

                        del record[name]

    def load(self, data):

        writer = MetadataWriter(format=self.format)
        writer.start_collection()

        for record in data:

            writer.start_record()

            for name in RhizomeField.values():

                value = record.get(name)
                if value and name:

                    writer.add_value(name=name, value=value)

            writer.end_record()

            # Running tests?
            if os.environ.get("RUNNING_UNITTESTS"):

                break

        writer.end_collection()