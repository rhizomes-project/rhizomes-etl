#!/usr/bin/env python

import abc
import os

from etl import setup
from etl.tools import MetadataWriter, RhizomeField


def clean_value(value):
    "Cleans value, including removing bad whitespace."

    if type(value) is str:

        return value.strip()

    elif type(value) is list:

        for idx, tmp in enumerate(value):

            value[idx] = tmp.strip()

    elif type(value) is set:

        value = { tmp.strip() for tmp in value }

    return value


class BaseETLProcess(abc.ABC):

    def __init__(self, format):

        self.format = format

        self.etl_env = setup.ETLEnv()
        self.etl_env.start()

    @abc.abstractmethod
    def get_field_map(self):    # pragma: no cover (should never get called)

        pass

    @abc.abstractmethod
    def extract(self):    # pragma: no cover (should never get called)

        pass

    def transform(self, data):
        "Transform the data."

        field_map = self.get_field_map()

        # Make sure that all transforms map from a field_map key to another field_map key, not
        # from a RhizomeField directly to another.
        if field_map.keys() & RhizomeField.values():

            raise Exception(f"Invalid field map keys found: {field_map.keys() & RhizomeField.values()}")

        # De-dupe the records (make sure no record appears more than once).
        record_ids = set()
        id_key = list(field_map.keys())[0]
        for record in data:

            id_val = record[id_key]

            if type(id_val) is list:

                id_val = id_val[0]

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
        "Load the data (into csv, json, database, etc.)"

        writer = MetadataWriter(format=self.format)
        writer.start_collection()

        for record in data:

            if record.get("ignore", False):

                continue

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
