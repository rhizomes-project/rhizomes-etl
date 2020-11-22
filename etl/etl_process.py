#!/usr/bin/env python


import setup
from tools import MetadataWriter, RhizomeField

# REVEW: TODO clean up "list" values so that they are easier to read (put each on its own line)


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

        field_map = self.get_field_map()

        for record in data:

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

                            record[description] += value

                        else:

                            record[description] = value

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

        writer.end_collection()
