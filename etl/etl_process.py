#!/usr/bin/env python

import abc
import re
import os

from etl.setup import ETLEnv
from etl.tools import MetadataWriter, RhizomeField


# REVIEW: Add a step to ETL process to create 1 display date and 1 searchable date, which should be a year.
# REVIEW: Add a step to ETL process to change title values of "[Unknown]" to "Unknown Title" ?
# REVIEW: Add collection name as a field to all metadata.


def get_date_first_four(date_val):

    return date_val[ : 4]

DATE_PARSERS = {

    r'\d{4}\-\d{2}\-\d{2}': get_date_first_four,
}

def get_searchable_date(record):
    "Parse the date val and extract a year from it."

    date_vals = record.get(RhizomeField.DATE.value, [])
    if type(date_vals) is not list:

        date_vals = [ date_vals ]

    searchable_date = None

    for date_val in date_vals:

        print(date_val)

        for pattern, parser in DATE_PARSERS.items():

            if re.match(pattern, date_val):

                searchable_date = parser(date_val=date_val)
                if searchable_date:

                    return int(searchable_date)


def clean_value(value):
    "Cleans value, including removing bad whitespace."

    if type(value) is set:

        value = list(value)

    if type(value) is str:

        return value.strip()

    elif type(value) is list:

        value.sort()
        for idx, tmp in enumerate(value):

            value[idx] = tmp.strip()

    return value


class BaseETLProcess(abc.ABC):

    def __init__(self, format):

        self.format = format

        self.etl_env = ETLEnv.instance()
        self.etl_env.start()

        if self.etl_env.are_tests_running():

            self.init_testing()

    @abc.abstractmethod
    def init_testing(self):    # pragma: no cover (should never get called)

        pass

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

        # Populate our Searchable Date.
        searchable_date = get_searchable_date(record=record)

        record[RhizomeField.SEARCHABLE_DATE] = searchable_date


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

        writer.end_collection()
