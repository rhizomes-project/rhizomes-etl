#!/usr/bin/env python

import abc
import re
import os
import sys

from etl.setup import ETLEnv
from etl.tools import MetadataWriter, get_previous_item_ids, RhizomeField, FIELDS_TO_DEDUPE, OUTPUT_COLS



# REVIEW: Add a step to ETL process to create 1 display date and 1 searchable date, which should be a year.
# REVIEW: Add a step to ETL process to change title values of "[Unknown]" to "Unknown Title" ? (DONE for PTH)
# REVIEW: Add collection name as a field to all metadata. (DONE for all)
# REVIEW: TODO pull list of unique keywords (column M) for each provider. keep phrases in tact, get a count of # of occurrences of each and make it case-insensitive.

# REVIEW: TODO: De-dupe all subject columns, in a case-insensitive manner, and make all subjects use title case.
# REVIEW: TODO: Strip opening and closing brackets from PTH titles. (DONE for PTH)
# REVIEW: TODO: de-dupe PTH resource ids, and possibly choose one of them as *the* resource id 
# (e.g., line 2073 - ark: ark:/67531/metapth279657 | info:ark/67531/metapth279657 | issn: 0495-3460 | lccn: sn 86-11780 | oclc: 4742678)
# ignore for now
# REVIEW: TODO: check if PTH has better titles? or if they can be improved somehow? e.g., "art lies" does not show up in row 2073
# REVIEW: TODO: for PTH try to figure out a way to "choose better title" (use "Main Title") (DONE for PTH)
# - see ark: ark:/67531/metapth225042 | info:ark/67531/metapth225042 | local-cont-no: 1390_VisionsWest_1986_PR1 Visions of the West: American Art from Dallas Collections | Visions of the West: American Art from Dallas Collections [Press Release]
# - possibly add a new "sub-title" or "alternate title" field?
# - check if each title is a substring of one of the other titles, and if it is then remove the substring title?
# (DONE for PTH)
# REVIEW: check why so many PTH records have no artist? (DONE for PTH)

# REVIEW DPLA (and possibley calisphere) TODO: remove ", artist" and ", creator" and ", organizer" and ", photographer" and ", painter" from end of artist field?
# REVIEW Calisphere: change "(title unknown)" to "Title Unknown" ... same for "(artist unknown)"

# REVIEW: for ICAA, try to remove trailing years from artist name. e.g., "artist name, 1932-" and "artist name, 1932-1934" (DONE for ICAA)


# REVIEW: for PTH, try to do a translation from their character entities to unicode - e.g., <dc:subject>Vela&amp;#769;zquez, Diego, 1599-1660.</dc:subject> (FIXED for PTH)
# REVIEW: for ICAA, do a sample run removing html tags. (DONE for ICAA)
# REVIEW: for DPLA & Caliphere, for now just spit out first 4-digit year I found as searchable date.
# REVIEW: generate new csvs for DPLA and Calisphere
# REVIEW: for DPLA & Calisphere, try to use URL to de-dupe records across institutions.
# See https://dp.la/item/d0814ef8a3d58a351e32ffc183ccae49?q=Day%20of%20the%20Dead%20%2781 and
#     https://calisphere.org/item/ark:/13030/hb2290044r/
# Note: in general, DPLA is more likely to contain Calisphere records than vice-versa...

# REVIEW: for DPLA and Calisphere, remove the following from end of artist name:
# , Artist
# , Author
# , Collaborator
# , Compiler
# , Contributor
# , Creator
# , Editor
# , Interviewer
# , Occupant 
# , Organizer
# , Photographer
# , Speaker
# Also try removing everything within parentheses at end of artist name.

# for ICAA, look into accented characters in description and notes, e.g., &oacute; FIXED for ICAA
# for ICAA, look into the seemingly truncated descriptions. FIXED for ICAA


def get_searchable_date(record, date_parsers):
    "Parse the date val and extract a year from it."

    date_vals = record.get(RhizomeField.DATE.value, [])
    if type(date_vals) is not list:

        date_vals = [ date_vals ]

    searchable_date = None

    for date_val in date_vals:

        for pattern, parser in date_parsers.items():

            if re.search(pattern, date_val):

                searchable_date = parser(date_val=date_val)
                if searchable_date:

                    searchable_date = int(searchable_date)
                    if searchable_date < 1000 or searchable_date > 3000:

                        raise Exception(f"Error: invalid searchable date found: {searchable_date}")

                    return searchable_date


    # REVIEW: remove this?
    if date_vals:

        print(f"Ignoring date val '{date_vals[0]}' - could not parse searchable date", file=sys.stderr)


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


def title_value(value):
    "Make value use title case."

    return value.title()


def de_dupe_list(values):

    if type(values) is not list:

        return values

    unique_values = set()

    for value in values:

        unique_values.add(title_value(value=value))

    return list(unique_values)


class BaseETLProcess(abc.ABC):

    def __init__(self, format):

        self.format = format

        self.etl_env = ETLEnv.instance()
        self.etl_env.start()

        self.date_parsers = self.get_date_parsers()

        if self.etl_env.are_tests_running():

            self.init_testing()

    @abc.abstractmethod
    def init_testing(self):    # pragma: no cover (should never get called)

        pass

    @abc.abstractmethod
    def get_date_parsers(self):     # pragma: no cover (should never get called)

        pass

    @abc.abstractmethod
    def get_field_map(self):    # pragma: no cover (should never get called)

        pass

    @abc.abstractmethod
    def get_collection_name(self):    # pragma: no cover (should never get called)

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

            for name, rhizome_fields in field_map.items():

                if not rhizome_fields:

                    continue

                if type(name) is RhizomeField:

                    name = name.value

                if type(rhizome_fields) is not list:

                    rhizome_fields = [ rhizome_fields ]

                value = record.get(name)
                if value:

                    for rhizome_field in rhizome_fields:

                        rhizome_field = rhizome_field.value

                        if rhizome_field == name:

                            continue

                        if record.get(rhizome_field):

                            prev_vals = record[rhizome_field]
                            if type(prev_vals) is not list:

                                prev_vals = [ prev_vals ]

                            clean_vals = clean_value(value=value)
                            if type(clean_vals) is not list:

                                clean_vals = [ clean_vals ]

                            record[rhizome_field] = prev_vals + clean_vals

                        else:

                            record[rhizome_field] = clean_value(value=value)

                        del record[name]


        # Remove records that are already loaded in the rhizomes website?
        if not self.etl_env.do_rebuild_previous_items():

            previous_record_urls = get_previous_item_ids()

            for record in data:

                if record.get("ignore", False):

                    continue

                url = record[RhizomeField.URL.value]

                if type(url) is list:

                    raise Exception(f"URL for record {record[RhizomeField.ID.value]} is a list - lists of urls are not supported.")

                if url in previous_record_urls:

                    record["ignore"] = True


        # Do some more tweaks to each record's data.
        for record in data:

            # Add collection name.
            record[RhizomeField.COLLECTION_NAME.value] = self.get_collection_name()

            # Replace null artist name with "Unknown"
            if not record.get(RhizomeField.AUTHOR_ARTIST.value):

                record[RhizomeField.AUTHOR_ARTIST.value] = "Unknown"

            # De-dupe individual values.
            for field in FIELDS_TO_DEDUPE:

                values = record.get(field.value)
                if values:

                    values = de_dupe_list(values=values)
                    record[field.value] = values

            # Overwrite the access-rights.
            record[RhizomeField.ACCESS_RIGHTS.value] = "Image is displayed for education and personal research only. For individual rights information about an item, please check the “Description” field, or follow the link to the digital object on the content provider’s website for more information. Reuse of copyright-protected images requires signed permission from the copyright holder. If you are the copyright holder of this item and its use online constitutes an infringement of your copyright, please contact us by email at rhizomes@umn.edu to discuss its removal from the portal."

        # Populate our Searchable Date.
        for record in data:

            if self.date_parsers:

                searchable_date = get_searchable_date(record=record, date_parsers=self.date_parsers)

            else:

                searchable_date = record.get(RhizomeField.DATE.value)

            record[RhizomeField.SEARCHABLE_DATE.value] = searchable_date


    def load(self, data):
        "Load the data (into csv, json, database, etc.)"

        writer = MetadataWriter(format=self.format)
        writer.start_collection()

        for record in data:

            if record.get("ignore", False):

                continue

            writer.start_record()

            for name in OUTPUT_COLS:

                name = name.value

                value = record.get(name)
                if value and name:

                    writer.add_value(name=name, value=value)

            writer.end_record()

        writer.end_collection()
