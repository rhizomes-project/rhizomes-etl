#!/usr/bin/env python

import csv
from enum import Enum
import json
import sys


class RhizomeField(Enum):

    # Note: see 'schema mapping', see: https://docs.google.com/spreadsheets/d/1qaJGEewrOdaDwPKRoliZVWBWo9k-OgbyZFHmqU7SLzY

    ID                              = "Resource Identifier"
    TITLE                           = "Title"
    ALTERNATE_TITLES                = "Alternate Titles"
    AUTHOR_ARTIST                   = "Author/Artist"
    DESCRIPTION                     = "Description"
    DATE                            = "Date"
    SEARCHABLE_DATE                 = "Searchable Date"
    RESOURCE_TYPE                   = "ResourceType"
    DIGITAL_FORMAT                  = "Digital Format"
    DIMENSIONS                      = "Dimensions"
    URL                             = "URL"
    SOURCE                          = "Source"
    LANGUAGE                        = "Language"
    SUBJECTS_HISTORICAL_ERA         = "Subjects (Historic Era)"
    SUBJECTS_TOPIC_KEYWORDS         = "Subjects (Topic/Keywords)"
    SUBJECTS_GEOGRAPHIC             = "Subjects (Geographic)"
    NOTES                           = "Notes"
    COPYRIGHT_STATUS                = "Copyright Status"
    COLLECTION_INFORMATION          = "Collection Information"
    COLLECTION_NAME                 = "Collection Name"
    CREDIT_LINE                     = "Credit Line"
    IMAGES                          = "Images"

    @staticmethod
    def values():

        return [ value.value for value in RhizomeField.__members__.values() ]


FIELDS_TO_DEDUPE = [
    RhizomeField.AUTHOR_ARTIST,
    RhizomeField.TITLE,
    RhizomeField.ALTERNATE_TITLES,
    RhizomeField.SUBJECTS_HISTORICAL_ERA,
    RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    RhizomeField.SUBJECTS_GEOGRAPHIC,
]


def add_oaipmh_value(data, value):

    if not value.name:

        return False

    text = value.get_text().strip()
    if not text:

        return False

    full_value = data.get(value.name, [])
    full_value.append(text)
    data[value.name] = full_value

    return True


def get_oaipmh_record(record):

    record_data = {}

    # If the record header is not available, ignore the record
    # (These records appear to be in some format other than OIAPMH - we would need
    # documentation to parse these, since all or most of the metadata appears to be codes.)
    if not record.header:

        return {}

    for value in record.header:

        add_oaipmh_value(data=record_data, value=value)

    metadata = record.metadata or record.dc

    if not metadata:

        return {}

    for value in metadata:

        if value.name:

            for child in value.children:

                add_oaipmh_value(data=record_data, value=child)

    return record_data


def pretty_print(name, value):

    pass


def get_value(value, format="json"):

    if type(value) is list:

        if len(value) == 1:

            return value[0]

        else:

            if format == 'json':

                return value

            buf = ""
            for idx, tmp in enumerate(value):

                if format == "csv":

                    buf += tmp
                    if idx + 1 < len(value):

                        buf += " | "

                else:

                    buf += "\n\t" + tmp

            if format == "csv":

                buf = buf.strip()

            return buf

    elif type(value) is dict:

        raise Exception("values of type dict are not supported")

    else:

        return value


class MetadataWriter():

    def __init__(self, format):

        self.format = format
        if self.format == "json":

            self.output = []

        elif self.format == "csv":

            self.output = csv.DictWriter(sys.stdout, fieldnames=RhizomeField.values(), dialect=csv.QUOTE_ALL)
            self.row_buf = {}

        else:

            self.output = ""

    def start_collection(self):

        if self.format == "csv":

            self.output.writeheader()

        else:

            pass

    def start_record(self):

        if self.format == "json":

            self.output.append({})

        elif self.format == "csv":

            self.row_buf = {}

        else:

            self.output += "\n"

    def add_value(self, name, value):

        value = get_value(value=value, format=self.format)
        if self.format == "json":

            pos = len(self.output) - 1
            self.output[pos][name] = value

        elif self.format == "csv":

            self.row_buf[name] = value

        else:

            self.output += f"{name}: {value}\n"

    def end_record(self):

        if self.format == "csv":

            self.output.writerow(self.row_buf)

        else:

            pass

    def end_collection(self):

        # REVIEW: just return the data everywhere here?

        if self.format == "json":

            buf = json.dumps(self.output)

        elif self.format == "csv":

            buf = ""

        else:

            buf = self.output

        print(buf)
