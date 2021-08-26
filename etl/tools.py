#!/usr/bin/env python

import csv
from enum import Enum
import json
import requests
import sys

from bs4 import BeautifulSoup


class RhizomeField(Enum):

    # Note: see 'schema mapping', see: https://docs.google.com/spreadsheets/d/1qaJGEewrOdaDwPKRoliZVWBWo9k-OgbyZFHmqU7SLzY

    ID                              = "Resource Identifier"
    TITLE                           = "Title"
    ALTERNATE_TITLES                = "Alternative Title"
    AUTHOR_ARTIST                   = "Creator"
    DESCRIPTION                     = "Description"
    DATE                            = "Display Date"
    SEARCHABLE_DATE                 = "Date"
    RESOURCE_TYPE                   = "Type"
    DIGITAL_FORMAT                  = "Format"
    DIMENSIONS                      = "Dimensions"
    URL                             = "Weblog"
    SOURCE                          = "Source"
    LANGUAGE                        = "Language"
    SUBJECTS_HISTORICAL_ERA         = "Subjects (Historic Era)"
    SUBJECTS_TOPIC_KEYWORDS         = "Subject"
    SUBJECTS_GEOGRAPHIC             = "Subjects (Geographic)"
    NOTES                           = "Notes"
    COPYRIGHT_STATUS                = "Copyright Status"
    COLLECTION_INFORMATION          = "Collection Information"
    COLLECTION_NAME                 = "Contributor"
    CREDIT_LINE                     = "Credit Line"
    IMAGES                          = "Image"
    ANNOTATES                       = "Annotates"
    ACCESS_RIGHTS                   = "Access Rights"

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

OUTPUT_COLS = [
    RhizomeField.TITLE,
    RhizomeField.ALTERNATE_TITLES,
    RhizomeField.AUTHOR_ARTIST,
    RhizomeField.IMAGES,
    RhizomeField.URL,
    RhizomeField.DESCRIPTION,
    RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    RhizomeField.SEARCHABLE_DATE,
    RhizomeField.RESOURCE_TYPE,
    RhizomeField.DIGITAL_FORMAT,
    RhizomeField.SOURCE,
    RhizomeField.LANGUAGE,
    RhizomeField.COLLECTION_NAME,
    RhizomeField.ANNOTATES,
    RhizomeField.ACCESS_RIGHTS,

    # REVIEW: add in "Access Rights" and "Annotates" columns once we know proper Omeka column names.
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


HTML_TAGS = {
    '<br>':      "\n",
    '<br/>':     "\n",
    '<p>':       "\n",
    '</p>':      "\n",
}

def remove_html_tags(values):

    if type(values) is not list:

        values = [ values ]

    for idx, value in enumerate(values):

        for tag, replacement in HTML_TAGS.items():

            value = value.replace(tag, replacement)

            # Now use BeautifulSoup to replace any remaining tags and convert html character entities into unicode.
            soup = BeautifulSoup(value, 'html.parser')
            values[idx] = soup.text

    return values


def remove_author_job_desc(values):

    DESC_LIST = [
        ", Artist",
        ", Author",
        ", Collaborator",
        ", Compiler",
        ", Contributor",
        ", Creator",
        ", Editor",
        ", Interviewer",
        ", Occupant ",
        ", Organizer",
        ", Photographer",
        ", Speaker",
    ]

    if type(values) is not list:

        values = [ values ]

    for idx, value in enumerate(values):

        for desc in DESC_LIST:

            if value.endswith(desc):

                value = value[ : len(desc) * -1 ]
                values[idx] = value
                break

    for idx, value in enumerate(values):

        if value.endswith(")"):

            pos = value.find("(")
            if pos > 8:

                values[idx] = value[ : pos]

    return values


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


def get_previous_item_ids():
    """
    Returns a list of IDs (actually urls) of all currently-loaded
    records in the Rhizomes website.
    """

    num_per_page = 250
    curr_page = 1

    base_url = "https://romogis.frankromo.com/rhizomes-dev/api/items"

    # REVIEW: Once data is loaded here, switch to this as base url.
    # base_url = "https://cla-rhizomes-prd.oit.umn.edu/api/items"

    item_ids = []

    # Do a loop that cannot go forever.
    while curr_page < 1000:

        response = requests.get(f"https://romogis.frankromo.com/rhizomes-dev/api/items?per_page={num_per_page}&page={curr_page}", timeout=60)
        if not response.ok:

            raise Exception(f"Omeka API returned error {response.status_code}, reason: '{response.reason}'")

        curr_items = response.json()
        if not curr_items:

            return item_ids

        ids = [ item["foaf:weblog"][0]["@id"] for item in curr_items ]
        item_ids += ids

        curr_page += 1

    return item_ids


class MetadataWriter():

    def __init__(self, format):

        self.format = format
        if self.format == "json":

            self.output = []

        elif self.format == "csv":

            fieldnames = [ col.value for col in OUTPUT_COLS ]

            self.output = csv.DictWriter(sys.stdout, fieldnames=fieldnames, dialect=csv.QUOTE_ALL)
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
