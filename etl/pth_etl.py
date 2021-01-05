#!/usr/bin/env python

import os
import re
import requests
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField

from bs4 import BeautifulSoup


running_tests = os.environ.get("RUNNING_UNITTESTS")


protocol = "https://"
domain = "texashistory.unt.edu"

list_sets_path = "/oai/?verb=ListSets"
list_sets_url = protocol + domain + list_sets_path

RECORD_LIMIT = None

partners = [ "partner:MAMU", "partner:UNT", "partner:UNTA", "partner:UNTGD", ]

if running_tests:

    partners = [ "partner:MAMU" ]
    RECORD_LIMIT = 1

# REVIEW: TODO Pull in all desired PTH partners
# REVIEW: TODO revisit ways of further filtering PTH metadata

records_path =       "/oai/?verb=ListRecords"
start_records_path = records_path + "&metadataPrefix=oai_dc&set="
start_records_url = protocol + domain + start_records_path
resume_records_url = protocol + domain + records_path


field_map = {
    "identifier":                             RhizomeField.ID,
    "title":                                  RhizomeField.TITLE,
    "creator":                                RhizomeField.AUTHOR_ARTIST,
    "contributor":                            RhizomeField.AUTHOR_ARTIST,
    "description":                            RhizomeField.DESCRIPTION,
    "date":                                   RhizomeField.DATE,
    "type":                                   RhizomeField.RESOURCE_TYPE,
    "format":                                 RhizomeField.DIGITAL_FORMAT,
    "dimensions":                             RhizomeField.DIMENSIONS,
    "url":                                    RhizomeField.URL,
    "source":                                 RhizomeField.SOURCE,
    "language":                               RhizomeField.LANGUAGE,
    "subjects_hist":                          RhizomeField.SUBJECTS_HISTORICAL_ERA,
    "subject":                                RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "subjects_geo":                           RhizomeField.SUBJECTS_GEOGRAPHIC,
}

KEYWORDS = [
    "chicano", "chicana",
    "mexican-american", "mexican american",
]

# REVIEW TODO Get canonical list of PTH formats.
KNOWN_FORMATS = ('image', 'text')


def has_number(value):

    return re.search(r'\d', value)

def add_value(data, value):

    if not value.name:

        return False

    text = value.get_text().strip()
    if not text:

        return False

    full_value = data.get(value.name, [])
    full_value.append(text)
    data[value.name] = full_value

    return True

def do_keep_record(record):
    "Returns True if the record should be retained"

    title = ''.join(record.get('title', [])).lower()
    description = ''.join(record.get('description', [])).lower()

    for keyword in KEYWORDS:

        if keyword in title or keyword in description:

            return True

    return False

def extract_records(records):

    data = []
    for record in records:

        record_data = {}

        for value in record.header:

            add_value(record_data, value)

        for value in record.metadata:

            if value.name == 'dc':

                for child in value.children:

                    add_value(record_data, child)

        if do_keep_record(record=record_data):

            data.append(record_data)

    return data

record_count = 0

def extract_partner(partner, resumption_token=None):

    if not resumption_token:

        url = f"{start_records_url}{partner}"

        global record_count
        record_count = 0

    else:

        url = f"{resume_records_url}&resumptionToken={resumption_token}"

    response = requests.get(url)
    xml_data = BeautifulSoup(markup=response.content, features="lxml-xml", from_encoding="utf-8")

    # Extract all records from this result set.
    records = extract_records(records=xml_data.find_all("record"))

    resumption_tokens = xml_data.find_all("resumptionToken")
    if resumption_tokens:

        if RECORD_LIMIT and record_count >= RECORD_LIMIT:

            return records


        record_count += len(records)
        print(f"{record_count} records ...", file=sys.stderr)

        # Make recursive call to extract all records.
        next_records = extract_partner(partner=partner, resumption_token=resumption_tokens[0].text)
        records += next_records

    return records


class PTHETLProcess(BaseETLProcess):

    def get_field_map(self):

        return field_map

    def extract(self):

        data = []

        for partner in partners:

            print(f"\nExtracting PTH partner {partner}:", file=sys.stderr)

            partner_data = extract_partner(partner=partner)
            data += partner_data

            print(f"\n... extracted {len(partner_data)} records for partner {partner}", file=sys.stderr)

        return data

    def transform(self, data):

        for record in data:

            # Split 'format' into digital format and dimensions.
            formats = record.get("format", [])
            if formats:

                new_formats = []
                new_dimensions = []

                for format in formats:

                    if format.lower() in KNOWN_FORMATS:

                        new_formats.append(format)

                    else:

                        new_dimensions.append(format)

                record["format"] = new_formats
                record["dimensions"] = new_dimensions

            # Add in a URL value.
            identifiers = record["identifier"]
            new_ids = []
            new_urls = []

            for identifier in identifiers:

                if identifier.startswith('http'):

                    new_urls.append(identifier)

                else:

                    new_ids.append(identifier)

            record["identifier"] = new_ids
            record["url"] = new_urls

            # Split 'coverage' into values dealing with geography and values dealing with history (dates).
            coverage_values = record.get("coverage", [])
            if coverage_values:

                hist_vals = []
                geo_vals = []

                for value in coverage_values:

                    if has_number(value=value):

                        hist_vals.append(value)

                    else:

                        geo_vals.append(value)

                record["subjects_hist"] = hist_vals
                record["subjects_geo"] = geo_vals

        # Let base class do rest of transform.
        super().transform(data=data)


if __name__ == "__main__":    # pragma: no cover

    etl_process = PTHETLProcess(format="csv")

    data = etl_process.extract()
    etl_process.transform(data=data)
    etl_process.load(data=data)
