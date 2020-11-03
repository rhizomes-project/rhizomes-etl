#!/usr/bin/env python


import requests
import sys

from bs4 import BeautifulSoup


protocol = "https://"
domain = "texashistory.unt.edu"

list_sets_path = "/oai/?verb=ListSets"
list_sets_url = protocol + domain + list_sets_path


collections = [ "partner:MAMU" ]
get_records_path = "/oai/?verb=ListRecords&metadataPrefix=oai_dc&set="
get_records_url = protocol + domain + get_records_path


field_map = {
    "title":            "Title",
    "identifier":       "Resource Identifier",
    # "datestamp":        "",
    "setspec":          "",
    "creator":          "Author/Artist",
    "subject":          "Subjects (Topic/Keywords)",
    "description":      "Description",
    "contributor":      "Author/Artist",
    "publisher":        "",
    "date":             "Date",
    "type":             "Resource Type",
    "format":           "Digital Format",
    "source":           "Source",
    "language":         "Language",
    "coverage":         "Subjects (geographic)",
}


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


def clean_value(value):

    if value.startswith("["):
        value = value[ 1 : ]

    if value.endswith("]"):
        value = value[ : -1 ]

    return value

def get_value(value):

    if len(value) == 1:

        return clean_value(value=value[0])

    result = []
    for tmp in value:

        result.append(clean_value(value=tmp))

    return result

def extract():

    data = []

    for collection in collections:

        response = requests.get(f"{get_records_url}{collection}")

        xml_data = BeautifulSoup(markup=response.content, features="lxml-xml", from_encoding="utf-8")
        records = xml_data.findAll("record")

        for record in records:

            record_data = {}

            for value in record.header:

                add_value(record_data, value)

            for value in record.metadata:

                if value.name == 'dc':

                    for child in value.children:

                        add_value(record_data, child)

            data.append(record_data)

    return data

def transform(data):

    for record in data:

        for name, description in field_map.items():

            if not description:

                continue

            value = record.get(name)
            if value:

                if record.get(description):

                    record[description] += value

                else:

                    record[description] = value

                del record[name]

def load(data):

    for record in data:

        print("")

        prev_values = set()

        for name in field_map.values():

            value = record.get(name)
            if value and name not in prev_values:

                print(f"{name}: {get_value(value=value)}")

                prev_values.add(name)


if __name__ == "__main__":

    data = extract()
    transform(data=data)
    load(data=data)
