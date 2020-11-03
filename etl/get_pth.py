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


def print_record(data):

    for name, description in field_map.items():

        if not description:

            continue

        value = data.get(name)
        if value:

            print(f"{description}: {get_value(value=value)}")


if __name__ == "__main__":

    for collection in collections:

        response = requests.get(f"{get_records_url}{collection}")

        # out = open("data.xml", "w+")
        # out.write(response.text)


        data = BeautifulSoup(markup=response.content, features="lxml-xml", from_encoding="utf-8")
        records = data.findAll("record")

        for record in records:

            print("")
            record_data = {}

            for value in record.header:

                add_value(record_data, value)

            for value in record.metadata:

                if value.name == 'dc':

                    for child in value.children:

                        add_value(record_data, child)

            print_record(data=record_data)
