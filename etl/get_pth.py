#!/usr/bin/env python


import requests
import csv
import sys

from bs4 import BeautifulSoup


protocol = "https://"
domain = "texashistory.unt.edu"

list_sets_path = "/oai/?verb=ListSets"
list_sets_url = protocol + domain + list_sets_path


collections = [ "partner:MAMU" ]
get_records_path = "/oai/?verb=ListRecords&metadataPrefix=oai_dc&set="
get_records_url = protocol + domain + get_records_path


def add_value(record_data, value):

	if not value.name:

		return False

	text = value.get_text().strip()
	if not text:

		return False

	full_value = record_data.get(value.name, '')
	if full_value:
		full_value += ', '

	full_value += text
	record_data[value.name] = full_value

	return True


if __name__ == "__main__":

	for collection in collections:

		response = requests.get(f"{get_records_url}{collection}")

		# out = open("data.xml", "w+")
		# out.write(response.text)


		fieldnames = [ 'identifier', 'datestamp', 'setspec', 'dc:title', 'dc:creator', 'dc:subject', 'dc:description', 'dc:contributor', 'dc:publisher', 'dc:date', 'dc:type', 'dc:format', 'dc:identifier', 'dc:source', 'dc:language', 'dc:coverage' ]

		data = BeautifulSoup(response.text, "lxml")
		records = data.findAll("record")

		writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, delimiter='|')
		writer.writeheader()

		for record in records:

			record_data = {}

			for value in record.header:

				add_value(record_data, value)

			for value in record.metadata:

				if value.name == 'oai_dc:dc':

					for child in value.children:

						add_value(record_data, child)

			writer.writerow(record_data)
